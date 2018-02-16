module Bosh::AwsCloud
  module Spotinst

    class Provider
      include Helpers

      attr_reader :client

      def initialize(config, logger)
        @config = config
        @logger = logger
        @client = Spotinst::Client.new(@config.credentials, @logger)
      end
    end

    class Manager
      include Helpers

      TOTAL_WAIT_TIME_IN_SECONDS = 300
      MAX_RETRY_COUNT = 30

      def initialize(client, ec2, registry, logger)
        @client = client
        @ec2 = ec2
        @registry = registry
        @logger = logger
      end

      def create_instance(instance_params, vm_cloud_props)
        begin
          @logger.info('Launching instance through Spotinst...')

          builder = Spotinst::Builder.new(instance_params, vm_cloud_props, @logger)
          elastigroup = builder.build
          elastigroup_id = @client.create(elastigroup)
          wait_for_instance(elastigroup_id)

        rescue HTTPClient::TimeoutError
          cloud_error('Timed out creating a new elastigroup')

        rescue Exception => exception
          cloud_error("Unexpected error while creating elastigroup: #{exception.inspect}")

        end
      end

      def delete_instance(instance_id)
        begin
          @logger.info("Terminating instance #{instance_id} through Spotinst...")
          terminated = false

          instance = find_instance(instance_id)
          if instance.nil? || instance.empty?
            @logger.info("Unknown instance #{instance_id}, ignoring termination request")
            return terminated
          end

          elastigroup_id = instance['groupId']
          instance_state = instance['lifeCycleState']

          @logger.info("Instance #{instance_id} (#{elastigroup_id}) found")
          terminated = true

          if instance_state != 'ACTIVE'
            @logger.info("Instance #{instance_id} (#{elastigroup_id}) terminated")
            return terminated
          end

          @logger.info("Terminating instance #{instance_id} (#{elastigroup_id})")
          @client.delete(elastigroup_id)

          terminated

        rescue HTTPClient::TimeoutError
          cloud_error("Timed out deleting instance #{instance_id}")

        rescue Exception => exception
          cloud_error("Unexpected error while deleting instance #{instance_id}: #{exception.inspect}")

        ensure
          @logger.info("Deleting instance settings for #{instance_id}")
          @registry.delete_settings(instance_id) unless @registry.nil?

        end
      end

      def tag_instance(instance_id, metadata)
        begin
          @logger.info("Tagging instance #{instance_id}")
          name = metadata['Name'].to_s

          instance = find_instance(instance_id)
          if instance.nil? || instance.empty?
            @logger.info("Unknown instance #{instance_id}, ignoring tag request")
            return
          end

          elastigroup_id = instance['groupId']
          instance_state = instance['lifeCycleState']

          @logger.info("Instance #{instance_id} (#{elastigroup_id}) found")

          if instance_state != 'ACTIVE'
            @logger.info("Instance #{instance_id} (#{elastigroup_id}) terminated")
            return
          end

          elastigroup_launch_spec = {tags: [{tagKey: 'Name', tagValue: name}]}
          elastigroup_compute = {launchSpecification: elastigroup_launch_spec}
          elastigroup = {group: {name: name, description: name, compute: elastigroup_compute}}
          @client.update(elastigroup_id, elastigroup)

        rescue HTTPClient::TimeoutError
          cloud_error('Timed out creating a new elastigroup')

        rescue Exception => exception
          cloud_error("Unexpected error while tagging instance #{instance_id}: #{exception.inspect}")

        end
      end

      private

      def wait_for_instance(elastigroup_id)
        @logger.info("Waiting for elastigroup #{elastigroup_id} to be ready")
        instance = nil

        errors = [Spotinst::NotReadyError]
        Bosh::Common.retryable(sleep: TOTAL_WAIT_TIME_IN_SECONDS / MAX_RETRY_COUNT, tries: MAX_RETRY_COUNT, on: errors) do |_, error|
          @logger.info("Retrying after expected error: #{error}") if error

          instance_details = get_instance(elastigroup_id)
          unless instance_details.nil?
            instance_id = instance_details['instanceId']

            unless instance_id.nil?
              @logger.info("Elastigroup #{elastigroup_id} is ready")
              instance = @ec2.instance(instance_id)
              return instance # stop execution
            end
          end

          @logger.info("Elastigroup #{elastigroup_id} is not ready yet")
          raise Spotinst::NotReadyError.new # retry execution
        end

        instance
      rescue Bosh::Common::RetryCountExceeded
        @logger.error("Timed out waiting for elastigroup #{elastigroup_id} to be ready")

      end

      def get_instance(elastigroup_id)
        @logger.info("Checking state of elastigroup #{elastigroup_id}")

        instances = @client.status(elastigroup_id)
        @logger.info("Instances of elastigroup #{elastigroup_id}: #{instances.inspect}")

        instances.first unless instances.empty?
      end

      def find_instance(instance_id)
        begin
          @logger.info("Attempting to find instance #{instance_id}")

          instance = @client.instance(instance_id)
          @logger.info("Instance: #{instance.inspect}")

          instance

        rescue HTTPClient::TimeoutError
          cloud_error("Timed out getting instance #{instance_id}")

        rescue Exception => exception
          cloud_error("Unexpected error while getting instance #{instance_id}: #{exception.inspect}")

        end
      end
    end

    class Builder
      include Helpers

      def initialize(instance_params, vm_cloud_props, logger)
        @instance_params = instance_params
        @vm_cloud_props = vm_cloud_props
        @logger = logger

        @logger.debug("Builder initialized with params: #{@instance_params.inspect}")
        @logger.debug("Builder initialized with cloud properties: #{@vm_cloud_props.inspect}")
      end

      def build
        name = 'bosh/unknown/' + SecureRandom.uuid.split('-')[4]
        {
          group: {
            name: name,
            description: name,
            capacity: capacity,
            strategy: strategy,
            compute: compute,
          },
        }
      end

      private

      def capacity
        {
          target: @instance_params[:min_count],
          minimum: @instance_params[:min_count],
          maximum: @instance_params[:max_count],
          unit: 'instance',
        }
      end

      def strategy
        {
          risk: @vm_cloud_props.spotinst_risk,
          fallbackToOd: @vm_cloud_props.spot_ondemand_fallback,
          utilizeReservedInstances: true,
          availabilityVsCost: 'balanced',
        }
      end

      def compute
        {
          product: @vm_cloud_props.spotinst_product,
          instanceTypes: compute_instance_types,
          availabilityZones: compute_zones,
          launchSpecification: compute_spec,
        }
      end

      def compute_instance_types
        {
          ondemand: @instance_params[:instance_type],
          spot: [
            @instance_params[:instance_type],
          ],
        }
      end

      def compute_zones
        zone = {
          name: @instance_params[:placement][:availability_zone]
        }

        ifaces = @instance_params[:network_interfaces]
        zone[:subnetId] = ifaces[0][:subnet_id] if ifaces[0][:subnet_id]

        [zone]
      end

      def compute_spec
        {
          monitoring: false,
          ebsOptimized: false,
          imageId: @instance_params[:image_id],
          keyPair: @instance_params[:key_name],
          userData: @instance_params[:user_data].delete!("\n"),
          tenancy: compute_tenancy,
          blockDeviceMappings: compute_spec_block_devices,
          networkInterfaces: compute_spec_network_ifaces,
          securityGroupIds: compute_spec_security_groups,
        }
      end

      def compute_tenancy
        @vm_cloud_props.tenancy.dedicated? ? 'dedicated' : 'default'
      end

      def compute_spec_block_devices
        devices = @instance_params[:block_device_mappings]
        devices.map {|device|
          dev = {}
          dev[:deviceName] = device[:device_name]
          unless device[:ebs].nil?
            dev[:ebs] = {}
            dev[:ebs][:volumeSize] = device[:ebs][:volume_size] if device[:ebs][:volume_size]
            dev[:ebs][:volumeType] = device[:ebs][:volume_type] if device[:ebs][:volume_type]
            dev[:ebs][:deleteOnTermination] = device[:ebs][:delete_on_termination] if device[:ebs][:delete_on_termination]
          end
          dev
        } unless devices.nil?
      end

      def compute_spec_network_ifaces
        ifaces = @instance_params[:network_interfaces]
        return [] if ifaces.nil?
        [
          {
            deviceIndex: ifaces[0][:device_index],
            privateIpAddress: ifaces[0][:private_ip_address],
            associatePublicIpAddress: @vm_cloud_props.auto_assign_public_ip,
          }
        ]
      end

      def compute_spec_security_groups
        ifaces = @instance_params[:network_interfaces]
        return [] if ifaces.nil?
        ifaces[0][:groups] unless ifaces[0][:groups].nil?
      end
    end

    class Credentials
      include Helpers

      attr_reader :token
      attr_reader :account

      # @param [String] token
      # @param [String] account
      def initialize(token, account = nil)
        @token = token
        @account = account
      end

      # @return [Credentials]
      def credentials
        self
      end

      # @return [Boolean] Returns `true` if the token is set.
      def set?
        !@token.nil?
      end
    end

    class Client
      include Helpers

      BASE_URL = 'https://api.spotinst.io/aws/ec2'
      TIMEOUT_IN_SECONDS = 60

      def initialize(credentials, logger)
        @credentials = credentials
        @logger = logger

        @http_client = HTTPClient.new
        @http_client.receive_timeout = TIMEOUT_IN_SECONDS
        @http_client.debug_dev = @logger
      end

      def create(elastigroup)
        request_body = elastigroup
        request_args = http_request_args(body: request_body)
        request_uri = BASE_URL + '/group'

        response = @http_client.post(request_uri, request_args)
        unless response.status == HTTP::Status::OK
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end

        http_response_items(response.body).first['id']
      end

      def read(elastigroup_id)
        request_args = http_request_args
        request_uri = BASE_URL + '/group/' + elastigroup_id.to_s

        response = @http_client.get(request_uri, request_args)
        unless response.status == HTTP::Status::OK
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end

        http_response_items(response.body).first
      end

      def update(elastigroup_id, elastigroup)
        request_body = elastigroup
        request_args = http_request_args(body: request_body)
        request_uri = BASE_URL + '/group/' + elastigroup_id.to_s

        response = @http_client.put(request_uri, request_args)
        unless response.status == HTTP::Status::OK
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end

        http_response_items(response.body).first
      end

      def delete(elastigroup_id)
        request_args = http_request_args
        request_uri = BASE_URL + '/group/' + elastigroup_id.to_s

        response = @http_client.delete(request_uri, request_args)
        unless response.status == HTTP::Status::OK || (response.status == HTTP::Status::BAD_REQUEST && http_response_error_code(response.body, 'GROUP_DOESNT_EXIST'))
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end
      end

      def status(elastigroup_id)
        request_args = http_request_args
        request_uri = BASE_URL + '/group/' + elastigroup_id.to_s + '/status'

        response = @http_client.get(request_uri, request_args)
        unless response.status == HTTP::Status::OK
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end

        http_response_items(response.body)
      end

      def instance(instance_id)
        request_args = http_request_args
        request_uri = BASE_URL + '/instance/' + instance_id.to_s

        response = @http_client.get(request_uri, request_args)
        unless response.status == HTTP::Status::OK || (response.status == HTTP::Status::BAD_REQUEST && http_response_error_code(response.body, 'INSTANCE_DOESNT_EXIST'))
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end

        http_response_items(response.body).first
      end

      private

      def http_request_args(body: nil)
        request_args = {
          header: {
            'Content-Type' => 'application/json',
            'User-Agent' => 'bosh-aws-cpi/0.1',
            Authorization: 'Bearer ' + @credentials.token.to_s,
          }
        }
        request_args[:query] = {accountId: @credentials.account.to_s} if @credentials.account
        request_args[:body] = body.to_json unless body.nil?
        request_args
      end

      def http_response_error_code(body, code)
        errors = http_response_errors(body)
        errors.any? {|error| error[:code] == code} unless errors.empty?
      end

      def http_response_items(body)
        body.nil? ? [] : JSON.parse(body)['response']['items']
      end

      def http_response_errors(body)
        body.nil? ? [] : JSON.parse(body)['response']['errors']
      end

    end

    # Raised for indicating a not ready instance error.
    class NotReadyError < StandardError; end
  end
end
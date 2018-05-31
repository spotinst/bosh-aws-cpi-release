module Bosh::AwsCloud
  module Spotinst

    class Provider
      include Helpers

      HTTP_CLIENT_RECEIVE_TIMEOUT = 60 # in seconds

      attr_reader :client

      def initialize(config, logger)
        @config = config
        @logger = logger

        http_client = HTTPClient.new
        http_client.receive_timeout = HTTP_CLIENT_RECEIVE_TIMEOUT
        http_client.debug_dev = @logger

        @client = Spotinst::Client.new(http_client, @config.credentials, @logger)
      end
    end

    class Manager
      include Helpers

      TOTAL_WAIT_TIME = 300 # in seconds
      MAX_RETRY_COUNT = 30

      def initialize(client, ec2, registry, logger)
        @client = client
        @ec2 = ec2
        @registry = registry
        @logger = logger
      end

      def create_elastigroup(instance_params, vm_cloud_props)
        begin
          @logger.info('Creating a new elastigroup')

          builder = Spotinst::Builder.new(instance_params, vm_cloud_props, @logger)
          elastigroup = builder.build

          elastigroup_id = @client.create(elastigroup)
          @logger.info("Elastigroup '#{elastigroup_id}' is ready")

          instance = wait_for_instance(elastigroup_id)
          Spotinst::Instance.new(elastigroup_id, instance)

        rescue HTTPClient::TimeoutError
          cloud_error('Timed out creating a new elastigroup')

        rescue Exception => exception
          cloud_error("Unexpected error while creating elastigroup: #{exception.inspect}")

        end
      end

      def delete_elastigroup(elastigroup_id)
        begin
          @logger.info("Deleting elastigroup '#{elastigroup_id}'")

          opts = {
            statefulDeallocation: {
              shouldDeleteNetworkInterfaces: true,
              shouldDeleteSnapshots: true,
              shouldDeleteVolumes: true,
              shouldDeleteImages: true,
            },
          }

          instance = find_instance(elastigroup_id)
          @client.delete(elastigroup_id, opts)
          ResourceWait.for_instance(instance: instance, state: 'terminated') unless instance.nil?

          true # deleted

        rescue HTTPClient::TimeoutError
          cloud_error("Timed out deleting elastigroup '#{elastigroup_id}'")

        rescue Exception => exception
          cloud_error("Unexpected error while deleting elastigroup '#{elastigroup_id}': #{exception.inspect}")

        ensure
          @logger.info("Deleting elastigroup settings for '#{elastigroup_id}'")
          @registry.delete_settings(elastigroup_id) unless @registry.nil?

        end
      end

      def tag_elastigroup(elastigroup_id, metadata)
        begin
          @logger.info("Tagging elastigroup '#{elastigroup_id}'")
          tags = Builder.format_tags(metadata)
          name = metadata['Name'].to_s

          payload = {
            group: {
              compute: {
                launchSpecification: {
                  tags: tags,
                },
              },
            },
          }

          unless name.empty?
            payload[:group][:name] = name
            payload[:group][:description] = name
          end

          @client.update(elastigroup_id, payload)
          @logger.info("Elastigroup '#{elastigroup_id}' tagged with the following K/V pairs: #{tags.inspect}")

        rescue HTTPClient::TimeoutError
          cloud_error("Timed out tagging elastigroup '#{elastigroup_id}'")

        rescue Exception => exception
          cloud_error("Unexpected error while tagging elastigroup '#{elastigroup_id}': #{exception.inspect}")

        end
      end

      def associate_elastic_ip(elastigroup_id, network_cloud_props)
        begin
          vip_network = nil
          network_cloud_props.networks.each do |net|
            if net.instance_of?(Bosh::AwsCloud::NetworkCloudProps::PublicNetwork)
              cloud_error("More than one vip network for '#{net.name}'") if vip_network
              vip_network = net
            end
          end

          if vip_network.nil?
            @logger.info("No vip networks found, skipping")
            return
          end

          # AWS accounts that support both EC2-VPC and EC2-Classic platform access explicitly require allocation_id instead of public_ip
          addresses = @ec2.client.describe_addresses(
            public_ips: [vip_network.ip],
            filters: [
              name: 'domain',
              values: [
                'vpc'
              ]
            ]
          ).addresses
          found_address = addresses.first
          cloud_error("Elastic IP with VPC scope not found with address '#{vip_network.ip}'") if found_address.nil?

          allocation_id = found_address.allocation_id
          @logger.info("Associating elastigroup '#{elastigroup_id}' with elastic IP #{vip_network.ip} (allocation_id:#{allocation_id})")

          elastigroup_elastic_ips = [allocation_id]

          elastigroup_compute = {
            elasticIps: elastigroup_elastic_ips,
          }

          elastigroup = {
            group: {
              compute: elastigroup_compute,
            },
          }

          @client.update(elastigroup_id, elastigroup)
          @logger.info("Elastic IP #{vip_network.ip} (allocation_id:#{allocation_id}) associated to elastigroup '#{elastigroup_id}'")

        rescue HTTPClient::TimeoutError
          cloud_error("Timed out associating elastic IP to elastigroup '#{elastigroup_id}'")

        rescue Exception => exception
          cloud_error("Unexpected error while associating elastic IP to elastigroup '#{elastigroup_id}': #{exception.inspect}")

        end
      end

      def register_load_balancers(elastigroup_id, elbs_v1, elbs_v2)
        begin
          @logger.info("Registering load balancers to elastigroup '#{elastigroup_id}'")
          lbs = []

          elbs_v1.each do |elb_name|
            lbs.push({type: 'CLASSIC', name: elb_name})
          end

          elbs_v2.each do |target_group_name|
            # TODO(liran): ARN?
          end

          if lbs.empty?
            @logger.info("No load balancers found, skipping")
            return
          end

          elastigroup_lbs = {
            loadBalancersConfig: lbs,
          }

          elastigroup_launch_spec = {
            loadBlancers: elastigroup_lbs,
          }

          elastigroup_compute = {
            launchSpecification: elastigroup_launch_spec,
          }

          elastigroup = {
            group: {
              compute: elastigroup_compute,
            },
          }

          @client.update(elastigroup_id, elastigroup)
          @logger.info("Load balancers registered")

        rescue HTTPClient::TimeoutError
          cloud_error("Timed out registering load balancers to elastigroup '#{elastigroup_id}'")

        rescue Exception => exception
          cloud_error("Unexpected error while registering load balancers to elastigroup '#{elastigroup_id}': #{exception.inspect}")

        end
      end

      def resolve_elastigroup(elastigroup_id)
        begin
          @logger.info("Attempting to resolve elastigroup '#{elastigroup_id}' to instance")

          instance = find_instance(elastigroup_id)
          Spotinst::Instance.new(elastigroup_id, instance)

        rescue HTTPClient::TimeoutError
          cloud_error("Timed out resolving elastigroup '#{elastigroup_id}'")

        rescue Exception => exception
          cloud_error("Unexpected error while resolving elastigroup: #{exception.inspect}")

        end
      end

      private

      def wait_for_instance(elastigroup_id)
        begin
          @logger.info("Waiting for elastigroup '#{elastigroup_id}' to be ready")
          instance = nil

          errors = [Spotinst::NotReadyError]
          Bosh::Common.retryable(sleep: TOTAL_WAIT_TIME / MAX_RETRY_COUNT, tries: MAX_RETRY_COUNT, on: errors) do |_, error|
            @logger.info("Retrying after expected error: #{error}") if error

            instance = find_instance(elastigroup_id)
            if instance.nil? # retry execution
              @logger.info("Elastigroup '#{elastigroup_id}' is not ready yet")
              raise Spotinst::NotReadyError.new
            end

            @logger.info("Instance #{instance.id} is ready")
          end

          instance
        rescue Bosh::Common::RetryCountExceeded
          @logger.error("Timed out waiting for elastigroup '#{elastigroup_id}' to be ready")

        end
      end

      def find_instance(elastigroup_id)
        @logger.info("Attempting to find instance, elastigroup: '#{elastigroup_id}'")
        instance = nil

        instances = @client.instances(elastigroup_id)
        @logger.info("Instances of elastigroup '#{elastigroup_id}': #{instances.inspect}")
        instance_details = instances.first

        unless instance_details.nil?
          instance_id = instance_details['instanceId']
          @logger.debug("Instance ID: #{instance_id}")
          instance = @ec2.instance(instance_id) unless instance_id.nil?
        end

        @logger.debug("Instance: #{instance.inspect}")
        instance
      end

    end

    class Builder
      include Helpers

      MAX_TAG_KEY_LENGTH = 127
      MAX_TAG_VALUE_LENGTH = 255

      def initialize(instance_params, vm_cloud_props, logger)
        @instance_params = instance_params
        @vm_cloud_props = vm_cloud_props
        @logger = logger

        @logger.debug("Builder initialized with params: #{@instance_params.inspect}")
        @logger.debug("Builder initialized with cloud properties: #{@vm_cloud_props.inspect}")
      end

      def build
        name = 'bosh/' + SecureRandom.uuid.split('-')[4]
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

      def self.format_tags(tags)
        tags["spotinst:cluster"] = "bosh"

        formatted_tags = tags.map do |k, v|
          if !k.nil? && !v.nil?
            trimmed_key = k.to_s.slice(0, MAX_TAG_KEY_LENGTH)
            trimmed_value = v.to_s.slice(0, MAX_TAG_VALUE_LENGTH)

            {
              tagKey: trimmed_key,
              tagValue: trimmed_value
            }
          end
        end

        formatted_tags.compact
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
          availabilityVsCost: strategy_orientation,
          persistence: (strategy_persistence if @vm_cloud_props.spotinst_risk > 0),
          revertToSpot: {performAt: 'always'},
        }
      end

      def strategy_orientation
        case @vm_cloud_props.spotinst_orientation
        when 'cost' then
          'costOriented'
        when 'availability' then
          'availabilityOriented'
        when 'equal-distribution' then
          'equalAzDistribution'
        else
          'balanced'
        end
      end

      def strategy_persistence
        {
          blockDevicesMode: 'reattach',
          shouldPersistRootDevice: true,
          shouldPersistBlockDevices: true,
          shouldPersistPrivateIp: true,
        }
      end

      def compute
        {
          product: @vm_cloud_props.spotinst_product,
          instanceTypes: compute_instance_types,
          availabilityZones: compute_zones,
          privateIps: compute_private_ips,
          launchSpecification: compute_spec,
        }
      end

      def compute_instance_types
        instance_types = @instance_params[:instance_type].split(',')

        {
          ondemand: instance_types[0],
          spot: instance_types,
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
          securityGroupIds: compute_spec_security_groups,
          networkInterfaces: compute_spec_network_ifaces,
          tags: Builder.format_tags(Hash.new),
        }
      end

      def compute_tenancy
        @vm_cloud_props.tenancy.dedicated? ? 'dedicated' : 'default'
      end

      def compute_private_ips
        ifaces = @instance_params[:network_interfaces]
        @vm_cloud_props.spotinst_risk > 0 && !ifaces.nil? ? [ifaces[0][:private_ip_address]] : nil
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

      def compute_spec_security_groups
        ifaces = @instance_params[:network_interfaces]
        return [] if ifaces.nil?
        ifaces[0][:groups] unless ifaces[0][:groups].nil?
      end

      def compute_spec_network_ifaces
        ifaces = @instance_params[:network_interfaces]
        @vm_cloud_props.spotinst_risk == 0 && !ifaces.nil? ? [
          {
            deviceIndex: ifaces[0][:device_index],
            privateIpAddress: ifaces[0][:private_ip_address],
            associatePublicIpAddress: @vm_cloud_props.auto_assign_public_ip,
          }
        ] : nil
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

      def initialize(http_client, credentials, logger)
        @http_client = http_client
        @credentials = credentials
        @logger = logger
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

      def delete(elastigroup_id, opts)
        request_body = opts
        request_args = http_request_args(body: request_body)
        request_uri = BASE_URL + '/group/' + elastigroup_id.to_s

        response = @http_client.delete(request_uri, request_args)
        unless response.status == HTTP::Status::OK || (response.status == HTTP::Status::BAD_REQUEST && http_response_error_code(response.body, 'GROUP_DOESNT_EXIST'))
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end
      end

      def instances(elastigroup_id)
        request_args = http_request_args
        request_uri = BASE_URL + '/group/' + elastigroup_id.to_s + '/status'

        response = @http_client.get(request_uri, request_args)
        unless response.status == HTTP::Status::OK
          cloud_error("Endpoint #{request_uri} returned HTTP #{response.status}")
        end

        http_response_items(response.body)
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

    class Instance
      include Helpers

      attr_reader :elastigroup_id
      attr_reader :ec2_instance

      def initialize(elastigroup_id, instance)
        @elastigroup_id = elastigroup_id
        @ec2_instance = instance
      end

      def to_s
        "elastigroup_id:#{@elastigroup_id}/instance_id:#{@ec2_instance.id}"
      end

    end

    # Raised for indicating a not ready instance error.
    class NotReadyError < StandardError; end
  end
end

require 'common/common'
require 'time'

module Bosh::AwsCloud
  class AbruptlyTerminated < Bosh::Clouds::CloudError; end
  class InstanceManager
    include Helpers

    def initialize(spotinst, ec2, registry, logger)
      @spotinst = spotinst
      @ec2 = ec2
      @registry = registry
      @logger = logger

      security_group_mapper = SecurityGroupMapper.new(@ec2)
      @param_mapper = InstanceParamMapper.new(security_group_mapper)
    end

    def create(stemcell_id, vm_cloud_props, networks_cloud_props, disk_locality, default_security_groups, block_device_mappings)
      abruptly_terminated_retries = 2
      begin
        instance_params = build_instance_params(
          stemcell_id,
          vm_cloud_props,
          networks_cloud_props,
          block_device_mappings,
          disk_locality,
          default_security_groups
        )

        redacted_instance_params = Bosh::Cpi::Redactor.clone_and_redact(
          instance_params,
          'user_data',
          'defaults.access_key_id',
          'defaults.secret_access_key'
        )
        @logger.info("Creating new instance with: #{redacted_instance_params.inspect}")

        instance = create_aws_instance(instance_params, vm_cloud_props)
        aws_instance = Bosh::AwsCloud::Instance.new(instance.ec2_instance, @registry, @logger)

        babysit_instance_creation(aws_instance, vm_cloud_props)
        instance
      rescue => e
        if e.is_a?(Bosh::AwsCloud::AbruptlyTerminated)
          @logger.warn("Failed to configure instance '#{instance.id}': #{e.inspect}")
          if (abruptly_terminated_retries -= 1) >= 0
            @logger.warn("Instance '#{instance.id}' was abruptly terminated, attempting to re-create': #{e.inspect}")
            retry
          end
        end
        raise
      end

      instance
    end

    # @param [String] instance_id EC2 instance id
    def find(instance_id)
      instance = @spotinst.resolve_elastigroup(instance_id)
      Instance.new(instance.ec2_instance, @registry, @logger)
    end

    def delete(instance_id, fast: false)
      deleted = @spotinst.delete_elastigroup(instance_id)
      find(instance_id).terminate(fast) unless deleted
    end

    private

    def babysit_instance_creation(instance, vm_cloud_props)
      begin
        # We need to wait here for the instance to be running, as if we are going to
        # attach to a load balancer, the instance must be running.
        instance.wait_for_running
        instance.update_routing_tables(vm_cloud_props.advertised_routes)
        instance.disable_dest_check unless vm_cloud_props.source_dest_check
      rescue => e
        if e.is_a?(Bosh::AwsCloud::AbruptlyTerminated)
          raise
        else
          @logger.warn("Failed to configure instance '#{instance.id}': #{e.inspect}")
          begin
            instance.terminate
          rescue => e
            @logger.error("Failed to terminate mis-configured instance '#{instance.id}': #{e.inspect}")
          end
          raise
        end
      end
    end

    def build_instance_params(stemcell_id, vm_cloud_props, networks_cloud_props, block_device_mappings, disk_locality = [], default_security_groups = [])
      volume_zones = (disk_locality || []).map { |volume_id| @ec2.volume(volume_id).availability_zone }

      @param_mapper.manifest_params = {
        stemcell_id: stemcell_id,
        vm_type: vm_cloud_props,
        registry_endpoint: @registry.endpoint,
        networks_spec: networks_cloud_props,
        default_security_groups: default_security_groups,
        volume_zones: volume_zones,
        subnet_az_mapping: subnet_az_mapping(networks_cloud_props),
        block_device_mappings: block_device_mappings
      }
      @param_mapper.validate
      @param_mapper.instance_params
    end

    def create_aws_instance(instance_params, vm_cloud_props)
      instance_params[:min_count] = 1
      instance_params[:max_count] = 1

      @spotinst.create_elastigroup(instance_params, vm_cloud_props)
    end

    def get_created_instance_id(resp)
      resp.instances.first.instance_id
    end

    def instance_create_wait_time
      30
    end

    def subnet_az_mapping(networks_cloud_props)
      subnet_ids = networks_cloud_props.filter('dynamic', 'manual').map do |net|
        net.subnet if net.cloud_properties?
      end
      filtered_subnets = @ec2.subnets(
        filters: [{
                    name: 'subnet-id',
                    values: subnet_ids
                  }]
      )

      filtered_subnets.inject({}) do |mapping, subnet|
        mapping[subnet.id] = subnet.availability_zone
        mapping
      end
    end
  end
end

module Bosh::CPI
  class CloudID < String; end
  class DiskCID < CloudID; end
  class VMCID < CloudID; end

  class VMs
    def self.new_vm_cid(cid)
      unless cid
        raise 'Internal incosistency: VM CID must not be empty'
      end

      VMCID.new(cid)
    end
  end

  class Disks
    def self.new_disk_cid(cid)
      unless cid
        raise 'Internal incosistency: Disk CID must not be empty'
      end

      DiskCID.new(cid)
    end

    def initialize(logger, volume_manager, aws_provider, az_selector, aws_config, registry)
      @logger = logger

      @volume_manager = volume_manager
      @aws_provider = aws_provider
      @az_selector = az_selector

      @ec2_client = @aws_provider.ec2_client
      @ec2_resource = @aws_provider.ec2_resource
      @aws_config = aws_config

      @registry = registry
    end

    # @param [Integer] size
    # @param [DiskCloudProps] disk_cloud_props
    # @param [VMCID] vm_cid
    def create_disk(size, disk_cloud_props, vm_cid)
      volume_properties = Bosh::AwsCloud::VolumeProperties.new(
        size: size,
        type: disk_cloud_props.type,
        iops: disk_cloud_props.iops,
        encrypted: disk_cloud_props.encrypted,
        kms_key_arn: disk_cloud_props.kms_key_arn,
        az: @az_selector.select_availability_zone(vm_cid)
      )

      volume_resp = @ec2_client.create_volume(volume_properties.persistent_disk_config)
      volume = Aws::EC2::Volume.new(
        id: volume_resp.volume_id,
        client: @ec2_client
      )

      @logger.info("Creating volume '#{volume.id}'")
      Bosh::AwsCloud::ResourceWait.for_volume(volume: volume, state: 'available')

      volume.id
    end

    # @param [DiskCID] disk_cid
    def delete_disk(disk_cid) # error
      volume = @ec2_resource.volume(disk_cid)

      @logger.info("Deleting volume `#{volume.id}'")

      # Retry 1, 6, 11, 15, 15, 15.. seconds. The total time is ~10 min.
      # VolumeInUse can be returned by AWS if disk was attached to VM
      # that was recently removed.
      tries = Bosh::AwsCloud::ResourceWait::DEFAULT_WAIT_ATTEMPTS
      sleep_cb = Bosh::AwsCloud::ResourceWait.sleep_callback(
        "Waiting for volume `#{volume.id}' to be deleted",
        {interval: 5, total: tries}
      )
      ensure_cb = Proc.new do |retries|
        cloud_error("Timed out waiting to delete volume `#{volume.id}'") if retries == tries
      end
      error = Aws::EC2::Errors::VolumeInUse

      Bosh::Common.retryable(tries: tries, sleep: sleep_cb, on: error, ensure: ensure_cb) do
        begin
          volume.delete
        rescue Aws::EC2::Errors::InvalidVolumeNotFound => e
          @logger.warn("Failed to delete disk '#{disk_cid}' because it was not found: #{e.inspect}")
          raise Bosh::Clouds::DiskNotFound.new(false), "Disk '#{disk_cid}' not found"
        end
        true # return true to only retry on Exceptions
      end

      if @aws_config.fast_path_delete?
        begin
          Bosh::AwsCloud::TagManager.tag(volume, 'Name', 'to be deleted')
          @logger.info("Volume `#{disk_cid}' has been marked for deletion")
        rescue Aws::EC2::Errors::InvalidVolumeNotFound
          # Once in a blue moon AWS if actually fast enough that the volume is already gone
          # when we get here, and if it is, our work here is done!
        end
        return
      end

      Bosh::AwsCloud::ResourceWait.for_volume(volume: volume, state: 'deleted')

      @logger.info("Volume `#{disk_cid}' has been deleted")
    end

    # @param [VMCID] vm_cid
    # @param [DiskCID] disk_cid
    def attach_disk(vm_cid, disk_cid) # error
      instance = @ec2_resource.instance(vm_cid)
      volume = @ec2_resource.volume(disk_cid)

      device_name = @volume_manager.attach_ebs_volume(instance, volume)

      update_agent_settings(instance) do |settings|
        settings['disks'] ||= {}
        settings['disks']['persistent'] ||= {}
        settings['disks']['persistent'][disk_cid] = device_name
      end
      @logger.info("Attached `#{disk_cid}' to `#{vm_cid}'")
    end

    # @param [VMCID] vm_cid
    # @param [DiskCID] disk_cid
    def detach_disk(vm_cid, disk_cid) # error
      instance = @ec2_resource.instance(vm_cid)
      volume = @ec2_resource.volume(disk_cid)

      if has_disk?(vm_cid)
        @volume_manager.detach_ebs_volume(instance, volume)
      else
        @logger.info("Disk `#{disk_cid}' not found while trying to detach it from vm `#{vm_cid}'...")
      end

      update_agent_settings(instance) do |settings|
        settings['disks'] ||= {}
        settings['disks']['persistent'] ||= {}
        settings['disks']['persistent'].delete(disk_cid)
      end

      @logger.info("Detached `#{disk_cid}' from `#{vm_cid}'")
    end

    # @param [DiskCID] disk_cid
    def has_disk?(disk_cid) # error
      @logger.info("Check the presence of disk with id `#{disk_cid}'...")
      volume = @ec2_resource.volume(disk_cid)
      begin
        volume.state
      rescue Aws::EC2::Errors::InvalidVolumeNotFound
        return false
      end
      true
    end

    def get_disks(vm_cid)
      disks = []
      @ec2_resource.instance(vm_cid).block_device_mappings.each do |block_device|
        if block_device.ebs
          disks << block_device.ebs.volume_id
        end
      end

      disks
    end

    # @param [DiskCID] disk_cid
    # @param [Hash] disk_metadata
    def set_disk_metadata(disk_cid, disk_metadata)
      begin
        volume = @ec2_resource.volume(disk_cid)
        Bosh::AwsCloud::TagManager.tags(volume, disk_metadata)
      rescue Aws::EC2::Errors::TagLimitExceeded => e
        @logger.error("could not tag #{volume.id}: #{e.message}")
      end
    end

    private

    def update_agent_settings(instance)
      unless block_given?
        raise ArgumentError, 'block is not provided'
      end

      settings = @registry.read_settings(instance.id)
      yield settings
      @registry.update_settings(instance.id, settings)
    end
  end
end
module Bosh::AwsCloud
  class AwsConfig
    attr_reader :max_retries, :credentials, :region, :ec2_endpoint, :elb_endpoint, :stemcell
    attr_reader :access_key_id, :secret_access_key, :default_key_name, :encrypted, :kms_key_arn
    attr_reader :default_iam_instance_profile, :default_security_groups

    CREDENTIALS_SOURCE_STATIC = 'static'.freeze
    CREDENTIALS_SOURCE_ENV_OR_PROFILE = 'env_or_profile'.freeze

    def initialize(aws_config_hash)
      @config = aws_config_hash

      @max_retries = @config['max_retries']

      @region = @config['region']
      @ec2_endpoint = @config['ec2_endpoint']
      @elb_endpoint = @config['elb_endpoint']

      @access_key_id = @config['access_key_id']
      @secret_access_key = @config['secret_access_key']
      @session_token = @config['session_token']
      @default_iam_instance_profile = @config['default_iam_instance_profile']
      @default_key_name = @config['default_key_name']
      @default_security_groups = @config['default_security_groups']

      @stemcell = @config['stemcell'] || {}
      @fast_path_delete = @config['fast_path_delete'] || false

      @encrypted = @config['encrypted']
      @kms_key_arn = @config['kms_key_arn']

      # credentials_source could be static (default) or env_or_profile
      # - if "static", credentials must be provided
      # - if "env_or_profile", credentials are read from instance metadata
      @credentials_source = @config['credentials_source'] || CREDENTIALS_SOURCE_STATIC
      @credentials =
        if @credentials_source == CREDENTIALS_SOURCE_STATIC
          Aws::Credentials.new(@access_key_id, @secret_access_key, @session_token)
        else
          Aws::InstanceProfileCredentials.new(retries: 10)
        end
    end

    def to_h
      @config
    end

    def fast_path_delete?
      @fast_path_delete
    end
  end

  class SpotinstConfig
    attr_reader :token, :account, :credentials

    CREDENTIALS_SOURCE_STATIC = 'static'.freeze

    def initialize(spotinst_config_hash)
      @config = spotinst_config_hash

      @token = @config['token']
      @account = @config['account']

      # credentials_source could be static (default) or env_or_profile
      # - if "static", credentials must be provided
      @credentials_source = @config['credentials_source'] || CREDENTIALS_SOURCE_STATIC
      @credentials =
        if @credentials_source == CREDENTIALS_SOURCE_STATIC
          Spotinst::Credentials.new(@token, @account)
        end
    end

    def to_h
      @config
    end
  end

  class RegistryConfig
    attr_reader :endpoint, :user, :password

    def initialize(registry_config_hash)
      @config = registry_config_hash

      @endpoint = @config['endpoint']
      @user =  @config['user']
      @password = @config['password']
    end
  end

  class AgentConfig
    def initialize(agent_config_hash)
      @config = agent_config_hash
    end

    def to_h
      @config
    end
  end

  class Config
    attr_reader :aws, :spotinst, :registry, :agent

    def self.build(config_hash)
      Config.validate(config_hash)
      new(config_hash)
    end

    def self.validate(config_hash)
      Config.validate_options(config_hash)
      Config.validate_credentials_source(config_hash)
    end

    private

    def initialize(config_hash)
      @config = config_hash
      @aws = AwsConfig.new(config_hash['aws'] || {})
      @spotinst = SpotinstConfig.new(config_hash['spotinst'] || {})
      @registry = RegistryConfig.new(config_hash['registry'] || {})
      @agent = AgentConfig.new(config_hash['agent'] || {})
    end

    ##
    # Checks if options passed to CPI are valid and can actually
    # be used to create all required data structures etc.
    #
    def self.validate_options(options)
      missing_keys = []

      REQUIRED_KEYS.each_pair do |key, values|
        values.each do |value|
          if (!options.has_key?(key) || !options[key].has_key?(value))
            missing_keys << "#{key}:#{value}"
          end
        end
      end

      raise ArgumentError, "missing configuration parameters > #{missing_keys.join(', ')}" unless missing_keys.empty?

      if !options['aws'].has_key?('region') && ! (options['aws'].has_key?('ec2_endpoint') && options['aws'].has_key?('elb_endpoint'))
        raise ArgumentError, 'missing configuration parameters > aws:region, or aws:ec2_endpoint and aws:elb_endpoint'
      end
    end

    ##
    # Checks AWS credentials settings to see if the CPI
    # will be able to authenticate to AWS.
    #
    def self.validate_credentials_source(options)
      credentials_source = options['aws']['credentials_source'] || AwsConfig::CREDENTIALS_SOURCE_STATIC

      if credentials_source != 'env_or_profile' && credentials_source != 'static'
        raise ArgumentError, "Unknown credentials_source #{credentials_source}"
      end

      if credentials_source == 'static'
        if options['aws']['access_key_id'].nil? || options['aws']['secret_access_key'].nil?
          raise ArgumentError, 'Must use access_key_id and secret_access_key with static credentials_source'
        end
      end

      if credentials_source == AwsConfig::CREDENTIALS_SOURCE_ENV_OR_PROFILE
        if !options['aws']['access_key_id'].nil? || !options['aws']['secret_access_key'].nil?
          raise ArgumentError, "Can't use access_key_id and secret_access_key with env_or_profile credentials_source"
        end
      end

      credentials_source = options['spotinst']['credentials_source'] || SpotinstConfig::CREDENTIALS_SOURCE_STATIC

      if credentials_source != 'static'
        raise ArgumentError, "Unknown credentials_source #{credentials_source}"
      end

      if credentials_source == 'static'
        if options['spotinst']['token'].nil?
          raise ArgumentError, 'Must use spotinst.token with static spotinst.credentials_source'
        end
      end
    end

    REQUIRED_KEYS = {
      'aws' => ['default_key_name', 'max_retries'],
      'registry' => ['endpoint', 'user', 'password'],
    }.freeze

  end
end

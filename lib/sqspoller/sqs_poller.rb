require "logger"
require "concurrent"
require "net/http"
require "yaml"
require "erb"
require "sqspoller/worker_task"
require "sqspoller/message_delegator"
require "sqspoller/queue_controller"

module Sqspoller

  class SqsPoller
    class << self

      def symbolize(map)
        if map.class == Hash
          map.inject({}) do |memo,(k,v)|
            memo[k.to_sym] = symbolize(v)
            memo
          end
        else
          map
        end
      end

      def daemonize(filename)
        raise 'Must run as root' if Process.euid != 0

        raise 'First fork failed' if (pid = fork) == -1
        exit unless pid.nil?

        Process.setsid
        raise 'Second fork failed' if (pid = fork) == -1
        exit unless pid.nil?
        puts "Daemon pid: #{Process.pid}" # Or save it somewhere, etc.

        Dir.chdir '/'
        File.umask 0000

        STDIN.reopen filename
        STDOUT.reopen '/dev/null', 'a'
        STDERR.reopen STDOUT
      end

      def start_poller_with_config(config, queue_config_name, access_key_id, secret_access_key, region, logger_file)
        puts "Started poller method"
        @logger = Logger.new(logger_file)

        qcs = []
        queues_config = config[queue_config_name] || config[queue_config_name.to_sym]
        total_poller_threads = queues_config.keys.reduce(0) do |sum, queue|
                                 sum += queues_config[queue][:polling_threads]
                               end
        message_delegator = initialize_worker config[:worker_configuration], total_poller_threads, logger_file
        queues_config.keys.each do |queue|
          if queues_config[queue][:polling_threads] == 0
            @logger.info "Polling disabled for queue: #{queue}"
          else
            @logger.info "Creating QueueController object for queue: #{queue}"
            qc = QueueController.new queue,
                                     queues_config[queue][:polling_threads],
                                     message_delegator,
                                     access_key_id,
                                     secret_access_key,
                                     region,
                                     logger_file
            qc.start
            qc.threads.each do |thread|
              thread.join
            end
          end
        end
      end

      def start_poller(filename, queue_config_name, access_key_id, secret_access_key, region, log_filename=nil)
        content = IO.read filename
        start_poller_from_content content, queue_config_name, access_key_id, secret_access_key, region, log_filename
      end

      def start_poller_from_content(content, queue_config_name, access_key_id, secret_access_key, region, log_filename)
        puts "Starting poller"
        erb = ERB.new content
        config = YAML.load erb.result
        config = symbolize config

        if log_filename.nil? || log_filename.empty?
          puts "Did not receive log file name"
          fork do
            Process.daemon
            start_poller_with_config config, queue_config_name, access_key_id, secret_access_key, region, STDOUT
          end
        else
          puts "Did receive log file name"
          daemonize log_filename
          start_poller_with_config config, queue_config_name, access_key_id, secret_access_key, region, log_filename
        end
      end

      def initialize_worker(worker_configuration, total_poller_threads, logger_file)
        worker_thread_count = worker_configuration[:concurrency] || total_poller_threads
        waiting_tasks_ratio = worker_configuration[:waiting_tasks_ratio] || 1

        klass = worker_configuration[:worker_class].split('::').inject(Object) {|o,c| o.const_get c}
        worker_task = klass.new(worker_configuration, logger_file)

        MessageDelegator.new worker_thread_count, waiting_tasks_ratio, worker_task, logger_file
      end
    end
  end

end

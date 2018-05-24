require "concurrent"
require "rest-client"

module Sqspoller
  class WorkerTask

    ALLOWED_METHODS = { 'post' => :post,
                        'get'  => :get
                      }

    def initialize worker_configuration
      @http_method = ALLOWED_METHODS[worker_configuration[:http_method].downcase]
      @http_url = worker_configuration[:http_url]
      @timeout = worker_configuration[:timeout] && worker_configuration[:timeout].to_i || 450
    end

    def process(message, _message_id)
      if @http_method
        RestClient::Request.execute(:method => @http_method,
                                    :url => @http_url,
                                    :payload => message,
                                    :headers => HEADERS,
                                    :timeout => @timeout,
                                    :open_timeout => 5) do |response, request, result|
          case response.code
          when 200
            return "OK"
          else
            raise "Service did not return 200 OK response. #{response.code}"
          end
        end
      else
        raise "Invalid http_method provided. #{http_method}"
      end
    end
  end
end

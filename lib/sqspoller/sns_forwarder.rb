require 'aws-sdk'

module Sqspoller
  class SnsForwarder
    def initialize args
      @sns = Aws::SNS::Client.new access_key_id: args[:access_key_id],
                                  secret_access_key: args[:secret_access_key],
                                  region: args[:region]
      @topic_arn = args[:sns_topic_arn]
      @logger = args[:logger]
      @logger.info "     Inializing SnsForwarder with SNS topic arn: #{@topic_arn}"
    end

    def process message, message_id
      @logger.info "        Processing message"
      @logger.info "          message_id: #{message_id}"
      @logger.info "          message body: #{message}"
      @logger.info "            Publishing to #{@topic_arn}"
      response = @sns.publish topic_arn: @topic_arn, message: message
      @logger.info "            SNS response: #{response.inspect}"
    end
  end
end

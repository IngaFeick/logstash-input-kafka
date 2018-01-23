require 'logstash/namespace'
require 'logstash/inputs/base'
require 'jruby-kafka'
require 'stud/interval'
require 'thread/pool'
require 'thread/future'

# This input will read events from a Kafka topic. It uses the high level consumer API provided
# by Kafka to read messages from the broker. It also maintains the state of what has been
# consumed using Zookeeper. The default input codec is json.
#
# Here's a compatibility matrix that shows the Kafka broker and client versions that are compatible with each combination
# of Logstash and the Kafka input plugin:
#
# [options="header"]
# |==========================================================
# |Kafka Broker Version |Kafka Client Version |Logstash Version |Plugin Version |Why?
# |0.8       |0.8       |2.0.0 - 2.x.x   |<2.10.0 |Legacy, 0.8 is still popular
# |0.8       |0.8       |2.0.0 - 2.x.x   | 2.10.x |Version 2.9 made compatible with LS5. Works with the new getter/setter APIs (`event.set('[product][price]', 10)`) but uses the old kafka lib from the 2.x branch
# |0.9       |0.9       |2.0.0 - 2.3.x   | 3.x.x |Works with the old Ruby Event API (`event['product']['price'] = 10`)
# |0.9       |0.9       |2.4.0 - 5.0.x   | 4.x.x |Works with the new getter/setter APIs (`event.set('[product][price]', 10)`)
# |0.10      |0.10      |2.4.0 - 5.0.x   | 5.x.x |Not compatible with the 0.9 broker
# |==========================================================
#
# NOTE: It's a good idea to upgrade brokers before consumers/producers because brokers target backwards compatibility.
# For example, the 0.9 broker will work with both the 0.8 consumer and 0.9 consumer APIs, but not the other way around.
#
# You must configure `topic_id`, `white_list` or `black_list`. By default it will connect to a
# Zookeeper running on localhost. All the broker information is read from Zookeeper state.
#
# Ideally you should have as many threads as the number of partitions for a perfect balance --
# more threads than partitions means that some threads will be idle
#
# For more information see http://kafka.apache.org/documentation.html#theconsumer
#
# Kafka consumer configuration: http://kafka.apache.org/documentation.html#consumerconfigs
#
class LogStash::Inputs::Kafka < LogStash::Inputs::Base
  config_name 'kafka'

  default :codec, 'json'

  # Specifies the ZooKeeper connection string in the form hostname:port where host and port are
  # the host and port of a ZooKeeper server. You can also specify multiple hosts in the form
  # `hostname1:port1,hostname2:port2,hostname3:port3`.
  #
  # The server may also have a ZooKeeper chroot path as part of it's ZooKeeper connection string
  # which puts its data under some path in the global ZooKeeper namespace. If so the consumer
  # should use the same chroot path in its connection string. For example to give a chroot path of
  # `/chroot/path` you would give the connection string as
  # `hostname1:port1,hostname2:port2,hostname3:port3/chroot/path`.
  config :zk_connect, :validate => :string, :default => 'localhost:2181'
  # A string that uniquely identifies the group of consumer processes to which this consumer
  # belongs. By setting the same group id multiple processes indicate that they are all part of
  # the same consumer group.
  config :group_id, :validate => :string, :default => 'logstash'
  # The topic to consume messages from
  config :topic_id, :validate => :string, :default => nil
  # Whitelist of topics to include for consumption.
  config :white_list, :validate => :string, :default => nil
  # Blacklist of topics to exclude from consumption.
  config :black_list, :validate => :string, :default => nil
  # Reset the consumer group to start at the earliest message present in the log by clearing any
  # offsets for the group stored in Zookeeper. This is destructive! Must be used in conjunction
  # with auto_offset_reset => 'smallest'
  config :reset_beginning, :validate => :boolean, :default => false
  # `smallest` or `largest` - (optional, default `largest`) If the consumer does not already
  # have an established offset or offset is invalid, start with the earliest message present in the
  # log (`smallest`) or after the last message in the log (`largest`).
  config :auto_offset_reset, :validate => %w( largest smallest ), :default => 'largest'
  # The frequency in ms that the consumer offsets are committed to zookeeper.
  config :auto_commit_interval_ms, :validate => :number, :default => 1000
  # Number of threads to read from the partitions. Ideally you should have as many threads as the
  # number of partitions for a perfect balance. More threads than partitions means that some
  # threads will be idle. Less threads means a single thread could be consuming from more than
  # one partition
  config :consumer_threads, :validate => :number, :default => 1
  # Internal Logstash queue size used to hold events in memory after it has been read from Kafka
  config :queue_size, :validate => :number, :default => 20
  # When a new consumer joins a consumer group the set of consumers attempt to "rebalance" the
  # load to assign partitions to each consumer. If the set of consumers changes while this
  # assignment is taking place the rebalance will fail and retry. This setting controls the
  # maximum number of attempts before giving up.
  config :rebalance_max_retries, :validate => :number, :default => 4
  # Backoff time between retries during rebalance.
  config :rebalance_backoff_ms, :validate => :number, :default => 2000
  # Throw a timeout exception to the consumer if no message is available for consumption after
  # the specified interval
  config :consumer_timeout_ms, :validate => :number, :default => -1
  # Option to restart the consumer loop on error
  config :consumer_restart_on_error, :validate => :boolean, :default => true
  # Time in millis to wait for consumer to restart after an error
  config :consumer_restart_sleep_ms, :validate => :number, :default => 0
  # Option to add Kafka metadata like topic, message size to the event.
  # This will add a field named `kafka` to the logstash event containing the following attributes:
  #   `msg_size`: The complete serialized size of this message in bytes (including crc, header attributes, etc)
  #   `topic`: The topic this message is associated with
  #   `consumer_group`: The consumer group used to read in this event
  #   `partition`: The partition this message is associated with
  #   `offset`: The offset from the partition this message is associated with
  #   `key`: A ByteBuffer containing the message key
  config :decorate_events, :validate => :boolean, :default => false
  # A unique id for the consumer; generated automatically if not set.
  config :consumer_id, :validate => :string, :default => nil
  # The number of byes of messages to attempt to fetch for each topic-partition in each fetch
  # request. These bytes will be read into memory for each partition, so this helps control
  # the memory used by the consumer. The fetch request size must be at least as large as the
  # maximum message size the server allows or else it is possible for the producer to send
  # messages larger than the consumer can fetch.
  config :fetch_message_max_bytes, :validate => :number, :default => 1048576
  # The serializer class for messages. The default decoder takes a byte[] and returns the same byte[]
  config :decoder_class, :validate => :string, :default => 'kafka.serializer.DefaultDecoder'
  # The serializer class for keys (defaults to the same default as for messages)
  config :key_decoder_class, :validate => :string, :default => 'kafka.serializer.DefaultDecoder'

  config :thread_pool_size, :validate => :number, :default => 4

  class KafkaShutdownEvent; end
  KAFKA_SHUTDOWN_EVENT = KafkaShutdownEvent.new

  public
  def register
    java_import 'kafka.common.ConsumerRebalanceFailedException'
    @runner_threads = []
    @logger.info('Registering kafka', :group_id => @group_id, :topic_id => @topic_id, :zk_connect => @zk_connect)
    @kf_options = {
        :zk_connect => @zk_connect,
        :group_id => @group_id,
        :topic_id => @topic_id,
        :auto_offset_reset => @auto_offset_reset,
        :auto_commit_interval => @auto_commit_interval_ms,
        :rebalance_max_retries => @rebalance_max_retries,
        :rebalance_backoff_ms => @rebalance_backoff_ms,
        :consumer_timeout_ms => @consumer_timeout_ms,
        :consumer_restart_on_error => @consumer_restart_on_error,
        :consumer_restart_sleep_ms => @consumer_restart_sleep_ms,
        :fetch_message_max_bytes => @fetch_message_max_bytes,
        :allow_topics => @white_list,
        :filter_topics => @black_list,
        :value_decoder_class => @decoder_class,
        :key_decoder_class => @key_decoder_class
    }
    if @reset_beginning
      @kf_options[:reset_beginning] = 'from-beginning'
    end # if :reset_beginning
    topic_or_filter = [@topic_id, @white_list, @black_list].compact
    if topic_or_filter.count == 0
      raise LogStash::ConfigurationError, 'topic_id, white_list or black_list required.'
    elsif topic_or_filter.count > 1
      raise LogStash::ConfigurationError, 'Invalid combination of topic_id, white_list or black_list. Use only one.'
    end
   
  end # def register

  public
  def run(logstash_queue)
    @runner_consumers = @consumer_threads.times.map { |i| create_consumer("#{i}") }
    @runner_threads = @runner_consumers.each_with_index.map { |consumer,i| thread_runner(logstash_queue, consumer,i) }
    @runner_threads.each { |t| t.join }
  end

  public
  def kafka_consumers
    @runner_consumers
  end

  private
  def create_consumer(client_id)
    begin
      options = @kf_options.clone
      options[:consumer_id ] = @consumer_id + "-" + client_id
      Kafka::Group.new(options) 
    rescue => e
      logger.error("Unable to create Kafka consumer from given configuration",
                   :kafka_error_message => e,
                   :cause => e.respond_to?(:getCause) ? e.getCause() : nil)
      throw e
    end
  end



  private
  def thread_runner(logstash_queue, consumer, threadnumber)
  
    Thread.new do
      @logger.info("Starting kafka thread #{threadnumber}")
      begin 
        kafka_client_queue = SizedQueue.new(@queue_size)
        consumer.run(1, kafka_client_queue)
        
        codec_instance = @codec.clone
        while !stop?
          message_and_metadata = kafka_client_queue.pop
          if message_and_metadata == KAFKA_SHUTDOWN_EVENT
            break
          end
          queue_event(message_and_metadata, logstash_queue, codec_instance)
        end

        until kafka_client_queue.empty?
          message_and_metadata = kafka_client_queue.pop
          if message_and_metadata == KAFKA_SHUTDOWN_EVENT
            break
          end
          queue_event(message_and_metadata, logstash_queue, codec_instance)
        end          

      rescue => e
        # raise e if !stop?
        # OR
        # @logger.warn('kafka client threw exception, restarting',
        #            :exception => e)
        # Stud.stoppable_sleep(Float(@consumer_restart_sleep_ms) * 1 / 1000) { stop? }
        # retry if !stop?
      ensure
        consumer.shutdown if consumer.running?
      end
    end
  end

  private 
  def queue_event(message_and_metadata, logstash_queue, codec_instance)
    begin
      codec_instance.decode("#{message_and_metadata.message}") do |event|
        decorate(event)
        if @decorate_events
          event.set("[@metadata][kafka][topic]", record.topic)
          event.set("[@metadata][kafka][consumer_group]", @group_id)
          event.set("[@metadata][kafka][partition]", record.partition)
          event.set("[@metadata][kafka][offset]", record.offset)
          event.set("[@metadata][kafka][key]", record.key)
          event.set("[@metadata][kafka][timestamp]", record.timestamp)
        end
        logstash_queue << event
      end # do
    end # begin
  end # def

  public
  def stop
    @runner_consumers.each { |c| c.shutdown if c.running? }
    @kafka_client_queue.push(KAFKA_SHUTDOWN_EVENT)
  end

  # public
  # def receive_from_codec(event, message_and_metadata, output_queue)
  #   begin
  #     decorate(event)
  #     if @decorate_events
  #       event.set('kafka', {'msg_size' => message_and_metadata.message.size,
  #                         'topic' => message_and_metadata.topic,
  #                         'consumer_group' => @group_id,
  #                         'partition' => message_and_metadata.partition,
  #                         'offset' => message_and_metadata.offset,
  #                         'key' => message_and_metadata.key} )
  #     end
  #     output_queue << event
  #   rescue => e # parse or event creation error
  #     @logger.error('Failed to create event', :message => "#{message_and_metadata.message}", :exception => e,
  #                   :backtrace => e.backtrace)
  #   end # begin
  # end



  # private
  # def queue_event(message_and_metadata, output_queue)
  #   @thread_pool.future {
  #       s =DecoderThread.new
  #       s.decode(self, @codec.clone, message_and_metadata, output_queue, @logger)        
  #   }
  # end # def queue_event
end #class LogStash::Inputs::Kafka


# class DecoderThread

#   def decode(kafka_input, codec, message_and_metadata, output_queue, logger)
#     begin
#       codec.decode("#{message_and_metadata.message}") do |event|
#         kafka_input.receive_from_codec(event, message_and_metadata, output_queue)
#       end # @codec.decode
#     rescue => e # parse or event creation error
#       logger.error('Failed to create event', :message => "#{message_and_metadata.message}", :exception => e,
#                     :backtrace => e.backtrace)
#     end # begin
#   end # decode
# end # class DecoderThread

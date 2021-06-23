// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Confluent.Kafka;
using Microsoft.Azure.WebJobs.Hosting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka
{
    /// <summary>
    /// Configuration for Kafka Web Jobs extension
    /// </summary>
    public class KafkaOptions : IOptionsFormatter, IProducerOptions
    {
        /// <summary>
        /// The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until `reconnect.backoff.max.ms` is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately.
        ///
        /// default: 100
        /// importance: medium
        /// 
        /// As defined in from Confluent.Kafka
        /// </summary>
        public int? ReconnectBackoffMs { get; set; }

        /// <summary>
        /// The maximum time to wait before reconnecting to a broker after the connection has been closed.
        ///
        /// default: 10000
        /// importance: medium
        /// 
        /// As defined in from Confluent.Kafka
        /// </summary>
        public int? ReconnectBackoffMaxMs { get; set; }

        /// <summary>
        /// librdkafka statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. The granularity is 1000ms. A value of 0 disables statistics.
        ///
        /// default: 0
        /// importance: high
        /// 
        /// As defined in from Confluent.Kafka
        /// </summary>
        public int? StatisticsIntervalMs { get; set; }


        /// <summary>
        /// Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. The allowed range is configured with the **broker** configuration properties `group.min.session.timeout.ms` and `group.max.session.timeout.ms`. Also see `max.poll.interval.ms`.
        ///
        /// default: 10000
        /// importance: high
        /// 
        /// As defined in from Confluent.Kafka
        /// </summary>
        public int? SessionTimeoutMs { get; set; }

        /// <summary>
        /// Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll()) for high-level consumers. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member. Warning: Offset commits may be not possible at this point. Note: It is recommended to set `enable.auto.offset.store=false` for long-time processing applications and then explicitly store offsets (using offsets_store()) *after* message processing, to make sure offsets are not auto-committed prior to processing has finished. The interval is checked two times per second. See KIP-62 for more information.
        ///
        /// default: 300000
        /// importance: high
        /// 
        /// As defined in from Confluent.Kafka
        /// </summary>
        public int? MaxPollIntervalMs { get; set; }

        /// <summary>
        /// Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.
        ///
        /// default: 100000
        /// importance: medium
        /// 
        /// As defined in Confluent.Kafka
        /// </summary>
        public int? QueuedMinMessages { get; set; }

        /// <summary>
        /// Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes. This property has higher priority than queued.min.messages.
        ///
        /// default: 1048576
        /// importance: medium
        /// 
        /// As defined in Confluent.Kafka
        /// </summary>
        public int? QueuedMaxMessagesKbytes { get; set; }

        /// <summary>
        /// Initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched.
        ///
        /// default: 1048576
        /// importance: medium
        /// 
        /// As defined in Confluent.Kafka
        /// </summary>
        public int? MaxPartitionFetchBytes { get; set; }

        /// <summary>
        /// Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in batches by the consumer and if the first message batch in the first non-empty partition of the Fetch request is larger than this value, then the message batch will still be returned to ensure the consumer can make progress. The maximum message batch size accepted by the broker is defined via `message.max.bytes` (broker config) or `max.message.bytes` (broker topic config). `fetch.max.bytes` is automatically adjusted upwards to be at least `message.max.bytes` (consumer config).
        ///
        /// default: 52428800
        /// importance: medium
        /// 
        /// As defined in Confluent.Kafka
        /// </summary>
        public int? FetchMaxBytes { get; set; }

        int maxBatchSize = 64;
        /// <summary>
        /// Max batch size when calling a Kafka trigger function
        /// 
        /// default: 64
        /// </summary>
        public int MaxBatchSize
        {
            get => this.maxBatchSize;
            set
            {
                if (value <= 0)
                {
                    throw new InvalidOperationException("Maximum batch size must be larger than 0.");
                }

                this.maxBatchSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the auto commit interval ms.
        /// Default = 200ms
        /// 
        /// Librdkafka: auto.commit.interval.ms (default 5000)
        /// </summary>
        /// <value>The auto commit interval ms.</value>
        public int AutoCommitIntervalMs { get; set; } = 200;

        /// <summary>
        /// Gets or sets the debug option for librdkafka library.
        /// Default = "" (disable)
        /// A comma-separated list of debug contexts to enable: all,generic,broker,topic,metadata,producer,queue,msg,protocol,cgrp,security,fetch
        /// Librdkafka: debug
        /// </summary>
        public string LibkafkaDebug { get; set; }

        // <summary>
        // Metadata cache max age. 
        // https://github.com/Azure/azure-functions-kafka-extension/issues/187
        // default: 180000 
        // </summary>
        public int? MetadataMaxAgeMs { get; set; }

        // <summary>
        // Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets 
        // https://github.com/Azure/azure-functions-kafka-extension/issues/187
        // default: true
        // </summary>
        public bool? SocketKeepaliveEnable { get; set; }

        int subscriberIntervalInSeconds = 1;
        /// <summary>
        /// Defines the minimum frequency in which messages will be executed by function. Only if the message volume is less than <see cref="MaxBatchSize"/> / <see cref="SubscriberIntervalInSeconds"/>
        /// 
        /// default: 1 second
        /// </summary>
        public int SubscriberIntervalInSeconds
        {
            get => this.subscriberIntervalInSeconds;
            set
            {
                if (value <= 0)
                {
                    throw new InvalidOperationException("Subscriber interval must be larger than 0.");
                }

                this.subscriberIntervalInSeconds = value;
            }
        }

        int executorChannelCapacity = 1;
        /// <summary>
        /// Defines the channel capacity in which messages will be sent to functions
        /// Once the capacity is reached the Kafka subscriber will pause until the function catches up
        /// 
        /// default: 1
        /// </summary>
        public int ExecutorChannelCapacity
        {
            get => this.executorChannelCapacity;
            set
            {
                if (value <= 0)
                {
                    throw new InvalidOperationException("Executor channel capacity must be larger than 0.");
                }

                this.executorChannelCapacity = value;
            }
        }

        int channelFullRetryIntervalInMs = 50;
        /// <summary>
        /// Defines the interval in milliseconds in which the subscriber should retry adding items to channel once it reaches the capacity
        /// 
        /// default: 50ms
        /// </summary>
        public int ChannelFullRetryIntervalInMs
        {
            get => this.channelFullRetryIntervalInMs;
            set
            {
                if (value <= 0)
                {
                    throw new InvalidOperationException("Channel full retry interval must be larger than 0.");
                }

                this.channelFullRetryIntervalInMs = value;
            }
        }

        /// <summary>
        /// Default compression level parameter for algorithm selected by configuration property <see cref="CompressionType"/>
        /// Can be overridden by setting <see cref="KafkaAttribute.CompressionLevel"/>
        /// 
        /// compression.level in librdkafka
        /// </summary>
        public int? CompressionLevel { get; set; }

        /// <summary>
        /// Default compression codec to use for compressing message sets.
        /// Can be overridden by setting <see cref="KafkaAttribute.CompressionType"/>
        /// 
        /// compression.codec in librdkafka
        /// </summary>
        public MessageCompressionType? CompressionType { get; set; }

        public BrokerAuthenticationMode? AuthenticationMode => throw new NotImplementedException();

        public int? BatchSize { get; set; }

        public bool? EnableIdempotence { get; set; }

        public int? MessageTimeoutMs { get; set; }

        public int? MaxMessageBytes { get; set; }

        public int? MaxRetries { get; set; }

        public BrokerProtocol? Protocol { get; set; }

        public int? RequestTimeoutMs { get; set; }

        public string SslCaLocation { get; set; }

        public string SslCertificateLocation { get; set; }

        public string SslKeyLocation { get; set; }

        public string SslKeyPassword { get; set; }

        [JsonIgnore]
        public string Password { get; set; }

        public string Username { get; set; }

        public string Format()
        {
            var serializerSettings = new JsonSerializerSettings()
            {
                DefaultValueHandling = DefaultValueHandling.Ignore,
                Formatting = Formatting.Indented,
            };

            return JsonConvert.SerializeObject(this, serializerSettings);
        }
    }
}

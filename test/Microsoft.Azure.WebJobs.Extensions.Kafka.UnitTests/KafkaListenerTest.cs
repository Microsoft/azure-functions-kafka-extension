// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Confluent.Kafka;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Protocols;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka.UnitTests
{
    public class KafkaListenerTest
    {
        private ConsumeResult<TKey, TValue> CreateConsumeResult<TKey, TValue>(TValue value, int partition, long offset)
        {
            var msg = new Message<TKey, TValue>()
            {
                Value = value,
                Timestamp = Timestamp.Default,
            };

            var res = new ConsumeResult<TKey, TValue>();
            res.Message = msg;
            res.Topic = "topic";
            res.Partition = partition;
            res.Offset = offset;

            return res;
        }

        [Fact]
        public async Task When_Using_SingleItem_Binding_10_Events_Should_Execute_Function_Ten_Times()
        {
            const int ExpectedEventCount = 10;

            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Ignore, string>>();

            var offset = 0L;
            consumer.Setup(x => x.Consume(It.IsNotNull<TimeSpan>()))
                .Returns(() =>
                {
                    if (offset < ExpectedEventCount)
                    {
                        offset++;

                        return CreateConsumeResult<Ignore, string>(offset.ToString(), 0, offset);
                    }

                    return null;
                });

            var executorFinished = new SemaphoreSlim(0);
            var executorCalls = 0;
            executor.Setup(x => x.TryExecuteAsync(It.IsNotNull<TriggeredFunctionData>(), It.IsAny<CancellationToken>()))
                .Callback(() =>
                {
                    Interlocked.Increment(ref executorCalls);
                    executorFinished.Release();
                })
                .ReturnsAsync(new FunctionResult(true));

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                ConsumerGroup = "group1",
            };

            var target = new KafkaListenerForTest<Ignore, string>(
                executor.Object,
                singleDispatch: true,
                options: new KafkaOptions(),
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                logger: NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default(CancellationToken));

            Assert.True(await executorFinished.WaitAsync(TimeSpan.FromSeconds(5)));

            await target.StopAsync(default(CancellationToken));
        }

        [Fact]
        public async Task When_Using_MultiItem_Binding_10_Events_Should_Execute_Function_Once()
        {
            const int ExpectedEventCount = 10;

            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Null, string>>();

            var offset = 0L;
            consumer.Setup(x => x.Consume(It.IsNotNull<TimeSpan>()))
                .Returns(() =>
                {
                    if (offset < ExpectedEventCount)
                    {
                        offset++;

                        return CreateConsumeResult<Null, string>(offset.ToString(), 0, offset);
                    }

                    return null;
                });

            var executorFinished = new SemaphoreSlim(0);
            var processedItemCount = 0;
            executor.Setup(x => x.TryExecuteAsync(It.IsNotNull<TriggeredFunctionData>(), It.IsAny<CancellationToken>()))
                .Callback<TriggeredFunctionData, CancellationToken>((td, _) =>
                {
                    var triggerData = (KafkaTriggerInput)td.TriggerValue;
                    var alreadyProcessed = Interlocked.Add(ref processedItemCount, triggerData.Events.Length);
                    if (alreadyProcessed == ExpectedEventCount)
                    {
                        executorFinished.Release();
                    }
                })
                .ReturnsAsync(new FunctionResult(true));

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                ConsumerGroup = "group1",
            };

            var target = new KafkaListenerForTest<Null, string>(
                executor.Object,
                singleDispatch: false,
                options: new KafkaOptions(),
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                logger: NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default(CancellationToken));

            Assert.True(await executorFinished.WaitAsync(TimeSpan.FromSeconds(5)));

            await target.StopAsync(default(CancellationToken));

            executor.Verify(x => x.TryExecuteAsync(It.IsNotNull<TriggeredFunctionData>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task When_Topic_Has_Multiple_Partitions_Should_Execute_And_Commit_In_Order(bool singleDispatch)
        {
            const int MessagesPerPartition = 5;
            const int PartitionCount = 2;
            const int BatchCount = 2;

            const long Offset_A = 0;
            const long Offset_B = 1;
            const long Offset_C = 2;
            const long Offset_D = 3;
            const long Offset_E = 4;

            const long Offset_1 = 0;
            const long Offset_2 = 1;
            const long Offset_3 = 2;
            const long Offset_4 = 3;
            const long Offset_5 = 4;

            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Null, string>>();

            var committed = new ConcurrentQueue<TopicPartitionOffset>();
           
            consumer.Setup(x => x.StoreOffset(It.IsNotNull<TopicPartitionOffset>()))
                .Callback<TopicPartitionOffset>((topicPartitionOffset) =>
                {
                    committed.Enqueue(topicPartitionOffset);
                });

            // Batch 1: AB12C
            // Batch 2: 34DE5
            consumer.SetupSequence(x => x.Consume(It.IsNotNull<TimeSpan>()))
                .Returns(CreateConsumeResult<Null, string>("A", 0, Offset_A))
                .Returns(CreateConsumeResult<Null, string>("B", 0, Offset_B))
                .Returns(CreateConsumeResult<Null, string>("1", 1, Offset_1))
                .Returns(CreateConsumeResult<Null, string>("2", 1, Offset_2))
                .Returns(CreateConsumeResult<Null, string>("C", 0, Offset_C))
                .Returns((ConsumeResult<Null, string>)null)
                .Returns(CreateConsumeResult<Null, string>("3", 1, Offset_3))
                .Returns(CreateConsumeResult<Null, string>("4", 1, Offset_4))
                .Returns(CreateConsumeResult<Null, string>("D", 0, Offset_D))
                .Returns(CreateConsumeResult<Null, string>("E", 0, Offset_E))
                .Returns(() =>
                {
                    // from now on return null
                    consumer.Setup(x => x.Consume(It.IsNotNull<TimeSpan>()))
                        .Returns((ConsumeResult<Null, string>)null);

                    return CreateConsumeResult<Null, string>("5", 1, Offset_5);
                });

            var partition0 = new ConcurrentQueue<string>();
            var partition1 = new ConcurrentQueue<string>();
            var executorFinished = new SemaphoreSlim(0);

            executor.Setup(x => x.TryExecuteAsync(It.IsNotNull<TriggeredFunctionData>(), It.IsAny<CancellationToken>()))
                .Callback<TriggeredFunctionData, CancellationToken>((t, _) =>
                {
                    var triggerData = (KafkaTriggerInput)t.TriggerValue;

                    if (singleDispatch)
                    {
                        Assert.Single(triggerData.Events);
                    }

                    foreach (var ev in triggerData.Events)
                    {
                        switch (ev.Partition)
                        {
                            case 0:
                                partition0.Enqueue(ev.Value.ToString());
                                break;

                            case 1:
                                partition1.Enqueue(ev.Value.ToString());
                                break;

                            default:
                                Assert.True(false, "Unknown partition");
                                break;
                        }
                    }

                    if (partition0.Count == 5 && partition1.Count == 5)
                    {
                        executorFinished.Release();
                    }
                })
                .ReturnsAsync(new FunctionResult(true));

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                ConsumerGroup = "group1",
            };

            var target = new KafkaListenerForTest<Null, string>(
                executor.Object,
                singleDispatch,
                new KafkaOptions(),
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default(CancellationToken));

            Assert.True(await executorFinished.WaitAsync(TimeSpan.FromSeconds(5)));

            // Give time for the commit to be saved
            await Task.Delay(1500);

            Assert.Equal(new[] { "A", "B", "C", "D", "E" }, partition0.ToArray());
            Assert.Equal(new[] { "1", "2", "3", "4", "5" }, partition1.ToArray());

            // Committing will be the one we read + 1
            // Batch 1: AB12C
            // Batch 2: 34DE5
            var committedArray = committed.ToArray();

            if (singleDispatch)
            {
                // In single dispatch we expected to commit once per message / per partition
                Assert.Equal(MessagesPerPartition * PartitionCount, committedArray.Length);

                // In single dispatch each item will be committed individually
                Assert.Equal(new[] { Offset_A + 1, Offset_B + 1, Offset_C + 1, Offset_D + 1, Offset_E + 1 }, committedArray.Where(x => x.Partition == 0).Select(x => (long)x.Offset).ToArray());
                Assert.Equal(new[] { Offset_1 + 1, Offset_2 + 1, Offset_3 + 1, Offset_4 + 1, Offset_5 + 1 }, committedArray.Where(x => x.Partition == 1).Select(x => (long)x.Offset).ToArray());
            }
            else
            {
                // In multi dispatch we expected to commit once per batch / per partition
                Assert.Equal(BatchCount * PartitionCount, committedArray.Length);

                // In multi dispatch we batch/partition pair will be committed
                Assert.Equal(new[] { Offset_C + 1, Offset_E + 1 }, committedArray.Where(x => x.Partition == 0).Select(x => (long)x.Offset).ToArray());
                Assert.Equal(new[] { Offset_2 + 1, Offset_5 + 1 }, committedArray.Where(x => x.Partition == 1).Select(x => (long)x.Offset).ToArray());
            }                       

            await target.StopAsync(default(CancellationToken));
        }

        [Fact]
        public async Task When_Options_Are_Set_Should_Be_Set_In_Consumer_Config()
        {
            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Ignore, string>>();

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                ConsumerGroup = "group1",
                SslKeyPassword = "password1",
                SslCertificateLocation = "path/to/cert",
                SslKeyLocation = "path/to/key",
                SslCaLocation = "path/to/cacert"
            };

            var kafkaOptions = new KafkaOptions();
            var target = new KafkaListenerForTest<Ignore, string>(
                executor.Object,
                true,
                kafkaOptions,
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default);

            Assert.Equal(12, target.ConsumerConfig.Count());
            Assert.Equal("testBroker", target.ConsumerConfig.BootstrapServers);
            Assert.Equal("group1", target.ConsumerConfig.GroupId);
            Assert.Equal("password1", target.ConsumerConfig.SslKeyPassword);
            Assert.Equal("path/to/cert", target.ConsumerConfig.SslCertificateLocation);
            Assert.Equal("path/to/key", target.ConsumerConfig.SslKeyLocation);
            Assert.Equal("path/to/cacert", target.ConsumerConfig.SslCaLocation);
            Assert.Equal(kafkaOptions.AutoCommitIntervalMs, target.ConsumerConfig.AutoCommitIntervalMs);
            Assert.Equal(true, target.ConsumerConfig.EnableAutoCommit);
            Assert.Equal(false, target.ConsumerConfig.EnableAutoOffsetStore);
            Assert.Equal(180000, target.ConsumerConfig.MetadataMaxAgeMs);
            Assert.Equal(true, target.ConsumerConfig.SocketKeepaliveEnable);
            Assert.Equal(AutoOffsetReset.Earliest, target.ConsumerConfig.AutoOffsetReset);

            await target.StopAsync(default);
        }

        [Fact]
        public async Task When_Options_With_Ssal_Are_Set_Should_Be_Set_In_Consumer_Config()
        {
            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Null, string>>();

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                ConsumerGroup = "group1",
                SaslMechanism = SaslMechanism.Plain,
                SaslPassword = "mypassword",
                SaslUsername ="myusername",
                SecurityProtocol = SecurityProtocol.SaslSsl,
            };

            var kafkaOptions = new KafkaOptions();
            var target = new KafkaListenerForTest<Null, string>(
                executor.Object,
                true,
                kafkaOptions,
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default);

            Assert.Equal(12, target.ConsumerConfig.Count());
            Assert.Equal("testBroker", target.ConsumerConfig.BootstrapServers);
            Assert.Equal("group1", target.ConsumerConfig.GroupId);
            Assert.Equal(kafkaOptions.AutoCommitIntervalMs, target.ConsumerConfig.AutoCommitIntervalMs);
            Assert.Equal(true, target.ConsumerConfig.EnableAutoCommit);
            Assert.Equal(false, target.ConsumerConfig.EnableAutoOffsetStore);
            Assert.Equal(AutoOffsetReset.Earliest, target.ConsumerConfig.AutoOffsetReset);
            Assert.Equal(SaslMechanism.Plain, target.ConsumerConfig.SaslMechanism);
            Assert.Equal("mypassword", target.ConsumerConfig.SaslPassword);
            Assert.Equal("myusername", target.ConsumerConfig.SaslUsername);
            Assert.Equal(SecurityProtocol.SaslSsl, target.ConsumerConfig.SecurityProtocol);

            await target.StopAsync(default);
        }
        
        [Fact]
        public async Task When_Options_With_Auto_Offset_Reset_Are_Set_Should_Be_Set_In_Consumer_Config()
        {
            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Null, string>>();

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                ConsumerGroup = "group1",
                SaslMechanism = SaslMechanism.Plain,
                SaslPassword = "mypassword",
                SaslUsername ="myusername",
                SecurityProtocol = SecurityProtocol.SaslSsl,
            };

            var kafkaOptions = new KafkaOptions
            {
                AutoOffsetReset = AutoOffsetReset.Latest,
            };

            var target = new KafkaListenerForTest<Null, string>(
                executor.Object,
                true,
                kafkaOptions,
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default);

            Assert.Equal(12, target.ConsumerConfig.Count());
            Assert.Equal("testBroker", target.ConsumerConfig.BootstrapServers);
            Assert.Equal("group1", target.ConsumerConfig.GroupId);
            Assert.Equal(kafkaOptions.AutoCommitIntervalMs, target.ConsumerConfig.AutoCommitIntervalMs);
            Assert.Equal(true, target.ConsumerConfig.EnableAutoCommit);
            Assert.Equal(false, target.ConsumerConfig.EnableAutoOffsetStore);
            Assert.Equal(kafkaOptions.AutoOffsetReset, target.ConsumerConfig.AutoOffsetReset);
            Assert.Equal(SaslMechanism.Plain, target.ConsumerConfig.SaslMechanism);
            Assert.Equal("mypassword", target.ConsumerConfig.SaslPassword);
            Assert.Equal("myusername", target.ConsumerConfig.SaslUsername);
            Assert.Equal(SecurityProtocol.SaslSsl, target.ConsumerConfig.SecurityProtocol);

            await target.StopAsync(default);
        }

        [Fact]
        public async Task When_Using_Invalid_Eventhubs_Certificate_File_Should_Fail()
        {
            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Null, string>>();

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                EventHubConnectionString = "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=reader;SharedAccessKey=fake",
                ConsumerGroup = "group1",
                SslCaLocation = "does-not-exists.pem",
            };

            var kafkaOptions = new KafkaOptions();
            var target = new KafkaListenerForTest<Null, string>(
                executor.Object,
                true,
                kafkaOptions,
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(() => target.StartAsync(default));
        }

        [Fact]
        public async Task When_Using_Default_Eventhubs_Certificate_File_Should_Contain_File_Location()
        {
            const string eventhubsConnectionString = "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=reader;SharedAccessKey=fake";
            const string broker = "testBroker";
            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Null, string>>();

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = broker,
                Topic = "topic",
                EventHubConnectionString = eventhubsConnectionString,
                ConsumerGroup = "group1",
            };

            var kafkaOptions = new KafkaOptions();
            var target = new KafkaListenerForTest<Null, string>(
                executor.Object,
                true,
                kafkaOptions,
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default);

            Assert.NotEmpty(target.ConsumerConfig.SslCaLocation);
            Assert.True(File.Exists(target.ConsumerConfig.SslCaLocation));
            var expectedBootstrapServers = $"{broker}{KafkaListenerForTest<Null, string>.EventHubsBrokerListDns}:{KafkaListenerForTest<Null, string>.EventHubsBrokerListPort}";
            Assert.Equal(expectedBootstrapServers, target.ConsumerConfig.BootstrapServers);
            Assert.Equal(KafkaListenerForTest<Null, string>.EventHubsSaslUsername, target.ConsumerConfig.SaslUsername);
            Assert.Equal(eventhubsConnectionString, target.ConsumerConfig.SaslPassword);
            Assert.Equal(SecurityProtocol.SaslSsl, target.ConsumerConfig.SecurityProtocol);
            Assert.Equal(SaslMechanism.Plain, target.ConsumerConfig.SaslMechanism);
            Assert.Equal(KafkaListenerForTest<Null, string>.EventHubsBrokerVersionFallback, target.ConsumerConfig.BrokerVersionFallback);

            await target.StopAsync(default);
        }


        /// <summary>
        /// Ensures that delays while processing a particular partition won't affect commits in different partitions
        /// Example:
        /// - Processing partition 0 is slow
        /// - Processing partition 1 is fast
        /// 
        /// Expected result is that partition 1 offset is commit without waiting for partition 0 to be finished
        /// </summary>
        [Fact]
        public async Task When_Using_Single_Dispatcher_Slow_Partition_Processing_Should_Not_Delay_Other_Partitions()
        {
            const int Offset1 = 1;
            const int Offset2 = 2;
            const int Offset3 = 3;
            const int Offset4 = 4;
            const int Offset5 = 5;


            var executor = new Mock<ITriggeredFunctionExecutor>();
            var consumer = new Mock<IConsumer<Null, string>>();

            DateTime? partitionDone0Time = null;
            var partition0Done = new ManualResetEvent(false);

            DateTime? partitionDone1Time = null;
            var partition1Done = new ManualResetEvent(false);            

            consumer.Setup(x => x.StoreOffset(It.IsNotNull<TopicPartitionOffset>()))
                .Callback<TopicPartitionOffset>((topicPartitionOffset) =>
                {
                    // If it is the last offset
                    if (topicPartitionOffset.Offset == (1 + Offset5))
                    {
                        switch (topicPartitionOffset.Partition)
                        {
                            case 0:
                                partitionDone0Time = DateTime.UtcNow;
                                partition0Done.Set();
                                break;

                            case 1:
                                partitionDone1Time = DateTime.UtcNow;
                                partition1Done.Set();
                                break;
                        }
                    }
                });

            // Batch: A12345
            consumer.SetupSequence(x => x.Consume(It.IsNotNull<TimeSpan>()))
                .Returns(CreateConsumeResult<Null, string>("A", 0, Offset5))
                .Returns(CreateConsumeResult<Null, string>("1", 1, Offset1))
                .Returns(CreateConsumeResult<Null, string>("2", 1, Offset2))
                .Returns(CreateConsumeResult<Null, string>("3", 1, Offset3))
                .Returns(CreateConsumeResult<Null, string>("4", 1, Offset4))
                .Returns(CreateConsumeResult<Null, string>("5", 1, Offset5))
                .Returns((ConsumeResult<Null, string>)null);

            executor.Setup(x => x.TryExecuteAsync(
                It.Is<TriggeredFunctionData>(t => ((KafkaTriggerInput)t.TriggerValue).Events[0].Partition == 0),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FunctionResult(true), TimeSpan.FromSeconds(1));

            executor.Setup(x => x.TryExecuteAsync(
                It.Is<TriggeredFunctionData>(t => ((KafkaTriggerInput)t.TriggerValue).Events[0].Partition == 1),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FunctionResult(true));

            var listenerConfig = new KafkaListenerConfiguration()
            {
                BrokerList = "testBroker",
                Topic = "topic",
                ConsumerGroup = "group1",
            };

            var target = new KafkaListenerForTest<Null, string>(
                executor.Object,
                singleDispatch: true,
                new KafkaOptions(),
                listenerConfig,
                requiresKey: true,
                valueDeserializer: null,
                NullLogger.Instance,
                functionId: "testId"
                );

            target.SetConsumer(consumer.Object);

            await target.StartAsync(default);

            Assert.True(partition1Done.WaitOne(TimeSpan.FromSeconds(5)));
            Assert.True(partition0Done.WaitOne(TimeSpan.FromSeconds(5)));

            Assert.True(partitionDone1Time < partitionDone0Time, "Partition 1 should have been committed before partition 0");

            await target.StopAsync(default(CancellationToken));
        }
    }
}
// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Confluent.Kafka;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka.UnitTests
{

    public class KafkaTopicScalerTest
    {
        const string TopicName = "topicTest";

        private readonly TopicPartition partition0;
        private readonly TopicPartition partition1;
        private readonly TopicPartition partition2;
        private readonly TopicPartition partition3;
        private readonly List<TopicPartition> partitions;
        private readonly KafkaTopicScalerForTest<string, byte[]> topicScaler;
        private readonly Mock<IConsumer<string, byte[]>> consumer;

        private Offset ZeroOffset => new Offset(0L);
        private TimeSpan AnyTimeSpan => It.IsAny<TimeSpan>();


        public KafkaTopicScalerTest()
        {
            consumer = new Mock<IConsumer<string, byte[]>>();

            partition0 = new TopicPartition(TopicName, new Partition(0));
            partition1 = new TopicPartition(TopicName, new Partition(1));
            partition2 = new TopicPartition(TopicName, new Partition(2));
            partition3 = new TopicPartition(TopicName, new Partition(3));

            partitions = new List<TopicPartition>
            { 
                partition0,
                partition1,
                partition2,
                partition3
            };

            topicScaler = new KafkaTopicScalerForTest<string, byte[]>(
                TopicName,
                "consumer-group-test",
                "testfunction",
                consumer.Object, new AdminClientConfig(),
                1000L,
                NullLogger.Instance);

            topicScaler.WithPartitions(partitions);    
        }

        
        [Fact]
        public void ScaleMonitor_Id_ReturnsExpectedValue()
        {
            Assert.Equal("testfunction-kafkatrigger-topictest-consumer-group-test", topicScaler.Descriptor.Id);
        }

        [Fact]
        public async Task When_Offset_Is_Zero_Should_Return_No_Lag()
        {
            consumer.Setup(x => x.Committed(It.IsNotNull<IEnumerable<TopicPartition>>(), AnyTimeSpan))
                .Returns(new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset(partition0, ZeroOffset),
                    new TopicPartitionOffset(partition1, ZeroOffset),
                    new TopicPartitionOffset(partition2, ZeroOffset),
                    new TopicPartitionOffset(partition3, ZeroOffset),
                });

            consumer.Setup(x => x.QueryWatermarkOffsets(It.IsAny<TopicPartition>(), AnyTimeSpan))
                .Returns(new WatermarkOffsets(ZeroOffset, ZeroOffset));
            

            var metrics = await topicScaler.GetMetricsAsync();
            Assert.Equal(partitions.Count, metrics.PartitionCount);
            Assert.Equal(0, metrics.TotalLag);
        }


        [Fact]
        public async Task When_Committed_Is_Behind_Offset_Should_Return_Combined_Lag()
        {
            const long currentOffset = 100;
            const long largestLagOffset = currentOffset - 50;
            const long minimalLagOffset = currentOffset - 1;

            consumer.Setup(x => x.Committed(It.IsNotNull<IEnumerable<TopicPartition>>(), AnyTimeSpan))
                .Returns(new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset(partition0, minimalLagOffset),
                    new TopicPartitionOffset(partition1, currentOffset),
                    new TopicPartitionOffset(partition2, largestLagOffset),
                    new TopicPartitionOffset(partition3, currentOffset),
                });

            consumer.Setup(x => x.QueryWatermarkOffsets(It.IsAny<TopicPartition>(), AnyTimeSpan))
                .Returns(new WatermarkOffsets(currentOffset, currentOffset));

            var metrics = await topicScaler.GetMetricsAsync();
            Assert.Equal(partitions.Count, metrics.PartitionCount);
            var diff1 = currentOffset - largestLagOffset;
            var diff2 = currentOffset - minimalLagOffset;
            Assert.Equal(diff1 + diff2, metrics.TotalLag);
        }

        [Fact]
        public void When_No_Lag_Is_Found_Should_Vote_Scale_Down()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(0, partitions.Count),
                    new KafkaTriggerMetrics(0, partitions.Count),
                    new KafkaTriggerMetrics(0, partitions.Count),
                    new KafkaTriggerMetrics(0, partitions.Count),
                    new KafkaTriggerMetrics(0, partitions.Count),
                },
                WorkerCount = 1,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.ScaleIn, result.Vote);
        }


        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void When_Not_Enough_Metrics_Are_Available_Should_Vote_None(int metricCount)
        {
            var metrics = new List<KafkaTriggerMetrics>();
            for (int i = 0; i < metricCount; i++)
            {
                metrics.Add(new KafkaTriggerMetrics(i, partitions.Count));
            }

            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = metrics.ToArray(),
                WorkerCount = 1,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.None, result.Vote);
        }


        [Fact]
        public void When_WorkerCount_More_PartitionCount_Should_Vote_Scale_Down()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(5, partitions.Count),
                    new KafkaTriggerMetrics(6, partitions.Count),
                    new KafkaTriggerMetrics(76, partitions.Count),
                    new KafkaTriggerMetrics(22, partitions.Count),
                    new KafkaTriggerMetrics(11, partitions.Count),
                },
                WorkerCount = partitions.Count + 1,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.ScaleIn, result.Vote);
        }

        [Fact]
        public void When_LagIncreasing_Should_Vote_Scale_Up()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(10, partitions.Count),
                    new KafkaTriggerMetrics(212, partitions.Count),
                    new KafkaTriggerMetrics(32323, partitions.Count),
                    new KafkaTriggerMetrics(121222, partitions.Count),
                    new KafkaTriggerMetrics(123456, partitions.Count),
                },
                WorkerCount = 1,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.ScaleOut, result.Vote);
        }

        [Fact]
        public void When_LagIncreasing_But_Under_Threshold_Should_Vote_None()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(10, partitions.Count),
                    new KafkaTriggerMetrics(100, partitions.Count),
                    new KafkaTriggerMetrics(200, partitions.Count),
                    new KafkaTriggerMetrics(300, partitions.Count),
                    new KafkaTriggerMetrics(400, partitions.Count),
                },
                WorkerCount = 1,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.None, result.Vote);
        }

        [Fact]
        public void When_LagDecreasing_Slowly_Should_Vote_None()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(10_005, partitions.Count),
                    new KafkaTriggerMetrics(10_004, partitions.Count),
                    new KafkaTriggerMetrics(10_003, partitions.Count),
                    new KafkaTriggerMetrics(10_002, partitions.Count),
                    new KafkaTriggerMetrics(10_001, partitions.Count),
                },
                WorkerCount = partitions.Count,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.None, result.Vote);
        }

        [Fact]
        public void When_LagDecreasing_Slowly_At_Threshold_Should_Vote_None()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(4_000, partitions.Count),
                    new KafkaTriggerMetrics(3_999, partitions.Count),
                    new KafkaTriggerMetrics(3_998, partitions.Count),
                    new KafkaTriggerMetrics(3_997, partitions.Count),
                    new KafkaTriggerMetrics(3_996, partitions.Count),
                },
                WorkerCount = partitions.Count,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.None, result.Vote);
        }

        [Fact]
        public void When_LagDecreasing_Slowly_Under_Threshold_Should_Vote_ScaleIn()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(4_000, partitions.Count),
                    new KafkaTriggerMetrics(3_999, partitions.Count),
                    new KafkaTriggerMetrics(3_998, partitions.Count),
                    new KafkaTriggerMetrics(3_997, partitions.Count),
                    new KafkaTriggerMetrics(2_999, partitions.Count),
                },
                WorkerCount = partitions.Count,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.ScaleIn, result.Vote);
        }

        [Fact]
        public void When_LagIncreasing_Last_Lag_Less_LagThreshold_Should_Vote_Scale_Out()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(10, partitions.Count),
                    new KafkaTriggerMetrics(212, partitions.Count),
                    new KafkaTriggerMetrics(333, partitions.Count),
                    new KafkaTriggerMetrics(1122, partitions.Count),
                    new KafkaTriggerMetrics(1356, partitions.Count),
                },
                WorkerCount = 1,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.ScaleOut, result.Vote);
        }

        [Fact]
        public void When_LagIncreasing_Equal_PartionCount_Should_Vote_Scale_None()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(10, partitions.Count),
                    new KafkaTriggerMetrics(212, partitions.Count),
                    new KafkaTriggerMetrics(32323, partitions.Count),
                    new KafkaTriggerMetrics(121222, partitions.Count),
                    new KafkaTriggerMetrics(123456, partitions.Count),
                },
                WorkerCount = partitions.Count,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.None, result.Vote);
        }

        [Fact]
        public void When_LagDecreasing_Should_Vote_Scale_In()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(10, partitions.Count),
                    new KafkaTriggerMetrics(9, partitions.Count),
                    new KafkaTriggerMetrics(8, partitions.Count),
                    new KafkaTriggerMetrics(7, partitions.Count),
                    new KafkaTriggerMetrics(6, partitions.Count),
                },
                WorkerCount = partitions.Count,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.ScaleIn, result.Vote);
        }

        [Fact]
        public void When_Lag_Consistently_Below_Threshold_Should_Vote_Scale_In()
        {
            var context = new ScaleStatusContext<KafkaTriggerMetrics>()
            {
                Metrics = new KafkaTriggerMetrics[]
                {
                    new KafkaTriggerMetrics(2, partitions.Count),
                    new KafkaTriggerMetrics(2, partitions.Count),
                    new KafkaTriggerMetrics(1, partitions.Count),
                    new KafkaTriggerMetrics(2, partitions.Count),
                    new KafkaTriggerMetrics(1, partitions.Count),
                },
                WorkerCount = 2,
            };

            var result = topicScaler.GetScaleStatus(context);
            Assert.NotNull(result);
            Assert.Equal(ScaleVote.ScaleIn, result.Vote);
        }
    }
}

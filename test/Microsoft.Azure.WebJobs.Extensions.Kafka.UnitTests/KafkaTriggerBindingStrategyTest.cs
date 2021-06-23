// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka.UnitTests
{
    public class KafkaTriggerBindingStrategyTest
    {
        [Fact]
        public void GetStaticBindingContract_ReturnsExpectedValue()
        {
            var strategy = new KafkaTriggerBindingStrategy<string, string>();
            var contract = strategy.GetBindingContract();

            Assert.Equal(6, contract.Count);
            Assert.Equal(typeof(object), contract["Key"]);
            Assert.Equal(typeof(int), contract["Partition"]);
            Assert.Equal(typeof(string), contract["Topic"]);
            Assert.Equal(typeof(DateTime), contract["Timestamp"]);
            Assert.Equal(typeof(long), contract["Offset"]);
            Assert.Equal(typeof(string), contract["ConsumerGroup"]);
        }

        [Fact]
        public void GetBindingContract_SingleDispatch_ReturnsExpectedValue()
        {
            var strategy = new KafkaTriggerBindingStrategy<string, string>();
            var contract = strategy.GetBindingContract(true);

            Assert.Equal(6, contract.Count);
            Assert.Equal(typeof(object), contract["Key"]);
            Assert.Equal(typeof(int), contract["Partition"]);
            Assert.Equal(typeof(string), contract["Topic"]);
            Assert.Equal(typeof(DateTime), contract["Timestamp"]);
            Assert.Equal(typeof(long), contract["Offset"]);
            Assert.Equal(typeof(string), contract["ConsumerGroup"]);
        }

        [Fact]
        public void SingleDispatch_GetBindingData_Should_Create_Data_From_Kafka_Event()
        {
            var kafkaEventData = new KafkaEventData<string, string>()
            {
                Key = "1",
                Offset = 100,
                Partition = 2,
                Timestamp = new DateTime(2019, 1, 10, 9, 21, 0, DateTimeKind.Utc),
                Topic = "myTopic",
                Value = "Nothing",
                ConsumerGroup = "myConsumerGroup"
            };

            var strategy = new KafkaTriggerBindingStrategy<string, string>();
            var binding = strategy.GetBindingData(KafkaTriggerInput.New(kafkaEventData));
            Assert.Equal("1", binding["Key"]);
            Assert.Equal(100L, binding["Offset"]);
            Assert.Equal(2, binding["Partition"]);
            Assert.Equal(new DateTime(2019, 1, 10, 9, 21, 0, DateTimeKind.Utc), binding["Timestamp"]);
            Assert.Equal("myTopic", binding["Topic"]);
            Assert.Equal("myConsumerGroup", binding["ConsumerGroup"]);

            // lower case too
            Assert.Equal("1", binding["key"]);
            Assert.Equal(100L, binding["offset"]);
            Assert.Equal(2, binding["partition"]);
            Assert.Equal(new DateTime(2019, 1, 10, 9, 21, 0, DateTimeKind.Utc), binding["timestamp"]);
            Assert.Equal("myTopic", binding["topic"]);
            Assert.Equal("myConsumerGroup", binding["consumergroup"]);
        }

        [Fact]
        public void MultiDispatch_GetBindingData_Should_Create_Data_From_Kafka_Event()
        {
            var triggerInput = KafkaTriggerInput.New(new[]
            {
                new KafkaEventData<string, string>()
                {
                    Key = "1",
                    Offset = 100,
                    Partition = 2,
                    Timestamp = new DateTime(2019, 1, 10, 9, 21, 0, DateTimeKind.Utc),
                    Topic = "myTopic",
                    Value = "Nothing1",
                    ConsumerGroup = "myConsumerGroup1"
                },
                new KafkaEventData<string, string>()
                {
                    Key = "2",
                    Offset = 101,
                    Partition = 2,
                    Timestamp = new DateTime(2019, 1, 10, 9, 21, 1, DateTimeKind.Utc),
                    Topic = "myTopic",
                    Value = "Nothing2",
                    ConsumerGroup = "myConsumerGroup2"
                },
            });

            var strategy = new KafkaTriggerBindingStrategy<string, string>();
            var binding = strategy.GetBindingData(triggerInput);
            Assert.Equal(new[] { "1", "2" }, binding["KeyArray"]);
            Assert.Equal(new[] { 100L, 101L }, binding["OffsetArray"]);
            Assert.Equal(new[] { 2, 2 }, binding["PartitionArray"]);
            Assert.Equal(new[] { new DateTime(2019, 1, 10, 9, 21, 0, DateTimeKind.Utc), new DateTime(2019, 1, 10, 9, 21, 1, DateTimeKind.Utc) }, binding["TimestampArray"]);
            Assert.Equal(new[] { "myTopic", "myTopic" }, binding["TopicArray"]);
            Assert.Equal(new[] { "myConsumerGroup1", "myConsumerGroup2" }, binding["ConsumerGroupArray"]);

            // lower case too
            Assert.Equal(new[] { "1", "2" }, binding["keyArray"]);
            Assert.Equal(new[] { 100L, 101L }, binding["offsetArray"]);
            Assert.Equal(new[] { 2, 2 }, binding["partitionArray"]);
            Assert.Equal(new[] { new DateTime(2019, 1, 10, 9, 21, 0, DateTimeKind.Utc), new DateTime(2019, 1, 10, 9, 21, 1, DateTimeKind.Utc) }, binding["timestampArray"]);
            Assert.Equal(new[] { "myTopic", "myTopic" }, binding["topicArray"]);
            Assert.Equal(new[] { "myConsumerGroup1", "myConsumerGroup2" }, binding["consumerGroupArray"]);
        }
    }
}

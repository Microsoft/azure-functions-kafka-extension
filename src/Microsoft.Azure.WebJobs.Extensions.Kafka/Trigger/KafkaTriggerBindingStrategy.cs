// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Triggers;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka
{
    public class KafkaTriggerBindingStrategy<TKey, TValue> : ITriggerBindingStrategy<IKafkaEventData, KafkaTriggerInput>
    {
        /// <summary>
        /// Given a raw string, convert to a TTriggerValue.
        /// This is primarily used in the "invoke from dashboard" path. 
        /// </summary>
        public KafkaTriggerInput ConvertFromString(string input)
        {
            // Need to dig up to see how "invoke from dashboard" works.
            // Returning null for now, since it is not being called
            return null;
        }

        // Single instance: Core --> EventData
        public IKafkaEventData BindSingle(KafkaTriggerInput value, ValueBindingContext context)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }
            return value.GetSingleEventData();
        }

        public IKafkaEventData[] BindMultiple(KafkaTriggerInput value, ValueBindingContext context)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }
            return value.Events;
        }

        public Dictionary<string, Type> GetBindingContract(bool isSingleDispatch = true)
        {
            var contract = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
            AddBindingContractMember(contract, nameof(KafkaEventData<TKey, TValue>.Key), typeof(object), isSingleDispatch);
            AddBindingContractMember(contract, nameof(KafkaEventData<TKey, TValue>.Partition), typeof(int), isSingleDispatch);
            AddBindingContractMember(contract, nameof(KafkaEventData<TKey, TValue>.Topic), typeof(string), isSingleDispatch);
            AddBindingContractMember(contract, nameof(KafkaEventData<TKey, TValue>.Timestamp), typeof(DateTime), isSingleDispatch);
            AddBindingContractMember(contract, nameof(KafkaEventData<TKey, TValue>.Offset), typeof(long), isSingleDispatch);
            AddBindingContractMember(contract, nameof(KafkaEventData<TKey, TValue>.ConsumerGroup), typeof(string), isSingleDispatch);
            return contract;
        }

        private static void AddBindingContractMember(Dictionary<string, Type> contract, string name, Type type, bool isSingleDispatch)
        {
            if (!isSingleDispatch)
            {
                name += "Array";
            }
            contract.Add(name, isSingleDispatch ? type : type.MakeArrayType());
        }

        public Dictionary<string, object> GetBindingData(KafkaTriggerInput value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var bindingData = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            if (value.IsSingleDispatch)
            {
                AddBindingData(bindingData, value.GetSingleEventData());
            }
            else
            {
                AddBindingData(bindingData, value.Events);
            }

            return bindingData;
        }

        internal static void AddBindingData(Dictionary<string, object> bindingData, IKafkaEventData[] events)
        {
            int length = events.Length;
            var partitions = new int[length];
            var offsets = new long[length];
            var timestamps = new DateTime[length];
            var topics = new string[length];
            var keys = new object[length];
            var consumerGroups = new string[length];

            bindingData.Add("PartitionArray", partitions);
            bindingData.Add("OffsetArray", offsets);
            bindingData.Add("TimestampArray", timestamps);
            bindingData.Add("TopicArray", topics);
            bindingData.Add("KeyArray", keys);
            bindingData.Add("ConsumerGroupArray", consumerGroups);

            for (int i = 0; i < events.Length; i++)
            {
                partitions[i] = events[i].Partition;
                offsets[i] = events[i].Offset;
                timestamps[i] = events[i].Timestamp;
                keys[i] = events[i].Key;
                topics[i] = events[i].Topic;
                consumerGroups[i] = events[i].ConsumerGroup;
            }
        }

        private static void AddBindingData(Dictionary<string, object> bindingData, IKafkaEventData eventData)
        {
            bindingData.Add(nameof(IKafkaEventData.Key), eventData.Key);
            bindingData.Add(nameof(IKafkaEventData.Partition), eventData.Partition);
            bindingData.Add(nameof(IKafkaEventData.Topic), eventData.Topic);
            bindingData.Add(nameof(IKafkaEventData.Timestamp), eventData.Timestamp);
            bindingData.Add(nameof(IKafkaEventData.Offset), eventData.Offset);
            bindingData.Add(nameof(IKafkaEventData.ConsumerGroup), eventData.ConsumerGroup);
        }
    }
}
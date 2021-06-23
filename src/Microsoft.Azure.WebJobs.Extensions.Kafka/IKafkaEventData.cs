// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka
{
    public interface IKafkaEventData
    {
        object Value { get; }
        object Key { get; }
        long Offset { get; }
        int Partition { get; }
        string Topic { get; }
        DateTime Timestamp { get; }
        IKafkaEventDataHeaders Headers { get; }
        string ConsumerGroup { get; }    
    }
}
// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Avro.Specific;
using Confluent.Kafka;
using Microsoft.Azure.WebJobs.Description;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka
{

    internal interface IProducerOptions
    {
        BrokerAuthenticationMode? AuthenticationMode { get; }
        int? BatchSize { get; }
        int? CompressionLevel { get; }
        MessageCompressionType? CompressionType { get; }
        bool? EnableIdempotence { get; }
        int? MessageTimeoutMs { get; }
        int? MaxMessageBytes { get; }
        int? MaxRetries { get; }
        int? MetadataMaxAgeMs { get; }
        string Password { get; }
        BrokerProtocol? Protocol { get; }
        int? RequestTimeoutMs { get; }
        bool? SocketKeepaliveEnable { get; }
        string SslCaLocation { get; }
        string SslCertificateLocation { get; }
        string SslKeyLocation { get; }
        string SslKeyPassword { get; }
        string Username { get; }
    }
}
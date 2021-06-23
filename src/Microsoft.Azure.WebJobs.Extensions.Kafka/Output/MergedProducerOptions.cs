// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka
{
    internal class MergedProducerOptions : IProducerOptions
    {
        private readonly IProducerOptions[] providers;

        public MergedProducerOptions(params IProducerOptions[] providers)
        {
            this.providers = providers ?? throw new ArgumentNullException(nameof(providers));
        }

        private TValue GetOrDefault<TValue>(Func<IProducerOptions, TValue> valueGetter, TValue defaultValue)
        {
            foreach (var provider in providers)
            {
                if (provider != null)
                {
                    var value = valueGetter(provider);
                    if (value != null)
                    {
                        return value;
                    }
                }
            }
            return defaultValue;
        }


        public BrokerAuthenticationMode? AuthenticationMode => GetOrDefault(x => x.AuthenticationMode, BrokerAuthenticationMode.NotSet);

        public int? BatchSize => GetOrDefault(x => x.BatchSize, 10_000);


        public int? CompressionLevel => GetOrDefault(x => x.BatchSize, -1);

        public MessageCompressionType? CompressionType => GetOrDefault(x => x.CompressionType, MessageCompressionType.NotSet);

        public bool? EnableIdempotence => GetOrDefault(x => x.EnableIdempotence, false);

        public int? MessageTimeoutMs => GetOrDefault(x => x.MaxMessageBytes, 300_000);

        public int? MaxMessageBytes => GetOrDefault(x => x.MaxMessageBytes, 1_000_000);

        public int? MaxRetries => GetOrDefault(x => x.MaxMessageBytes, 2);

        public int? MetadataMaxAgeMs => GetOrDefault(x => x.MaxMessageBytes, 180_000);

        public string Password => GetOrDefault(x => x.Password, null);

        public BrokerProtocol? Protocol => GetOrDefault(x => x.Protocol, BrokerProtocol.NotSet);

        public int? RequestTimeoutMs => GetOrDefault(x => x.RequestTimeoutMs, 5000);

        public bool? SocketKeepaliveEnable => GetOrDefault(x => x.SocketKeepaliveEnable, true);

        public string SslCaLocation => GetOrDefault(x => x.SslCaLocation, null);

        public string SslCertificateLocation => GetOrDefault(x => x.SslCertificateLocation, null);

        public string SslKeyLocation => GetOrDefault(x => x.SslKeyLocation, null);

        public string SslKeyPassword => GetOrDefault(x => x.SslKeyPassword, null);

        public string Username => GetOrDefault(x => x.Username, null);
    }
}
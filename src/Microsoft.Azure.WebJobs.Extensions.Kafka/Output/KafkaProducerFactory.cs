// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using Avro.Generic;
using Avro.Specific;
using Confluent.Kafka;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Extensions.Kafka
{
    /// <summary>
    /// Factory for <see cref="IKafkaProducer"/>
    /// Those matching the broker, key type and value type are shared
    /// </summary>
    public class KafkaProducerFactory : IKafkaProducerFactory
    {
        private readonly IConfiguration config;
        private readonly INameResolver nameResolver;
        private readonly ILoggerProvider loggerProvider;
        private readonly ConcurrentDictionary<string, IProducer<byte[], byte[]>> baseProducers = new ConcurrentDictionary<string, IProducer<byte[], byte[]>>();

        public KafkaProducerFactory(
            IConfiguration config,
            INameResolver nameResolver,
            ILoggerProvider loggerProvider)
        {
            this.config = config;
            this.nameResolver = nameResolver;
            this.loggerProvider = loggerProvider;
        }

        public IKafkaProducer Create(KafkaProducerEntity entity)
        {
            AzureFunctionsFileHelper.InitializeLibrdKafka(this.loggerProvider.CreateLogger(LogCategories.CreateTriggerCategory("Kafka")));

            // Goal is to create as less producers as possible
            // We can group producers based on following criterias
            // - Broker List
            // - Configuration
            var producerConfig = this.GetProducerConfig(entity);
            var producerKey = CreateKeyForConfig(producerConfig);

            var baseProducer = baseProducers.GetOrAdd(producerKey, (k) => CreateBaseProducer(producerConfig));
            return Create(baseProducer.Handle, entity);
        }

        /// <summary>
        /// Creates a config key by concatenating all property key and values
        /// </summary>
        private string CreateKeyForConfig(ProducerConfig producerConfig)
        {
            var keyBuilder = new StringBuilder();

            foreach (var kv in producerConfig)
            {
                keyBuilder
                    .Append(kv.Key)
                    .Append('=')
                    .Append(kv.Value)
                    .Append(';');
            }

            return keyBuilder.ToString();
        }

        private IProducer<byte[], byte[]> CreateBaseProducer(ProducerConfig producerConfig)
        {
            var builder = new ProducerBuilder<byte[], byte[]>(producerConfig);
            ILogger logger = this.loggerProvider.CreateLogger("Kafka");
            builder.SetLogHandler((_, m) =>
            {
                logger.Log((LogLevel)m.LevelAs(LogLevelType.MicrosoftExtensionsLogging), $"Libkafka: {m?.Message}");
            });

            return builder.Build();
        }

        private IKafkaProducer Create(Handle producerBaseHandle, KafkaProducerEntity entity)
        {
            var valueType = entity.ValueType ?? typeof(byte[]);
            var keyType = entity.KeyType ?? typeof(Null);

            var valueSerializer = SerializationHelper.ResolveValueSerializer(valueType, entity.AvroSchema);

            return (IKafkaProducer)Activator.CreateInstance(
                typeof(KafkaProducer<,>).MakeGenericType(keyType, valueType),
                producerBaseHandle,
                valueSerializer,
                loggerProvider.CreateLogger(LogCategories.CreateTriggerCategory("Kafka")));
        }

        public ProducerConfig GetProducerConfig(KafkaProducerEntity entity)
        {
            if (!AzureFunctionsFileHelper.TryGetValidFilePath(entity.Attribute.SslCertificateLocation, out var resolvedSslCertificationLocation))
            {
                resolvedSslCertificationLocation = entity.Attribute.SslCertificateLocation;
            }

            if (!AzureFunctionsFileHelper.TryGetValidFilePath(entity.Attribute.SslCaLocation, out var resolvedSslCaLocation))
            {
                resolvedSslCaLocation = entity.Attribute.SslCaLocation;
            }

            if (!AzureFunctionsFileHelper.TryGetValidFilePath(entity.Attribute.SslKeyLocation, out var resolvedSslKeyLocation))
            {
                resolvedSslKeyLocation = entity.Attribute.SslKeyLocation;
            }
            var kafkaOptions = this.config.Get<KafkaOptions>();
            var conf = new ProducerConfig()
            {
                BootstrapServers = this.config.ResolveSecureSetting(nameResolver, entity.Attribute.BrokerList),
                BatchNumMessages = entity.Attribute.BatchSize,
                EnableDeliveryReports = entity.Attribute.EnableDeliveryReports,
                EnableIdempotence = entity.Attribute.EnableIdempotence,
                MessageMaxBytes = entity.Attribute.MaxMessageBytes,
                MessageSendMaxRetries = entity.Attribute.MaxRetries,
                MessageTimeoutMs = entity.Attribute.MessageTimeoutMs,
                RequestTimeoutMs = entity.Attribute.RequestTimeoutMs,
                SaslPassword = this.config.ResolveSecureSetting(nameResolver, entity.Attribute.Password),
                SaslUsername = this.config.ResolveSecureSetting(nameResolver, entity.Attribute.Username),
                SslKeyLocation = resolvedSslKeyLocation,
                SslKeyPassword = entity.Attribute.SslKeyPassword,
                SslCertificateLocation = resolvedSslCertificationLocation,
                SslCaLocation = resolvedSslCaLocation,
                Debug = kafkaOptions?.LibkafkaDebug,
                MetadataMaxAgeMs = kafkaOptions?.MetadataMaxAgeMs,
                SocketKeepaliveEnable = kafkaOptions?.SocketKeepaliveEnable
            };

            if (entity.Attribute.AuthenticationMode != BrokerAuthenticationMode.NotSet)
            {
                conf.SaslMechanism = (SaslMechanism)entity.Attribute.AuthenticationMode;
            }

            if (entity.Attribute.Protocol != BrokerProtocol.NotSet)
            {
                conf.SecurityProtocol = (SecurityProtocol)entity.Attribute.Protocol;
            }

            return conf;
        }
    }
}
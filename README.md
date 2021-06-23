Azure Functions extensions for Apache Kafka
===
|Branch|Status|
|---|---|
|master|[![Build Status](https://azfunc.visualstudio.com/Azure%20Functions/_apis/build/status/azure-functions-kafka-extension-ci?branchName=master)](https://azfunc.visualstudio.com/Azure%20Functions/_build/latest?definitionId=7&branchName=master)
|dev|[![Build Status](https://azfunc.visualstudio.com/Azure%20Functions/_apis/build/status/azure-functions-kafka-extension-ci?branchName=dev)](https://azfunc.visualstudio.com/Azure%20Functions/_build/latest?definitionId=7&branchName=dev)

This repository contains Kafka binding extensions for the **Azure WebJobs SDK**. The communication with Kafka is based on library **Confluent.Kafka**.

Please find samples [here](https://github.com/Azure/azure-functions-kafka-extension/tree/master/samples)

**DISCLAIMER**: This library is supported in the Premium Plan along with support for scaling as Go-Live - supported in Production with a SLA. It is also fully supported when using Azure Functions on Kubernetes where scaling will be handed by KEDA - scaling based on Kafka queue length. It is currently not supported on the Consumption plan (there will be no scale from zero) - this is something the Azure Functions team is still working on.

## Quick Start

This library provides Quick Start for each language. General information of the samples, refer to: 

* [Samples Overview Documentation](samples/README.md)

| Language | Description | Link | DevContainer |
| -------- | ----------- | ---- | ------------ |
| C# | C# precompiled sample with Visual Studio | [Readme](samples/dotnet/README.md)| No |
| Java | Java 8 sample | [Readme](samples/java/README.md) | Yes |
| JavaScript | Node 12 sample | [Readme](samples/javascript/README.md)| Yes |
| PowerShell | PowerShell 6 Sample | [Readme](samples/powershell/README.md)| No |
| Python | Python 3.8 sample | [Readme](samples/python/README.md)| Yes |
| TypeScript | TypeScript sample (Node 12) | [Readme](samples/typescript/kafka-trigger/README.md)| Yes |

The following direction is for C#. However, other languages work with C# extension. You can refer to the configuration parameters. 

## Bindings

There are two binding types in this repo: trigger and output. To get started using the extension in a WebJob project add reference to Microsoft.Azure.WebJobs.Extensions.Kafka project and call `AddKafka()` on the startup:

```csharp
static async Task Main(string[] args)
{
  var builder = new HostBuilder()
        .UseEnvironment("Development")
        .ConfigureWebJobs(b =>
        {
            b.AddKafka();
        })
        .ConfigureAppConfiguration(b =>
        {
        })
        .ConfigureLogging((context, b) =>
        {
            b.SetMinimumLevel(LogLevel.Debug);
            b.AddConsole();
        })
        .ConfigureServices(services =>
        {
            services.AddSingleton<Functions>();
        })
        .UseConsoleLifetime();

    var host = builder.Build();
    using (host)
    {
        await host.RunAsync();
    }
}

public class Functions
{
    const string Broker = "localhost:9092";
    const string StringTopicWithOnePartition = "stringTopicOnePartition";
    const string StringTopicWithTenPartitions = "stringTopicTenPartitions";

    /// <summary>
    /// Trigger for the topic
    /// </summary>
    public void MultiItemTriggerTenPartitions(
        [KafkaTrigger(Broker, StringTopicWithTenPartitions, ConsumerGroup = "myConsumerGroup")] KafkaEventData<string> events,
        ILogger log)
    {
        foreach (var kafkaEvent in events)
        {
            log.LogInformation(kafkaEvent.Value);
        }
    }
}
```

### Trigger Binding

Trigger bindings are designed to consume messages from a Kafka topics.

```csharp
public static void StringTopic(
    [KafkaTrigger("BrokerList", "myTopic", ConsumerGroup = "myGroupId")] KafkaEventData<string>[] kafkaEvents,
    ILogger logger)
{
    foreach (var kafkaEvent in kafkaEvents)
        logger.LogInformation(kafkaEvent.Value);
}
```

Kafka messages can be serialized in multiple formats. Currently the following formats are supported: string, Avro and Protobuf.

#### Avro Binding Support

The Kafka trigger supports two methods for consuming Avro format:

- Specific: where the concrete user defined class will be instantiated and filled during message deserialization
- Generic: where the user provides the avro schema and a generic record is created during message deserialization

*Using Avro specific*

1. Define a class that inherits from `ISpecificRecord`.
1. The parameter where `KafkaTrigger` is added should have a value type of the class defined in previous step: `KafkaEventData<MySpecificRecord>`

```csharp
public class UserRecord : ISpecificRecord
{
    public const string SchemaText = @"    {
  ""type"": ""record"",
  ""name"": ""UserRecord"",
  ""namespace"": ""KafkaFunctionSample"",
  ""fields"": [
    {
      ""name"": ""registertime"",
      ""type"": ""long""
    },
    {
      ""name"": ""userid"",
      ""type"": ""string""
    },
    {
      ""name"": ""regionid"",
      ""type"": ""string""
    },
    {
      ""name"": ""gender"",
      ""type"": ""string""
    }
  ]
}";
    public static Schema _SCHEMA = Schema.Parse(SchemaText);

    [JsonIgnore]
    public virtual Schema Schema => _SCHEMA;
    public long RegisterTime { get; set; }
    public string UserID { get; set; }
    public string RegionID { get; set; }
    public string Gender { get; set; }

    public virtual object Get(int fieldPos)
    {
        switch (fieldPos)
        {
            case 0: return this.RegisterTime;
            case 1: return this.UserID;
            case 2: return this.RegionID;
            case 3: return this.Gender;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
        };
    }
    public virtual void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: this.RegisterTime = (long)fieldValue; break;
            case 1: this.UserID = (string)fieldValue; break;
            case 2: this.RegionID = (string)fieldValue; break;
            case 3: this.Gender = (string)fieldValue; break;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        };
    }
}


public static void User(
    [KafkaTrigger("BrokerList", "users", ConsumerGroup = "myGroupId")] KafkaEventData<UserRecord>[] kafkaEvents,
    ILogger logger)
{
    foreach (var kafkaEvent in kafkaEvents)
    {
        var user = kafkaEvent.Value;
        logger.LogInformation($"{JsonConvert.SerializeObject(kafkaEvent.Value)}");
    }
}
```

*Using Avro Generic*

1. In `KafkaTrigger` attribute set the value of `AvroSchema` to the string representation of it.
1. The parameter type used with the trigger must be of type `KafkaEventData<GenericRecord>`.

The sample function contains 1 consumer using avro generic. Check the class `AvroGenericTriggers`

```csharp
public static class AvroGenericTriggers
{
      const string PageViewsSchema = @"{
  ""type"": ""record"",
  ""name"": ""pageviews"",
  ""namespace"": ""ksql"",
  ""fields"": [
    {
      ""name"": ""viewtime"",
      ""type"": ""long""
    },
    {
      ""name"": ""userid"",
      ""type"": ""string""
    },
    {
      ""name"": ""pageid"",
      ""type"": ""string""
    }
  ]
}";

[FunctionName(nameof(PageViews))]
public static void PageViews(
    [KafkaTrigger("BrokerList", "pageviews", AvroSchema = PageViewsSchema, ConsumerGroup = "myGroupId")] KafkaEventData<GenericRecord> kafkaEvent,
    ILogger logger)
{
    if (kafkaEvent.Value != null)
    {
        // Get the field values manually from genericRecord (kafkaEvent.Value)
    }
}
```

#### Protobuf Binding Support

Protobuf is supported in the trigger based on the `Google.Protobuf` nuget package. To consume a topic that is using protobuf as serialization set the TValue generic argument to be of a type that implements `Google.Protobuf.IMessage`. The sample producer has a producer for topic `protoUser` (must be created). The sample function has a trigger handler for this topic in class `ProtobufTriggers`.

```csharp
public static class ProtobufTriggers
{
    [FunctionName(nameof(ProtobufUser))]
    public static void ProtobufUser(
        [KafkaTrigger("BrokerList", "protoUser", ConsumerGroup = "myGroupId")] KafkaEventData<ProtoUser>[] kafkaEvents,
        ILogger logger)
    {
        foreach (var kafkaEvent in kafkaEvents)
        {
            var user = kafkaEvent.Value;
            logger.LogInformation($"{JsonConvert.SerializeObject(user)}");
        }
    }
}
```

### Output Binding

Output binding are designed to produce messages to a Kafka topic. It supports different keys and values types. Avro and Protobuf serialisation are built-in.

```csharp
[FunctionName("ProduceStringTopic")]
public static async Task<IActionResult> Run(
    [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
    [Kafka("stringTopicTenPartitions", BrokerList = "LocalBroker")] IAsyncCollector<KafkaEventData<string>> events,
    ILogger log)
{
    var kafkaEvent = new KafkaEventData<string>()
    {
        Value = await new StreamReader(req.Body).ReadToEndAsync(),
    };

    await events.AddAsync(kafkaEvent);

    return new OkResult();
}
```

To set a key value use `KafkaEventData<string, string>` to define a key of type string (supported key types: int, long, string, byte[]).

To produce messages using Protobuf serialisation use a `KafkaEventData<MyProtobufClass>` as message type. `MyProtobufClass` must implements the IMessage interface.

For Avro provide a type that implements ISpecificRecord.
If nothing is defined the value will be of type `byte[]` and no key will be set.

## Configuration

Customization of the Kafka extensions is available in the host file. As mentioned before, the interface to Kafka is built based on **Confluent.Kafka** library, therefore some of the configuration is just a bridge to the producer/consumer.

```json
{
  "version": "2.0",
  "extensions": {
    "kafka": {
      "maxBatchSize": 100
    }
  }
}
```

### Configuration Settings

Confluent.Kafka is based on librdkafka C library. Some of the configuration required by the library is exposed by the extension in this repository. The complete configuration for librdkafka can be found [here](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

#### Extension configuration

|Setting|Description|Default Value
|-|-|-|
|MaxBatchSize|Maximum batch size when calling a Kafka trigger function|64
|SubscriberIntervalInSeconds|Defines the minimum frequency in which messages will be executed by function. Only if the message volume is less than MaxBatchSize / SubscriberIntervalInSeconds|1
|ExecutorChannelCapacity|Defines the channel capacity in which messages will be sent to functions. Once the capacity is reached the Kafka subscriber will pause until the function catches up|1
|ChannelFullRetryIntervalInMs|Defines the interval in milliseconds in which the subscriber should retry adding items to channel once it reaches the capacity|50

#### librdkafka configuration

The settings exposed here are targeted to more advanced users that want to customize how librdkafka works. Please check the librdkafka [documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for more information.

|Setting|librdkafka property|Trigger or Output|
|-|-|-|
|ReconnectBackoffMs|reconnect.backoff.max.ms|Trigger
|ReconnectBackoffMaxMs|reconnect.backoff.max.ms|Trigger
|StatisticsIntervalMs|statistics.interval.ms|Trigger
|SessionTimeoutMs|session.timeout.ms|Trigger
|MaxPollIntervalMs|max.poll.interval.ms|Trigger
|QueuedMinMessages|queued.min.messages|Trigger
|QueuedMaxMessagesKbytes|queued.max.messages.kbytes|Trigger
|MaxPartitionFetchBytes|max.partition.fetch.bytes|Trigger
|FetchMaxBytes|fetch.max.bytes|Trigger
|AutoCommitIntervalMs|auto.commit.interval.ms|Trigger
|LibkafkaDebug|debug|Both
|MetadataMaxAgeMs|metadata.max.age.ms|Both
|SocketKeepaliveEnable|socket.keepalive.enable|Both
|CompressionType|compression.codec|Output
|CompressionLevel|compression.level|Output

**NOTE:** `MetadataMaxAgeMs` default is `180000` `SocketKeepaliveEnable` default is `true` otherwise, the default value is the same as the [Configuration properties](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). The reason of the default settings, refer to this [issue](https://github.com/Azure/azure-functions-kafka-extension/issues/187).

If you are missing an configuration setting please create an issue and describe why you need it.

## Connecting to a secure Kafka broker

Both, trigger and output, can connect to a secure Kafka broker. The following attribute properties are available to establish a secure connection:

|Setting|librdkafka property|Description|
|-|-|-|
|AuthenticationMode|sasl.mechanism|SASL mechanism to use for authentication|
|Username|sasl.username|SASL username for use with the PLAIN and SASL-SCRAM|
|Password|sasl.password|SASL password for use with the PLAIN and SASL-SCRAM|
|Protocol|security.protocol|Security protocol used to communicate with brokers|
|SslKeyLocation|ssl.key.location|Path to client's private key (PEM) used for authentication|
|SslKeyPassword|ssl.key.password|Password for client's certificate|
|SslCertificateLocation|ssl.certificate.location|Path to client's certificate|
|SslCaLocation|ssl.ca.location|Path to CA certificate file for verifying the broker's certificate|

Username and password should reference a Azure function configuration variable and not be hardcoded.

## Language support configuration

For the non-C# languages, you can specify `cardinality` for choosing if the KafkaTrigger is executed in batch. 
|Setting|Description|Option|
|-|-|-|
|cardinality|Set to many in order to enable batching. If omitted or set to one, a single message is passed to the function. For Java functions, if you set "MANY", you need to set a `dataType`. |"ONE", "MANY"|
|dataType|For java functions, the type of the deserialize a kafka event. It requires when you use cardinality = "MANY" |"string", "binary"|

## Linux Premium plan configuration
Currently when running a function in a Linux Premium plan environment there will be an error indicating that we could not load the librdkafka library. To address the problem, at least for now, please add the setting below. It will include the extension location as one of the paths where libraries are searched. We are working on avoiding this setting in future releases.

|Setting|Value|Description|
|-|-|-|
|LD_LIBRARY_PATH|/home/site/wwwroot/bin/runtimes/linux-x64/native|Librakafka library path|

## .NET Quickstart

For samples take a look at the [samples folder](./samples).

## Connecting to Confluent Cloud in Azure

Connecting to a managed Kafka cluster as the one provided by [Confluent in Azure](https://www.confluent.io/azure/) requires a few additional steps:

1. In the function trigger ensure that Protocol, AuthenticationMode, Username, Password and SslCaLocation are set.

```c#
public static class ConfluentCloudTrigger
{
    [FunctionName(nameof(ConfluentCloudStringTrigger))]
    public static void ConfluentCloudStringTrigger(
        [KafkaTrigger("BootstrapServer", "my-topic",
            ConsumerGroup = "azfunc",
            Protocol = BrokerProtocol.SaslSsl,
            AuthenticationMode = BrokerAuthenticationMode.Plain,
            Username = "ConfluentCloudUsername",
            Password = "ConfluentCloudPassword")]
        KafkaEventData<string> kafkaEvent,
        ILogger logger)
    {
        logger.LogInformation(kafkaEvent.Value.ToString());
    }
}
```

2. In the Function App application settings (or local.settings.json during development), set the authentication credentials for your Confluent Cloud environment<br>
**BootstrapServer**: should contain the value of Bootstrap server found in Confluent Cloud settings page. Will be something like "xyz-xyzxzy.westeurope.azure.confluent.cloud:9092".<br>
**ConfluentCloudUsername**: is you API access key, obtained from the Confluent Cloud web 
site.
**ConfluentCloudPassword**: is you API secret, obtained from the Confluent Cloud web site.

## Testing

This repo includes unit and end to end tests. End to end tests require a Kafka instance. A quick way to provide one is to use the Kafka quick start example mentioned previously or use a simpler single node docker-compose solution (also based on Confluent Docker images):

Getting simple single node Kafka running:

```bash
docker-compose -f ./test/Microsoft.Azure.WebJobs.Extensions.Kafka.EndToEndTests/kafka-singlenode-compose.yaml up -d
```

To shutdown the single node Kafka:

```bash
docker-compose -f ./test/Microsoft.Azure.WebJobs.Extensions.Kafka.EndToEndTests/kafka-singlenode-compose.yaml down
```

By default end to end tests will try to connect to Kafka on `localhost:9092`. If your Kafka broker is located in a different location create a `local.appsettings.tests.json` file in folder `./test/Microsoft.Azure.WebJobs.Extensions.Kafka.EndToEndTests/` overwritting the value of LocalBroker setting like the example below:

```json
{
    "LocalBroker": "location-of-your-kafka-broker:9092"
}
```

# slocalce-commons

A reusable Solace listener library built on **Spring Cloud Stream**, providing:

- ✅ Config-driven topic subscriptions
- ✅ Topic-aware message parsing pipeline
- ✅ Pluggable enricher chain
- ✅ Pluggable sinks: downstream publisher (StreamBridge) + database persistence hook

---

## Quick Start

### 1. Add dependency

```xml
<dependency>
  <groupId>com.numaansystems.solace</groupId>
  <artifactId>slocalce-commons</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Also add the Solace Spring Cloud Stream binder:

```xml
<dependency>
  <groupId>com.solace.spring.cloud</groupId>
  <artifactId>spring-cloud-starter-stream-solace</artifactId>
  <version>4.2.0</version>
</dependency>
```

### 2. Configure `application.yml`

See [docs/sample-application.yml](docs/sample-application.yml) for a complete example.

```yaml
solace:
  commons:
    enabled: true
    topics:
      "orders/created":
        parser: orderMessageParser         # Spring bean name
        enrichers:
          - customerEnricher
          - inventoryEnricher
        sink-publish-enabled: true
        sink-publish-target: processedOrders-out-0
        sink-database-enabled: true
      "payments/processed":
        parser: paymentMessageParser
        enrichers:
          - fraudEnricher
    sinks:
      publish:
        enabled: false                     # global default; overridden per-topic above
      database:
        enabled: false

spring:
  cloud:
    stream:
      function:
        definition: solaceMessageConsumer
      bindings:
        solaceMessageConsumer-in-0:
          destination: "orders/created,payments/processed"
          group: my-service
      solace:
        bindings:
          solaceMessageConsumer-in-0:
            consumer:
              queueAdditionalSubscriptions:
                - "orders/created"
                - "payments/processed"
```

### 3. Implement parsers

```java
@Component("orderMessageParser")
public class OrderMessageParser implements MessageParser<OrderEvent> {
    private final ObjectMapper mapper;
    
    @Override
    public OrderEvent parse(byte[] payload, MessageHeaders headers) throws Exception {
        return mapper.readValue(payload, OrderEvent.class);
    }
}
```

### 4. Implement enrichers

```java
@Component("customerEnricher")
public class CustomerEnricher implements MessageEnricher<OrderEvent> {
    @Override
    public ProcessedMessage<OrderEvent> enrich(ProcessedMessage<OrderEvent> message) {
        // Call customer service, add metadata, etc.
        return message.withMetadata("customerId", "C-123");
    }
}
```

### 5. Implement a database persistence hook (optional)

```java
@Component
@ConditionalOnProperty("solace.commons.sinks.database.enabled", havingValue = "true")
public class OrderRepository implements DatabasePersistenceHook<OrderEvent> {
    @Override
    public void save(ProcessedMessage<OrderEvent> message) {
        // Save to database
    }
}
```

---

## Architecture

```
Solace Topic (Spring Cloud Stream binding)
          │
          ▼
  solaceMessageConsumer        ← Consumer<Message<byte[]>> auto-configured
          │
          ▼
    TopicExtractor              ← extract topic name from headers
          │
          ▼
    MessagePipeline<T>          ← built by PipelineFactory from config
      │    │    │
      │    │    └── MessageSink(s)    → DownstreamPublishSink (StreamBridge)
      │    │                          → DatabaseSink (DatabasePersistenceHook)
      │    │                          → custom MessageSink beans
      │    └── EnricherChain<T>       ← ordered MessageEnricher beans
      └── MessageParser<T>            ← resolves parser by topic name
```

## Configuration Reference

| Property | Default | Description |
|---|---|---|
| `solace.commons.enabled` | `true` | Enable/disable the auto-configuration |
| `solace.commons.topics.<topic>.parser` | — | Bean name of `MessageParser` for this topic |
| `solace.commons.topics.<topic>.enrichers` | `[]` | Ordered bean names of `MessageEnricher`s |
| `solace.commons.topics.<topic>.sinks` | `[]` | Extra `MessageSink` bean names for this topic |
| `solace.commons.topics.<topic>.sink-publish-enabled` | (global) | Enable publish sink for this topic |
| `solace.commons.topics.<topic>.sink-publish-target` | (global) | Output binding name for this topic |
| `solace.commons.topics.<topic>.sink-database-enabled` | (global) | Enable DB sink for this topic |
| `solace.commons.sinks.publish.enabled` | `false` | Global default: enable publish sink |
| `solace.commons.sinks.publish.target` | — | Global default: output binding name |
| `solace.commons.sinks.database.enabled` | `false` | Global default: enable DB sink |

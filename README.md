# slocalce-commons

A reusable **Solace PubSub+ Messaging Commons** library for Spring Boot microservices.

Add it as a dependency, provide your broker connection details, and start receiving
messages with zero boilerplate.

---

## Table of contents

1. [Features](#features)
2. [Getting started](#getting-started)
3. [Configuration reference](#configuration-reference)
4. [Consumer modes](#consumer-modes)
5. [Processing pipeline hooks](#processing-pipeline-hooks)
6. [Examples](#examples)
7. [Architecture overview](#architecture-overview)

---

## Features

| # | Capability |
|---|-----------|
| 1 | **Streaming** – continuously receive messages from subscribed topics |
| 2 | **Snapshot** – receive retained/snapshot payload(s), optionally stop on first |
| 3 | **Trigger-then-consume** – publish a trigger, wait for correlated responses |
| 4 | **Extensible processing pipeline** – plug in decoders, filters, enrichers, handlers |
| 5 | **Spring Boot auto-configuration** – zero-code setup via `application.yml` |

---

## Getting started

### 1. Add the dependency

```xml
<dependency>
    <groupId>com.numaansystems</groupId>
    <artifactId>slocalce-commons</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure the broker connection

```yaml
solace:
  commons:
    connection:
      host: tcp://localhost:55555
      vpn: default
      username: admin
      password: admin
    subscriptions:
      - topic: my/streaming/topic
        mode: STREAMING
```

That is it – the `CommonsConsumerManager` bean is auto-configured and will start a
streaming consumer when your application starts.

---

## Configuration reference

All properties live under the `solace.commons` prefix.

### Connection settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `solace.commons.enabled` | `boolean` | `true` | Master switch – set to `false` to disable all consumers |
| `solace.commons.connection.host` | `String` | — | Broker host URI, e.g. `tcp://localhost:55555` |
| `solace.commons.connection.vpn` | `String` | `default` | Message VPN name |
| `solace.commons.connection.username` | `String` | — | Authentication username |
| `solace.commons.connection.password` | `String` | — | Authentication password |
| `solace.commons.connection.client-name` | `String` | auto | Optional client name |

### Tuning settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `solace.commons.reconnect-retries` | `int` | `3` | Reconnect attempts before giving up |
| `solace.commons.reconnect-retry-wait-ms` | `int` | `3000` | Milliseconds between reconnect attempts |
| `solace.commons.snapshot-timeout-ms` | `int` | `10000` | Max wait for snapshot receipt |
| `solace.commons.trigger-timeout-ms` | `int` | `30000` | Max wait for trigger response |

### Subscription settings

`solace.commons.subscriptions` is a list of `SubscriptionDescriptor` objects.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `topic` | `String` | — | Solace topic (wildcards `*` supported) |
| `mode` | `ConsumerMode` | `STREAMING` | `STREAMING`, `SNAPSHOT`, or `TRIGGER_THEN_CONSUME` |
| `stop-on-first-snapshot` | `boolean` | `false` | Stop after receiving the first snapshot message |
| `trigger-topic` | `String` | — | Topic to publish the trigger on (TRIGGER_THEN_CONSUME only) |
| `trigger-payload` | `String` | — | Text payload of the trigger message |

---

## Consumer modes

### STREAMING

Continuously receives messages as they arrive. The consumer runs until the
application shuts down.

```yaml
solace:
  commons:
    subscriptions:
      - topic: market/prices/>
        mode: STREAMING
```

### SNAPSHOT

Connects, receives messages for up to `snapshot-timeout-ms` milliseconds, then
disconnects. Set `stop-on-first-snapshot: true` to disconnect after the first
message arrives (ideal for retained/cached topics).

```yaml
solace:
  commons:
    snapshot-timeout-ms: 5000
    subscriptions:
      - topic: market/reference/data
        mode: SNAPSHOT
        stop-on-first-snapshot: true
```

### TRIGGER_THEN_CONSUME

Publishes a trigger message to `trigger-topic`, then waits up to
`trigger-timeout-ms` for a correlated response on the subscription `topic`.
Messages whose `correlationId` does not match are discarded.

```yaml
solace:
  commons:
    trigger-timeout-ms: 15000
    subscriptions:
      - topic: market/response/AAPL
        mode: TRIGGER_THEN_CONSUME
        trigger-topic: market/request
        trigger-payload: '{"symbol":"AAPL"}'
```

---

## Processing pipeline hooks

Every subscription is backed by a `MessageProcessingPipeline` that chains:

```
BytesXMLMessage ──► MessageDecoder<T>
                         │
                   MessageFilter<T>   (zero or more, evaluated in order)
                         │
                   MessageEnricher<T> (zero or more, applied in order)
                         │
                   MessageHandler<T>  (your business logic)
```

| Interface | Role |
|-----------|------|
| `MessageDecoder<T>` | Converts raw `BytesXMLMessage` to a typed payload. |
| `MessageFilter<T>` | Returns `true` to accept, `false` to drop the message. |
| `MessageEnricher<T>` | Transforms / annotates the payload before the handler. |
| `MessageHandler<T>` | Receives the final payload + `SolaceMessage<T>` envelope. |

Default implementations provided:

| Class | Description |
|-------|-------------|
| `NoOpDecoder` | Extracts the body as a UTF-8 `String`. |
| `NoOpFilter<T>` | Accepts all messages. |
| `NoOpEnricher<T>` | Returns the payload unchanged. |
| `LoggingMessageHandler<T>` | Logs every message at INFO level (default handler). |

---

## Examples

### Streaming consumer example

Register a pipeline in a `@PostConstruct` or `@Bean`:

```java
@Autowired
PipelineRegistry registry;

@PostConstruct
void configure() {
    registry.register("market/prices/AAPL",
        PipelineBuilder.<PriceDto>create()
            .decoder(new JsonDecoder<>(PriceDto.class, objectMapper))
            .filter(price -> price.getValue() > 0)
            .handler((price, msg) -> priceService.update(price))
            .build());
}
```

```yaml
solace:
  commons:
    connection:
      host: tcp://broker:55555
      vpn: prod
      username: svc-user
      password: ${SOLACE_PASSWORD}
    subscriptions:
      - topic: market/prices/AAPL
        mode: STREAMING
```

### Snapshot consumer example

```java
registry.register("config/reference",
    PipelineBuilder.<ReferenceData>create()
        .decoder(new JsonDecoder<>(ReferenceData.class, objectMapper))
        .handler((data, msg) -> referenceDataStore.load(data))
        .build());
```

```yaml
solace:
  commons:
    snapshot-timeout-ms: 8000
    subscriptions:
      - topic: config/reference
        mode: SNAPSHOT
        stop-on-first-snapshot: true
```

### Trigger-then-consume example

```java
registry.register("orders/response/MY-ACCOUNT",
    PipelineBuilder.<OrderBook>create()
        .decoder(new JsonDecoder<>(OrderBook.class, objectMapper))
        .handler((book, msg) -> orderService.refresh(book))
        .build());
```

```yaml
solace:
  commons:
    trigger-timeout-ms: 20000
    subscriptions:
      - topic: orders/response/MY-ACCOUNT
        mode: TRIGGER_THEN_CONSUME
        trigger-topic: orders/request
        trigger-payload: '{"account":"MY-ACCOUNT","action":"GET_BOOK"}'
```

### Custom pipeline hooks example

#### Custom JSON decoder

```java
public class JsonDecoder<T> implements MessageDecoder<T> {

    private final Class<T> type;
    private final ObjectMapper mapper;

    public JsonDecoder(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public T decode(BytesXMLMessage rawMessage) throws Exception {
        String json;
        if (rawMessage instanceof TextMessage) {
            json = ((TextMessage) rawMessage).getText();
        } else {
            byte[] buf = new byte[((BytesMessage) rawMessage).getContentLength()];
            ((BytesMessage) rawMessage).readBytes(buf);
            json = new String(buf, StandardCharsets.UTF_8);
        }
        return mapper.readValue(json, type);
    }
}
```

#### Custom enricher – stamp the received topic onto the DTO

```java
MessageEnricher<MyDto> topicEnricher = (dto, raw) -> {
    dto.setSourceTopic(raw.getDestination().getName());
    return dto;
};
```

#### Full pipeline combining all hooks

```java
PipelineBuilder.<MyDto>create()
    .decoder(new JsonDecoder<>(MyDto.class, objectMapper))
    .filter(dto -> !dto.isEmpty())
    .enricher(topicEnricher)
    .enricher((dto, raw) -> { dto.setCorrelationId(raw.getCorrelationId()); return dto; })
    .handler((dto, msg) -> {
        log.info("Processing {} from {}", dto, msg.getTopic());
        myService.process(dto);
    })
    .build();
```

---

## Architecture overview

```
slocalce-commons
├── config/
│   ├── SolaceCommonsProperties      @ConfigurationProperties(prefix="solace.commons")
│   ├── SubscriptionDescriptor       per-topic config (topic, mode, trigger settings)
│   └── ConsumerMode                 enum: STREAMING | SNAPSHOT | TRIGGER_THEN_CONSUME
├── model/
│   └── SolaceMessage<T>             immutable message envelope (payload + metadata)
├── pipeline/
│   ├── MessageDecoder<T>            interface: raw → typed payload
│   ├── MessageFilter<T>             interface: accept/reject
│   ├── MessageEnricher<T>           interface: transform payload
│   ├── MessageHandler<T>            interface: final business callback
│   ├── MessageProcessingPipeline<T> interface: orchestrates the chain
│   ├── DefaultMessageProcessingPipeline<T>  standard implementation
│   ├── PipelineBuilder<T>           fluent builder
│   ├── NoOpDecoder                  UTF-8 String extractor (default decoder)
│   ├── NoOpFilter<T>                pass-through filter
│   ├── NoOpEnricher<T>              pass-through enricher
│   └── LoggingMessageHandler<T>     default INFO-level logger
├── connection/
│   ├── SolaceClientFactory          creates SolaceSessionWrapper from config
│   └── SolaceSessionWrapper         lifecycle-aware JCSMP session wrapper
├── consumer/
│   ├── StreamingConsumer            continuously receives from topic
│   ├── SnapshotConsumer             receives up to timeout / stop-on-first
│   └── TriggerThenConsumeService    publish trigger → await correlated response
├── publisher/
│   └── TriggerPublisher             sends text trigger messages with correlation ID
└── autoconfigure/
    ├── PipelineRegistry             maps topics → pipelines (register your handlers here)
    ├── CommonsConsumerManager       SmartLifecycle bean: starts/stops all consumers
    └── SolaceCommonsAutoConfiguration  Spring Boot auto-config entry-point
```

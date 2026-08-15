# streamix-client

Thin Java client for the Streamix broker. Java 17+, no Spring required — runtime dependencies are just `jackson-databind` and `slf4j-api`; HTTP uses the JDK's built-in client.

```xml
<dependency>
    <groupId>com.example.streamix</groupId>
    <artifactId>streamix-client</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

## Produce

```java
var producer = StreamixProducer.create("http://streamix:8080");
PublishAck ack = producer.send("orders", "user-7", Map.of("orderId", 101));   // keyed → per-key ordering
producer.sendBatch("orders", List.of(ProducerMessage.of("m1"), ProducerMessage.of("m2")));
```

## Consume — continuous (long-polled loop)

```java
var consumer = StreamixConsumer.builder("http://streamix:8080")
        .group("billing").topics("orders")
        .build();
consumer.start(batch -> batch.forEach(m -> process(m.valueAs(OrderEvent.class))));
// poll loop, heartbeats, commits, eviction recovery all handled; close() leaves the group
```

## Consume — scheduled batch drain (e.g. a 9 AM cron)

```java
var consumer = StreamixConsumer.builder("http://streamix:8080")
        .group("nightly-report").topics("orders")
        .build();
long processed = consumer.drainAll(batch -> report.add(batch));  // register → drain → commit → leave
```

## Admin

```java
var admin = StreamixAdmin.create("http://streamix:8080");
admin.ensureTopic("orders", 3);              // idempotent, ideal at service startup
admin.groupOffsets("billing");               // committed offsets + lag per partition
```

## Semantics you should know

- **At-least-once.** Commits happen after your handler returns. If your handler throws, the client resets to the last commit and the same batch is **redelivered** — make handlers idempotent.
- **Ordering** is guaranteed within a partition only; the same key always lands on the same partition.
- **Long polling** (`waitMs`, default 10s) means near-instant delivery with almost no idle traffic; set `waitMs(0)` for immediate-return polls.
- **Eviction is self-healing.** If your handler stalls past the session timeout, the broker evicts the consumer; the client re-registers automatically and uncommitted messages are redelivered.
- **Duplicate consumerId:** by default the client takes over a stale session (crashed instance restarting). Two *live* processes sharing a consumerId is a configuration error the broker cannot fence — give each instance its own id (the default id is `hostname-<random>`).
- The full REST API is browsable at `/swagger-ui.html` on the broker, and the OpenAPI document at `/v3/api-docs` — non-JVM services can generate typed clients from it.

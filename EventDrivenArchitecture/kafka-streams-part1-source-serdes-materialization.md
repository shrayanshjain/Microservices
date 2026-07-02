# KAFKA STREAMS SYNTAX — PART 1
# Source (KStream / KTable / GlobalKTable) + Serdes + Materialization

This is Part 1 of the Kafka Streams syntax series.
It covers the three foundational concepts you MUST understand before writing any topology:
1. **Source** — How to read from topics (KStream, KTable, GlobalKTable)
2. **Serdes** — How Kafka Streams serializes/deserializes data at EVERY step
3. **Materialization** — When and why RocksDB state stores are created

Source types (KStream, KTable, GlobalKTable) are already covered in detail in
`kafka-streams-syntax-deep-dive.md` Parts 1-2. Here we briefly recap and focus
on the Serdes and Materialization aspects.

---

# SECTION 1: SOURCE RECAP (Brief)

## 1.1 The Three Source Types

```
┌──────────────────┬────────────────────────────────────────────────────────────┐
│ Source            │ What it does                                              │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ builder.stream() │ KStream — every record is an independent event            │
│                  │ Stateless source. No RocksDB. No changelog.               │
│                  │ 3 records with same key = 3 separate events               │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ builder.table()  │ KTable — latest value per key (like a DB table)           │
│                  │ STATEFUL source. Creates RocksDB + changelog.             │
│                  │ 3 records with same key = only latest value kept          │
├──────────────────┼────────────────────────────────────────────────────────────┤
│ builder.        │ GlobalKTable — same as KTable but FULL copy on every task │
│  globalTable()   │ Every instance/task reads ALL partitions.                 │
│                  │ No co-partitioning needed for joins.                      │
│                  │ Use only for small reference data.                        │
└──────────────────┴────────────────────────────────────────────────────────────┘
```

## 1.2 Source Syntax At a Glance

```java
// KStream — stateless source
KStream<String, String> stream = builder.stream("topic-name",
        Consumed.with(Serdes.String(), Serdes.String()));

// KTable — stateful source (creates RocksDB)
KTable<String, String> table = builder.table("topic-name",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.as("store-name"));              // ← names the RocksDB store

// GlobalKTable — stateful, full copy everywhere
GlobalKTable<String, String> globalTable = builder.globalTable("topic-name",
        Consumed.with(Serdes.String(), Serdes.String()));
```

**Key insight from our discussion:**
- KTable is a STATEFUL operation — it reads records one by one and updates RocksDB at each step
- It does NOT wait for all records before processing — record by record, same as KStream
- KTable is essentially a shortcut for "read topic + store.put(key, value) for every record"
- Its main purpose: serve as a LOOKUP table for joins


---

# SECTION 2: SERDES DEEP DIVE

## 2.1 What Is a Serde?

Serde = **Ser**ializer + **De**serializer combined into one object.

```
SERIALIZER:   Java Object → bytes (to write to Kafka / RocksDB)
DESERIALIZER: bytes → Java Object (to read from Kafka / RocksDB)
SERDE:        Both in one package
```

### Why do we need Serdes?

Kafka stores EVERYTHING as bytes. Not Strings. Not JSON. Not objects. **Raw bytes.**
```
What you write in Java:          "Hello World"
What Kafka actually stores:      [72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100]
```

Every time data moves, Serdes are needed:
```
Producer → [serialize] → Kafka Topic → [deserialize] → Kafka Streams → [serialize] → RocksDB
RocksDB → [deserialize] → Kafka Streams → [serialize] → Output Topic
```

---

## 2.2 Built-in Serdes

Kafka Streams provides these out of the box:

```java
Serdes.String()       // String ↔ bytes (UTF-8 encoding)
Serdes.Long()         // Long ↔ bytes (8 bytes, big-endian)
Serdes.Integer()      // Integer ↔ bytes (4 bytes)
Serdes.Double()       // Double ↔ bytes (8 bytes)
Serdes.ByteArray()    // byte[] ↔ bytes (pass-through, no conversion)
Serdes.Bytes()        // Bytes object ↔ bytes
Serdes.UUID()         // UUID ↔ bytes
Serdes.Float()        // Float ↔ bytes
Serdes.Short()        // Short ↔ bytes
Serdes.Boolean()      // Boolean ↔ bytes
```

### When to use which:

```
Your data is plain text (JSON string, CSV)     → Serdes.String()
Your data is a counter/count                    → Serdes.Long()
Your data is raw bytes (images, protobuf)       → Serdes.ByteArray()
Your data is a Java object (Order, Customer)    → CUSTOM SERDE (Section 2.5)
```

---

## 2.3 The 5 Serde Boundaries — Where Exactly Are Serdes Needed?

Serdes don't appear in just one place. They appear at **every boundary** where data
crosses between Java objects and bytes. There are exactly 5 such boundaries in
Kafka Streams, and each has its own configuration class:

```
┌─────────────────────┬────────────────────────┬──────────────────────────────────────┐
│ Boundary            │ Config Class           │ When it's used                       │
├─────────────────────┼────────────────────────┼──────────────────────────────────────┤
│ 1. Reading topic    │ Consumed.with(K, V)    │ builder.stream() / .table()          │
│                     │                        │ bytes → Java (DESERIALIZE)           │
├─────────────────────┼────────────────────────┼──────────────────────────────────────┤
│ 2. Writing topic    │ Produced.with(K, V)    │ stream.to("output-topic")            │
│                     │                        │ Java → bytes (SERIALIZE)             │
├─────────────────────┼────────────────────────┼──────────────────────────────────────┤
│ 3. Grouping         │ Grouped.with(K, V)     │ stream.groupByKey() / .groupBy()     │
│                     │                        │ needs serdes for repartition topic   │
├─────────────────────┼────────────────────────┼──────────────────────────────────────┤
│ 4. Joining          │ Joined.with(K, V1, V2) │ stream.join(table, ...)              │
│                     │                        │ needs serdes for internal state      │
├─────────────────────┼────────────────────────┼──────────────────────────────────────┤
│ 5. State store      │ Materialized.with(K,V) │ .count(), .aggregate(), .table()     │
│                     │                        │ Java → bytes for RocksDB storage     │
└─────────────────────┴────────────────────────┴──────────────────────────────────────┘
```

### Why 5 boundaries and not just 1?

Because the **types can CHANGE** at each step!

```java
// SOURCE: key=String, value=String
KStream<String, String> orders = builder.stream("orders",
        Consumed.with(Serdes.String(), Serdes.String()));     // ← boundary 1

// PROCESSOR: value changes from String to Long (count)
KTable<String, Long> counts = orders
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))  // ← boundary 3
        .count(Materialized.as("order-counts"));                     // ← boundary 5
                                                                      //   (key=String, value=Long now!)

// SINK: writing String key + Long value
counts.toStream().to("order-counts-output",
        Produced.with(Serdes.String(), Serdes.Long()));              // ← boundary 2
```

Notice how:
- Input was `<String, String>` (Consumed)
- After `count()`, it became `<String, Long>` (Materialized)
- Output is `<String, Long>` (Produced)

**Each boundary needs its OWN serdes matching the types AT THAT POINT.**


---

## 2.4 The Three Levels of Serde Configuration

You can configure serdes at three levels. They override each other:

```
Level 1 (LOWEST priority): Default serdes — set once, apply everywhere
Level 2: Per-topology serdes — set on StreamsBuilder (rare, usually skip this)
Level 3 (HIGHEST priority): Per-operation serdes — Consumed, Produced, Grouped, etc.
```

### Level 1: Default Serdes (application.yml or Properties)

```yaml
# application.yml
spring:
  kafka:
    streams:
      properties:
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        default.value.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
```

Or in pure Java:
```java
Properties props = new Properties();
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
```

**When default serdes are set, you can write simpler code:**

```java
// WITHOUT default serdes — must specify everywhere
KStream<String, String> stream = builder.stream("orders",
        Consumed.with(Serdes.String(), Serdes.String()));

// WITH default serdes set to String — can omit Consumed
KStream<String, String> stream = builder.stream("orders");
// ↑ uses default String serdes automatically
```

### Level 3: Per-Operation Serdes (Highest Priority — ALWAYS Wins)

```java
// Even if defaults are String, this uses Long for value
KStream<String, Long> counts = builder.stream("counts-topic",
        Consumed.with(Serdes.String(), Serdes.Long()));
// ↑ Consumed.with() overrides default serdes
```

### Best Practice:

```
✅ Set default serdes to String (most common case)
✅ Override per-operation when types differ (Long, custom objects)
❌ Don't rely on defaults for non-String types — be explicit
```

---

## 2.5 Custom JSON Serde — The Real-World Pattern

In any serious application, you work with Java objects, not plain strings.
You need a custom serde that converts `Order ↔ bytes` using JSON (Jackson).

### Step 1: The Java Object

```java
public class Order {
    private String orderId;
    private String customerId;
    private double amount;
    private String status;

    // constructors, getters, setters
    public Order() {}

    public Order(String orderId, String customerId, double amount, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
    }

    // getters and setters...
}
```

### Step 2: The Generic JSON Serializer

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;     // ← null = tombstone, don't serialize
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing " + data.getClass().getSimpleName(), e);
        }
    }
}
```

**What this does:**
```
Order{orderId="001", customerId="alice", amount=150.0, status="CREATED"}
    ↓ objectMapper.writeValueAsBytes()
[123, 34, 111, 114, 100, ...]   ← bytes of: {"orderId":"001","customerId":"alice",...}
```

### Step 3: The Generic JSON Deserializer

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> targetType;

    public JsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, targetType);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing to " + targetType.getSimpleName(), e);
        }
    }
}
```

**Why does Deserializer need `Class<T>` but Serializer doesn't?**
```
Serializer:    gets an Order object → calls objectMapper.writeValueAsBytes(order)
               Jackson knows the type because it HAS the object

Deserializer:  gets raw bytes → needs to know "turn these into Order"
               Jackson needs the target class: objectMapper.readValue(bytes, Order.class)
```

### Step 4: Combine into a Serde

```java
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerde<T> implements Serde<T> {

    private final JsonSerializer<T> serializer;
    private final JsonDeserializer<T> deserializer;

    public JsonSerde(Class<T> targetType) {
        this.serializer = new JsonSerializer<>();
        this.deserializer = new JsonDeserializer<>(targetType);
    }

    @Override
    public JsonSerializer<T> serializer() {
        return serializer;
    }

    @Override
    public JsonDeserializer<T> deserializer() {
        return deserializer;
    }
}
```

### Step 5: Use It Everywhere

```java
// Create the serde
Serde<Order> orderSerde = new JsonSerde<>(Order.class);

// SOURCE — deserialize bytes from topic into Order objects
KStream<String, Order> orders = builder.stream("orders",
        Consumed.with(Serdes.String(), orderSerde));

// Now you work with REAL objects, not strings!
orders.filter((key, order) -> order.getAmount() > 100)
      .mapValues(order -> {
          order.setStatus("HIGH_VALUE");
          return order;
      })
      .to("high-value-orders",
          Produced.with(Serdes.String(), orderSerde));  // serialize back to bytes
```

**The flow:**
```
Kafka Topic (bytes)
    → [JsonDeserializer] → Order object in Java
    → filter, mapValues, etc. (work with Order)
    → [JsonSerializer] → bytes
    → Kafka Output Topic
```

---

## 2.6 Serde for Every Boundary — Complete Example

Here's a topology that hits ALL 5 serde boundaries with different types:

```java
@Bean
public KStream<String, Order> orderAnalytics(StreamsBuilder builder) {

    // Create serdes for our custom types
    Serde<Order> orderSerde = new JsonSerde<>(Order.class);
    Serde<Customer> customerSerde = new JsonSerde<>(Customer.class);
    Serde<EnrichedOrder> enrichedSerde = new JsonSerde<>(EnrichedOrder.class);

    // ═══════════════════════════════════════════════════════════════════
    // BOUNDARY 1: Consumed — reading from source topics
    // ═══════════════════════════════════════════════════════════════════
    KStream<String, Order> orders = builder.stream("orders",
            Consumed.with(Serdes.String(), orderSerde));
    //                     ↑ key serde      ↑ value serde
    //                     bytes→String      bytes→Order

    KTable<String, Customer> customers = builder.table("customers",
            Consumed.with(Serdes.String(), customerSerde),
    //                     bytes→String      bytes→Customer

    // ═══════════════════════════════════════════════════════════════════
    // BOUNDARY 5: Materialized — state store serdes (for KTable source)
    // ═══════════════════════════════════════════════════════════════════
            Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as("customer-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(customerSerde));
    //                               ↑ how to store Customer in RocksDB

    // ═══════════════════════════════════════════════════════════════════
    // BOUNDARY 4: Joined — join serdes
    // ═══════════════════════════════════════════════════════════════════
    KStream<String, EnrichedOrder> enriched = orders.join(customers,
            (order, customer) -> new EnrichedOrder(order, customer),
            Joined.with(Serdes.String(), orderSerde, customerSerde));
    //               ↑ key serde    ↑ left value    ↑ right value

    // ═══════════════════════════════════════════════════════════════════
    // BOUNDARY 3: Grouped — grouping serdes (needed for repartition topic)
    // ═══════════════════════════════════════════════════════════════════
    KTable<String, Long> ordersByCity = enriched
            .selectKey((key, eo) -> eo.getCustomer().getCity())  // key changes!
            .groupByKey(Grouped.with(Serdes.String(), enrichedSerde))
    //                               ↑ new key type    ↑ value type
    //                  key changed → repartition topic created
    //                  Grouped serdes used to serialize INTO that repartition topic
            .count(Materialized.as("orders-by-city"));
    //                          ↑ BOUNDARY 5 again: count stores Long in RocksDB

    // ═══════════════════════════════════════════════════════════════════
    // BOUNDARY 2: Produced — writing to output topic
    // ═══════════════════════════════════════════════════════════════════
    ordersByCity.toStream().to("order-city-counts",
            Produced.with(Serdes.String(), Serdes.Long()));
    //                       String→bytes    Long→bytes

    return orders;
}
```



## 2.7 What Happens When Serialization / Deserialization Fails?

There are **TWO different failure points** — and each has its own handler:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ FAILURE POINT 1: DESERIALIZATION (reading bytes FROM Kafka topic)            │
│                                                                              │
│  When:   bytes → Java object fails                                          │
│  Where:  SOURCE node — builder.stream(), builder.table()                    │
│  Config: default.deserialization.exception.handler                          │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│ FAILURE POINT 2: SERIALIZATION (writing Java object TO Kafka topic)          │
│                                                                              │
│  When:   Java object → bytes fails                                          │
│  Where:  SINK node — stream.to(), or internal repartition/changelog topics  │
│  Config: default.production.exception.handler                               │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.7.1 DESERIALIZATION Failure — Reading Bad Data From Topic

**When does this happen?**

```
Use Case 1: Wrong serde type
  Topic has:  {"orderId":"001","amount":150}   (JSON string)
  You read:   Consumed.with(Serdes.String(), Serdes.Long())
  Result:     Tries to read JSON bytes as 8-byte Long → BOOM!

Use Case 2: Corrupted/malformed data
  Topic has:  [some garbage bytes that aren't valid JSON]
  You read:   Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
  Result:     Jackson can't parse → JsonParseException → BOOM!

Use Case 3: Schema mismatch
  Topic has:  {"name":"alice","age":25}
  You read:   JsonSerde<>(Customer.class) where Customer has "firstName" not "name"
  Result:     Jackson throws UnrecognizedPropertyException → BOOM!

Use Case 4: Null key/value not handled
  Topic has:  key=null (tombstone record)
  You read:   Serde that doesn't handle null
  Result:     NullPointerException → BOOM!
```

**What happens by DEFAULT when deserialization fails?**

```
DEFAULT BEHAVIOR: LogAndFailExceptionHandler
  → Logs the error
  → Throws the exception
  → Stream thread DIES
  → If all stream threads die → APP STOPS

This is INTENTIONAL — Kafka Streams doesn't want to silently skip bad data
by default because that could lead to data loss you don't notice.
```

**The 3 options for handling deserialization errors:**

```
┌──────────────────────────────────────┬───────────────────────────────────────┐
│ Handler                              │ Behavior                              │
├──────────────────────────────────────┼───────────────────────────────────────┤
│ LogAndFailExceptionHandler (DEFAULT) │ Log error → throw → stream thread    │
│                                      │ dies → app stops if all threads die  │
├──────────────────────────────────────┼───────────────────────────────────────┤
│ LogAndContinueExceptionHandler       │ Log error → SKIP the bad record      │
│                                      │ → continue processing next record    │
│                                      │ → bad record is LOST forever         │
├──────────────────────────────────────┼───────────────────────────────────────┤
│ Custom handler (you write it)        │ YOU decide: skip, fail, send to      │
│                                      │ dead-letter topic, alert, etc.       │
└──────────────────────────────────────┴───────────────────────────────────────┘
```

**Option A: LogAndFailExceptionHandler (DEFAULT — no config needed)**

```properties
# application.properties — this is the DEFAULT, you don't need to set it
# spring.kafka.streams.properties[default.deserialization.exception.handler]=\
#   org.apache.kafka.streams.errors.LogAndFailExceptionHandler
```

```
Bad record arrives → exception logged → stream thread crashes → app dies
When to use: Development, or when you MUST process every single record.
```

**Option B: LogAndContinueExceptionHandler**

```properties
# application.properties
spring.kafka.streams.properties[default.deserialization.exception.handler]=\
  org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
```

```
Bad record arrives → exception logged → record SKIPPED → processing continues
When to use: When bad records are expected (e.g., topic has mixed formats)
             and you're OK losing a few bad ones.
WARNING: The bad record is gone forever — no retry, no dead-letter queue.
```

**Option C: Custom DeserializationExceptionHandler (RECOMMENDED for production)**

```java
package com.eda.streams.errorhandling;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CustomDeserializationHandler implements DeserializationExceptionHandler {

    private static final AtomicLong errorCount = new AtomicLong(0);
    private static final long MAX_ERRORS = 100;  // fail after 100 bad records

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                  ConsumerRecord<byte[], byte[]> record,
                                                  Exception exception) {

        long errors = errorCount.incrementAndGet();

        // Log the bad record details (but NOT the raw bytes — could be huge)
        System.err.println("[DESERIALIZATION ERROR #" + errors + "]");
        System.err.println("  Topic:     " + record.topic());
        System.err.println("  Partition: " + record.partition());
        System.err.println("  Offset:    " + record.offset());
        System.err.println("  Exception: " + exception.getMessage());

        // STRATEGY: skip bad records, but if too many errors → fail the app
        if (errors > MAX_ERRORS) {
            System.err.println("  → Too many errors (" + errors + "). FAILING.");
            return DeserializationHandlerResponse.FAIL;   // → stream thread dies
        }

        System.err.println("  → Skipping bad record. Continuing...");
        return DeserializationHandlerResponse.CONTINUE;    // → skip and move on
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // You can read custom config properties here if needed
    }
}
```

Register it in application.properties:
```properties
# application.properties
spring.kafka.streams.properties[default.deserialization.exception.handler]=\
  com.eda.streams.errorhandling.CustomDeserializationHandler
```

**What happens at runtime:**

```
Record 1: good     → deserialized → processed normally
Record 2: bad      → exception → handler called → CONTINUE (skipped, error #1)
Record 3: good     → deserialized → processed normally
Record 4: bad      → exception → handler called → CONTINUE (skipped, error #2)
...
Record 150: bad    → exception → handler called → error #101 > MAX_ERRORS → FAIL
                   → stream thread dies
```

---

### 2.7.2 SERIALIZATION Failure — Writing Bad Data TO Topic

**When does this happen?**

```
Use Case 1: Wrong output serde
  You have:   Long value (from count())
  You write:  Produced.with(Serdes.String(), Serdes.String())
  Result:     Tries to serialize Long as String → ClassCastException → BOOM!

Use Case 2: Object can't be serialized
  You have:   Order object with a circular reference
  You write:  JsonSerde<>(Order.class)
  Result:     Jackson can't serialize → JsonMappingException → BOOM!

Use Case 3: Value too large
  You have:   Aggregated result that's 2MB
  Kafka limit: max.message.bytes = 1MB (default)
  Result:     RecordTooLargeException → BOOM!
```

**What happens by DEFAULT when serialization fails?**

```
DEFAULT BEHAVIOR: DefaultProductionExceptionHandler
  → Throws exception → stream thread DIES → app stops

Same as deserialization — fail-fast by default.
```

**The 3 options for handling serialization / production errors:**

```
┌─────────────────────────────────────────┬────────────────────────────────────┐
│ Handler                                 │ Behavior                           │
├─────────────────────────────────────────┼────────────────────────────────────┤
│ DefaultProductionExceptionHandler       │ FAIL always (DEFAULT)              │
│ (DEFAULT)                               │                                    │
├─────────────────────────────────────────┼────────────────────────────────────┤
│ AlwaysContinueProductionExceptionHandler│ SKIP always → record lost          │
│                                         │ (not built-in, you write it)       │
├─────────────────────────────────────────┼────────────────────────────────────┤
│ Custom handler (you write it)           │ YOU decide per error type          │
└─────────────────────────────────────────┴────────────────────────────────────┘
```

**Option A: DefaultProductionExceptionHandler (DEFAULT — no config needed)**

This is what Kafka Streams uses if you don't set anything.

```properties
# application.properties — this is the DEFAULT, you don't need to set it
# spring.kafka.streams.properties[default.production.exception.handler]=\
#   org.apache.kafka.streams.errors.DefaultProductionExceptionHandler
```

What it does internally:
```java
// Kafka Streams source code — simplified
public class DefaultProductionExceptionHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                      Exception exception) {
        return ProductionExceptionHandlerResponse.FAIL;   // ← ALWAYS fail
    }
}
```

```
What happens at runtime:

  stream.to("output-topic") tries to serialize Order → bytes
      → Serialization fails (e.g., wrong serde type)
      → DefaultProductionExceptionHandler.handle() called
      → Returns FAIL
      → Stream thread DIES
      → If all stream threads die → APP STOPS

When to use:
  ✅ Development — you WANT to see errors immediately and fix them
  ✅ When every single record MUST be produced — no silent data loss
  ❌ Production with noisy data — one bad record kills the whole app
```

**Option B: AlwaysContinueProductionExceptionHandler (Skip everything)**

There is NO built-in "LogAndContinue" for production errors like there is for
deserialization. You must write a simple one yourself:

```java
package com.eda.streams.errorhandling;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

/**
 * Always skip failed records — equivalent of LogAndContinueExceptionHandler
 * but for production/serialization errors.
 *
 * WARNING: Bad records are LOST forever. Use only when you can afford data loss.
 */
public class AlwaysContinueProductionHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                      Exception exception) {

        // Log the error so you at least know it happened
        System.err.println("[PRODUCTION ERROR — SKIPPING]");
        System.err.println("  Topic:     " + record.topic());
        System.err.println("  Exception: " + exception.getClass().getSimpleName()
                                            + ": " + exception.getMessage());

        return ProductionExceptionHandlerResponse.CONTINUE;   // ← ALWAYS skip
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
```

```properties
# application.properties
spring.kafka.streams.properties[default.production.exception.handler]=\
  com.eda.streams.errorhandling.AlwaysContinueProductionHandler
```

```
What happens at runtime:

  Record 1: serialized OK     → produced to output topic ✅
  Record 2: serialization FAIL → handler returns CONTINUE → record SKIPPED, lost
  Record 3: serialized OK     → produced to output topic ✅

When to use:
  ✅ When you can tolerate occasional data loss
  ✅ When bad records are expected (mixed data, legacy systems)
  ❌ Financial systems, billing — cannot afford lost records
```

**Option C: Custom ProductionExceptionHandler (RECOMMENDED for production)**

You decide per-error-type: skip some, fail on others.

```java
package com.eda.streams.errorhandling;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

/**
 * Smart production error handler:
 *   - RecordTooLarge → skip (can't fix by retrying)
 *   - Serialization error → FAIL (code bug, needs fixing)
 *   - Other → FAIL
 */
public class CustomProductionHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                      Exception exception) {

        System.err.println("[PRODUCTION/SERIALIZATION ERROR]");
        System.err.println("  Topic:     " + record.topic());
        System.err.println("  Exception: " + exception.getClass().getSimpleName()
                                            + ": " + exception.getMessage());

        // RecordTooLarge — can't fix by retrying, skip it
        if (exception.getClass().getSimpleName().contains("RecordTooLarge")) {
            System.err.println("  → Record too large. Skipping.");
            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        // All other errors — fail (serialization bugs should be fixed in code)
        System.err.println("  → Unexpected error. FAILING.");
        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
```

```properties
# application.properties
spring.kafka.streams.properties[default.production.exception.handler]=\
  com.eda.streams.errorhandling.CustomProductionHandler
```

```
What happens at runtime:

  Record 1: serialized OK          → produced ✅
  Record 2: RecordTooLargeException → handler returns CONTINUE → skipped
  Record 3: ClassCastException      → handler returns FAIL → stream thread dies
  Record 4: never processed (thread is dead)

When to use:
  ✅ Production — smart error handling based on error type
  ✅ Skip known-recoverable errors, fail on unknown ones
  ✅ Can extend: send failed records to dead-letter topic, alert, etc.
```

---

### 2.7.3 Complete Error Handling Setup — application.properties

```properties
# ═══════════════════════════════════════════════════════════════
#  ERROR HANDLING — all serde failure configs in one place
# ═══════════════════════════════════════════════════════════════

# DESERIALIZATION errors (reading FROM topic)
# Option 1: Fail on bad record (DEFAULT — don't need this line)
# spring.kafka.streams.properties[default.deserialization.exception.handler]=\
#   org.apache.kafka.streams.errors.LogAndFailExceptionHandler

# Option 2: Skip bad records (log and continue)
# spring.kafka.streams.properties[default.deserialization.exception.handler]=\
#   org.apache.kafka.streams.errors.LogAndContinueExceptionHandler

# Option 3: Custom handler (RECOMMENDED)
spring.kafka.streams.properties[default.deserialization.exception.handler]=\
  com.eda.streams.errorhandling.CustomDeserializationHandler

# SERIALIZATION / PRODUCTION errors (writing TO topic)
spring.kafka.streams.properties[default.production.exception.handler]=\
  com.eda.streams.errorhandling.CustomProductionHandler
```

---

### 2.7.4 Summary: Deserialization vs Serialization Error Handling

```
┌──────────────────┬──────────────────────────┬─────────────────────────────────┐
│                  │ DESERIALIZATION           │ SERIALIZATION                   │
│                  │ (reading FROM topic)      │ (writing TO topic)              │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ When             │ Source node:              │ Sink node:                      │
│                  │ builder.stream()          │ stream.to()                     │
│                  │ builder.table()           │ Also: repartition, changelog    │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ Direction        │ bytes → Java object       │ Java object → bytes             │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ Config key       │ default.deserialization   │ default.production              │
│                  │ .exception.handler        │ .exception.handler              │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ Default          │ LogAndFailException       │ DefaultProduction               │
│                  │ Handler (FAIL)            │ ExceptionHandler (FAIL)         │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ Built-in skip    │ LogAndContinue            │ None — must write custom        │
│                  │ ExceptionHandler          │                                 │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ Custom interface │ DeserializationException  │ ProductionException             │
│                  │ Handler                   │ Handler                         │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ Return values    │ CONTINUE = skip record    │ CONTINUE = skip record          │
│                  │ FAIL = kill thread         │ FAIL = kill thread              │
├──────────────────┼──────────────────────────┼─────────────────────────────────┤
│ Common causes    │ Wrong serde type          │ Wrong output serde              │
│                  │ Corrupted bytes           │ Object not serializable         │
│                  │ Schema mismatch           │ Record too large                │
│                  │ Null not handled           │ Broker unreachable (temporary)  │
└──────────────────┴──────────────────────────┴─────────────────────────────────┘
```

### What about errors INSIDE processor logic (not serde)?

```java
orders.mapValues(value -> {
    // What if this throws NullPointerException?
    return value.toUpperCase();
});
```

```
This is NOT a serde error — it's a processing error.
Neither handler catches this.

Processing exceptions → uncaught → stream thread dies → same as FAIL

To handle: wrap your logic in try-catch yourself:

orders.mapValues(value -> {
    try {
        return value.toUpperCase();
    } catch (Exception e) {
        return "ERROR: " + e.getMessage();  // return a fallback value
    }
});
```

---

## 2.8 Spring Kafka's JsonSerde — Don't Reinvent the Wheel

In Section 2.5 we built a custom JSON serde from scratch. In practice, Spring Kafka
provides one out of the box:

```java
import org.springframework.kafka.support.serializer.JsonSerde;

// Spring's built-in JsonSerde — no need to write your own
Serde<Order> orderSerde = new JsonSerde<>(Order.class);
```

**This is exactly what we built in 2.5**, but Spring's version adds:
- Trusted packages configuration
- Type header support (so consumer knows which class to deserialize to)
- More robust error handling

```java
// Using Spring's JsonSerde in a topology
@Bean
public KStream<String, Order> processOrders(StreamsBuilder builder) {

    Serde<Order> orderSerde = new JsonSerde<>(Order.class);

    KStream<String, Order> orders = builder.stream("orders",
            Consumed.with(Serdes.String(), orderSerde));

    orders.filter((key, order) -> order.getAmount() > 100)
          .to("high-value-orders",
              Produced.with(Serdes.String(), orderSerde));

    return orders;
}
```

**Which to use:**
```
Learning / understanding how it works    → Build your own (Section 2.5)
Production code / Spring Boot app        → Use Spring's JsonSerde
Avro / Schema Registry                   → Use Confluent's KafkaAvroSerde (not covered here)
```


---

# SECTION 3: MATERIALIZATION DEEP DIVE

## 3.1 What Is Materialization?

Materialization = **"I want a state store (RocksDB), and here's how to configure it."**

It is the **configuration bridge** between the DSL operation and the physical state store.

```
┌──────────────────────────────────────────────────────────────────┐
│ Materialized = Configuration Object for State Stores             │
│                                                                  │
│ It answers 3 questions:                                          │
│   1. What is the store's NAME?        → .as("my-store")         │
│   2. What SERDES to use in RocksDB?   → .withKeySerde/ValueSerde│
│   3. What STORAGE TYPE?               → persistent or in-memory │
└──────────────────────────────────────────────────────────────────┘
```

## 3.2 When Does Materialization Happen?

**Rule: Every stateful operation creates a state store. Materialized configures it.**

```
STATEFUL operations that accept Materialized:
  ✅ builder.table("topic", ..., Materialized.as("..."))
  ✅ grouped.count(Materialized.as("..."))
  ✅ grouped.reduce(..., Materialized.as("..."))
  ✅ grouped.aggregate(..., Materialized.as("..."))
  ✅ stream.join(table, ...) — implicitly creates stores

STATELESS operations — NO Materialized:
  ❌ builder.stream()         — no state
  ❌ filter(), mapValues()    — no state
  ❌ stream.to()              — just writes, no state
```

### Critical Point: Materialized is ALWAYS Optional

```java
// WITHOUT Materialized — still creates RocksDB, but with auto-generated name
grouped.count();
// Store name: KSTREAM-AGGREGATE-STATE-STORE-0000000003  (auto-generated)
// Changelog:  app-id-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog

// WITH Materialized — YOU choose the name
grouped.count(Materialized.as("order-counts"));
// Store name: order-counts
// Changelog:  app-id-order-counts-changelog
```

**Both create RocksDB + changelog. The ONLY difference is the name.**
If you don't need to query the store via REST (Interactive Queries), you can skip Materialized.

---

## 3.3 The Full Materialized Syntax

```java
Materialized.<KeyType, ValueType, StoreType>as("store-name")
        .withKeySerde(keySerde)
        .withValueSerde(valueSerde)
        .withRetention(Duration.ofDays(7))        // how long to keep data
        .withLoggingEnabled(logConfig)             // changelog topic config
        .withLoggingDisabled()                     // NO changelog (risky!)
        .withCachingEnabled()                      // enable record cache
        .withCachingDisabled()                     // disable cache
```

Let's break down each:

### 3.3.1 Store Name — `.as("store-name")`

```java
grouped.count(Materialized.as("order-counts"));
```

This name determines THREE things:
```
1. RocksDB directory:   /tmp/kafka-streams/{app-id}/0_0/order-counts/
2. Changelog topic:     {app-id}-order-counts-changelog
3. REST query name:     StoreQueryParameters.fromNameAndType("order-counts", ...)
```

### 3.3.2 Store Serdes — `.withKeySerde()` / `.withValueSerde()`

```java
// count() produces <String, Long> — tell RocksDB how to store them
grouped.count(
    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-counts")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Long())
);
```

**Why does RocksDB need separate serdes?**
```
RocksDB stores data as bytes on disk, just like Kafka.
When Kafka Streams does:
  rocksDB.put(key, value)     → needs serializer to convert key/value to bytes
  rocksDB.get(key)            → needs deserializer to convert bytes back to key/value

These serdes can be DIFFERENT from the topic serdes!
Example:
  Topic value = String (JSON)
  After count() = Long (number)
  → Topic serde: Serdes.String()
  → RocksDB serde: Serdes.Long()     ← different!
```

### 3.3.3 Storage Type — Persistent vs In-Memory

```java
// Default: persistent (RocksDB on disk)
Materialized.as("my-store")
// ↑ Data survives app restart (reads from changelog to rebuild if needed)

// In-memory store — faster but data lost on restart
Materialized.as(Stores.inMemoryKeyValueStore("my-store"))
// ↑ No disk I/O, but must rebuild entirely from changelog on restart
```

When to use which:
```
Persistent (RocksDB) → Default. Use for everything unless you have a reason not to.
                        Data is on disk. Survives restart without full rebuild.

In-Memory             → When data is small AND you need maximum speed.
                        On restart: must re-read entire changelog topic.
                        If changelog has 1 crore records, that's a long restart.
```

### 3.3.4 Retention — `.withRetention()`

```java
// Keep windowed state for 7 days (for windowed aggregations only)
grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
       .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("hourly-counts")
               .withRetention(Duration.ofDays(7)));
```

**This only applies to windowed stores.** It controls how long old windows are kept.
After 7 days, the window data is deleted from RocksDB AND the changelog.

### 3.3.5 Changelog Control — `.withLoggingEnabled()` / `.withLoggingDisabled()`

```java
// Custom changelog topic configuration
Map<String, String> changelogConfig = new HashMap<>();
changelogConfig.put("retention.ms", "604800000");     // 7 days
changelogConfig.put("cleanup.policy", "compact");

grouped.count(
    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-counts")
        .withLoggingEnabled(changelogConfig)          // changelog ON with custom config
);
```

```java
// DANGER: Disable changelog entirely
grouped.count(
    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-counts")
        .withLoggingDisabled()                        // NO changelog topic created
);
// ⚠️ If app crashes, RocksDB data is LOST and cannot be rebuilt
// Only use if you can afford to re-process the entire source topic
```

### 3.3.6 Caching — `.withCachingEnabled()` / `.withCachingDisabled()`

```java
// With caching (DEFAULT) — deduplicate rapid updates
grouped.count(
    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-counts")
        .withCachingEnabled()
);
```

**What does caching do?**
```
WITHOUT cache:
  Record 1 (key=alice) → count=1 → forward downstream + write to changelog
  Record 2 (key=alice) → count=2 → forward downstream + write to changelog
  Record 3 (key=alice) → count=3 → forward downstream + write to changelog
  = 3 downstream records, 3 changelog writes

WITH cache (default):
  Record 1 (key=alice) → count=1 → store in cache
  Record 2 (key=alice) → count=2 → update cache (DON'T forward yet)
  Record 3 (key=alice) → count=3 → update cache (DON'T forward yet)
  [cache flush / commit interval]  → count=3 → forward downstream + write to changelog
  = 1 downstream record, 1 changelog write
```

Caching reduces load on downstream processors and changelog topics.
Disable it only if you need **every single intermediate result** downstream.


---

## 3.4 Materialized in Every Context — Complete Examples

### Context 1: KTable Source

```java
// Reading a topic as a table — materializes the ENTIRE topic into RocksDB
KTable<String, String> customers = builder.table("customers",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("customer-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

// What happens:
// 1. Reads every record from "customers" topic
// 2. For each record: rocksDB.put(key, value) — keeps latest per key
// 3. RocksDB directory: /tmp/kafka-streams/{app-id}/X_X/customer-store/
// 4. Changelog topic: {app-id}-customer-store-changelog
```

### Context 2: count()

```java
KTable<String, Long> orderCounts = orders
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-counts")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

// VALUE TYPE CHANGED: String (input) → Long (count output)
// That's why we specify withValueSerde(Serdes.Long()) on Materialized
```

### Context 3: reduce()

```java
KTable<String, String> latestStatus = orders
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .reduce(
                (oldValue, newValue) -> newValue,    // keep latest
                Materialized.as("latest-order-status"));

// VALUE TYPE UNCHANGED: String → String
// So Materialized inherits serdes from Grouped (or defaults)
// You CAN still specify them explicitly for clarity
```

### Context 4: aggregate()

```java
Serde<OrderSummary> summarySerde = new JsonSerde<>(OrderSummary.class);

KTable<String, OrderSummary> summaries = orders
        .groupByKey(Grouped.with(Serdes.String(), orderSerde))
        .aggregate(
                () -> new OrderSummary(0, 0.0),              // initializer
                (key, order, summary) -> {                     // aggregator
                    summary.setCount(summary.getCount() + 1);
                    summary.setTotal(summary.getTotal() + order.getAmount());
                    return summary;
                },
                Materialized.<String, OrderSummary, KeyValueStore<Bytes, byte[]>>as("order-summaries")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(summarySerde));

// VALUE TYPE CHANGED: Order → OrderSummary
// Materialized MUST specify the new value serde (summarySerde)
```

### Context 5: Windowed Aggregation

```java
KTable<Windowed<String>, Long> hourlyCounts = orders
        .groupByKey(Grouped.with(Serdes.String(), orderSerde))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
        .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("hourly-order-counts")
                .withRetention(Duration.ofDays(7)));

//                              ↑ WindowStore, not KeyValueStore!
// Windowed aggregations use WindowStore because they need to track
// which time window each count belongs to.
```

---

## 3.5 The Materialization Decision Tree

```
Do I need to query this state store via REST (Interactive Queries)?
  YES → Use Materialized.as("meaningful-name")
  NO  ↓

Is the value type different from the input type?
  YES → Use Materialized with explicit value serde
        (e.g., count() changes String→Long, aggregate() changes to new type)
  NO  ↓

Do I need custom store config (retention, no changelog, in-memory)?
  YES → Use Materialized with those settings
  NO  → Skip Materialized entirely — Kafka Streams auto-handles it
```

---

## 3.6 What Materialized Does NOT Do

Common misconceptions:

```
❌ Materialized does NOT decide IF a state store is created
   → The operation (count, aggregate, table) decides that
   → Materialized only CONFIGURES the store

❌ Materialized does NOT create extra state stores
   → count() always creates ONE store, with or without Materialized

❌ Materialized does NOT affect the topology logic
   → The topology does the same thing regardless
   → Materialized only controls naming, serdes, and storage config

✅ Materialized IS optional for ALL stateful operations
   → Without it: auto-generated name, inherited serdes, default config
   → With it: you control the name, serdes, and config
```


---

# SECTION 4: PUTTING IT ALL TOGETHER — Complete Working Example

```java
@Configuration
@EnableKafkaStreams
public class OrderPipelineTopology {

    @Bean
    public KStream<String, Order> fullPipeline(StreamsBuilder builder) {

        // ─── SERDES ────────────────────────────────────────────────
        Serde<Order> orderSerde = new JsonSerde<>(Order.class);
        Serde<Customer> customerSerde = new JsonSerde<>(Customer.class);
        Serde<OrderSummary> summarySerde = new JsonSerde<>(OrderSummary.class);

        // ─── SOURCE 1: Orders as KStream ───────────────────────────
        // Boundary: Consumed (bytes → Java)
        KStream<String, Order> orders = builder.stream("orders",
                Consumed.with(Serdes.String(), orderSerde));

        // ─── SOURCE 2: Customers as KTable ─────────────────────────
        // Boundary: Consumed (bytes → Java) + Materialized (Java → RocksDB bytes)
        KTable<String, Customer> customers = builder.table("customers",
                Consumed.with(Serdes.String(), customerSerde),
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as("customer-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(customerSerde));

        // ─── PROCESSOR: Filter high-value orders ───────────────────
        KStream<String, Order> highValue = orders
                .filter((customerId, order) -> order.getAmount() > 100);
        // No serde needed — stateless, no bytes boundary

        // ─── PROCESSOR: Enrich with customer data ──────────────────
        // Boundary: Joined (serdes for join state)
        KStream<String, String> enriched = highValue.join(customers,
                (order, customer) -> String.format(
                    "{\"order\":\"%s\",\"customer\":\"%s\",\"amount\":%.2f,\"tier\":\"%s\"}",
                    order.getOrderId(), customer.getName(),
                    order.getAmount(), customer.getTier()),
                Joined.with(Serdes.String(), orderSerde, customerSerde));

        // ─── PROCESSOR: Group and aggregate ────────────────────────
        // Boundary: Grouped (serdes for repartition topic — but no repartition here,
        //           key unchanged, groupByKey doesn't create repartition topic)
        KTable<String, Long> orderCounts = highValue
                .groupByKey(Grouped.with(Serdes.String(), orderSerde))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("high-value-counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));
        // Boundary: Materialized (Long → bytes for RocksDB)

        // ─── SINK: Write results to output topics ──────────────────
        // Boundary: Produced (Java → bytes)
        enriched.to("enriched-high-value-orders",
                Produced.with(Serdes.String(), Serdes.String()));

        orderCounts.toStream().to("high-value-order-counts",
                Produced.with(Serdes.String(), Serdes.Long()));

        return orders;
    }
}
```

### What this topology creates:

```
Topics read:      orders, customers
Topics written:   enriched-high-value-orders, high-value-order-counts

RocksDB stores:   customer-store        (for KTable source)
                  high-value-counts     (for count() aggregation)

Changelog topics: {app-id}-customer-store-changelog
                  {app-id}-high-value-counts-changelog

Repartition topics: NONE (because we used groupByKey, not groupBy)
```

---

# SECTION 5: QUICK REFERENCE

## 5.1 Serde Cheat Sheet

```
┌─────────────────────┬──────────────────────┬──────────────────────────────────────┐
│ Data Type           │ Serde                │ When to Use                          │
├─────────────────────┼──────────────────────┼──────────────────────────────────────┤
│ Text/JSON string    │ Serdes.String()      │ Keys, simple values, raw JSON        │
│ Counter/Count       │ Serdes.Long()        │ After count(), numeric aggregations  │
│ Integer ID          │ Serdes.Integer()     │ Numeric keys                         │
│ Decimal amounts     │ Serdes.Double()      │ Financial calculations               │
│ Java object (POJO)  │ new JsonSerde<>(T)   │ Order, Customer, any custom class    │
│ Raw bytes           │ Serdes.ByteArray()   │ Protobuf, Avro (without registry)    │
│ Avro + Registry     │ KafkaAvroSerde       │ Schema Registry integration          │
└─────────────────────┴──────────────────────┴──────────────────────────────────────┘
```

## 5.2 Serde Boundary Cheat Sheet

```
┌──────────────────┬──────────────────────┬────────────────────────────────────────┐
│ Boundary         │ Config Class         │ Direction                              │
├──────────────────┼──────────────────────┼────────────────────────────────────────┤
│ Read from topic  │ Consumed.with(K, V)  │ DESERIALIZE: bytes → Java             │
│ Write to topic   │ Produced.with(K, V)  │ SERIALIZE:   Java → bytes             │
│ Group/Repartition│ Grouped.with(K, V)   │ BOTH: Java→bytes→Java (repartition)   │
│ Join state       │ Joined.with(K,V1,V2) │ SERIALIZE: Java→bytes (internal state)│
│ State store      │ Materialized.with()  │ BOTH: Java↔bytes (RocksDB read/write) │
└──────────────────┴──────────────────────┴────────────────────────────────────────┘
```

## 5.3 Materialization Cheat Sheet

```
┌─────────────────────────────────────────────────────────────────────┐
│ Materialized.as("name")     → Name the store (queryable via REST)  │
│ .withKeySerde(serde)        → How to store KEY in RocksDB          │
│ .withValueSerde(serde)      → How to store VALUE in RocksDB        │
│ .withRetention(duration)    → How long to keep windowed data       │
│ .withLoggingEnabled(config) → Custom changelog topic settings      │
│ .withLoggingDisabled()      → No changelog (data lost on crash!)   │
│ .withCachingEnabled()       → Deduplicate rapid updates (DEFAULT)  │
│ .withCachingDisabled()      → Forward every single update          │
├─────────────────────────────────────────────────────────────────────┤
│ OPTIONAL: All stateful ops create stores even without Materialized │
│ USE IT WHEN: You need a name, custom serdes, or custom config      │
└─────────────────────────────────────────────────────────────────────┘
```


# Kafka Streams — Syntax Deep Dive (From Basics to Advanced)

> This document teaches you to BUILD Kafka Streams topologies step by step.
> Architecture is already covered in the other notes. Here = pure syntax + working examples.

---

# PART 1: THE FOUNDATION

## 1.1 Where Does Your Code Live?

In Kafka Streams, you write a **topology** — a pipeline that reads from Kafka, processes, and writes back.

```
Kafka Topic (source) → YOUR CODE (processors) → Kafka Topic (sink)
```

There are **two ways** to write this code:

### Way 1: Spring Boot (what we use)
```java
@Configuration
@EnableKafkaStreams       // ← tells Spring to manage the Kafka Streams lifecycle
public class MyTopology {

    @Bean
    public KStream<String, String> processOrders(StreamsBuilder builder) {
        // StreamsBuilder is auto-injected by Spring
        // You define your topology here
        KStream<String, String> stream = builder.stream("input-topic");
        stream.to("output-topic");
        return stream;
    }
}
```

### Way 2: Plain Java (no Spring)
```java
public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    StreamsBuilder builder = new StreamsBuilder();

    // Define topology
    KStream<String, String> stream = builder.stream("input-topic");
    stream.to("output-topic");

    // Build and start
    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();
}
```

**Both do the same thing.** Spring just manages the lifecycle for you.

The key object is `StreamsBuilder` — everything starts from it.

---

## 1.2 SERDES — You MUST Understand This First

### The Problem

Kafka stores everything as **bytes** (`byte[]`). Your Java code works with `String`, `Long`, `Order`, etc.

Someone needs to convert between the two. That someone is a **Serde** (Serializer + Deserializer).

```
Your Code                  Kafka Broker
─────────                  ────────────
String "hello"  ──serialize──►  [68 65 6C 6C 6F]  (bytes)
String "hello"  ◄──deserialize──  [68 65 6C 6C 6F]  (bytes)
```

### Without Serde = ClassCastException at runtime
This is the #1 error beginners hit. Kafka Streams won't know HOW to read/write your data.

### Built-in Serdes (for simple types)
```java
Serdes.String()      // String ↔ byte[]
Serdes.Long()        // Long ↔ byte[]
Serdes.Integer()     // Integer ↔ byte[]
Serdes.Double()      // Double ↔ byte[]
Serdes.ByteArray()   // byte[] ↔ byte[] (no conversion)
```

### When do you need Serdes?

**Answer: At every "boundary" where data crosses between your code and Kafka.**

There are exactly 3 boundaries:

```
Boundary 1: SOURCE (reading FROM Kafka)     → need Deserializer
Boundary 2: SINK   (writing TO Kafka)       → need Serializer
Boundary 3: STATE STORE (RocksDB read/write) → need both
```

### How to specify Serdes

**Option A: Default Serdes (in application.properties)**
```properties
spring.kafka.streams.properties[default.key.serde]=org.apache.kafka.common.serialization.Serdes$StringSerde
spring.kafka.streams.properties[default.value.serde]=org.apache.kafka.common.serialization.Serdes$StringSerde
```
This means: "Unless I say otherwise, treat ALL keys as String and ALL values as String."

**Option B: Per-Operation Serdes (overrides default)**
```java
// At SOURCE
builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));

// At SINK
stream.to("topic", Produced.with(Serdes.String(), Serdes.String()));

// At STATE STORE
.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("my-store")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Long()));
```

**Rule: Always use Option B (explicit).** Default serdes cause silent bugs when types don't match.

---

### 1.3 The Three Serde Scenarios You'll Face

#### Scenario 1: Both key and value are simple types
```java
// Key = String, Value = String → both have built-in Serdes
builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
```

#### Scenario 2: Value is a JSON object (most common in real apps)
```java
// Key = String (orderId), Value = JSON string like {"orderId":"001","amount":500}
// TWO approaches:

// Approach A: Read as String, parse manually (SIMPLER, what we use)
KStream<String, String> stream = builder.stream("orders",
        Consumed.with(Serdes.String(), Serdes.String()));

// Then parse inside processors:
stream.filter((key, jsonString) -> {
    Order order = objectMapper.readValue(jsonString, Order.class);
    return order.getAmount() > 100;

---

# PART 2: SOURCE — Reading From Kafka

The **source** is the first node in your topology. It reads records from a Kafka topic.

`StreamsBuilder` gives you **3 ways** to read a topic. Each one gives you a DIFFERENT abstraction.
Understanding WHEN to use which is critical.

---

## 2.1 KStream — The Event Stream

### What is it?
A `KStream` treats every record as an **independent, immutable event**.

Think of it as a **log of things that happened**:
```
offset 0: { orderId: "001", status: "CREATED" }
offset 1: { orderId: "002", status: "CREATED" }
offset 2: { orderId: "001", status: "SHIPPED" }   ← same orderId, but it's a NEW event
offset 3: { orderId: "001", status: "DELIVERED" }  ← another NEW event for same orderId
```

**Key point:** offset 0 and offset 2 both have key="001", but KStream sees them as **3 separate events**, not "updates" to the same order. Every record matters.

### Syntax
```java
// Basic — uses default serdes from config
KStream<String, String> stream = builder.stream("order-events");

// Explicit serdes (RECOMMENDED — always know what types you're working with)
KStream<String, String> stream = builder.stream("order-events",
        Consumed.with(Serdes.String(), Serdes.String()));

// Read from multiple topics
KStream<String, String> stream = builder.stream(
        List.of("order-events", "payment-events"),
        Consumed.with(Serdes.String(), Serdes.String()));

// Read from topics matching a pattern
KStream<String, String> stream = builder.stream(
        Pattern.compile("order-.*"),    // matches order-events, order-returns, etc.
        Consumed.with(Serdes.String(), Serdes.String()));
```

### Complete Working Example: Read orders, print them
```java
@Configuration
public class SimpleStreamExample {

    @Bean
    public KStream<String, String> readAndPrint(StreamsBuilder builder) {

        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // peek = look at each record without changing it
        orders.peek((key, value) ->
                System.out.println("Got order: key=" + key + " value=" + value));

        orders.to("processed-orders",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### When to use KStream?
- **Events that happened** — orders placed, payments made, clicks, logs
- **Every occurrence matters** — you want to count ALL orders, not just the latest
- **Append-only data** — new records keep coming, old ones are never "updated"

---

## 2.2 KTable — The Changelog Table

### What is it?
A `KTable` treats the topic as a **table** where each key has only **ONE current value**.

Same topic, but interpreted differently:
```
offset 0: key="001", value="CREATED"     ← KTable state: { "001": "CREATED" }
offset 1: key="002", value="CREATED"     ← KTable state: { "001": "CREATED", "002": "CREATED" }
offset 2: key="001", value="SHIPPED"     ← KTable state: { "001": "SHIPPED", "002": "CREATED" }
                                            ↑ "001" was UPDATED, not added again
offset 3: key="001", value=null          ← KTable state: { "002": "CREATED" }
                                            ↑ null value = DELETE (tombstone)
```

**Key point:** KTable only keeps the **LATEST value per key**. Old values are overwritten. It's like a database table where the key is the primary key.

### Syntax
```java
// Basic
KTable<String, String> table = builder.table("user-profiles",
        Consumed.with(Serdes.String(), Serdes.String()));

// With a named state store (so you can QUERY it later via REST)
KTable<String, String> table = builder.table("user-profiles",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.as("user-profiles-store"));
```

### What "Materialized" means
When you add `Materialized.as("store-name")`:
- A **RocksDB store** is created on disk with that name
- A **changelog topic** is created: `{app-id}-store-name-changelog`
- You can **query** the store at runtime (interactive queries)

Without Materialized: the table still works, but you can't query it from outside.

### Complete Working Example: Maintain latest user profile
```java
@Configuration
public class UserProfileTable {

    @Bean
    public KTable<String, String> userProfiles(StreamsBuilder builder) {

        // This reads the "user-profiles" topic as a TABLE
        // If user "john" has 5 records in the topic, KTable keeps ONLY the last one
        KTable<String, String> profiles = builder.table("user-profiles",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("user-profiles-store"));

        // Convert to stream to see changes (for debugging)
        profiles.toStream().peek((userId, profile) ->
                System.out.println("Profile updated: " + userId + " → " + profile));

        return profiles;
    }
}
```

### When to use KTable?
- **Current state matters, not history** — user's current address, product's current price
- **Latest-value-per-key** — config settings, feature flags
- **Lookup/reference data** — enrich events with current customer info
- **The topic IS a changelog** — CDC (Change Data Capture) from a database

### KStream vs KTable — The Same Topic, Two Meanings

```
Topic "user-updates" has these records:
  key=john, value={"city":"NYC"}
  key=jane, value={"city":"LA"}
  key=john, value={"city":"SF"}

If you read as KStream:
  → 3 events: john/NYC, jane/LA, john/SF
  → count("john") = 2

If you read as KTable:
  → 2 entries: john/SF (latest), jane/LA
  → john's value = "SF" (NYC was overwritten)
```

**The topic doesn't change. YOUR INTERPRETATION changes.**

---

## 2.3 GlobalKTable — Full Copy Everywhere

### The Problem GlobalKTable Solves

Imagine you have:
- `orders` topic with 6 partitions → 2 app instances, each handles 3 partitions
- `countries` topic with 3 partitions → used for lookup

With regular `KTable`:
- Instance 1 gets countries partitions 0,1 → only sees SOME countries
- Instance 2 gets countries partition 2 → only sees OTHER countries
- An order on instance 1 might need a country that's on instance 2 → **JOIN FAILS**

With `GlobalKTable`:
- Instance 1 gets ALL countries (all 3 partitions)
- Instance 2 gets ALL countries (all 3 partitions)
- Every instance can look up ANY country → **JOIN ALWAYS WORKS**

### Syntax
```java
GlobalKTable<String, String> countries = builder.globalTable("countries",
        Consumed.with(Serdes.String(), Serdes.String()));
```

### When to use GlobalKTable?
- **Small reference data** — countries, currencies, config, categories
- **Need to join without co-partitioning** — topics have different partition counts
- **Data fits in memory/disk on EVERY instance** — if 100GB, don't use GlobalKTable

### When NOT to use GlobalKTable?
- Large datasets → eats memory/disk on every instance
- Frequently changing data → every instance must process every update
- If you can co-partition → use regular KTable instead (more scalable)

---

## 2.4 Source Summary

```
┌─────────────────────────────────────────────────────────────────────┐
│                     builder.stream("topic")                        │
│                         → KStream                                  │
│                                                                     │
│  "Every record is an independent event"                            │
│  Use for: orders, payments, clicks, logs                           │
│  3 records with key="001" = 3 events                               │
├─────────────────────────────────────────────────────────────────────┤
│                     builder.table("topic")                         │
│                         → KTable                                   │
│                                                                     │
│  "Only the latest value per key matters"                           │
│  Use for: user profiles, product catalog, current state            │
│  3 records with key="001" = 1 entry (the latest)                   │
│  Backed by RocksDB state store                                     │
├─────────────────────────────────────────────────────────────────────┤
│                  builder.globalTable("topic")                      │
│                      → GlobalKTable                                │
│                                                                     │
│  "Full copy on EVERY app instance"                                 │
│  Use for: small lookup data (countries, config)                    │
│  Same as KTable but not partitioned across instances               │
└─────────────────────────────────────────────────────────────────────┘
```



---

# PART 3: STATELESS OPERATIONS ON KStream

"Stateless" means these operations:
- Do NOT create RocksDB stores
- Do NOT create changelog topics
- Process each record independently (no memory of previous records)
- Are **cheap** — no disk, no network overhead

We'll cover each one with a **complete working example** using this input topic:

```
Topic: "order-events"
Records:
  key=order-001, value={"orderId":"order-001","customerId":"cust-A","productId":"laptop","quantity":1,"totalAmount":1500.0,"status":"CREATED"}
  key=order-002, value={"orderId":"order-002","customerId":"cust-B","productId":"mouse","quantity":5,"totalAmount":50.0,"status":"CREATED"}
  key=order-003, value={"orderId":"order-003","customerId":"cust-A","productId":"tablet","quantity":2,"totalAmount":450.0,"status":"CREATED"}
```

---

## 3.1 filter() — Keep only records that match a condition

### What it does
Takes a predicate (condition). If true → record passes through. If false → record is dropped.

### Syntax
```java
KStream<String, String> filtered = stream.filter(
        (key, value) -> condition);
```

### Complete Example: Keep only high-value orders (> $100)
```java
@Configuration
public class FilterExample {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> filterTopology(StreamsBuilder builder) {

        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // FILTER: keep only orders where totalAmount > 100
        KStream<String, String> highValueOrders = orders.filter((key, value) -> {
            try {
                Order order = mapper.readValue(value, Order.class);
                return order.getTotalAmount() > 100;
            } catch (Exception e) {
                return false;   // bad JSON → drop it
            }
        });

        // Result: order-001 (1500) ✅, order-002 (50) ❌, order-003 (450) ✅

        highValueOrders.to("high-value-orders",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### filterNot() — Inverse of filter
```java
// Keep records that DON'T match the condition
KStream<String, String> lowValueOrders = orders.filterNot((key, value) -> {
    Order order = mapper.readValue(value, Order.class);
    return order.getTotalAmount() > 100;
});
// Result: only order-002 (50)
```

---

## 3.2 mapValues() — Transform the value, keep the key

### What it does
Changes the VALUE of each record. The KEY stays the same.

### Why this matters
Since the key doesn't change, Kafka Streams knows the record still belongs to the same partition.
**No repartition needed.** This is cheap.

### Syntax
```java
KStream<String, NewValueType> mapped = stream.mapValues(
        value -> transformedValue);
```

### Complete Example: Extract just the status from each order
```java
@Configuration
public class MapValuesExample {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> mapValuesTopology(StreamsBuilder builder) {

        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // mapValues: full JSON → just the status string
        KStream<String, String> statuses = orders.mapValues(value -> {
            try {
                Order order = mapper.readValue(value, Order.class);
                return order.getStatus();   // "CREATED", "SHIPPED", etc.
            } catch (Exception e) {
                return "UNKNOWN";
            }
        });

        // Input:  key=order-001, value={"orderId":"order-001",...,"status":"CREATED"}
        // Output: key=order-001, value="CREATED"

        statuses.to("order-statuses",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

---

## 3.3 map() — Transform BOTH key and value

### What it does
Changes BOTH the key AND the value. Returns a new `KeyValue` pair.

### ⚠️ THE REPARTITION WARNING
**If you change the key, the record might now belong to a DIFFERENT partition.**

Why? Because Kafka assigns partitions based on `hash(key) % numPartitions`.
New key → new hash → possibly different partition.

If you later do `groupByKey()` or a join, Kafka Streams MUST repartition:
- Creates an internal repartition topic
- Writes records to it with the new key
- Reads them back from the correct partition

**This costs: network I/O + disk I/O + latency.**

### Rule: Use `mapValues()` if you only need to change the value. Use `map()` ONLY when you genuinely need a new key.

### Syntax
```java
KStream<NewKeyType, NewValueType> mapped = stream.map(
        (key, value) -> KeyValue.pair(newKey, newValue));
```

### Complete Example: Re-key orders by customerId
```java
@Configuration
public class MapExample {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> mapTopology(StreamsBuilder builder) {

        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // map: change key from orderId → customerId
        KStream<String, String> customerKeyed = orders.map((key, value) -> {
            try {
                Order order = mapper.readValue(value, Order.class);
                return KeyValue.pair(order.getCustomerId(), value);  // new key!
            } catch (Exception e) {
                return KeyValue.pair("UNKNOWN", value);
            }
        });

        // Input:  key=order-001, value={...customerId:"cust-A"...}
        // Output: key=cust-A,    value={...customerId:"cust-A"...}
        //         ↑ KEY CHANGED → if you groupByKey next, repartition will happen

        customerKeyed.to("orders-by-customer",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

---

## 3.4 selectKey() — Change only the key (shortcut for map)

### What it does
Same as `map()` but ONLY changes the key. Value stays unchanged.
It's just syntactic sugar — still triggers repartition.

```java
KStream<String, String> rekeyed = orders.selectKey((key, value) -> {
    Order order = mapper.readValue(value, Order.class);
    return order.getCustomerId();   // new key = customerId
});

// Equivalent to:
// orders.map((key, value) -> KeyValue.pair(parseCustomerId(value), value));
```

---

## 3.5 flatMap() / flatMapValues() — One input → multiple outputs

### What it does
A single input record can produce **zero, one, or many** output records.

### flatMapValues() — Multiple outputs, key stays same (✅ no repartition)
```java
// Example: one order with multiple items → one record per item
KStream<String, String> items = orders.flatMapValues(value -> {
    Order order = mapper.readValue(value, Order.class);
    // Return a LIST of values
    List<String> result = new ArrayList<>();
    for (int i = 0; i < order.getQuantity(); i++) {
        result.add(order.getProductId() + "-item-" + i);
    }
    return result;
});

// Input:  key=order-001, value={productId:"laptop", quantity:3}
// Output: key=order-001, value="laptop-item-0"
//         key=order-001, value="laptop-item-1"
//         key=order-001, value="laptop-item-2"
```

### flatMap() — Multiple outputs WITH new keys (⚠️ repartition)
```java
// Example: split a sentence into words, each word becomes its own key
KStream<String, String> words = sentences.flatMap((key, sentence) -> {
    List<KeyValue<String, String>> result = new ArrayList<>();
    for (String word : sentence.split(" ")) {
        result.add(KeyValue.pair(word, word));   // each word = new key
    }
    return result;
});

// Input:  key=line1, value="hello world"
// Output: key=hello, value="hello"
//         key=world, value="world"
```

---

## 3.6 peek() — Look at records without changing them

### What it does
Side effect only. The record passes through **unchanged**. Used for logging, debugging, metrics.

```java
KStream<String, String> stream = orders.peek((key, value) ->
        System.out.println("[DEBUG] Processing: key=" + key));

// The record is NOT modified. peek() returns the same stream.
```

**Don't use peek() for business logic.** It's meant for observation only.

---

## 3.7 split() (branch) — Route records to different streams

### What it does
Splits one stream into multiple streams based on conditions. Like an `if-else` for streams.

### Complete Example: Route orders by value tier
```java
@Configuration
public class BranchExample {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> branchTopology(StreamsBuilder builder) {

        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Split into 3 streams
        Map<String, KStream<String, String>> branches = orders
                .split(Named.as("tier-"))
                .branch((key, value) -> parseAmount(value) > 1000,  Branched.as("premium"))
                .branch((key, value) -> parseAmount(value) > 100,   Branched.as("standard"))
                .defaultBranch(Branched.as("budget"));

        // Get each branch
        KStream<String, String> premium  = branches.get("tier-premium");   // > $1000
        KStream<String, String> standard = branches.get("tier-standard");  // $100-$1000
        KStream<String, String> budget   = branches.get("tier-budget");    // < $100

        // Send each to different topic
        premium.to("premium-orders",   Produced.with(Serdes.String(), Serdes.String()));
        standard.to("standard-orders", Produced.with(Serdes.String(), Serdes.String()));
        budget.to("budget-orders",     Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }

    private double parseAmount(String json) {
        try { return mapper.readValue(json, Order.class).getTotalAmount(); }
        catch (Exception e) { return 0; }
    }
}
```

```
Input:
  order-001: $1500 → premium-orders  ✅
  order-002: $50   → budget-orders   ✅
  order-003: $450  → standard-orders ✅
```

---

## 3.8 merge() — Combine two streams into one

### What it does
Opposite of split. Takes two streams and unions them into one. No processing, just merging.

```java
KStream<String, String> allOrders = premiumOrders.merge(standardOrders);
// Now allOrders has records from BOTH streams
```

---

## 3.9 repartition() — Force records to a new partition layout

### What it does
Explicitly writes records to an internal repartition topic and reads them back.
Use when you need to change partition count or force co-partitioning before a join.

```java
KStream<String, String> repartitioned = orders.repartition(
        Repartitioned.<String, String>as("orders-repartitioned")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withNumberOfPartitions(6));   // new partition count
```

---

## 3.10 Stateless Operations Summary

```
┌────────────────┬──────────────────────────────────────────────────────┐
│ Operation      │ What it does                                        │
├────────────────┼──────────────────────────────────────────────────────┤
│ filter()       │ Keep records matching condition                     │
│ filterNot()    │ Keep records NOT matching condition                  │
│ mapValues()    │ Transform value, keep key (✅ no repartition)        │
│ map()          │ Transform key AND value (⚠️ triggers repartition)   │
│ selectKey()    │ Change key only (⚠️ triggers repartition)           │
│ flatMapValues()│ One → many records, keep key (✅ no repartition)    │
│ flatMap()      │ One → many records, new keys (⚠️ repartition)      │
│ peek()         │ Side effect (logging), no change                    │
│ split/branch() │ Route to multiple streams                           │
│ merge()        │ Combine two streams                                 │
│ repartition()  │ Force new partition layout                          │
└────────────────┴──────────────────────────────────────────────────────┘

KEY RULE:
  Operations that CHANGE THE KEY → mark stream for repartition
  Operations that KEEP THE KEY   → no repartition, cheap

  ✅ Prefer: mapValues, flatMapValues, filter
  ⚠️ Use carefully: map, flatMap, selectKey, groupBy
```




---

# PART 4: STATEFUL OPERATIONS — Grouping & Aggregation

## 4.1 Why Do We Need Grouping?

Let's say you have this stream of orders:
```
key=order-001, value={customerId: "alice", amount: 100}
key=order-002, value={customerId: "bob",   amount: 200}
key=order-003, value={customerId: "alice", amount: 300}
key=order-004, value={customerId: "alice", amount: 50}
```

Now you want: **"How many orders did each customer place?"**

The answer is: alice=3, bob=1

But there's a problem. These records might be on DIFFERENT partitions:
```
Partition 0: order-001 (alice), order-004 (alice)
Partition 1: order-002 (bob)
Partition 2: order-003 (alice)
```

To count "all of alice's orders", you need ALL of alice's records on the SAME partition, processed by the SAME task. That's what **grouping** does.

### The Two-Step Process

**EVERY stateful operation in Kafka Streams follows this pattern:**

```
Step 1: GROUP    → "Gather all records with the same key together"
Step 2: AGGREGATE → "Do something with those grouped records" (count, sum, etc.)
```

**Step 1 is MANDATORY.** You cannot skip it. You cannot call `.count()` on a raw `KStream`.
Why? Because without grouping, Kafka Streams doesn't know WHICH key to count by.

```java
// ❌ WRONG — won't compile
stream.count();

// ✅ CORRECT — group first, then count
stream.groupByKey().count();
```

---

## 4.2 Grouping — groupByKey() vs groupBy()

### groupByKey() — Group by the EXISTING key (no repartition)

```java
KGroupedStream<String, String> grouped = orders.groupByKey(
        Grouped.with(Serdes.String(), Serdes.String()));
```

**When to use:** When the key ALREADY IS what you want to group by.

Example: If your key is `customerId` and you want to count per customer → `groupByKey()`.

**What happens internally:** Nothing expensive. Records already have the right key.
No new topic created. No data movement.

### groupBy() — Group by a NEW key (⚠️ causes repartition)

```java
KGroupedStream<String, String> grouped = orders.groupBy(
        (key, value) -> parseCustomerId(value),     // new key = customerId
        Grouped.with(Serdes.String(), Serdes.String()));
```

**When to use:** When you want to group by something OTHER than the current key.

Example: If your key is `orderId` but you want to count per `customerId` → `groupBy()`.

**What happens internally:**
1. Creates an internal repartition topic: `{app-id}-KSTREAM-KEY-SELECT-...-repartition`
2. Re-produces every record to this topic with the NEW key
3. Re-consumes from this topic so records with same key land on same partition

**This is expensive:** network I/O, disk I/O, added latency.

### groupByKey vs groupBy Decision

```
Is the current key what I want to group by?
  YES → groupByKey()  ✅ cheap
  NO  → groupBy()     ⚠️ expensive (repartition)
```

### What is KGroupedStream?

After grouping, you get a `KGroupedStream`. This is NOT a stream you can do `.filter()` or `.map()` on.
It's a **transitional object** that ONLY exists to call aggregation methods on:

```java
KGroupedStream<String, String> grouped = stream.groupByKey();

// You can ONLY call these on KGroupedStream:
grouped.count(...)          // → KTable<String, Long>
grouped.reduce(...)         // → KTable<String, V>
grouped.aggregate(...)      // → KTable<String, VR>
grouped.windowedBy(...)     // → for windowed aggregation (covered in Part 6)
```

---

## 4.3 count() — Count records per key

### What it does
Counts how many records have been seen for each key.

### What it creates
- **RocksDB state store** on disk (to persist counts across restarts)
- **Changelog topic** in Kafka (to backup the state store)
- Returns a **KTable** (because count = "current value per key" = table)

### Complete Working Example
```java
@Configuration
public class CountExample {

    @Bean
    public KStream<String, String> countTopology(StreamsBuilder builder) {

        // SOURCE: read orders (key = customerId)
        KStream<String, String> orders = builder.stream("orders",
                Consumed.with(Serdes.String(), Serdes.String()));

        // STEP 1: Group by existing key (customerId)
        KGroupedStream<String, String> grouped = orders.groupByKey(
                Grouped.with(Serdes.String(), Serdes.String()));

        // STEP 2: Count
        KTable<String, Long> counts = grouped.count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // SINK: write counts to output topic
        counts.toStream()
                .mapValues(count -> String.valueOf(count))   // Long → String
                .to("order-counts", Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### What happens record by record:
```
Record 1: key=alice, value={...}
  → count("alice") = 1
  → RocksDB: { "alice": 1 }
  → Changelog topic: key=alice, value=1

Record 2: key=bob, value={...}
  → count("bob") = 1
  → RocksDB: { "alice": 1, "bob": 1 }
  → Changelog topic: key=bob, value=1

Record 3: key=alice, value={...}
  → count("alice") = 2
  → RocksDB: { "alice": 2, "bob": 1 }
  → Changelog topic: key=alice, value=2

Output topic "order-counts":
  key=alice, value="1"
  key=bob,   value="1"
  key=alice, value="2"   ← updated count
```

### Is Materialized mandatory?
**For count: NO, but highly recommended.**
- Without it: Kafka Streams auto-generates a store name. You can't query it.
- With it: You name the store AND specify serdes explicitly. You CAN query it via REST.

```java
// Without Materialized — works but store has auto-generated name
grouped.count();

// With Materialized — named store, explicit serdes, queryable
grouped.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("my-store")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Long()));
```

---

## 4.4 reduce() — Combine values into one

### What it does
Takes two values and combines them into one. Like folding a list.

### Key constraint: input type = output type
`reduce()` takes `(V, V) → V`. The result must be the SAME type as the input.

### Complete Working Example: Keep the latest order per customer
```java
@Configuration
public class ReduceExample {

    @Bean
    public KStream<String, String> reduceTopology(StreamsBuilder builder) {

        // Key = customerId, Value = order JSON
        KStream<String, String> orders = builder.stream("orders",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Group by customerId (already the key)
        KGroupedStream<String, String> grouped = orders.groupByKey(
                Grouped.with(Serdes.String(), Serdes.String()));

        // Reduce: keep only the LATEST order per customer
        KTable<String, String> latestOrders = grouped.reduce(
                (oldValue, newValue) -> newValue,   // always keep the newer one
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("latest-order-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        // Result: for each customer, only their most recent order survives

        latestOrders.toStream().to("latest-orders",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### Another reduce example: find max amount
```java
// Assume value is already a Long (amount)
KTable<String, Long> maxPerCustomer = groupedAmounts.reduce(
        (amount1, amount2) -> Math.max(amount1, amount2),
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("max-amount-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));
```

### When to use reduce vs aggregate?
- **reduce:** When output type = input type (keep latest, find max, concatenate strings)
- **aggregate:** When output type ≠ input type (input=JSON string, output=Long count)

---

## 4.5 aggregate() — The most flexible aggregation

### What it does
Most powerful aggregation. Can change the output type. Has an initializer + aggregator.

### The 3 parts:
1. **Initializer** — "What's the starting value for a key we've never seen?" → runs ONCE per new key
2. **Aggregator** — "How do we combine a new record with the existing aggregate?" → runs for EVERY record
3. **Materialized** — "Where to store the result" (RocksDB store name + serdes)

### Complete Working Example: Bank Balance (sum of amounts)
```java
//@Configuration
public class AggregateExample {

    private static final ObjectMapper mapper = new ObjectMapper();

    //@Bean
    public KStream<String, String> bankBalanceTopology(StreamsBuilder builder) {

        // Key = customerName, Value = JSON like {"name":"alice","amount":100}
        KStream<String, String> transactions = builder.stream("transactions",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Group by key (customerName)
        KGroupedStream<String, String> grouped = transactions.groupByKey(
                Grouped.with(Serdes.String(), Serdes.String()));

        // Aggregate: running sum of amounts
        KTable<String, Long> balances = grouped.aggregate(

                // 1. INITIALIZER: first time we see a customer → start at 0
                () -> 0L,

                // 2. AGGREGATOR: for each new transaction, add amount to current balance
                (customerName, transactionJson, currentBalance) -> {
                    try {
                        int amount = mapper.readValue(transactionJson, Transaction.class).getAmount();
                        return currentBalance + amount;
                    } catch (Exception e) {
                        return currentBalance;   // bad JSON → don't change balance
                    }
                },

                // 3. MATERIALIZED: name the RocksDB store + specify serdes
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("bank-balance-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())     // ← output is Long, NOT String!
        );

        // Write balances to output topic
        balances.toStream()
                .mapValues(balance -> String.valueOf(balance))
                .to("bank-balance", Produced.with(Serdes.String(), Serdes.String()));

        return transactions;
    }
}
```

### What happens step by step:
```
Transaction 1: key=alice, value={"name":"alice","amount":100}
  → Initializer runs: 0L (first time seeing "alice")
  → Aggregator runs: 0 + 100 = 100
  → RocksDB: { "alice": 100 }
  → Changelog: key=alice, value=100

Transaction 2: key=bob, value={"name":"bob","amount":500}
  → Initializer runs: 0L (first time seeing "bob")
  → Aggregator runs: 0 + 500 = 500
  → RocksDB: { "alice": 100, "bob": 500 }

Transaction 3: key=alice, value={"name":"alice","amount":200}
  → Initializer does NOT run (alice already exists)
  → Aggregator runs: 100 + 200 = 300
  → RocksDB: { "alice": 300, "bob": 500 }

Transaction 4: key=alice, value={"name":"alice","amount":-50}
  → Aggregator runs: 300 + (-50) = 250
  → RocksDB: { "alice": 250, "bob": 500 }
```

### Is aggregate MANDATORY?
No. You can use `count()` or `reduce()` instead. But aggregate is the most FLEXIBLE:

```
count()     → always counts (no customization)
reduce()    → custom logic, but output type = input type
aggregate() → custom logic, output type can be ANYTHING

count() and reduce() are just special cases of aggregate().
Internally, count() is roughly: aggregate(() -> 0L, (k, v, count) -> count + 1)
```

---

## 4.6 Grouping When Key Needs to Change — Full Example

A common pattern: your key is `orderId`, but you want to count per `productId`.

```java
//@Configuration
public class GroupByExample {

    private static final ObjectMapper mapper = new ObjectMapper();

   // @Bean
    public KStream<String, String> productCountTopology(StreamsBuilder builder) {

        // Key = orderId, Value = order JSON
        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // We want to count per PRODUCT, but key is orderId.
        // Two options:

        // OPTION A: selectKey + groupByKey
        KStream<String, String> productKeyed = orders.selectKey((orderId, value) -> {
            try { return mapper.readValue(value, Order.class).getProductId(); }
            catch (Exception e) { return "UNKNOWN"; }
        });
        KTable<String, Long> counts = productKeyed
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("product-count-store"));

        // OPTION B: groupBy (does the re-keying internally)
        KTable<String, Long> countsB = orders
                .groupBy((orderId, value) -> {
                    try { return mapper.readValue(value, Order.class).getProductId(); }
                    catch (Exception e) { return "UNKNOWN"; }
                }, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("product-count-store-b"));

        // BOTH options create a repartition topic internally.
        // BOTH produce the same result.
        // Option B is shorter. Option A is more explicit.

        counts.toStream()
                .mapValues(String::valueOf)
                .to("product-counts", Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### What gets created internally:
```
Repartition topic: order-analytics-KSTREAM-KEY-SELECT-0000000003-repartition
  → contains all orders re-keyed by productId

State store: product-count-store (RocksDB on disk)
  → contains: { "laptop": 5, "phone": 3, "tablet": 2 }

Changelog topic: order-analytics-product-count-store-changelog
  → backup of the state store
```

---

## 4.7 Part 4 Summary

```
STATEFUL OPERATION = GROUP + AGGREGATE

Step 1: GROUP (mandatory before any aggregation)
┌────────────────┬──────────────────────────────────────────────────┐
│ groupByKey()   │ Group by existing key. ✅ No repartition.        │
│ groupBy()      │ Group by new key.      ⚠️ Creates repartition.  │
└────────────────┴──────────────────────────────────────────────────┘

Step 2: AGGREGATE (pick one)
┌────────────────┬──────────────────────────────────────────────────┐
│ count()        │ Count records per key                           │
│                │ Input: any → Output: Long                       │
│                │ Simplest. No custom logic.                      │
├────────────────┼──────────────────────────────────────────────────┤
│ reduce()       │ Combine values. Input type = Output type.       │
│                │ (String, String) → String                       │
│                │ Use for: max, min, concat, keep latest          │
├────────────────┼──────────────────────────────────────────────────┤
│ aggregate()    │ Most flexible. Input type ≠ Output type allowed.│
│                │ Has initializer + aggregator.                   │
│                │ Use for: sum amounts, build complex objects     │
└────────────────┴──────────────────────────────────────────────────┘

All three produce a KTable (latest computed value per key).
All three create a RocksDB store + changelog topic.
```



---

# PART 5: KTable OPERATIONS

In Part 2, we learned that a KTable is a **changelog stream** — it tracks the LATEST value per key.
In Part 4, we saw that `count()`, `reduce()`, and `aggregate()` all produce KTables.

Now let's learn what operations you can do ON a KTable — and how they differ from KStream operations.

---

## 5.1 How KTable Differs From KStream (Recap + New Detail)

```
KStream: every record is an INDEPENDENT event (fact)
  key=alice, value=100  →  "alice deposited 100"
  key=alice, value=200  →  "alice deposited 200"
  Both records exist. Total = 300.

KTable: every record is an UPDATE to the LATEST state
  key=alice, value=100  →  "alice's balance IS 100"
  key=alice, value=200  →  "alice's balance IS 200" (replaces 100)
  Only the latest matters. Balance = 200.
```

**Why this matters for operations:**
When you do `filter()` on a KStream, you just drop records.
When you do `filter()` on a KTable, you might need to send a **tombstone** (null value) downstream
to indicate "this key no longer passes the filter."

---

## 5.2 KTable filter() — Filtering with tombstones

### What it does
Same as KStream filter — keeps records that match the condition.
BUT: if a key previously passed the filter and now DOESN'T, the KTable emits a **tombstone** (key=X, value=null)
to tell downstream: "this key is deleted from the table."

### Example: Keep only customers with balance > 500

```java
//@Configuration
public class KTableFilterExample {

   // @Bean
    public KStream<String, String> ktableFilterTopology(StreamsBuilder builder) {

        // SOURCE: read as KTable (compacted topic — latest balance per customer)
        KTable<String, Long> balances = builder.table("bank-balance",
                Consumed.with(Serdes.String(), Serdes.Long()));

        // FILTER: keep only balances > 500
        KTable<String, Long> highBalances = balances.filter(
                (customerId, balance) -> balance != null && balance > 500);

        // Write to output topic
        highBalances.toStream()
                .mapValues(String::valueOf)
                .to("high-balance-customers",
                        Produced.with(Serdes.String(), Serdes.String()));

        return builder.stream("bank-balance",
                Consumed.with(Serdes.String(), Serdes.String()));
    }
}
```

### What happens step by step:
```
Update 1: key=alice, value=600
  → 600 > 500? YES → emit: key=alice, value=600 ✅

Update 2: key=bob, value=300
  → 300 > 500? NO → nothing emitted (bob was never in the filtered table)

Update 3: key=alice, value=400   ← alice's balance dropped!
  → 400 > 500? NO
  → But alice WAS in the filtered table before!
  → Emit TOMBSTONE: key=alice, value=null  ← "alice is removed from high-balance table"

Update 4: key=alice, value=700
  → 700 > 500? YES → emit: key=alice, value=700 ✅ (alice is back)
```

### Why tombstones matter
Without tombstones, downstream consumers would think alice STILL has a high balance.
The tombstone explicitly says "alice no longer qualifies." This is how KTable maintains correctness.

---

## 5.3 KTable mapValues() — Transform values

### What it does
Transforms the value of each entry in the table. Key stays the same (always — KTable never allows key changes).

### Why KTable does NOT have map() or selectKey()
A KTable represents "latest value per key." If you changed the key, you'd need to:
1. Delete the old key entry
2. Insert a new key entry
3. Handle conflicts if the new key already exists

This is complex and error-prone. So Kafka Streams simply doesn't allow it.
**If you need to change a KTable's key, convert to KStream first:** `table.toStream().selectKey(...)`.

### Example: Convert balance from cents to dollars
```java
KTable<String, Long> balancesInCents = builder.table("bank-balance",
        Consumed.with(Serdes.String(), Serdes.Long()));

KTable<String, Double> balancesInDollars = balancesInCents.mapValues(
        cents -> cents / 100.0);

// Input:  key=alice, value=15000 (cents)
// Output: key=alice, value=150.0 (dollars)
```

---

## 5.4 KTable toStream() — Convert KTable back to KStream

### What it does
Converts a KTable into a KStream. Every update in the KTable becomes a record in the KStream.

### Why you need this
Many operations are only available on KStream (like `map()`, `flatMap()`, writing to a topic with `to()`).
Also, `to()` works differently on KTable vs KStream:
- KTable `to()` → creates a **compacted** topic (keeps only latest per key)
- KStream `to()` → creates a regular topic (keeps all records)

Most of the time, after aggregation you do `table.toStream().to(...)`.

```java
KTable<String, Long> counts = grouped.count(Materialized.as("count-store"));

// Convert to stream so we can write to a regular topic
KStream<String, Long> countStream = counts.toStream();

// Now you can use KStream operations
countStream
    .filter((key, count) -> count > 10)       // KStream filter (no tombstones)
    .mapValues(count -> "count=" + count)      // KStream mapValues
    .to("active-customers", Produced.with(Serdes.String(), Serdes.String()));
```

---

## 5.5 KTable suppress() — Control WHEN updates are emitted

### The problem suppress() solves
By default, KTable emits an update downstream for EVERY input record:

```
key=alice, value=1   → emit count=1
key=alice, value=2   → emit count=2
key=alice, value=3   → emit count=3    ← do we really need ALL of these?
```

If you're counting per minute, you might get 1000 intermediate updates.
You only want the FINAL count at the end of the window.

### What suppress() does
Holds back updates and only emits when a condition is met (usually: window closes).

### Example: Emit only the final count per window
```java
// Count orders per customer in 1-minute windows
KTable<Windowed<String>, Long> windowedCounts = orders
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        .count(Materialized.as("windowed-count-store"));

// SUPPRESS: emit only when the window closes
KTable<Windowed<String>, Long> finalCounts = windowedCounts.suppress(
        Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

// Without suppress: emits count=1, count=2, count=3, ..., count=1000
// With suppress:    emits only count=1000 (when the 1-minute window closes)
```

### suppress() options:
```
┌──────────────────────────────────┬──────────────────────────────────────────────┐
│ Suppressed.untilWindowCloses()   │ Emit only when window closes.               │
│                                  │ Most common. Used with windowed aggregation. │
├──────────────────────────────────┼──────────────────────────────────────────────┤
│ Suppressed.untilTimeLimit()      │ Emit at most once per time period.           │
│                                  │ Rate-limiting for non-windowed tables.       │
└──────────────────────────────────┴──────────────────────────────────────────────┘
```

### Buffer config:
```java
// Unbounded: use as much memory as needed (safe but can be costly)
Suppressed.BufferConfig.unbounded()

// Bounded: limit memory, choose what to do when full
Suppressed.BufferConfig.maxRecords(1000).shutDownWhenFull()
Suppressed.BufferConfig.maxBytes(64_000).emitEarlyWhenFull()
```

---

## 5.6 KGroupedTable — Aggregating a KTable (not KStream)

### What is KGroupedTable?
Just like `KGroupedStream` is what you get after grouping a KStream,
`KGroupedTable` is what you get after grouping a KTable.

```java
KTable<String, String> userProfiles = builder.table("user-profiles",
        Consumed.with(Serdes.String(), Serdes.String()));

// Group the TABLE by a new key (e.g., by city)
KGroupedTable<String, String> groupedByCity = userProfiles.groupBy(
        (userId, profile) -> KeyValue.pair(parseCity(profile), profile),
        Grouped.with(Serdes.String(), Serdes.String()));
```

### The Adder + Subtractor requirement

**This is the critical difference from KGroupedStream.**

When you aggregate a KGroupedStream (regular stream), records only get ADDED.
When you aggregate a KGroupedTable (table), records can be UPDATED or DELETED.

If alice moves from "NYC" to "LA":
1. The count for "NYC" must DECREASE by 1  ← this is the **subtractor**
2. The count for "LA" must INCREASE by 1   ← this is the **adder**

### Example: Count users per city
```java
KTable<String, Long> usersPerCity = groupedByCity.aggregate(
        // INITIALIZER: start at 0
        () -> 0L,

        // ADDER: new record arrives for this key → add to count
        (city, profile, currentCount) -> currentCount + 1,

        // SUBTRACTOR: old record is removed/updated → subtract from count
        (city, profile, currentCount) -> currentCount - 1,

        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("users-per-city")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
);
```

### Step-by-step example:
```
Initial state: empty

1. alice joins, city="NYC"
   → ADDER runs for "NYC": 0 + 1 = 1
   → State: { "NYC": 1 }

2. bob joins, city="NYC"
   → ADDER runs for "NYC": 1 + 1 = 2
   → State: { "NYC": 2 }

3. alice moves to "LA" (update: alice's city changes from NYC → LA)
   → SUBTRACTOR runs for "NYC": 2 - 1 = 1  ← remove alice from NYC count
   → ADDER runs for "LA": 0 + 1 = 1        ← add alice to LA count
   → State: { "NYC": 1, "LA": 1 }

4. bob is deleted (tombstone)
   → SUBTRACTOR runs for "NYC": 1 - 1 = 0
   → State: { "NYC": 0, "LA": 1 }
```

### Why KGroupedStream doesn't need a subtractor
A KStream is append-only. Records are never "updated" or "deleted."
- Event: "alice ordered a laptop" → that fact happened. You can't un-order it.
- So the aggregator only ever ADDS.

A KTable tracks current state. Records CAN be updated or deleted.
- State: "alice lives in NYC" → can change to "alice lives in LA."
- So you need both an adder (for the new state) AND a subtractor (for the old state).

---

## 5.7 Part 5 Summary

```
KTable Operations:
┌─────────────────┬───────────────────────────────────────────────────────────┐
│ filter()        │ Keep entries matching condition.                         │
│                 │ Emits TOMBSTONES when entries stop matching.             │
├─────────────────┼───────────────────────────────────────────────────────────┤
│ mapValues()     │ Transform values. No key change allowed.                │
│                 │ Use toStream().selectKey() if you need to change key.    │
├─────────────────┼───────────────────────────────────────────────────────────┤
│ toStream()      │ Convert KTable → KStream.                               │
│                 │ Every table update becomes a stream record.              │
├─────────────────┼───────────────────────────────────────────────────────────┤
│ suppress()      │ Hold back updates. Emit only on window close or time    │
│                 │ limit. Prevents intermediate result flooding.            │
├─────────────────┼───────────────────────────────────────────────────────────┤
│ groupBy()       │ Re-key and group the table → KGroupedTable              │
│                 │ Aggregation requires ADDER + SUBTRACTOR                 │
└─────────────────┴───────────────────────────────────────────────────────────┘

KEY INSIGHT:
  KStream operations are "fire and forget" — records flow through independently.
  KTable operations must maintain CORRECTNESS — tombstones, subtractors, etc.
  This is because KTable represents CURRENT STATE, not historical events.
```


---

# PART 6: JOINS

Joins combine data from TWO sources into ONE output. Just like SQL joins, but on streaming data.

## 6.1 Why Joins Are Important

Real-world data is always split across multiple topics:
- `orders` topic: orderId, customerId, productId, amount
- `customers` topic: customerId, name, email, tier
- `products` topic: productId, name, category, price

To send an email like "Hi Alice, your laptop has shipped!" you need data from BOTH orders AND customers.
That's a **join**.

---

## 6.2 The Co-Partitioning Requirement

**Before ANY join (except GlobalKTable), Kafka Streams requires CO-PARTITIONING:**

```
Co-partitioning means:
1. Both topics must have the SAME number of partitions
2. Both topics must use the SAME partitioning strategy (default = hash(key) % numPartitions)
3. Records that need to be joined must have the SAME KEY
```

### Why?
Kafka Streams assigns one task per partition. A task can only access records from its own partition.
If alice's order is on partition 0 of `orders`, but alice's profile is on partition 2 of `customers`,
no single task can see both → join fails.

### How to ensure co-partitioning:
```
Option 1: Create both topics with the same partition count from the start
  kafka-topics --create --topic orders --partitions 3
  kafka-topics --create --topic customers --partitions 3

Option 2: If keys don't match, use selectKey() or repartition() first
  orders.selectKey((orderId, value) -> parseCustomerId(value))
        .repartition(Repartitioned.numberOfPartitions(3))
```

### GlobalKTable exception:
GlobalKTable loads ALL partitions on EVERY instance. So co-partitioning is NOT needed.
That's its superpower (and its cost — uses more memory).

---

## 6.3 The Four Join Types

```
┌─────────────────────────────┬───────────────────────────────────────────────┐
│ Join Type                   │ What it does                                 │
├─────────────────────────────┼───────────────────────────────────────────────┤
│ KStream × KStream           │ Join two event streams within a TIME WINDOW  │
│                             │ Both sides are events. Must specify window.  │
├─────────────────────────────┼───────────────────────────────────────────────┤
│ KStream × KTable            │ Enrich stream events with table lookups      │
│                             │ Stream = events, Table = current state       │
├─────────────────────────────┼───────────────────────────────────────────────┤
│ KStream × GlobalKTable      │ Same as above, but NO co-partition needed    │
│                             │ Table is fully replicated on every instance  │
├─────────────────────────────┼───────────────────────────────────────────────┤
│ KTable × KTable              │ Join two tables (current state × current     │
│                             │ state). Like SQL join on two tables.         │
└─────────────────────────────┴───────────────────────────────────────────────┘
```

---

## 6.4 KStream × KStream Join (Windowed)

### When to use
Both sides are EVENTS. Neither is a lookup table.
Example: match an order-placed event with a payment-received event.

### Why a window is REQUIRED
A KStream is infinite. Without a window, you'd have to buffer EVERY event forever.
The window says: "only join events that arrive within X time of each other."

### Complete Working Example: Match orders with payments

```java
@Configuration
public class StreamStreamJoinExample {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> orderPaymentJoin(StreamsBuilder builder) {

        // Both keyed by orderId
        KStream<String, String> orders = builder.stream("orders",
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> payments = builder.stream("payments",
                Consumed.with(Serdes.String(), Serdes.String()));

        // JOIN: match orders with payments within 10-minute window
        KStream<String, String> joined = orders.join(
                payments,                                           // right stream

                // VALUE JOINER: how to combine the two values
                (orderValue, paymentValue) ->
                        "{\"order\":" + orderValue + ",\"payment\":" + paymentValue + "}",

                // WINDOW: only match if payment arrives within 10 min of order
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)),

                // SERDES for both sides
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        // Result: only orders that have a matching payment (same key, within window)
        joined.to("confirmed-orders",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### What happens:
```
Time 0:00  → orders:   key=ORD-1, value={"item":"laptop","amount":1500}
Time 0:03  → payments: key=ORD-1, value={"method":"credit","amount":1500}
  → MATCH! Same key (ORD-1), within 10-min window
  → Output: key=ORD-1, value={"order":{...},"payment":{...}}

Time 0:05  → orders:   key=ORD-2, value={"item":"mouse","amount":50}
Time 0:20  → payments: key=ORD-2, value={"method":"debit","amount":50}
  → NO MATCH! 15 minutes apart > 10-minute window
  → Nothing emitted for ORD-2
```

### Join variants for KStream × KStream:
```java
// INNER JOIN — both sides must have a matching record
orders.join(payments, joiner, window, streamJoined);

// LEFT JOIN — emit even if right side (payment) has no match
orders.leftJoin(payments, joiner, window, streamJoined);
// Output: key=ORD-2, value={"order":{...},"payment":null}

// OUTER JOIN — emit if EITHER side has no match
orders.outerJoin(payments, joiner, window, streamJoined);
// Emits unpaired orders AND unpaired payments
```

### What gets created internally:
```
Two state stores (RocksDB):
  - order-payment-join-store-this     ← buffers orders waiting for payments
  - order-payment-join-store-other    ← buffers payments waiting for orders

Two changelog topics:
  - {app-id}-...-this-changelog
  - {app-id}-...-other-changelog

Records are buffered in the store until the window expires.
```

---

## 6.5 KStream × KTable Join (Lookup Enrichment)

### When to use
You have EVENTS (stream) that you want to ENRICH with current state (table).
Example: an order event arrives, you look up the customer's name and tier from the customer table.

### Why NO window is needed
The KTable always has the LATEST value. When a stream record arrives, you just look up
the current table value. No need to wait.

### Complete Working Example: Enrich orders with customer data

```java
@Configuration
public class StreamTableJoinExample {

    @Bean
    public KStream<String, String> enrichOrders(StreamsBuilder builder) {

        // Stream: order events, keyed by customerId
        KStream<String, String> orders = builder.stream("orders-by-customer",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Table: customer profiles, keyed by customerId
        KTable<String, String> customers = builder.table("customers",
                Consumed.with(Serdes.String(), Serdes.String()));

        // JOIN: enrich each order with customer data
        KStream<String, String> enriched = orders.join(
                customers,

                // VALUE JOINER: combine order + customer
                (orderValue, customerValue) ->
                        "{\"order\":" + orderValue + ",\"customer\":" + customerValue + "}",

                // SERDES
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        enriched.to("enriched-orders",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### What happens:
```
Customer table state: { "cust-A": {"name":"Alice","tier":"GOLD"} }

Order arrives: key=cust-A, value={"orderId":"ORD-1","amount":500}
  → Look up "cust-A" in KTable → found: {"name":"Alice","tier":"GOLD"}
  → Joiner produces: {"order":{...},"customer":{"name":"Alice","tier":"GOLD"}}
  → Output: key=cust-A, value={combined JSON}

Order arrives: key=cust-Z, value={"orderId":"ORD-2","amount":100}
  → Look up "cust-Z" in KTable → NOT FOUND
  → INNER JOIN: nothing emitted (no match)
  → LEFT JOIN: emits {"order":{...},"customer":null}
```

### Key requirement:
**Both must be keyed by the SAME key** (customerId in this case).
If orders are keyed by orderId, you must `selectKey()` first to re-key by customerId.

---

## 6.6 KStream × GlobalKTable Join (No Co-Partitioning Needed)

### When to use
Same as KStream × KTable, but when you CAN'T guarantee co-partitioning,
or the lookup table is small enough to replicate everywhere.

### How GlobalKTable works differently
- Regular KTable: each instance gets SOME partitions (partition 0, 1 on instance A; partition 2, 3 on instance B)
- GlobalKTable: EVERY instance gets ALL partitions (every instance has the complete table)

### Why co-partitioning is not needed
Since every instance has the FULL table, any key can be looked up on any instance.
No need for matching partition assignments.

### The trade-off
```
KTable join:             GlobalKTable join:
✅ Memory efficient       ❌ Uses more memory (full table on every instance)
❌ Requires co-partition  ✅ No co-partitioning needed
❌ Same key required      ✅ Can join on any field (custom KeyValueMapper)
```

### Complete Working Example: Enrich orders with product data

```java
@Configuration
public class StreamGlobalTableJoinExample {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> enrichWithProduct(StreamsBuilder builder) {

        // Stream: orders keyed by orderId
        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // GlobalKTable: products keyed by productId (ALL data on every instance)
        GlobalKTable<String, String> products = builder.globalTable("products",
                Consumed.with(Serdes.String(), Serdes.String()));

        // JOIN: look up product for each order
        KStream<String, String> enriched = orders.join(
                products,

                // KEY MAPPER: extract the join key from the stream record
                // "How do I find the matching product for this order?"
                (orderId, orderValue) -> {
                    try {
                        return mapper.readValue(orderValue, Order.class).getProductId();
                    } catch (Exception e) {
                        return "UNKNOWN";
                    }
                },

                // VALUE JOINER: combine order + product
                (orderValue, productValue) ->
                        "{\"order\":" + orderValue + ",\"product\":" + productValue + "}"
        );

        enriched.to("enriched-with-product",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### Notice the KEY MAPPER
With a regular KTable join, the join key is always the stream record's key.
With GlobalKTable, you provide a **KeyValueMapper** that extracts the join key from the stream record.
This is why you can join on ANY field, not just the key.

```
Order: key=ORD-1, value={"productId":"PROD-42",...}
KeyMapper extracts: "PROD-42"
GlobalKTable lookup: key="PROD-42" → {"name":"Laptop","price":1500}
Result: combined order + product
```

---

## 6.7 KTable × KTable Join

### When to use
You want to join two TABLES (current state × current state).
Example: join customer-profiles table with customer-settings table.

### How it works
Whenever EITHER table updates, the join re-fires with the latest values from both sides.

```java
@Configuration
public class TableTableJoinExample {

    @Bean
    public KStream<String, String> joinProfilesAndSettings(StreamsBuilder builder) {

        // Both keyed by customerId
        KTable<String, String> profiles = builder.table("customer-profiles",
                Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, String> settings = builder.table("customer-settings",
                Consumed.with(Serdes.String(), Serdes.String()));

        // JOIN: combine profiles + settings
        KTable<String, String> combined = profiles.join(
                settings,
                (profileValue, settingsValue) ->
                        "{\"profile\":" + profileValue + ",\"settings\":" + settingsValue + "}"
        );

        combined.toStream().to("customer-complete",
                Produced.with(Serdes.String(), Serdes.String()));

        return builder.stream("customer-profiles",
                Consumed.with(Serdes.String(), Serdes.String()));
    }
}
```

### What happens:
```
1. profiles: key=alice, value={"name":"Alice","age":30}
   settings for alice doesn't exist yet → no output (inner join)

2. settings: key=alice, value={"theme":"dark","notifications":true}
   → NOW both sides exist → join fires!
   → Output: {"profile":{"name":"Alice",...},"settings":{"theme":"dark",...}}

3. profiles: key=alice, value={"name":"Alice","age":31}  ← birthday update
   → Table updated → join re-fires with latest from BOTH sides
   → Output: {"profile":{"name":"Alice","age":31},"settings":{"theme":"dark",...}}
```

---

## 6.8 Join Decision Flowchart

```
Do you need to join two data sources?
│
├─ Are BOTH sources event streams (append-only)?
│   YES → KStream × KStream join (must specify time window)
│   │
│   └─ Inner? Left? Outer?
│       inner  = both must match
│       left   = left side always emits
│       outer  = both sides always emit
│
├─ Is one source events and the other current state?
│   YES → Is the table small AND can you avoid co-partitioning?
│       │
│       ├─ YES → KStream × GlobalKTable (no co-partition needed)
│       │
│       └─ NO  → KStream × KTable (must co-partition)
│
└─ Are BOTH sources current state (tables)?
    YES → KTable × KTable
```



---

# PART 7: WINDOWING

## 7.1 What Is Windowing and Why Do We Need It?

Without windowing, aggregations (count, sum, etc.) run over ALL data forever:
```
"How many orders total?" → count keeps growing: 1, 2, 3, ..., 1,000,000
```

With windowing, you aggregate over a TIME RANGE:
```
"How many orders in the last 5 minutes?" → resets every 5 minutes
```

Windowing groups records by **time** in addition to **key**.

---

## 7.2 The Three Window Types

```
┌──────────────────┬───────────────────────────────────────────────────────┐
│ Window Type      │ What it does                                         │
├──────────────────┼───────────────────────────────────────────────────────┤
│ Tumbling         │ Fixed-size, non-overlapping windows                  │
│                  │ [0-5min] [5-10min] [10-15min] ...                    │
│                  │ Every record falls into exactly ONE window           │
├──────────────────┼───────────────────────────────────────────────────────┤
│ Hopping          │ Fixed-size, OVERLAPPING windows                      │
│                  │ Window=5min, advance=1min                            │
│                  │ [0-5] [1-6] [2-7] [3-8] ...                         │
│                  │ A record can fall into MULTIPLE windows              │
├──────────────────┼───────────────────────────────────────────────────────┤
│ Session          │ Dynamic size based on ACTIVITY gaps                  │
│                  │ Window closes after X time of inactivity             │
│                  │ Good for: user sessions, click sessions              │
└──────────────────┴───────────────────────────────────────────────────────┘
```

---

## 7.3 Tumbling Window — Complete Example

### What it does
Fixed-size windows with no overlap. Each record belongs to exactly one window.

### Example: Count orders per customer per 5-minute window
```java
@Configuration
public class TumblingWindowExample {

    @Bean
    public KStream<String, String> tumblingWindowTopology(StreamsBuilder builder) {

        KStream<String, String> orders = builder.stream("orders",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Group by customer (key)
        KGroupedStream<String, String> grouped = orders.groupByKey(
                Grouped.with(Serdes.String(), Serdes.String()));

        // WINDOWED aggregation: count per 5-minute tumbling window
        KTable<Windowed<String>, Long> windowedCounts = grouped
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count(Materialized.as("tumbling-count-store"));

        // Write to output topic
        windowedCounts.toStream()
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().start() + "-" + windowedKey.window().end(),
                        String.valueOf(count)))
                .to("order-counts-5min",
                        Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }
}
```

### What happens:
```
Timeline:
  00:01 → alice order   ┐
  00:03 → alice order   │ Window [00:00 - 00:05)
  00:04 → bob order     ┘

  00:06 → alice order   ┐
  00:08 → alice order   │ Window [00:05 - 00:10)
  00:09 → bob order     ┘

Results:
  Window [00:00-00:05): alice=2, bob=1
  Window [00:05-00:10): alice=2, bob=1
```

### The Windowed<K> key type
After windowed aggregation, the key becomes `Windowed<String>` instead of just `String`.
This wraps the original key WITH the window time range:
```java
Windowed<String> windowedKey = ...;
windowedKey.key()            // "alice"
windowedKey.window().start() // 1719388800000 (epoch millis)
windowedKey.window().end()   // 1719389100000
```

---

## 7.4 Hopping Window — Complete Example

### What it does
Windows overlap. A single record can appear in MULTIPLE windows.
Useful for: "moving averages" or "orders in the last 5 minutes, updated every 1 minute."

### Example: Count orders in 5-min windows, advancing every 1 minute
```java
// Hopping: 5-minute window, advancing every 1 minute
KTable<Windowed<String>, Long> hoppingCounts = grouped
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))
                .advanceBy(Duration.ofMinutes(1)))               // ← this makes it hopping
        .count(Materialized.as("hopping-count-store"));
```

### What happens:
```
Record at 00:03 (alice)
  → Falls into windows: [00:00-00:05), [00:01-00:06), [00:02-00:07), [00:03-00:08)
  → Count increments in ALL of these windows

This gives you a "sliding" view of activity.
Tumbling: "in the 00:00-00:05 window, alice had 3 orders"
Hopping:  "at any given minute, alice had X orders in the last 5 minutes"
```

### Tumbling vs Hopping — the only difference
```java
// Tumbling: advance = size (no overlap)
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))
// internally: advance = 5 min = window size

// Hopping: advance < size (overlap)
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))
            .advanceBy(Duration.ofMinutes(1))
// advance = 1 min < 5 min window → records fall into multiple windows
```

**Tumbling is just a special case of hopping where advance = window size.**

---

## 7.5 Session Window — Complete Example

### What it does
Windows based on ACTIVITY. A session window closes when there's no activity for a specified gap.
No fixed size — sessions can be 1 second or 1 hour long.

### Example: Group user clicks into sessions (5-min inactivity gap)
```java
// Session: window closes after 5 minutes of inactivity
KTable<Windowed<String>, Long> sessionCounts = grouped
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
        .count(Materialized.as("session-count-store"));
```

### What happens:
```
00:01 → alice click  ┐
00:02 → alice click  │ Session 1 (gap < 5 min between each)
00:04 → alice click  ┘
                        ← 7 minutes of silence (> 5 min gap) → session closes
00:11 → alice click  ┐
00:12 → alice click  │ Session 2 (new session)
00:13 → alice click  ┘

Results:
  Session 1: alice, window [00:01 - 00:04], count=3
  Session 2: alice, window [00:11 - 00:13], count=3
```

### Session merging
If a late record arrives that bridges two sessions, Kafka Streams MERGES them:
```
Session A: [00:01 - 00:04]
Session B: [00:11 - 00:13]

Late record arrives: 00:07 (within 5 min of both sessions)
  → Sessions A and B are MERGED into one: [00:01 - 00:13], count=7
```

---

## 7.6 Grace Period — Handling Late Data

### The problem
Records can arrive LATE (network delays, producer retries, etc.).
If a window has already closed, should we still count the late record?

### Grace period syntax
```java
// Tumbling with 2-minute grace period
TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(2))

// Session with grace
SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(2))

// No grace (strict — drop all late records)
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))
```

### What happens with grace:
```
Window [00:00 - 00:05) with 2-minute grace

00:06 → late record for window [00:00-00:05)
  → 00:06 is within grace (00:05 + 2 = 00:07)
  → Record IS counted. Window updates.

00:08 → late record for window [00:00-00:05)
  → 00:08 is PAST grace (> 00:07)
  → Record is DROPPED. Window does NOT update.
```

---

## 7.7 Part 7 Summary

```
WINDOWING = Time-based grouping for aggregations

┌──────────────────┬──────────────────────────────────────────────────────┐
│ Tumbling         │ Fixed, non-overlapping. Simplest.                   │
│                  │ TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))│
├──────────────────┼──────────────────────────────────────────────────────┤
│ Hopping          │ Fixed, overlapping. Records in multiple windows.    │
│                  │ TimeWindows.ofSizeWithNoGrace(5min).advanceBy(1min) │
├──────────────────┼──────────────────────────────────────────────────────┤
│ Session          │ Dynamic, activity-based. Closes on inactivity gap.  │
│                  │ SessionWindows.ofInactivityGapWithNoGrace(5min)     │
├──────────────────┼──────────────────────────────────────────────────────┤
│ Grace Period     │ How long to accept late data after window closes.   │
│                  │ ofSizeAndGrace(5min, 2min)                          │
└──────────────────┴──────────────────────────────────────────────────────┘

After windowed aggregation:
  Key type changes: String → Windowed<String>
  Windowed<String> contains: original key + window start/end times
```



---

# PART 8: SINK OPERATIONS — Writing Output

After processing, you need to send results somewhere. That's what sink operations do.

---

## 8.1 to() — Write to a Kafka topic

### The most common sink. Writes every record to a specified topic.

```java
// Basic: write to a topic
stream.to("output-topic");

// With explicit serdes (recommended)
stream.to("output-topic",
        Produced.with(Serdes.String(), Serdes.String()));

// KTable → must convert to stream first
table.toStream().to("output-topic",
        Produced.with(Serdes.String(), Serdes.String()));
```

### `to()` is a TERMINAL operation
After calling `to()`, you CANNOT chain more operations on that branch.
If you need to both write to a topic AND continue processing, use `through()` or `repartition()`.

---

## 8.2 Dynamic Topic Routing with to()

### Write different records to different topics based on content

```java
// Route records to different topics dynamically
stream.to(
        (key, value, recordContext) -> {
            // Choose topic based on the key or value
            if (key.startsWith("premium")) {
                return "premium-orders";
            } else if (key.startsWith("budget")) {
                return "budget-orders";
            } else {
                return "standard-orders";
            }
        },
        Produced.with(Serdes.String(), Serdes.String())
);
```

### When to use dynamic routing vs split()
```
split()/branch() → You need to do DIFFERENT processing on each branch before sinking
dynamic to()     → All records go through the SAME processing, just different output topics
```

---

## 8.3 foreach() — Side effect, no output

### What it does
Executes a side effect for each record. Does NOT produce any output.
Terminal operation.

```java
// Log every record (no output topic)
stream.foreach((key, value) ->
        System.out.println("Processed: " + key + " → " + value));

// Send to external system (HTTP call, database insert, etc.)
stream.foreach((key, value) -> {
    externalService.notify(key, value);
});
```

### ⚠️ Warning about foreach()
`foreach()` provides NO guarantees about delivery:
- If the app crashes mid-processing, some records may be lost
- No Kafka transaction wraps the side effect
- Use only for "best effort" operations (logging, metrics, non-critical notifications)

For reliable external delivery, use the Processor API with state stores.

---

## 8.4 print() — Debug output to stdout

```java
// Print all records to console
stream.print(Printed.<String, String>toSysOut()
        .withLabel("DEBUG"));

// Output: [DEBUG]: key1, value1
// Output: [DEBUG]: key2, value2
```

**Use only in development.** Remove before production.

---

## 8.5 Part 8 Summary

```
SINK OPERATIONS:
┌──────────────────┬───────────────────────────────────────────────────────┐
│ to("topic")      │ Write to fixed topic. Terminal.                      │
│ to(topicChooser) │ Write to dynamic topic based on record content.      │
│ foreach()        │ Side effect only. No output. Terminal. No guarantees.│
│ print()          │ Debug: write to stdout. Development only.            │
└──────────────────┴───────────────────────────────────────────────────────┘

All sink operations are TERMINAL — you can't chain after them.
```


---

# PART 9: PROCESSOR API (Low-Level)

The DSL (`stream.filter().mapValues().groupByKey().count()`) is the high-level API.
The **Processor API** is the low-level API that gives you full control.

## 9.1 When to Use Processor API

```
Use DSL when:
  ✅ Your logic fits filter/map/group/aggregate/join patterns
  ✅ You want concise, readable code
  ✅ 90% of use cases

Use Processor API when:
  ⚠️ You need to access record metadata (headers, timestamp, partition, offset)
  ⚠️ You need fine-grained state store access (point lookups, range scans)
  ⚠️ You need periodic actions (run something every 30 seconds)
  ⚠️ You need to forward records selectively to specific downstream nodes
  ⚠️ Your logic doesn't fit any DSL operator
```

---

## 9.2 Processor vs FixedKeyProcessor

```
┌────────────────────────┬──────────────────────────────────────────────┐
│ Processor<K, V>        │ Can change the key. Full flexibility.       │
│                        │ ⚠️ May trigger repartition downstream.     │
├────────────────────────┼──────────────────────────────────────────────┤
│ FixedKeyProcessor<K,V> │ Key is FIXED. Cannot change it.             │
│                        │ ✅ More efficient, no repartition risk.     │
│                        │ Use this unless you NEED to change the key. │
└────────────────────────┴──────────────────────────────────────────────┘
```

---

## 9.3 FixedKeyProcessor — Complete Example

### Example: Enrich each record with a counter from a state store

```java
public class EnrichWithCountProcessor
        implements FixedKeyProcessor<String, String, String> {

    private FixedKeyProcessorContext<String, String> context;
    private KeyValueStore<String, Long> countStore;

    @Override
    public void init(FixedKeyProcessorContext<String, String> context) {
        this.context = context;
        // Access the state store by name
        this.countStore = context.getStateStore("enrichment-count-store");
    }

    @Override
    public void process(FixedKeyRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        // Read current count from state store
        Long currentCount = countStore.get(key);
        if (currentCount == null) currentCount = 0L;

        // Increment count
        currentCount++;
        countStore.put(key, currentCount);

        // Forward enriched record downstream
        String enrichedValue = value + ",\"processCount\":" + currentCount;
        context.forward(record.withValue(enrichedValue));
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }
}
```

### Wiring it into a topology:
```java
@Configuration
public class ProcessorApiExample {

    @Bean
    public KStream<String, String> processorTopology(StreamsBuilder builder) {

        // Create the state store
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("enrichment-count-store"),
                        Serdes.String(),
                        Serdes.Long());

        builder.addStateStore(storeBuilder);

        KStream<String, String> stream = builder.stream("input-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Apply the processor
        KStream<String, String> processed = stream.processValues(
                () -> new EnrichWithCountProcessor(),
                "enrichment-count-store"     // ← names of state stores this processor uses
        );

        processed.to("output-topic",
                Produced.with(Serdes.String(), Serdes.String()));

        return stream;
    }
}
```

---

## 9.4 Punctuator — Periodic Actions

### What it does
Runs a function at regular intervals, independent of incoming records.
Use for: periodic cleanup, emitting aggregated results on a timer, heartbeats.

### Example: Every 30 seconds, emit the count and reset
```java
public class PeriodicEmitProcessor
        implements FixedKeyProcessor<String, String, String> {

    private FixedKeyProcessorContext<String, String> context;
    private KeyValueStore<String, Long> countStore;

    @Override
    public void init(FixedKeyProcessorContext<String, String> context) {
        this.context = context;
        this.countStore = context.getStateStore("periodic-count-store");

        // Schedule: every 30 seconds, run the punctuate method
        context.schedule(
                Duration.ofSeconds(30),
                PunctuationType.WALL_CLOCK_TIME,     // real clock, not event time
                this::emitAndReset                     // method to call
        );
    }

    private void emitAndReset(long timestamp) {
        // Iterate over all keys in the state store
        try (KeyValueIterator<String, Long> iter = countStore.all()) {
            while (iter.hasNext()) {
                KeyValue<String, Long> entry = iter.next();
                // Forward count as a record
                context.forward(new FixedKeyRecord<>(
                        entry.key, "count=" + entry.value, timestamp));
                // Reset count
                countStore.put(entry.key, 0L);
            }
        }
    }

    @Override
    public void process(FixedKeyRecord<String, String> record) {
        // Just count each incoming record
        Long count = countStore.get(record.key());
        countStore.put(record.key(), (count == null ? 0L : count) + 1);
        // Don't forward — punctuator will handle emission
    }

    @Override
    public void close() {}
}
```

### PunctuationType:
```
WALL_CLOCK_TIME  → based on real system clock (30 seconds of real time)
STREAM_TIME      → based on record timestamps (advances only when records arrive)
                   If no records → punctuate never fires
```

---

## 9.5 Pure Topology API (No DSL at all)

### Instead of StreamsBuilder, you can build the raw Topology directly:

```java
Topology topology = new Topology();

// Add source
topology.addSource("source-node", "input-topic");

// Add processor
topology.addProcessor("process-node",
        () -> new MyProcessor(),       // Processor supplier
        "source-node");                // Parent node name

// Add state store connected to processor
topology.addStateStore(storeBuilder, "process-node");

// Add sink
topology.addSink("sink-node",
        "output-topic",
        "process-node");               // Parent node name
```

### When to use pure Topology API:
- Almost NEVER in practice. The DSL + `.process()` covers 99% of cases.
- Use only if you need complete control over node naming and topology wiring.
- The DSL actually GENERATES a Topology internally. You can inspect it:

```java
StreamsBuilder builder = new StreamsBuilder();
// ... add your DSL operations ...
Topology topology = builder.build();
System.out.println(topology.describe());
// Prints the full node graph that the DSL compiled
```

---

## 9.6 Part 9 Summary

```
PROCESSOR API = Low-level control over record processing

┌─────────────────────────┬─────────────────────────────────────────────┐
│ FixedKeyProcessor       │ Process records without changing key.       │
│                         │ ✅ Preferred. No repartition.              │
├─────────────────────────┼─────────────────────────────────────────────┤
│ Processor               │ Process records, CAN change key.           │
│                         │ ⚠️ Use only when key change is needed.    │
├─────────────────────────┼─────────────────────────────────────────────┤
│ Punctuator              │ Periodic action (timer-based).             │
│                         │ WALL_CLOCK_TIME or STREAM_TIME.            │
├─────────────────────────┼─────────────────────────────────────────────┤
│ State Store access      │ context.getStateStore("name")              │
│                         │ get(), put(), range(), all() operations    │
├─────────────────────────┼─────────────────────────────────────────────┤
│ Pure Topology API       │ Build topology manually (rarely needed)    │
└─────────────────────────┴─────────────────────────────────────────────┘

DSL compiles to Processor API. So everything you write in DSL
becomes Processor nodes internally. The Processor API just lets you
write those nodes yourself when DSL isn't flexible enough.
```


---

# PART 10: INTERACTIVE QUERIES — Querying State at Runtime

## 10.1 What Are Interactive Queries?

Every stateful operation (count, reduce, aggregate) creates a **RocksDB state store**.
Interactive Queries let you **read from these stores at runtime** via your application's REST API.

```
Normal Kafka Streams flow:
  Input topic → Process → Output topic
  (consumers must read the output topic to see results)

With Interactive Queries:
  Input topic → Process → State Store (RocksDB)
                             ↓
                    Your REST API can query it directly
                    GET /api/customer-count/alice → 42
```

---

## 10.2 Querying a Key-Value Store

```java
@RestController
@RequestMapping("/api")
public class QueryController {

    private final StreamsBuilderFactoryBean factoryBean;

    public QueryController(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @GetMapping("/count/{key}")
    public ResponseEntity<Long> getCount(@PathVariable String key) {
        KafkaStreams streams = factoryBean.getKafkaStreams();

        // Get the state store by name
        ReadOnlyKeyValueStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        "order-count-store",                    // store name from Materialized.as(...)
                        QueryableStoreTypes.keyValueStore()));

        Long count = store.get(key);
        if (count == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(count);
    }

    // Get ALL key-value pairs
    @GetMapping("/counts")
    public Map<String, Long> getAllCounts() {
        ReadOnlyKeyValueStore<String, Long> store = getStore();

        Map<String, Long> result = new HashMap<>();
        try (KeyValueIterator<String, Long> iter = store.all()) {
            while (iter.hasNext()) {
                KeyValue<String, Long> entry = iter.next();
                result.put(entry.key, entry.value);
            }
        }
        return result;
    }
}
```

---

## 10.3 Querying a Windowed Store

```java
@GetMapping("/windowed-count/{key}")
public Map<String, Long> getWindowedCount(@PathVariable String key) {
    KafkaStreams streams = factoryBean.getKafkaStreams();

    ReadOnlyWindowStore<String, Long> store = streams.store(
            StoreQueryParameters.fromNameAndType(
                    "tumbling-count-store",
                    QueryableStoreTypes.windowStore()));

    Map<String, Long> result = new HashMap<>();

    // Fetch all windows for this key in the last hour
    Instant from = Instant.now().minus(Duration.ofHours(1));
    Instant to = Instant.now();

    try (WindowStoreIterator<Long> iter = store.fetch(key, from, to)) {
        while (iter.hasNext()) {
            KeyValue<Long, Long> entry = iter.next();
            String windowStart = Instant.ofEpochMilli(entry.key).toString();
            result.put(windowStart, entry.value);
        }
    }
    return result;
    // Returns: { "2026-06-26T10:00:00Z": 5, "2026-06-26T10:05:00Z": 3 }
}
```

---

## 10.4 The Store Name Connection

The store name you pass to `Materialized.as(...)` is the SAME name you use to query:

```java
// In topology:
grouped.count(Materialized.as("order-count-store"));   // ← name defined here

// In REST controller:
StoreQueryParameters.fromNameAndType(
        "order-count-store",                             // ← same name used here
        QueryableStoreTypes.keyValueStore());
```

If you don't use `Materialized.as(...)`, Kafka Streams auto-generates a name.
You CAN still query it, but the name is unpredictable and changes if topology changes.
**Always name your stores.**

---

## 10.5 Part 10 Summary

```
INTERACTIVE QUERIES = Read RocksDB state stores via your REST API

┌──────────────────────────────────┬──────────────────────────────────────┐
│ Store Type                       │ Query API                           │
├──────────────────────────────────┼──────────────────────────────────────┤
│ KeyValueStore (count, aggregate) │ store.get(key)                      │
│                                  │ store.all() → iterator              │
│                                  │ store.range(from, to) → iterator    │
├──────────────────────────────────┼──────────────────────────────────────┤
│ WindowStore (windowed agg)       │ store.fetch(key, from, to)          │
│                                  │ store.all() → iterator              │
├──────────────────────────────────┼──────────────────────────────────────┤
│ SessionStore (session agg)       │ store.fetch(key)                    │
│                                  │ store.fetch(from, to) → iterator    │
└──────────────────────────────────┴──────────────────────────────────────┘

Key rule: Materialized.as("store-name") in topology = store-name in query API
```


---

# PART 11: COMPLETE CHEAT SHEET

## 11.1 All Operations at a Glance

```
┌─────────────────────┬────────────┬─────────────┬──────────────┬───────────────┐
│ Operation           │ Type       │ Key Change? │ State Store? │ Repartition?  │
├─────────────────────┼────────────┼─────────────┼──────────────┼───────────────┤
│ filter()            │ Stateless  │ No          │ No           │ No            │
│ filterNot()         │ Stateless  │ No          │ No           │ No            │
│ mapValues()         │ Stateless  │ No          │ No           │ No            │
│ map()               │ Stateless  │ YES         │ No           │ ⚠️ Marks      │
│ selectKey()         │ Stateless  │ YES         │ No           │ ⚠️ Marks      │
│ flatMapValues()     │ Stateless  │ No          │ No           │ No            │
│ flatMap()           │ Stateless  │ YES         │ No           │ ⚠️ Marks      │
│ peek()              │ Stateless  │ No          │ No           │ No            │
│ split()/branch()    │ Stateless  │ No          │ No           │ No            │
│ merge()             │ Stateless  │ No          │ No           │ No            │
├─────────────────────┼────────────┼─────────────┼──────────────┼───────────────┤
│ groupByKey()        │ Grouping   │ No          │ No           │ No            │
│ groupBy()           │ Grouping   │ YES         │ No           │ ⚠️ Creates    │
│ count()             │ Stateful   │ No          │ YES          │ No            │
│ reduce()            │ Stateful   │ No          │ YES          │ No            │
│ aggregate()         │ Stateful   │ No          │ YES          │ No            │
├─────────────────────┼────────────┼─────────────┼──────────────┼───────────────┤
│ join()              │ Stateful   │ No          │ YES*         │ If not co-p   │
│ leftJoin()          │ Stateful   │ No          │ YES*         │ If not co-p   │
│ outerJoin()         │ Stateful   │ No          │ YES*         │ If not co-p   │
├─────────────────────┼────────────┼─────────────┼──────────────┼───────────────┤
│ windowedBy()        │ Windowing  │ No          │ YES          │ No            │
├─────────────────────┼────────────┼─────────────┼──────────────┼───────────────┤
│ to()                │ Sink       │ No          │ No           │ No            │
│ foreach()           │ Sink       │ No          │ No           │ No            │
│ print()             │ Sink       │ No          │ No           │ No            │
└─────────────────────┴────────────┴─────────────┴──────────────┴───────────────┘

* KStream×KTable and GlobalKTable joins don't create stores for the stream side
  KStream×KStream joins create 2 stores (buffer both sides)
```

## 11.2 The Three Golden Rules

```
RULE 1: KEY CHANGE = REPARTITION
  map(), selectKey(), flatMap(), groupBy() → trigger repartition
  mapValues(), flatMapValues(), filter(), groupByKey() → safe, no repartition
  ALWAYS prefer the safe version unless you genuinely need to change the key.

RULE 2: STATEFUL = ROCKSDB + CHANGELOG
  count(), reduce(), aggregate(), joins, windowed ops → create state
  State = RocksDB on disk + changelog topic in Kafka
  State survives restarts (rebuilt from changelog if needed)

RULE 3: GROUP BEFORE AGGREGATE
  You CANNOT call count()/reduce()/aggregate() on a raw KStream.
  You MUST call groupByKey() or groupBy() first.
  Grouping ensures all records with the same key are on the same partition.
```

## 11.3 The Complete Topology Pattern

```java
// Every Kafka Streams topology follows this pattern:

StreamsBuilder builder = new StreamsBuilder();

// 1. SOURCE: Read from Kafka
KStream<K, V> stream = builder.stream("input-topic", Consumed.with(...));
KTable<K, V>  table  = builder.table("lookup-topic", Consumed.with(...));

// 2. PROCESS: Transform, filter, enrich
KStream<K, V2> processed = stream
        .filter(...)
        .mapValues(...)
        .selectKey(...)          // ⚠️ if needed

// 3. GROUP: Prepare for aggregation
KGroupedStream<K, V2> grouped = processed.groupByKey(Grouped.with(...));

// 4. AGGREGATE: Count, sum, etc. (creates state store)
KTable<K, Long> result = grouped.count(Materialized.as("my-store"));

// 5. SINK: Write output
result.toStream().to("output-topic", Produced.with(...));
```

---

**END OF KAFKA STREAMS SYNTAX DEEP DIVE**

```
Parts covered:
  Part 1:  Foundation & Serdes
  Part 2:  SOURCE — KStream vs KTable vs GlobalKTable
  Part 3:  Stateless Operations (filter, map, flatMap, peek, branch, merge)
  Part 4:  Stateful Operations (groupByKey/groupBy + count/reduce/aggregate)
  Part 5:  KTable Operations (filter with tombstones, mapValues, suppress, KGroupedTable)
  Part 6:  Joins (Stream×Stream, Stream×Table, Stream×GlobalKTable, Table×Table)
  Part 7:  Windowing (Tumbling, Hopping, Session, Grace Period)
  Part 8:  Sink Operations (to, foreach, print)
  Part 9:  Processor API (FixedKeyProcessor, Punctuator, Pure Topology)
  Part 10: Interactive Queries (query RocksDB via REST)
  Part 11: Cheat Sheet (all operations, golden rules, complete pattern)
```
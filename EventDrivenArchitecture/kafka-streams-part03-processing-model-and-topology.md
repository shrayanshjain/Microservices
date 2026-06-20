# Part 3: Processing Model & Topology

## 3.1 What is a Topology?

A Topology is the **blueprint** of your entire stream processing logic.

It's a **Directed Acyclic Graph (DAG)** — data flows in ONE direction, no cycles.

```
You define it ONCE at startup → Kafka Streams executes it FOREVER.
```

Think of it like a **factory floor plan**:
- You design the assembly line (topology)
- Workers (threads) follow the plan
- Raw materials (events) flow through the line continuously
- You CANNOT change the floor plan while the factory is running

---

## 3.2 Three Types of Processors

Every node in the topology is one of three types:

### Source Processor
- **Entry point** — reads records from one or more Kafka topics
- Has NO parent processor (it's the start)
- Deserializes bytes → key-value pairs
- Forwards records to its child processors

```
┌─────────────────────────────┐
│  SOURCE PROCESSOR           │
│                             │
│  Reads from: order-events   │
│  Deserializes: Avro → POJO  │
│  Forwards to: next node     │
│                             │
│  Has NO upstream processor  │
└──────────────┬──────────────┘
               │
               ▼
         (child processors)
```

### Stream Processor (aka Processing Node)
- **The business logic** — where transformation happens
- Receives records from parent processor
- Processes them (filter, map, aggregate, join)
- Forwards results to child processors
- Can access **State Stores** for stateful operations

```
         (parent processor)
               │
               ▼
┌─────────────────────────────┐
│  STREAM PROCESSOR           │
│                             │
│  filter(amount > 100)       │  ← stateless
│  --- or ---                 │
│  count(per customerId)      │  ← stateful (needs state store)
│                             │
│  Has upstream AND downstream│
└──────────────┬──────────────┘
               │
               ▼
         (child processors)
```

### Sink Processor
- **Exit point** — writes records to a Kafka topic
- Has NO child processor (it's the end)
- Serializes key-value pairs → bytes
- Writes to the output topic

```
         (parent processor)
               │
               ▼
┌─────────────────────────────┐
│  SINK PROCESSOR             │
│                             │
│  Writes to: order-counts    │
│  Serializes: POJO → bytes   │
│                             │
│  Has NO downstream processor│
└─────────────────────────────┘
```

---

## 3.3 The DAG Model — Full Picture

A real topology is a **graph**, not just a straight line.
It can branch, merge, and have multiple sources/sinks.

### Simple Linear Topology
```
SOURCE(order-events) → FILTER → MAP → SINK(filtered-orders)
```

### Branching Topology
```
                         ┌→ FILTER(high) → SINK(high-value-orders)
SOURCE(order-events) → ─┤
                         └→ FILTER(low)  → SINK(low-value-orders)
```

### Multi-Source Join Topology
```
SOURCE(order-events) ──────┐
                            ├→ JOIN → SINK(enriched-orders)
SOURCE(customer-profiles) ──┘
```

### Complex Real-World Topology
```
SOURCE(orders) ──→ FILTER ──→ GROUP ──→ COUNT ──→ SINK(order-counts)
                      │
                      └──→ MAP ──→ SINK(order-audit-log)

SOURCE(payments) ──→ JOIN(with orders) ──→ SINK(completed-orders)
```

**Key rule:** It's a DAG — data flows forward. No loops. No cycles.

---

## 3.4 DSL vs Processor API

Kafka Streams gives you **two ways** to define a topology:

### DSL (Domain Specific Language) — High Level
```java
StreamsBuilder builder = new StreamsBuilder();

builder.stream("order-events")                    // SOURCE
    .filter((k, v) -> v.getAmount() > 100)        // PROCESSOR
    .mapValues(v -> v.getAmount())                 // PROCESSOR
    .to("high-value-amounts");                     // SINK
```

**Like SQL** — you say WHAT you want, not HOW.

### Processor API — Low Level
```java
Topology topology = new Topology();

topology.addSource("source", "order-events")
    .addProcessor("filter", () -> new Processor<>() {
        @Override
        public void process(Record<String, Order> record) {
            if (record.value().getAmount() > 100) {
                context().forward(record);
            }
        }
    }, "source")
    .addSink("sink", "high-value-amounts", "filter");
```

**Like Assembly** — you control EVERY step manually.

### Comparison

| | DSL | Processor API |
|--|-----|---------------|
| **Ease** | Simple, readable | Verbose, manual |
| **Control** | Kafka decides internals | You decide everything |
| **State access** | Implicit (via count, reduce) | Explicit (you manage stores) |
| **Use when** | 90% of use cases | Custom logic, periodic punctuations, complex routing |
| **Topology** | Built automatically | Built manually node-by-node |
| **Learning curve** | Low | High |

**Rule of thumb:** Start with DSL. Only drop to Processor API when DSL can't express your logic.

---

## 3.5 Topology Optimization

Kafka Streams can **optimize** your topology to reduce unnecessary processing.

### Without Optimization (default before 2.6)
```
SOURCE → FILTER → MAP → REPARTITION → GROUP → COUNT → SINK
                         ↑
                    internal topic created
                    (extra network hop)
```

### With Optimization
```
SOURCE → FILTER → MAP+REPARTITION(merged) → GROUP → COUNT → SINK
                         ↑
                    fewer internal topics
```

### Enable It
```java
Properties props = new Properties();
props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
```

### What It Optimizes
| Optimization | What it does |
|-------------|-------------|
| **Merge repartitions** | Combines consecutive repartition steps into one |
| **Reuse source topics** | If source topic = changelog topic, skip duplicate |
| **Reduce internal topics** | Fewer internal topics = less network I/O |

### Should You Always Enable It?
- **New app?** Yes, enable it
- **Existing app in production?** Be careful — optimization changes internal topic names, which can break state recovery

---

## 3.6 Seeing Your Topology

You can **print** the topology to understand what Kafka Streams built:

```java
Topology topology = builder.build();
System.out.println(topology.describe());
```

Output:
```
Topologies:
   Sub-topology: 0
    Source:  KSTREAM-SOURCE-0000000000 (topics: [order-events])
      --> KSTREAM-FILTER-0000000001
    Processor: KSTREAM-FILTER-0000000001 (stores: [])
      --> KSTREAM-MAP-0000000002
      <-- KSTREAM-SOURCE-0000000000
    Sink: KSTREAM-SINK-0000000003 (topic: high-value-orders)
      <-- KSTREAM-MAP-0000000002
```

This is extremely useful for debugging — you can see exactly what nodes exist,
what connects to what, and which state stores are used.

---

## 3.7 Summary

```
┌─────────────────────────────────────────────────────────┐
│                    TOPOLOGY                              │
│                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌──────────┐      │
│  │  SOURCE   │───→│  PROCESSORS  │───→│   SINK   │      │
│  │ (read)    │    │ (transform)  │    │ (write)  │      │
│  └──────────┘    └──────────────┘    └──────────┘      │
│                         │                               │
│                  ┌──────▼──────┐                        │
│                  │ STATE STORE │ (if stateful)           │
│                  └─────────────┘                        │
│                                                         │
│  Defined: DSL (high level) or Processor API (low level) │
│  Structure: DAG — no cycles                             │
│  Lifecycle: defined once, runs forever                  │
│  Printable: topology.describe()                         │
│  Optimizable: StreamsConfig.OPTIMIZE                    │
└─────────────────────────────────────────────────────────┘
```

**Next → Part 4: Tasks, Threads & Parallelism — How Kafka Streams splits the topology into tasks, assigns threads, and scales**

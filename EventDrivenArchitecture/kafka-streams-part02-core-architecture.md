# Part 2: Core Architecture

## 2.1 What Is Kafka Streams?

Kafka Streams is a **CLIENT LIBRARY** for building stream processing applications on top of Kafka.

**The single most important thing to understand:**

```
┌──────────────────────────────────────────────────┐
│  It is NOT a separate cluster or infrastructure. │
│  It is a JAR file you add to your application.   │
│  It runs INSIDE your Spring Boot app.            │
└──────────────────────────────────────────────────┘
```

Your application after adding Kafka Streams:
```
┌─────────────────────────────────────────────┐
│  order-analytics-service (Spring Boot)      │
│                                             │
│  ┌──────────────────────────────┐           │
│  │  Your business logic         │           │
│  │  (controllers, services)     │           │
│  └──────────────────────────────┘           │
│  ┌──────────────────────────────┐           │
│  │  Kafka Streams library       │  ← JAR    │
│  │  (kafka-streams)             │           │
│  └──────────────────────────────┘           │
│  ┌──────────────────────────────┐           │
│  │  Spring Kafka                │  ← JAR    │
│  │  (spring-kafka)              │           │
│  └──────────────────────────────┘           │
└─────────────────────────────────────────────┘
```

No Nimbus, no JobManager, no YARN, no Mesos. Just your app + Kafka.

---

## 2.2 Library vs Cluster — Streams vs Flink vs Spark

This is what makes Kafka Streams fundamentally different from other stream processors:

| | Apache Spark Streaming | Apache Flink | Apache Storm | **Kafka Streams** |
|--|----------------------|-------------|-------------|-----------------|
| **Deployment** | Separate cluster (YARN/Mesos/K8s) | Separate cluster (JobManager + TaskManagers) | Separate cluster (Nimbus + Supervisors) | **No cluster. Runs in YOUR app** |
| **Infrastructure** | Need Spark cluster + Kafka | Need Flink cluster + Kafka | Need Storm cluster + Kafka | **Just Kafka** |
| **Scaling** | Scale the cluster | Scale the cluster | Scale the cluster | **Start more app instances** |
| **Ops complexity** | High (manage cluster) | High (manage cluster) | High (manage cluster) | **Low (it's just your app)** |
| **Input sources** | Kafka, HDFS, S3, JDBC, etc. | Kafka, files, sockets, etc. | Kafka, spouts, etc. | **Kafka ONLY** |
| **Processing model** | Micro-batch (+ structured streaming) | True streaming | True streaming | **True streaming** |
| **State management** | External (HDFS checkpoints) | Managed (RocksDB + checkpoints) | External | **Managed (RocksDB + changelog topics)** |

### When to choose what?

```
Need to process data ONLY from Kafka?
  → Kafka Streams (simplest, no extra infra)

Need to process from Kafka + S3 + JDBC + files?
  → Flink or Spark (multi-source support)

Need complex event processing (CEP)?
  → Flink (first-class CEP library)

Need batch + stream unified?
  → Flink or Spark

Need ML pipeline on streams?
  → Spark (MLlib) or Flink (FlinkML)

Want ZERO extra infrastructure?
  → Kafka Streams (nothing to deploy except your app)
```

### The Trade-off

```
Kafka Streams = SIMPLICITY (no cluster, Kafka-only)
Flink/Spark   = POWER (multi-source, CEP, ML, batch+stream)
```

Kafka Streams is not "less powerful" — it's **intentionally scoped** to Kafka.
If your data is in Kafka, you don't need a Flink cluster.

---

## 2.3 Stream Processing Concepts

Before diving into topology, let's establish the core vocabulary:

### What is a Stream?

A stream is an **unbounded, continuously updating dataset**.
It has no beginning and no end — events keep arriving.

```
Traditional DB:  "Here is ALL the data" (bounded, finite)
Stream:          "Here is the NEXT piece of data" (unbounded, infinite)
```

A Kafka topic IS a stream:
```
order-events:  [Order1] → [Order2] → [Order3] → [Order4] → ... → [never ends]
```

### What is Stream Processing?

Processing events **as they arrive**, continuously, in real-time.

```
Batch processing:   Collect data → process all at once → output
                    (run every hour/day)

Stream processing:  Event arrives → process immediately → output
                    (always running, always processing)
```

### What is a Stream Processing Pipeline?

A series of **transformations** applied to a stream, producing another stream:

```
Input Stream → [Filter] → [Transform] → [Aggregate] → Output Stream
```

Real example:
```
order-events → filter(amount > 100) → map(to revenue) → sum(per customer) → revenue-per-customer
```

Each step reads from the previous step and writes to the next.
The entire pipeline runs **continuously** — not as a batch job.

---

## 2.4 Topology Overview — The Heart of Kafka Streams

Every Kafka Streams application defines a **Topology**.

A Topology is a **Directed Acyclic Graph (DAG)** of processing steps.

Think of it like a factory assembly line:
```
Raw material → Cut → Shape → Paint → Package → Ship
(source)       (processor)                      (sink)
```

In Kafka Streams:

### In Code

```java
StreamsBuilder builder = new StreamsBuilder();

builder.stream("order-events")                              // SOURCE
    .filter((key, order) -> order.getTotalAmount() > 100)   // PROCESSOR
    .groupByKey()                                           // PROCESSOR
    .count(Materialized.as("order-counts-store"))           // PROCESSOR (stateful)
    .toStream()                                             // KTable → KStream
    .to("customer-order-counts");                           // SINK

KafkaStreams streams = new KafkaStreams(builder.build(), config);
streams.start();
```

### Key Properties of a Topology

| Property | Detail |
|----------|--------|
| Defined once | At application startup |
| Immutable | Cannot be changed while running |
| Continuous | Runs forever — not a batch job |
| Real-time | Processes events as they arrive |
| `builder.build()` | Creates the Topology object |
| `streams.start()` | Starts the processing threads |

---

## 2.5 How Kafka Streams Runs — The Big Picture

```
┌─────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER                           │
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐                    │
│  │  order-events   │    │ customer-order   │                    │
│  │  (input topic)  │    │ -counts (output) │                    │
│  └────────┬────────┘    └────────▲─────────┘                    │
│           │                      │                              │
└───────────┼──────────────────────┼──────────────────────────────┘
            │                      │
            │   reads              │   writes
            │                      │
┌───────────▼──────────────────────┼──────────────────────────────┐
│           YOUR SPRING BOOT APP (Instance 1)                     │
│                                                                 │
│  ┌───────────────────────────────────────────────────┐          │
│  │  Kafka Streams Topology                           │          │
│  │                                                   │          │
│  │  SOURCE → FILTER → GROUP → COUNT → SINK           │          │
│  │                              │                    │          │
│  │                     ┌────────▼────────┐           │          │
│  │                     │  State Store    │           │          │
│  │                     │  (RocksDB)     │           │          │
│  │                     └─────────────────┘           │          │
│  └───────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

No separate cluster. No YARN. No Mesos. Just your app talking to Kafka.

Want to scale? Start another instance of the same app. Kafka Streams automatically
rebalances partitions and tasks across instances — exactly like consumer groups.

**Next → Part 3: Processing Model & Topology — Deep dive into Source/Stream/Sink processors, DAG model, DSL vs Processor API**

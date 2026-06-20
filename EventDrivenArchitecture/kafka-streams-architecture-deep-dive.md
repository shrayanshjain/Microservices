# Kafka Streams Architecture — Deep Dive (1hr Session)

> Audience: Backend engineers who know Kafka Producer/Consumer.
> Goal: After this session, you should be able to whiteboard the ENTIRE Kafka Streams
> architecture — from Kafka cluster layout to failure recovery — in any interview or design review.

---

# Section 1: The Kafka Cluster — Our Starting Point

Before we touch Kafka Streams, let's establish what our Kafka cluster looks like.

## 1.1 Cluster Layout

We're running KRaft mode (no ZooKeeper). 2 Brokers + 2 Controllers.

```
┌──────────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER (KRaft)                        │
│                                                                      │
│   ┌──────────────────┐              ┌──────────────────┐            │
│   │  CONTROLLER-1    │              │  CONTROLLER-2    │            │
│   │  (Raft leader)   │◄────────────►│  (Raft follower) │            │
│   │                  │   metadata   │                  │            │
│   │  Manages:        │   replication│  Manages:        │            │
│   │  - topic config  │              │  - failover      │            │
│   │  - partition map │              │  - takes over if │            │
│   │  - broker health │              │    leader dies    │            │
│   └──────────────────┘              └──────────────────┘            │
│                                                                      │
│   ┌──────────────────┐              ┌──────────────────┐            │
│   │  BROKER-1        │              │  BROKER-2        │            │
│   │  (node.id=1)     │              │  (node.id=2)     │            │
│   │                  │              │                  │            │
│   │  Stores data     │              │  Stores data     │            │
│   │  Serves clients  │              │  Serves clients  │            │
│   │  Replicates      │◄────────────►│  Replicates      │            │
│   └──────────────────┘   replicas   └──────────────────┘            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

**Controllers** = brain (metadata, who owns what)
**Brokers** = muscle (store data, serve clients)

---

## 1.2 Topics & Partitions on the Cluster

We have 2 topics: `orders` (3 partitions) and `payments` (3 partitions).
Each partition has a **leader** on one broker and a **replica** on the other.

```
┌──────────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER                                │
│                                                                      │
│   BROKER-1                              BROKER-2                     │
│   ┌──────────────────────┐              ┌──────────────────────┐    │
│   │                      │              │                      │    │
│   │  orders-P0  (LEADER) │ ────────────►│  orders-P0  (replica)│    │
│   │  orders-P2  (LEADER) │ ────────────►│  orders-P2  (replica)│    │
│   │  payments-P1 (LEADER)│ ────────────►│  payments-P1(replica)│    │
│   │                      │              │                      │    │
│   │  orders-P1  (replica)│◄──────────── │  orders-P1  (LEADER) │    │
│   │  payments-P0(replica)│◄──────────── │  payments-P0 (LEADER)│    │
│   │  payments-P2(replica)│◄──────────── │  payments-P2 (LEADER)│    │
│   │                      │              │                      │    │
│   └──────────────────────┘              └──────────────────────┘    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘

Partition assignment (example):
  orders:     P0 → Broker-1,  P1 → Broker-2,  P2 → Broker-1
  payments:   P0 → Broker-2,  P1 → Broker-1,  P2 → Broker-2
```

Arrows = replication direction (leader → follower).
Clients (producers/consumers) ONLY talk to leaders.

This is our baseline. Now let's add Kafka Streams on top of it.

---

# Section 2: Where Does Kafka Streams Live?

## 2.1 The Big Question — Separate App or Inside Consumer?

```
Option A: Inside existing consumer app
┌──────────────────────────────────────┐
│  order-service (Spring Boot)         │
│                                      │
│  @KafkaListener(...)   ← consumer   │
│  KafkaStreams(...)      ← streams    │  ← BOTH in same app
│  OrderController        ← REST      │
└──────────────────────────────────────┘

Option B: Separate dedicated app (✓ RECOMMENDED)
┌─────────────────────┐    ┌──────────────────────────┐
│  order-service      │    │  order-analytics-streams │
│  (Spring Boot)      │    │  (Spring Boot)           │
│                     │    │                          │
│  @KafkaListener     │    │  KafkaStreams topology    │
│  REST controllers   │    │  State stores (RocksDB)  │
│  Business logic     │    │  Stream processing       │
└─────────────────────┘    └──────────────────────────┘
      consumer app              streams app
```

### Why separate?

| Concern | Same App | Separate App |
|---------|----------|-------------|
| Scaling | Can't scale consumer and streams independently | Scale each based on its own load |
| Failure blast radius | Stream crash kills your consumer | Isolated failures |
| Resource contention | RocksDB + business logic fight for memory | Each has its own resources |
| Deployment | One change redeploys everything | Deploy independently |
| Clarity | Mixed responsibilities | Clear ownership |

**For production at Meta-scale: ALWAYS separate.**

From here on, we'll work with Option B — a dedicated Kafka Streams application.

---

# Section 3: Sub-Topologies — How Kafka Streams Splits Your Logic

This is where most people's understanding breaks down.

## 3.1 What is a Sub-Topology?

When you define a topology, Kafka Streams analyzes the DAG and splits it into
**independent sub-topologies** — groups of processors that can run independently.

**Rule:** A new sub-topology is created whenever data needs to be **repartitioned**
(written to an internal topic and read back with different partitioning).

### Example: Simple topology — ONE sub-topology

```
Use case: Filter orders > $100

  SOURCE(orders) → FILTER(amount>100) → SINK(high-value-orders)

  This is ONE sub-topology because data flows straight through.
  No repartitioning needed.

  ┌─────────────────────────────────────────┐
  │            SUB-TOPOLOGY 0               │
  │                                         │
  │  SOURCE → FILTER → SINK                 │
  └─────────────────────────────────────────┘
```

### Example: GroupBy changes key — TWO sub-topologies

```
Use case: Count orders per PRODUCT (but key is orderId, not productId)

  SOURCE(orders) → selectKey(productId) → groupByKey → count → SINK

  selectKey CHANGES the key from orderId → productId.
  This means data must be REPARTITIONED (sent to internal topic keyed by productId).

  ┌─────────────────────────────────────────┐
  │            SUB-TOPOLOGY 0               │
  │                                         │
  │  SOURCE(orders) → selectKey(productId)  │
  │                     │                   │
  │                     ▼                   │
  │            SINK(repartition-topic)       │
  └─────────────────────────────────────────┘
                        │
            internal repartition topic
            (keyed by productId now)
                        │
  ┌─────────────────────────────────────────┐
  │            SUB-TOPOLOGY 1               │
  │                                         │
  │  SOURCE(repartition-topic)              │
  │         │                               │
  │         ▼                               │
  │  groupByKey → count → SINK(product-     │
  │                        order-counts)    │
  └─────────────────────────────────────────┘
```

**Why does this matter?** Because tasks are created PER sub-topology PER partition.
This directly controls parallelism. More on this next.

---

# Section 4: Tasks, Threads & Parallelism — The Execution Engine

This is the CORE of Kafka Streams architecture. Everything else builds on this.

## 4.1 What is a Task?

A **Task** is the unit of parallelism in Kafka Streams.

**Formula:**
```
Number of Tasks = Number of Partitions × Number of Sub-Topologies
```

For our example (1 sub-topology, `orders` topic with 3 partitions):

```
Sub-Topology 0 reads from: orders (3 partitions)

Tasks created:
  Task 0_0  →  processes orders-P0
  Task 0_1  →  processes orders-P1
  Task 0_2  →  processes orders-P2

  (format: subtopology_partition)
```

For TWO sub-topologies (repartition case) with 3 partitions each:

```
Sub-Topology 0 reads from: orders (3 partitions)
Sub-Topology 1 reads from: repartition-topic (3 partitions)

Tasks created:
  Task 0_0  →  orders-P0          Task 1_0  →  repartition-P0
  Task 0_1  →  orders-P1          Task 1_1  →  repartition-P1
  Task 0_2  →  orders-P2          Task 1_2  →  repartition-P2

  Total: 6 tasks
```

### Key Rule: ONE partition is processed by EXACTLY ONE task. NEVER shared.

```
  orders-P0 ──→ Task 0_0    ✓  (1 partition → 1 task)
  orders-P0 ──→ Task 0_0 }
  orders-P0 ──→ Task 0_1 }  ✗  NEVER (1 partition → 2 tasks)
```

This is how ordering is guaranteed — all records in a partition go through
the same task, in order.

---

## 4.2 What is a Stream Thread?

A **Stream Thread** is a JVM thread that executes one or more tasks.

You configure the number of threads with:
```
num.stream.threads = 2  (default: 1)
```

### How Tasks Map to Threads

```
┌────────────────────────────────────────────────────────────────┐
│           KAFKA STREAMS APP — Instance 1                       │
│           (Spring Boot on JVM)                                 │
│                                                                │
│   num.stream.threads = 2                                       │
│                                                                │
│   ┌────────────────────────────┐  ┌────────────────────────────┐│
│   │    STREAM THREAD-1         │  │    STREAM THREAD-2         ││
│   │                            │  │                            ││
│   │  ┌──────────┐ ┌──────────┐│  │  ┌──────────┐             ││
│   │  │ Task 0_0 │ │ Task 0_1 ││  │  │ Task 0_2 │             ││
│   │  │          │ │          ││  │  │          │             ││
│   │  │orders-P0 │ │orders-P1 ││  │  │orders-P2 │             ││
│   │  │          │ │          ││  │  │          │             ││
│   │  │ SOURCE → │ │ SOURCE → ││  │  │ SOURCE → │             ││
│   │  │ FILTER → │ │ FILTER → ││  │  │ FILTER → │             ││
│   │  │ SINK     │ │ SINK     ││  │  │ SINK     │             ││
│   │  └──────────┘ └──────────┘│  │  └──────────┘             ││
│   └────────────────────────────┘  └────────────────────────────┘│
│                                                                │
│   Each task runs the FULL topology on its assigned partition    │
└────────────────────────────────────────────────────────────────┘
```

**Critical insight:** Each task contains the COMPLETE copy of the topology.
Task 0_0 runs SOURCE → FILTER → SINK on orders-P0.
Task 0_1 runs SOURCE → FILTER → SINK on orders-P1.
They are completely independent.

### Thread Processing Model

A thread processes its tasks **one record at a time, round-robin**:

```
Thread-1 owns: Task 0_0, Task 0_1

Processing loop:
  1. Poll records from Kafka for ALL owned tasks
  2. Pick Task 0_0 → process ONE record through its topology
  3. Pick Task 0_1 → process ONE record through its topology
  4. Back to Task 0_0 → process next record
  5. ... repeat forever

  NOT parallel within a thread — tasks are interleaved.
  Parallelism comes from MULTIPLE threads.
```

---

## 4.3 Scaling — The Power of the Model

### Scenario A: 1 App Instance, 1 Thread

```
┌──────────────────────────────────────┐
│  App Instance 1 (num.stream.threads=1)│
│                                      │
│  ┌────────────────────────────────┐  │
│  │  Thread-1                      │  │
│  │  Task 0_0 (P0)                │  │
│  │  Task 0_1 (P1)   ← overloaded │  │
│  │  Task 0_2 (P2)                │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘
1 thread doing ALL the work. Bottleneck.
```

### Scenario B: 1 App Instance, 3 Threads

```
┌──────────────────────────────────────────────────────┐
│  App Instance 1 (num.stream.threads=3)                │
│                                                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│  │  Thread-1     │ │  Thread-2     │ │  Thread-3     │ │
│  │  Task 0_0(P0) │ │  Task 0_1(P1) │ │  Task 0_2(P2) │ │
│  └──────────────┘ └──────────────┘ └──────────────┘ │
└──────────────────────────────────────────────────────┘
Perfect — 1 task per thread. Maximum parallelism within 1 JVM.
```

### Scenario C: 2 App Instances, Each with 2 Threads (Horizontal Scaling)

```
┌───────────────────────────────┐    ┌───────────────────────────────┐
│  App Instance 1 (threads=2)   │    │  App Instance 2 (threads=2)   │
│                               │    │                               │
│  ┌───────────┐ ┌───────────┐ │    │  ┌───────────┐               │
│  │ Thread-1   │ │ Thread-2   │ │    │  │ Thread-3   │               │
│  │ Task 0_0  │ │ Task 0_1  │ │    │  │ Task 0_2  │               │
│  │ (P0)      │ │ (P1)      │ │    │  │ (P2)      │               │
│  └───────────┘ └───────────┘ │    │  └───────────┘               │
└───────────────────────────────┘    └───────────────────────────────┘

Kafka Streams automatically rebalanced tasks across instances.
Instance 2's Thread-4 is IDLE (no more partitions to assign).
```

### Scenario D: OVER-PROVISIONED (more threads than partitions)

```
3 partitions, but 6 threads across 3 instances:

  Instance 1: Thread-1 (Task 0_0), Thread-2 (IDLE)
  Instance 2: Thread-3 (Task 0_1), Thread-4 (IDLE)
  Instance 3: Thread-5 (Task 0_2), Thread-6 (IDLE)

  ⚠️ IDLE threads = wasted resources.
  Max useful threads = number of partitions = 3
```

### Scaling Rule

```
  Max parallelism = max(partitions across all input topics per sub-topology)

  orders has 3 partitions → max 3 tasks for sub-topology 0
  → max 3 threads doing useful work
  → adding more threads/instances beyond 3 = waste
```

**This is why partition count is an ARCHITECTURE DECISION, not an ops decision.**
Choose partition count at topic creation time based on expected parallelism needs.

---

## 4.4 How Tasks Are Assigned — The Consumer Group Protocol

Kafka Streams uses the **same consumer group protocol** as regular consumers.

```
  application.id = "order-analytics"    ← this IS the consumer group ID

  ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
  │  Instance 1    │    │  Instance 2    │    │  Instance 3    │
  │  app.id =      │    │  app.id =      │    │  app.id =      │
  │  order-        │    │  order-        │    │  order-        │
  │  analytics     │    │  analytics     │    │  analytics     │
  └───────┬───────┘    └───────┬───────┘    └───────┬───────┘
          │                    │                    │
          └────────────────────┼────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Kafka Broker        │
                    │  Group Coordinator   │
                    │                      │
                    │  "order-analytics"   │
                    │  group has 3 members │
                    │  Assign partitions:  │
                    │  P0→Inst1            │
                    │  P1→Inst2            │
                    │  P2→Inst3            │
                    └─────────────────────┘
```

When you start/stop an instance → **rebalance** happens (just like consumer groups).
Tasks get redistributed across surviving instances.

---

## 4.5 Registration Flow — How Instances & Threads Join the Group

Kafka Streams doesn't magically know about your instances. Every instance
and every thread inside it must **register** with the Kafka cluster's
Group Coordinator. Only AFTER registration does partition assignment happen.

### Step-by-Step: What Happens When You Start 2 Instances

```
Timeline:

t=0  Instance 1 starts (num.stream.threads=2)
     ┌──────────────────────────────────────────────────────────────┐
     │  Instance 1 JVM boots up                                     │
     │                                                              │
     │  Thread-1 → sends JoinGroup("order-analytics") to Broker    │
     │  Thread-2 → sends JoinGroup("order-analytics") to Broker    │
     │                                                              │
     │  Each thread is a SEPARATE consumer in the group.            │
     │  So 2 threads = 2 members in the consumer group.             │
     └──────────────────────────────────────────────────────────────┘
                              │
                              ▼
     ┌──────────────────────────────────────────────────────────────┐
     │  Kafka Broker — Group Coordinator                            │
     │                                                              │
     │  Group: "order-analytics"                                    │
     │  Members: [Thread-1@Inst1, Thread-2@Inst1]                  │
     │                                                              │
     │  Coordinator picks Thread-1@Inst1 as GROUP LEADER            │
     │  (leader = the thread that runs the partition assignor)      │
     │                                                              │
     │  Leader calculates assignment:                                │
     │    Thread-1@Inst1 → Task 0_0 (P0), Task 0_1 (P1)           │
     │    Thread-2@Inst1 → Task 0_2 (P2)                           │
     │                                                              │
     │  Sends SyncGroup response with assignments                   │
     └──────────────────────────────────────────────────────────────┘
                              │
                              ▼
     Instance 1 is now processing ALL 3 partitions.


t=5  Instance 2 starts (num.stream.threads=1)
     ┌──────────────────────────────────────────────────────────────┐
     │  Instance 2 JVM boots up                                     │
     │                                                              │
     │  Thread-1 → sends JoinGroup("order-analytics") to Broker    │
     │                                                              │
     │  NEW member joining → triggers REBALANCE                     │
     └──────────────────────────────────────────────────────────────┘
                              │
                              ▼
     ┌──────────────────────────────────────────────────────────────┐
     │  REBALANCE triggered                                         │
     │                                                              │
     │  Step 1: ALL existing threads PAUSE processing               │
     │          (with cooperative rebalancing, only affected tasks)  │
     │                                                              │
     │  Step 2: ALL threads re-send JoinGroup                       │
     │                                                              │
     │  Group members now:                                          │
     │  [Thread-1@Inst1, Thread-2@Inst1, Thread-1@Inst2]           │
     │  = 3 threads total                                           │
     │                                                              │
     │  Step 3: Leader recalculates assignment:                     │
     │    Thread-1@Inst1 → Task 0_0 (P0)                           │
     │    Thread-2@Inst1 → Task 0_1 (P1)                           │
     │    Thread-1@Inst2 → Task 0_2 (P2)  ← moved here             │
     │                                                              │
     │  Step 4: SyncGroup response sent to all                      │
     │  Step 5: All threads resume processing                       │
     └──────────────────────────────────────────────────────────────┘
```

### Key Insight: Threads Register, NOT Instances

```
  Instance 1 (threads=2)    Instance 2 (threads=1)
  ┌─────────────────────┐   ┌─────────────────────┐
  │ Thread-1 ──────────────────────────────────────────┐
  │ Thread-2 ──────────────────────────────────┐       │
  └─────────────────────┘   │ Thread-1 ────┐   │       │
                            └──────────────┼───┼───────┘
                                           │   │   │
                                           ▼   ▼   ▼
                            ┌──────────────────────────────┐
                            │  Group Coordinator            │
                            │                              │
                            │  Group: "order-analytics"    │
                            │  Members:                    │
                            │    1. Thread-1@Instance1     │
                            │    2. Thread-2@Instance1     │
                            │    3. Thread-1@Instance2     │
                            │                              │
                            │  Sees 3 CONSUMERS, not 2    │
                            │  instances.                  │
                            │                              │
                            │  Assigns partitions to       │
                            │  THREADS, not to instances.  │
                            └──────────────────────────────┘

  From Kafka's perspective:
    - It sees 3 consumers (threads) in group "order-analytics"
    - It doesn't know or care they're in 2 JVMs
    - It assigns partitions to threads, period
```

### What Happens When a Thread is Added/Removed

```
Scenario: Instance 1 changes num.stream.threads from 2 → 3 at runtime
(Kafka Streams 2.8+ supports dynamic thread add/remove via addStreamThread())

  BEFORE:
  ┌──────────────────────┐
  │  Group Members:       │
  │  Thread-1@Inst1       │  → P0
  │  Thread-2@Inst1       │  → P1
  │  Thread-1@Inst2       │  → P2
  └──────────────────────┘

  Instance 1 calls: kafkaStreams.addStreamThread()

  → Thread-3@Inst1 sends JoinGroup
  → REBALANCE
  → Reassignment with 4 threads for 3 partitions:

  AFTER:
  ┌──────────────────────┐
  │  Group Members:       │
  │  Thread-1@Inst1       │  → P0
  │  Thread-2@Inst1       │  → P1
  │  Thread-3@Inst1       │  → (IDLE, no partition left)
  │  Thread-1@Inst2       │  → P2
  └──────────────────────┘

  Thread-3 is IDLE because there are only 3 partitions.
  4 threads > 3 partitions = 1 thread wasted.
```

### Heartbeats — How Kafka Knows a Thread is Alive

```
  After registration, each thread sends periodic HEARTBEATS:

  Thread-1@Inst1 ──heartbeat──► Group Coordinator  (every 3s by default)
  Thread-2@Inst1 ──heartbeat──► Group Coordinator
  Thread-1@Inst2 ──heartbeat──► Group Coordinator

  If no heartbeat received within session.timeout.ms (default 45s):
  → Coordinator considers that thread DEAD
  → Triggers REBALANCE
  → Reassigns its partitions/tasks to other threads

  Config:
    heartbeat.interval.ms = 3000    (how often to heartbeat)
    session.timeout.ms    = 45000   (how long before declared dead)
```

### Summary: The Registration Protocol

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                                                                 │
  │  1. App starts → each Stream Thread sends JoinGroup request    │
  │  2. Coordinator collects all members                            │
  │  3. Picks one thread as GROUP LEADER                            │
  │  4. Leader runs StreamsPartitionAssignor:                       │
  │     - Knows how many partitions each input topic has           │
  │     - Creates tasks (sub-topology × partition)                 │
  │     - Distributes tasks evenly across all threads              │
  │  5. Coordinator sends assignments via SyncGroup response       │
  │  6. Each thread starts processing its assigned tasks           │
  │  7. Threads send heartbeats continuously                       │
  │  8. Any thread joins/leaves → repeat from step 1              │
  │                                                                 │
  │  This is the EXACT same protocol as regular Kafka consumers.   │
  │  Kafka Streams just uses a custom PartitionAssignor that       │
  │  also handles task assignment and state store distribution.    │
  └─────────────────────────────────────────────────────────────────┘
```


---

# Section 5: State Stores & RocksDB — The Local Database

## 5.1 Why Do We Need Local State?

Stateful operations (count, aggregate, join) need to REMEMBER previous values.

```
Use case: Count orders per customer

  Record arrives: { customerId: "C1", orderId: "O5" }

  To count, you need to know: "How many orders has C1 had so far?"
  → You need to READ the current count
  → INCREMENT it
  → WRITE it back

  WHERE do you read/write? → State Store
```

### Options for State Storage

```
Option A: Remote database (PostgreSQL, Redis)

  App Instance → network call → PostgreSQL → response
                    ↑
            SLOW (network latency on EVERY record)
            BOTTLENECK (DB becomes single point of failure)
            ANTI-PATTERN for stream processing

Option B: Local embedded database (RocksDB) ← KAFKA STREAMS' CHOICE

  App Instance → local disk read → RocksDB → instant
                    ↑
            FAST (no network, local SSD)
            SCALES with instances (each has its own DB)
            NO shared state between instances
```

---

## 5.2 RocksDB — What Is It?

RocksDB is a **high-performance embedded key-value store** developed by Facebook.

```
  NOT a server.
  NOT a database you connect to.
  It's a LIBRARY (C++ with Java bindings) that stores data in LOCAL FILES.

  Think of it as: a very fast HashMap that persists to disk.

  ┌──────────────────────────────────────────────┐
  │  Your Spring Boot JVM                         │
  │                                               │
  │  ┌─────────────────────────────────────────┐  │
  │  │  Kafka Streams Library                   │  │
  │  │                                         │  │
  │  │  ┌───────────────────────────────────┐  │  │
  │  │  │  RocksDB Instance                  │  │  │
  │  │  │                                   │  │  │
  │  │  │  In-memory: Block Cache (read)    │  │  │
  │  │  │  In-memory: MemTable (write)      │  │  │
  │  │  │  On-disk:   SST files (persisted) │  │  │
  │  │  │                                   │  │  │
  │  │  │  Path: /tmp/kafka-streams/        │  │  │
  │  │  │        <app-id>/<task-id>/        │  │  │
  │  │  └───────────────────────────────────┘  │  │
  │  └─────────────────────────────────────────┘  │
  └──────────────────────────────────────────────┘
```

---

## 5.3 Each Task Has Its OWN State Store

This is crucial. State is NOT shared between tasks.

```
┌────────────────────────────────────────────────────────────────┐
│           KAFKA STREAMS APP — Instance 1                       │
│                                                                │
│   ┌────────────────────────┐  ┌────────────────────────────┐  │
│   │  Thread-1               │  │  Thread-2                   │  │
│   │                         │  │                             │  │
│   │  ┌───────────────────┐ │  │  ┌───────────────────┐     │  │
│   │  │ Task 0_0          │ │  │  │ Task 0_2          │     │  │
│   │  │ orders-P0         │ │  │  │ orders-P2         │     │  │
│   │  │                   │ │  │  │                   │     │  │
│   │  │ ┌───────────────┐ │ │  │  │ ┌───────────────┐ │     │  │
│   │  │ │ RocksDB       │ │ │  │  │ │ RocksDB       │ │     │  │
│   │  │ │ customer-     │ │ │  │  │ │ customer-     │ │     │  │
│   │  │ │ counts for    │ │ │  │  │ │ counts for    │ │     │  │
│   │  │ │ keys in P0    │ │ │  │  │ │ keys in P2    │ │     │  │
│   │  │ └───────────────┘ │ │  │  │ └───────────────┘ │     │  │
│   │  └───────────────────┘ │  │  └───────────────────┘     │  │
│   │                         │  │                             │  │
│   │  ┌───────────────────┐ │  │                             │  │
│   │  │ Task 0_1          │ │  │                             │  │
│   │  │ orders-P1         │ │  │                             │  │
│   │  │ ┌───────────────┐ │ │  │                             │  │
│   │  │ │ RocksDB       │ │ │  │                             │  │
│   │  │ │ customer-     │ │ │  │                             │  │
│   │  │ │ counts for    │ │ │  │                             │  │
│   │  │ │ keys in P1    │ │ │  │                             │  │
│   │  │ └───────────────┘ │ │  │                             │  │
│   │  └───────────────────┘ │  │                             │  │
│   └────────────────────────┘  └────────────────────────────┘  │
│                                                                │
│   Disk:                                                        │
│   /tmp/kafka-streams/order-analytics/0_0/rocksdb/             │
│   /tmp/kafka-streams/order-analytics/0_1/rocksdb/             │
│   /tmp/kafka-streams/order-analytics/0_2/rocksdb/             │
└────────────────────────────────────────────────────────────────┘
```

**Why per-task?**
- Customer "C1" is in orders-P0 → Task 0_0's RocksDB has C1's count
- Customer "C7" is in orders-P2 → Task 0_2's RocksDB has C7's count
- They NEVER need to talk to each other
- No locking, no coordination, no distributed transactions

---

## 5.4 Changelog Topics — The Safety Net

**Problem:** RocksDB is on local disk. If the machine dies, state is LOST.

**Solution:** Every state store change is ALSO written to a **changelog topic** in Kafka.

```
  Record arrives: { customerId: "C1", orderId: "O5" }

  Task 0_0 processes it:
    1. Read current count from RocksDB:  C1 → 4
    2. Increment:                         C1 → 5
    3. Write to RocksDB:                  C1 → 5   ← LOCAL (fast)
    4. Write to changelog topic:          C1 → 5   ← KAFKA (durable)

  ┌─────────────────────────┐          ┌──────────────────────────┐
  │  Task 0_0               │          │  KAFKA CLUSTER            │
  │                         │          │                           │
  │  RocksDB                │  step 4  │  changelog topic:         │
  │  ┌───────────────────┐  │ ───────► │  order-analytics-         │
  │  │ C1 → 5            │  │          │  customer-counts-         │
  │  │ C2 → 3            │  │          │  changelog-0              │
  │  │ C3 → 12           │  │          │                           │
  │  └───────────────────┘  │          │  Records:                 │
  │                         │          │  C1→1, C1→2, C1→3,       │
  │  step 3 = local write   │          │  C1→4, C1→5              │
  └─────────────────────────┘          └──────────────────────────┘
```

The changelog topic has **compaction** enabled:
- Kafka keeps only the LATEST value per key
- Old values (C1→1, C1→2, etc.) are eventually deleted
- Keeps the topic small

**Changelog topic naming:** `{application.id}-{store-name}-changelog-{partition}`

---

## 5.5 State Recovery — Rebuilding RocksDB from Changelog

When a task starts on a NEW machine (after failure or rebalance):

```
  Machine dies → Task 0_0 moves to Instance 2

  Instance 2 has NO RocksDB data for Task 0_0.

  Recovery process:
  ┌─────────────────────────┐          ┌──────────────────────────┐
  │  Instance 2 (new home)  │          │  KAFKA CLUSTER            │
  │                         │          │                           │
  │  Task 0_0 starting...   │  read    │  changelog topic:         │
  │                         │ ◄─────── │  ...customer-counts-      │
  │  RocksDB (empty)        │  ALL     │  changelog-0              │
  │  ┌───────────────────┐  │ records  │                           │
  │  │ (rebuilding...)   │  │          │  Compacted records:       │
  │  │ C1 → 5  ✓ rebuilt │  │          │  C1→5, C2→3, C3→12      │
  │  │ C2 → 3  ✓ rebuilt │  │          │                           │
  │  │ C3 → 12 ✓ rebuilt │  │          │                           │
  │  └───────────────────┘  │          └──────────────────────────┘
  │                         │
  │  ✓ Recovery complete    │
  │  Task 0_0 resumes       │
  │  processing new records  │
  └─────────────────────────┘

  Time to recover = size of compacted changelog
  For large state stores → recovery can take MINUTES to HOURS
```

### Standby Replicas — Faster Recovery

To avoid long recovery times, you can configure **standby replicas**:

```
  num.standby.replicas = 1   (default: 0)

  ┌──────────────────┐              ┌──────────────────┐
  │  Instance 1       │              │  Instance 2       │
  │                   │              │                   │
  │  Task 0_0 ACTIVE  │              │  Task 0_0 STANDBY │
  │  RocksDB (live)   │              │  RocksDB (copy)   │
  │  ┌──────────────┐ │   changelog  │  ┌──────────────┐ │
  │  │ C1→5, C2→3   │ │ ──────────► │  │ C1→5, C2→3   │ │
  │  └──────────────┘ │   replays    │  └──────────────┘ │
  └──────────────────┘              └──────────────────┘

  If Instance 1 dies:
  → Instance 2 already has the state
  → Promotes standby to active INSTANTLY
  → No replay from changelog needed
  → Near-zero downtime
```



---

# Section 6: Failure Scenarios — What Breaks and How Kafka Streams Recovers

This is the most important section for production systems.

## 6.1 Failure Scenario 1: Application Instance Crashes

```
BEFORE (2 instances, 3 tasks):

  Instance 1          Instance 2
  ┌───────────┐      ┌───────────┐
  │ Task 0_0  │      │ Task 0_2  │
  │ Task 0_1  │      │           │
  │ RocksDB×2 │      │ RocksDB×1 │
  └───────────┘      └───────────┘

  Instance 1 CRASHES 💥

AFTER (rebalance triggered):

  Instance 1          Instance 2
  ┌───────────┐      ┌───────────┐
  │   DEAD    │      │ Task 0_0  │ ← reassigned
  │           │      │ Task 0_1  │ ← reassigned
  │           │      │ Task 0_2  │ ← already here
  └───────────┘      └───────────┘

  Recovery steps for Task 0_0 and 0_1 on Instance 2:
  1. Kafka detects Instance 1 left the consumer group
  2. Group coordinator triggers REBALANCE
  3. Tasks 0_0 and 0_1 assigned to Instance 2
  4. Instance 2 has NO state for these tasks
  5. Reads changelog topics to rebuild RocksDB ← SLOW (minutes)
  6. Once rebuilt, tasks resume processing

  With standby replicas (num.standby.replicas=1):
  → Step 5 is SKIPPED (state already replicated)
  → Recovery in SECONDS, not minutes
```

---

## 6.2 Failure Scenario 2: Broker Goes Down

```
  BROKER-1 dies 💥

  BEFORE:
    Broker-1: orders-P0 (LEADER), orders-P2 (LEADER), payments-P1 (LEADER)
    Broker-2: orders-P1 (LEADER), payments-P0 (LEADER), payments-P2 (LEADER)

  AFTER:
    Broker-1: DEAD
    Broker-2: orders-P0 (LEADER ← promoted from replica)
              orders-P1 (LEADER)
              orders-P2 (LEADER ← promoted)
              payments-P0 (LEADER)
              payments-P1 (LEADER ← promoted)
              payments-P2 (LEADER)

  Impact on Kafka Streams:
  ┌─────────────────────────────────────────────────┐
  │  Streams App                                     │
  │                                                 │
  │  Task 0_0 was reading orders-P0 from Broker-1  │
  │       → Brief pause while leader election       │
  │       → Automatically reconnects to Broker-2   │
  │       → Resumes processing (no data loss)      │
  │                                                 │
  │  NO REBALANCE of tasks needed.                  │
  │  NO state loss (RocksDB is local to the app).  │
  │  Only a brief pause during leader election.    │
  └─────────────────────────────────────────────────┘

  Key: Broker failure does NOT affect Kafka Streams state.
  State lives in your app's RocksDB, not in the broker.
  Only the READ path is temporarily disrupted.
```

---

## 6.3 Failure Scenario 3: A Thread Dies (but JVM lives)

```
  Instance 1 has 2 threads:

  ┌──────────────────────────────────────┐
  │  Instance 1                          │
  │                                      │
  │  Thread-1: Task 0_0, Task 0_1  ✓    │
  │  Thread-2: Task 0_2            💥   │ ← uncaught exception
  └──────────────────────────────────────┘

  What happens depends on your error handler:

  Option A: DEFAULT — entire app shuts down
    streams.setUncaughtExceptionHandler(...)
    → StreamsUncaughtExceptionHandler.SHUTDOWN_CLIENT
    → ALL threads stop, ALL tasks stop
    → Full rebalance across remaining instances

  Option B: REPLACE_THREAD (recommended)
    → StreamsUncaughtExceptionHandler.REPLACE_THREAD
    → Dead thread is replaced by a NEW thread
    → Task 0_2 is reassigned to the new thread
    → RocksDB state for Task 0_2 is STILL on disk
    → Recovery is INSTANT (no changelog replay)

  Option C: SHUTDOWN_APPLICATION
    → The entire application exits (System.exit)
    → Kubernetes/container orchestrator restarts it
    → Full state recovery from changelog on restart
```

---

## 6.4 Failure Scenario 4: RocksDB Corruption

```
  Scenario: Disk error corrupts RocksDB SST files

  ┌──────────────────────────────┐
  │  Task 0_0                    │
  │                              │
  │  RocksDB: CORRUPTED 💥      │
  │  ┌────────────────────────┐  │
  │  │  SST files damaged     │  │
  │  │  Cannot read state     │  │
  │  └────────────────────────┘  │
  └──────────────────────────────┘

  Recovery:
  1. Task throws exception → thread dies or is replaced
  2. Delete the corrupted local state directory:
     /tmp/kafka-streams/order-analytics/0_0/
  3. On restart, task detects MISSING state
  4. Rebuilds ENTIRE state from changelog topic
  5. Task resumes normally

  The changelog topic IS the source of truth.
  RocksDB is just a CACHE of the changelog.
  You can ALWAYS delete local state and rebuild.
```

---

## 6.5 Failure Scenario 5: Rebalance Storm

```
  Problem: Instances keep joining/leaving rapidly
  → Continuous rebalances → NO progress on processing

  Timeline:
  t=0   Instance 3 joins     → REBALANCE (all tasks pause)
  t=5s  Instance 2 crashes   → REBALANCE (all tasks pause again)
  t=10s Instance 2 restarts  → REBALANCE (again!)
  t=15s Instance 4 joins     → REBALANCE (again!!)

  During EACH rebalance:
  - ALL tasks on ALL instances STOP processing
  - Partitions are reassigned
  - State may need to be rebuilt
  - Total processing halted

  Solutions:

  1. Static Membership (group.instance.id)
     → Each instance has a PERMANENT identity
     → If it restarts within session.timeout, NO rebalance
     → Partitions stay assigned to the same instance ID

  2. Incremental Cooperative Rebalancing (default since 2.4)
     → Only AFFECTED tasks pause during rebalance
     → Other tasks continue processing
     → Much less disruptive than "stop-the-world" rebalancing

     Stop-the-World:  ALL tasks stop → reassign → ALL restart
     Cooperative:     Only migrating tasks stop → others keep running
```

---

## 6.6 Failure Scenario 6: Poison Pill (Bad Record)

```
  A malformed record arrives that crashes the processor:

  orders-P0: [..., good, good, POISON💀, good, good, ...]

  Task 0_0 processes records in order:
  → good  ✓
  → good  ✓
  → POISON → NullPointerException → Thread dies
  → Task restarts → reads POISON again → dies again
  → INFINITE LOOP 💀

  Solutions:

  1. DeserializationExceptionHandler
     → LogAndContinueExceptionHandler: skip bad record, log it
     → LogAndFailExceptionHandler: stop processing (default)
     → Custom handler: send to dead-letter topic

  2. ProductionExceptionHandler
     → Handles failures during serialization/production
     → Same options: continue or fail

  3. Try-catch in your processor logic
     → Wrap business logic
     → Send failures to a "dead-letter-queue" topic
     → Continue processing

  Dead Letter Queue Pattern:
  ┌──────────┐     ┌───────────────┐     ┌──────────┐
  │ orders   │ ──► │  PROCESSOR    │ ──► │  output  │
  │          │     │               │     │  topic   │
  └──────────┘     │  try/catch:   │     └──────────┘
                   │  if error ──────────► dead-letter-
                   │               │       orders
                   └───────────────┘
```

---

## 6.7 Failure Scenario 7: Kafka Cluster Completely Down

```
  ALL brokers down simultaneously:

  ┌──────────────────────────────────┐
  │  Kafka Streams App                │
  │                                  │
  │  Cannot read input topics   ✗   │
  │  Cannot write output topics ✗   │
  │  Cannot write changelogs    ✗   │
  │                                  │
  │  BUT:                            │
  │  RocksDB state is INTACT   ✓   │
  │  App is still running       ✓   │
  │  Threads are alive          ✓   │
  │  (just retrying connections)    │
  └──────────────────────────────────┘

  Recovery:
  → Kafka cluster comes back online
  → Streams app automatically reconnects
  → Resumes from last committed offset
  → NO state loss (RocksDB was never affected)

  Key insight: Kafka Streams is RESILIENT to temporary cluster outages.
  It keeps retrying with backoff until the cluster is available again.
```

---

# Section 7: The Complete Picture — Putting It All Together

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              KAFKA CLUSTER                                   │
│                                                                              │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────────────────┐  │
│  │ orders-P0  │ │ orders-P1  │ │ orders-P2  │ │  Internal Topics:        │  │
│  │            │ │            │ │            │ │                          │  │
│  │ (Broker-1) │ │ (Broker-2) │ │ (Broker-1) │ │  - repartition topics   │  │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ │  - changelog topics     │  │
│        │              │              │         │  - output topics         │  │
│        │              │              │         └──────────────────────────┘  │
└────────┼──────────────┼──────────────┼──────────────────────────────────────┘
         │              │              │
         │    reads     │    reads     │    reads
         │              │              │
┌────────▼──────┐ ┌─────▼──────┐ ┌────▼─────────┐
│  Instance 1   │ │ Instance 2  │ │  Instance 3   │
│  (Spring Boot)│ │ (Spring Boot)│ │  (Spring Boot)│
│               │ │              │ │               │
│  Thread-1     │ │  Thread-1    │ │  Thread-1     │
│  ┌──────────┐│ │  ┌──────────┐│ │  ┌──────────┐│
│  │ Task 0_0 ││ │  │ Task 0_1 ││ │  │ Task 0_2 ││
│  │          ││ │  │          ││ │  │          ││
│  │ TOPOLOGY:││ │  │ TOPOLOGY:││ │  │ TOPOLOGY:││
│  │ SRC→FIL→ ││ │  │ SRC→FIL→ ││ │  │ SRC→FIL→ ││
│  │ GRP→CNT→ ││ │  │ GRP→CNT→ ││ │  │ GRP→CNT→ ││
│  │ SINK     ││ │  │ SINK     ││ │  │ SINK     ││
│  │          ││ │  │          ││ │  │          ││
│  │ RocksDB  ││ │  │ RocksDB  ││ │  │ RocksDB  ││
│  │ [C1→5]   ││ │  │ [C4→2]   ││ │  │ [C7→8]   ││
│  │ [C2→3]   ││ │  │ [C5→1]   ││ │  │ [C8→4]   ││
│  └──────────┘│ │  └──────────┘│ │  └──────────┘│
│              │ │              │ │               │
│ app.id =     │ │ app.id =     │ │ app.id =      │
│ "order-      │ │ "order-      │ │ "order-       │
│  analytics"  │ │  analytics"  │ │  analytics"   │
└──────────────┘ └──────────────┘ └───────────────┘

Same application.id = same consumer group = tasks distributed automatically
```

---

# Section 8: Summary — What to Whiteboard in Interviews

## The 7 Key Concepts

| # | Concept | One-Liner |
|---|---------|-----------|
| 1 | **Library, not cluster** | Kafka Streams is a JAR inside your Spring Boot app |
| 2 | **Sub-Topology** | DAG split at repartition boundaries |
| 3 | **Task** | Unit of parallelism = 1 sub-topology × 1 partition |
| 4 | **Thread** | JVM thread that executes 1+ tasks (round-robin) |
| 5 | **RocksDB** | Local state store per task, embedded in JVM |
| 6 | **Changelog** | Kafka topic that backs up RocksDB for recovery |
| 7 | **Standby replica** | Pre-warmed copy of state for instant failover |

## The Scaling Formula

```
Max parallelism = number of input topic partitions (per sub-topology)
More partitions → more tasks → more threads/instances can be used
```

## The Recovery Model

```
State lost?  → Rebuild from changelog topic
Slow rebuild? → Use standby replicas (num.standby.replicas=1)
Thread dies?  → REPLACE_THREAD handler (instant, state on disk)
Broker down?  → Brief pause, auto-reconnect, state unaffected
Cluster down? → App waits, retries, resumes when cluster is back
Bad record?   → Dead-letter queue pattern
Rebalance storm? → Static membership + cooperative rebalancing
```

**Next → Coding session: Implementing a Kafka Streams topology in Spring Boot**

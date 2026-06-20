# Kafka Streams — Complete Step-by-Step Flow

> Similar to the Kafka Consumer flow diagram, but for Kafka Streams.
> Covers: Startup → Registration → Task Assignment → State Restoration → Processing → Commit → Failure Recovery

---

## Setup Context

```
Kafka Cluster:
  - Broker-1, Broker-2, Broker-3
  - Active Controller (KRaft)
  - Topic: "order-events" → 3 partitions (P0, P1, P2)
  - Group Coordinator for "order-analytics" → Broker-2

Kafka Streams App:
  - application.id = "order-analytics"
  - num.stream.threads = 2
  - Topology: SOURCE(order-events) → FILTER(amount>100) → GROUP → COUNT → SINK(order-counts)
  - This topology has a stateful operation (COUNT) → needs RocksDB + changelog topic
```

---

## PHASE 1: STARTUP & REGISTRATION (Steps 1–6)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 1:                                                                            │
│  App starts. KafkaStreams.start() is called.                                        │
│  Kafka Streams creates 2 StreamThreads (num.stream.threads=2).                     │
│  Each StreamThread internally creates its OWN KafkaConsumer + KafkaProducer.        │
│                                                                                     │
│  ┌─────────────────────────────────────┐                                            │
│  │  Spring Boot App (JVM)              │                                            │
│  │                                     │                                            │
│  │  KafkaStreams.start()               │                                            │
│  │     │                               │                                            │
│  │     ├── StreamThread-1              │                                            │
│  │     │    ├── KafkaConsumer (own)     │    ← each thread = separate consumer      │
│  │     │    ├── KafkaProducer (own)     │                                            │
│  │     │    └── tasks = [] (empty)     │                                            │
│  │     │                               │                                            │
│  │     └── StreamThread-2              │                                            │
│  │          ├── KafkaConsumer (own)     │                                            │
│  │          ├── KafkaProducer (own)     │                                            │
│  │          └── tasks = [] (empty)     │                                            │
│  └─────────────────────────────────────┘                                            │
│                                                                                     │
│  Note: No tasks assigned yet. No processing. Just thread creation.                  │
│  Note: Unlike regular consumer where YOU create the KafkaConsumer,                  │
│        Kafka Streams creates consumers INTERNALLY inside each thread.               │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 2:                                                                            │
│  Each StreamThread's internal consumer contacts ANY broker to get metadata.         │
│  (Same as regular consumer Step 2-3 in your diagram)                               │
│                                                                                     │
│  StreamThread-1 Consumer ──── "Give me metadata" ────► Broker-1 (any broker)       │
│  StreamThread-2 Consumer ──── "Give me metadata" ────► Broker-1                    │
│                                                                                     │
│  Broker-1 responds with:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  Metadata Response:                                                 │            │
│  │                                                                     │            │
│  │  topic: order-events                                                │            │
│  │    partition 0: leader = Broker-1                                   │            │
│  │    partition 1: leader = Broker-2                                   │            │
│  │    partition 2: leader = Broker-3                                   │            │
│  │                                                                     │            │
│  │  topic: _consumer_offsets                                           │            │
│  │    partition 23: leader = Broker-2                                  │            │
│  │    (hash("order-analytics") % 50 = 23)                             │            │
│  │                                                                     │            │
│  │  Group Coordinator for "order-analytics" → Broker-2                │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  Now threads know: "Go talk to Broker-2 for group coordination."                   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 3:                                                                            │
│  Both StreamThreads send JoinGroup request to Group Coordinator (Broker-2).        │
│  (Same JoinGroup protocol as regular consumers — but with a CUSTOM assignor)       │
│                                                                                     │
│  StreamThread-1 ── JoinGroup("order-analytics") ──► Broker-2 (Group Coordinator)   │
│  StreamThread-2 ── JoinGroup("order-analytics") ──► Broker-2                       │
│                                                                                     │
│  JoinGroup request includes:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  group.id = "order-analytics"                                       │            │
│  │  member.id = "StreamThread-1-<uuid>"                                │            │
│  │  protocol = "stream"   ← NOT "range" or "roundrobin"              │            │
│  │                                                                     │            │
│  │  subscription metadata (Kafka Streams adds EXTRA info):            │            │
│  │  {                                                                  │            │
│  │    subscribed_topics: ["order-events"],                             │            │
│  │    owned_partitions: [],          ← empty on first join            │            │
│  │    process_id: <uuid>,            ← unique per JVM instance        │            │
│  │    user_endpoint: "host:port",    ← for Interactive Queries        │            │
│  │    task_assignment_info: {        ← standby task info              │            │
│  │      active_tasks: [],                                              │            │
│  │      standby_tasks: []                                              │            │
│  │    }                                                                │            │
│  │  }                                                                  │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  DIFFERENCE from regular consumer:                                                  │
│  Regular consumer sends: subscribed topics only                                     │
│  Kafka Streams sends:    subscribed topics + process_id + owned tasks + endpoint    │
│  This extra info is used by StreamsPartitionAssignor for TASK-aware assignment.     │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 4:                                                                            │
│  Broker-2 (Group Coordinator) waits for all members to join (group.initial...       │
│  .rebalance.delay.ms = 3000ms by default), then picks ONE thread as GROUP LEADER.  │
│                                                                                     │
│  Broker-2 sends JoinGroup Response:                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  To StreamThread-1: "You are the LEADER"                           │            │
│  │    members: [StreamThread-1, StreamThread-2]                       │            │
│  │    subscription info of ALL members attached                       │            │
│  │                                                                     │            │
│  │  To StreamThread-2: "You are a FOLLOWER"                           │            │
│  │    members: []  (followers don't see other members)                │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  IMPORTANT: The Leader thread now has to run the assignment algorithm.              │
│  In regular consumer: uses RangeAssignor / RoundRobinAssignor                      │
│  In Kafka Streams:    uses StreamsPartitionAssignor (CUSTOM)                       │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 5:                                                                            │
│  StreamThread-1 (the Leader) runs StreamsPartitionAssignor.assign()                │
│  This is the CRITICAL step unique to Kafka Streams.                                │
│                                                                                     │
│  What the assignor does:                                                            │
│                                                                                     │
│  5a. Discover topology:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  Topology parsed:                                                   │            │
│  │  Sub-topology 0:                                                    │            │
│  │    SOURCE  → reads from "order-events" (3 partitions)              │            │
│  │    FILTER  → stateless                                              │            │
│  │    GROUP   → repartition needed? (depends on key)                  │            │
│  │    COUNT   → STATEFUL → needs state store + changelog topic        │            │
│  │    SINK    → writes to "order-counts"                               │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  5b. Create Tasks (1 task per partition per sub-topology):                          │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  "order-events" has 3 partitions → 3 tasks                         │            │
│  │                                                                     │            │
│  │  Task 0_0 → sub-topology 0, partition 0                            │            │
│  │  Task 0_1 → sub-topology 0, partition 1                            │            │
│  │  Task 0_2 → sub-topology 0, partition 2                            │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  5c. Auto-create internal topics (if they don't exist):                             │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  COUNT is stateful → needs a changelog topic:                       │            │
│  │  Creates: "order-analytics-KSTREAM-AGGREGATE-STATE-STORE-          │            │
│  │            0000000003-changelog"                                     │            │
│  │  Partitions: 3 (same as input topic)                                │            │
│  │  Cleanup policy: compact (keeps latest value per key)              │            │
│  │                                                                     │            │
│  │  If repartition needed, also creates:                               │            │
│  │  "order-analytics-KSTREAM-AGGREGATE-STATE-STORE-                    │            │
│  │   0000000003-repartition"                                           │            │
│  │  Partitions: 3                                                      │            │
│  │  Cleanup policy: delete                                             │            │
│  │                                                                     │            │
│  │  These topics are created by the Leader using AdminClient.          │            │
│  │  application.id is used as PREFIX for topic names.                  │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  5d. Assign tasks to threads:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  2 threads, 3 tasks → distribute evenly:                           │            │
│  │                                                                     │            │
│  │  StreamThread-1 → Task 0_0 (P0), Task 0_1 (P1)                    │            │
│  │  StreamThread-2 → Task 0_2 (P2)                                    │            │
│  │                                                                     │            │
│  │  If num.standby.replicas=1:                                         │            │
│  │  StreamThread-1 → standby of Task 0_2                              │            │
│  │  StreamThread-2 → standby of Task 0_0 or 0_1                      │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 6:                                                                            │
│  Leader sends assignment to Group Coordinator via SyncGroup.                       │
│  Coordinator distributes assignment to ALL members.                                │
│                                                                                     │
│  StreamThread-1 ── SyncGroup(assignments) ──► Broker-2 (Coordinator)               │
│  Broker-2 ── SyncGroup Response ──► StreamThread-1: [Task 0_0, Task 0_1]          │
│  Broker-2 ── SyncGroup Response ──► StreamThread-2: [Task 0_2]                    │
│                                                                                     │
│  Now each thread KNOWS which tasks/partitions it owns.                             │
│  But it can NOT start processing yet — state stores need restoration first.        │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```


---

## PHASE 3: PROCESSING LOOP (Steps 10–15)
## (This is the steady-state — where actual work happens)
## (Runs continuously until app is shut down or rebalance triggers)

```
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 10:                                                                           │
│  Each StreamThread runs an INFINITE LOOP (the "processing loop").                  │
│  It is NOT the same as a simple consumer.poll() loop.                              │
│                                                                                     │
│  StreamThread loop (simplified):                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  while (isRunning) {                                                │            │
│  │      1. pollPhase()      → consumer.poll(Duration)                  │            │
│  │      2. processPhase()   → run records through topology             │            │
│  │      3. punctuatePhase() → fire scheduled timers                    │            │
│  │      4. commitPhase()    → maybe commit offsets + flush state       │            │
│  │  }                                                                  │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  IMPORTANT DETAIL:                                                                  │
│  - poll() fetches records for ALL partitions this thread owns                      │
│  - Records are placed into TASK-SPECIFIC record queues                             │
│  - Each task has its OWN queue (one per assigned partition)                         │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  StreamThread-1 after poll():                                       │            │
│  │                                                                     │            │
│  │  Task 0_0 record queue: [rec1, rec2, rec3]  ← from P0              │            │
│  │  Task 0_1 record queue: [rec4, rec5]        ← from P1              │            │
│  │                                                                     │            │
│  │  Process phase picks ONE record at a time from each task,           │            │
│  │  round-robin across tasks (configurable via max.task.idle.ms)       │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 11:                                                                           │
│  Record flows through the TOPOLOGY — node by node.                                 │
│  This is the actual "stream processing" happening inside a single task.             │
│                                                                                     │
│  Example: Processing ONE record from Task 0_0 (partition 0):                       │
│                                                                                     │
│  Input record: key="laptop", value=OrderEvent{amount=250, ...}                     │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │                                                                     │            │
│  │  ① SOURCE node                                                      │            │
│  │     Deserialize: bytes → key="laptop", value=OrderEvent             │            │
│  │     Timestamp extracted (event-time / ingestion-time / wall-clock)  │            │
│  │                         │                                           │            │
│  │                         ▼                                           │            │
│  │  ② FILTER node (stateless)                                          │            │
│  │     amount > 100? → YES (250 > 100) → forward                      │            │
│  │     If NO → record is DROPPED, no further processing               │            │
│  │                         │                                           │            │
│  │                         ▼                                           │            │
│  │  ③ GROUP-BY-KEY node                                                │            │
│  │     Key = "laptop" (already correct key, no repartition needed)    │            │
│  │     Groups records by key for downstream aggregation                │            │
│  │                         │                                           │            │
│  │                         ▼                                           │            │
│  │  ④ COUNT node (STATEFUL)                                            │            │
│  │     Read from RocksDB: GET("laptop") → current count = 2           │            │
│  │     Compute: 2 + 1 = 3                                              │            │
│  │     Write to RocksDB:  PUT("laptop", 3)                             │            │
│  │     Write to changelog: PRODUCE("...-changelog" P0, "laptop"→3)    │            │
│  │     Forward: key="laptop", value=3                                  │            │
│  │                         │                                           │            │
│  │                         ▼                                           │            │
│  │  ⑤ SINK node                                                        │            │
│  │     Serialize: key="laptop", value=3 → bytes                        │            │
│  │     PRODUCE to "order-counts" topic (partition based on key hash)  │            │
│  │                                                                     │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  KEY INSIGHT:                                                                       │
│  All 5 steps happen SYNCHRONOUSLY for ONE record before the next record            │
│  is processed. There is NO parallelism WITHIN a task.                              │
│  Parallelism is ACROSS tasks (each task runs independently).                        │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 12:                                                                           │
│  STATE STORE interaction in detail (what happened at ④ above).                     │
│  This is the heart of stateful processing.                                          │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │                                                                     │            │
│  │  Application code calls: count()                                    │            │
│  │                                                                     │            │
│  │  Internally:                                                        │            │
│  │                                                                     │            │
│  │  1. READ path:                                                      │            │
│  │     ┌──────────┐    miss     ┌──────────┐   miss   ┌─────────┐    │            │
│  │     │  Record   │ ────────► │  RocksDB  │ ──────► │  RocksDB │    │            │
│  │     │  Cache    │           │  MemTable │         │  SST     │    │            │
│  │     │ (in-heap) │           │(in-memory)│         │ (disk)   │    │            │
│  │     └──────────┘    hit     └──────────┘  hit     └─────────┘    │            │
│  │          ▲                       ▲                      ▲         │            │
│  │          └── fastest             └── fast               └── slow  │            │
│  │                                                                     │            │
│  │  2. WRITE path:                                                     │            │
│  │     Record Cache ← PUT("laptop", 3)                                │            │
│  │          │                                                          │            │
│  │          │ (cache is NOT immediately flushed)                       │            │
│  │          │ (waits for flush trigger)                                │            │
│  │          ▼                                                          │            │
│  │     On flush: → RocksDB MemTable → SST files (compaction)         │            │
│  │                → Changelog topic (PRODUCE to broker)               │            │
│  │                                                                     │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  Record Cache (statestore.cache.max.bytes, default 10MB per thread):               │
│  - Deduplicates rapid updates to SAME key                                          │
│  - If "laptop" count goes 1→2→3→4→5 within cache window,                          │
│    only "laptop"→5 is written to RocksDB and changelog                             │
│  - REDUCES writes to RocksDB and changelog topic significantly                     │
│  - Tradeoff: downstream sees FEWER intermediate results                            │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 13:                                                                           │
│  COMMIT — the consistency boundary.                                                │
│  Controlled by commit.interval.ms (default: 30000ms = 30 seconds).                 │
│                                                                                     │
│  What happens on commit:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │                                                                     │            │
│  │  For EACH task owned by this thread:                                │            │
│  │                                                                     │            │
│  │  1. FLUSH record cache → writes to RocksDB + changelog topic       │            │
│  │  2. FLUSH RocksDB MemTable → SST files on disk                     │            │
│  │  3. PRODUCE all buffered changelog records to broker                │            │
│  │  4. PRODUCE all buffered sink records to output topic               │            │
│  │  5. Wait for ALL produces to be ACK'd by broker                    │            │
│  │  6. COMMIT consumer offsets to __consumer_offsets topic             │            │
│  │  7. Write checkpoint file to disk                                   │            │
│  │                                                                     │            │
│  │  Order matters! If crash happens:                                   │            │
│  │  - After step 6: offsets committed, won't reprocess                │            │
│  │  - Before step 6: offsets NOT committed, WILL reprocess            │            │
│  │    → at-least-once guarantee                                        │            │
│  │                                                                     │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  For EXACTLY-ONCE (processing.guarantee = exactly_once_v2):                        │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │                                                                     │            │
│  │  Uses Kafka TRANSACTIONS:                                           │            │
│  │                                                                     │            │
│  │  1. producer.beginTransaction()                                     │            │
│  │  2. PRODUCE changelog records                                       │            │
│  │  3. PRODUCE sink output records                                     │            │
│  │  4. producer.sendOffsetsToTransaction(offsets)   ← atomic!         │            │
│  │  5. producer.commitTransaction()                                    │            │
│  │                                                                     │            │
│  │  ALL of the above succeed or NONE do.                              │            │
│  │  Downstream consumers with isolation.level=read_committed           │            │
│  │  will only see records AFTER transaction commits.                  │            │
│  │                                                                     │            │
│  │  If crash mid-transaction → transaction times out → auto-aborted   │            │
│  │  → no partial output visible to downstream consumers               │            │
│  │                                                                     │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  COMPARISON:                                                                        │
│  ┌──────────────────┬──────────────────────────────────────────┐                    │
│  │  at_least_once   │  Fast, may duplicate on crash            │                    │
│  │  exactly_once_v2 │  Slower (transactions), no duplicates    │                    │
│  └──────────────────┴──────────────────────────────────────────┘                    │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 14:                                                                           │
│  OUTPUT — records arrive at the sink topic.                                        │
│                                                                                     │
│  From our example:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │                                                                     │            │
│  │  Topic: "order-counts"                                              │            │
│  │                                                                     │            │
│  │  P0: key=laptop → 3,  key=tablet → 1                              │            │
│  │  P1: key=phone  → 5,  key=headset → 2                             │            │
│  │  P2: key=monitor → 1, key=keyboard → 4                            │            │
│  │                                                                     │            │
│  │  Partition is determined by key hash (same as any Kafka producer). │            │
│  │  Key = groupBy key, NOT the original input key (if rekeyed).       │            │
│  │                                                                     │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  These output records can be consumed by:                                           │
│  - Another Kafka Streams app (chaining pipelines)                                  │
│  - A regular KafkaListener / consumer                                              │
│  - A Kafka Connect sink (to DB, S3, Elasticsearch, etc.)                           │
│  - ksqlDB for further querying                                                     │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 15:                                                                           │
│  PUNCTUATORS — scheduled callbacks (wall-clock or stream-time).                    │
│  These are OPTIONAL but important for time-based operations.                       │
│                                                                                     │
│  Two types:                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │                                                                     │            │
│  │  STREAM_TIME punctuator:                                            │            │
│  │    Fires based on EVENT timestamps in incoming records.             │            │
│  │    If no records arrive → it does NOT fire.                        │            │
│  │    Used for: windowed aggregations, session windows, etc.           │            │
│  │                                                                     │            │
│  │  WALL_CLOCK_TIME punctuator:                                        │            │
│  │    Fires based on actual system clock.                              │            │
│  │    Fires even if no records arrive.                                 │            │
│  │    Used for: periodic cleanup, heartbeat emissions, timeouts.       │            │
│  │                                                                     │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  Example: Windowed count with 5-minute tumbling window:                            │
│  - STREAM_TIME punctuator fires when event-time advances past window boundary     │
│  - Emits final window result to downstream                                         │
│  - Triggers window expiry and cleanup of old state                                 │
│                                                                                     │
│  The loop then goes back to Step 10 (poll) and repeats forever.                    │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │                                                                     │            │
│  │      ┌──────────────────────────────────────────────────┐           │            │
│  │      │                                                  │           │            │
│  │      ▼                                                  │           │            │
│  │   poll() → process() → punctuate() → commit() ─────────┘           │            │
│  │                                                                     │            │
│  │   This loop runs UNTIL:                                             │            │
│  │   - kafkaStreams.close() is called (graceful shutdown)              │            │
│  │   - Uncaught exception (depends on exception handler)              │            │
│  │   - Rebalance triggers (loop pauses, re-enters after rebalance)   │            │
│  │                                                                     │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```
---

## PHASE 2: STATE STORE RESTORATION (Steps 7–9)
## (Only for STATEFUL operations like count, aggregate, reduce, join)
## (Stateless topologies skip directly to Phase 3)

```
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 7:                                                                            │
│  Each thread creates local RocksDB state stores for its stateful tasks.            │
│  State stores are PER-TASK, stored on LOCAL DISK.                                  │
│                                                                                     │
│  StreamThread-1:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  Task 0_0:                                                          │            │
│  │    Creates RocksDB at:                                              │            │
│  │    /tmp/kafka-streams/order-analytics/0_0/rocksdb/                  │            │
│  │    KSTREAM-AGGREGATE-STATE-STORE-0000000003/                        │            │
│  │                                                                     │            │
│  │  Task 0_1:                                                          │            │
│  │    Creates RocksDB at:                                              │            │
│  │    /tmp/kafka-streams/order-analytics/0_1/rocksdb/                  │            │
│  │    KSTREAM-AGGREGATE-STATE-STORE-0000000003/                        │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  StreamThread-2:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  Task 0_2:                                                          │            │
│  │    Creates RocksDB at:                                              │            │
│  │    /tmp/kafka-streams/order-analytics/0_2/rocksdb/                  │            │
│  │    KSTREAM-AGGREGATE-STATE-STORE-0000000003/                        │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  Path format: {state.dir}/{application.id}/{taskId}/rocksdb/{storeName}            │
│  Default state.dir = /tmp/kafka-streams                                            │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 8:                                                                            │
│  State Restoration — replay changelog topic into RocksDB.                          │
│  Each thread creates a SEPARATE "Restore Consumer" (not the main consumer).        │
│                                                                                     │
│  Why needed?                                                                        │
│  If app crashed/restarted or task moved to different thread, RocksDB is empty.     │
│  Changelog topic has ALL historical state changes → replay them to rebuild state.  │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐            │
│  │  StreamThread-1 — Restore Consumer:                                 │            │
│  │                                                                     │            │
│  │  "order-analytics-...-changelog" topic:                             │            │
│  │                                                                     │            │
│  │  Read from P0 (for Task 0_0):                                      │            │
│  │    offset 0:    key=laptop  → value=count:1                        │            │
│  │    offset 1:    key=phone   → value=count:1                        │            │
│  │    offset 2:    key=laptop  → value=count:2                        │            │
│  │    offset 3:    key=tablet  → value=count:1                        │            │
│  │    ... replay until END of partition                                 │            │
│  │                                                                     │            │
│  │  Each record → PUT into RocksDB for Task 0_0                       │            │
│  │  After replay: RocksDB has { laptop:2, phone:1, tablet:1 }        │            │
│  │                                                                     │            │
│  │  Similarly, read from P1 (for Task 0_1):                           │            │
│  │    replay all records → PUT into RocksDB for Task 0_1              │            │
│  └─────────────────────────────────────────────────────────────────────┘            │
│                                                                                     │
│  During restoration:                                                                │
│  - App state = REBALANCING (not RUNNING)                                           │
│  - NO input records are processed                                                  │
│  - The main consumer is PAUSED                                                     │
│  - Only the Restore Consumer is active                                             │
│  - Can take MINUTES or HOURS if state is large (GBs of data)                      │
│                                                                                     │
│  OPTIMIZATION: If RocksDB files already exist on disk (from previous run,          │
│  same machine), only need to replay from checkpoint offset, NOT from beginning.    │
│  Checkpoint file: /tmp/kafka-streams/order-analytics/0_0/.checkpoint              │
│  Contains: changelog partition → last-restored-offset                              │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                     │
│  Step 9:                                                                            │
│  Restoration complete. Tasks transition from RESTORING → RUNNING.                  │
│  App state changes: REBALANCING → RUNNING.                                         │
│  Now the main consumer is unpaused and ready to poll.                              │
│                                                                                     │
│  StreamThread-1:                                                                    │
│    Task 0_0: RUNNING (RocksDB fully restored)                                     │
│    Task 0_1: RUNNING (RocksDB fully restored)                                     │
│                                                                                     │
│  StreamThread-2:                                                                    │
│    Task 0_2: RUNNING (RocksDB fully restored)                                     │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```


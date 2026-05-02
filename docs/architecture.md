# Architecture Overview

This document describes the high-level architecture of the Real-Time E-commerce Order Intelligence Platform built on Apache Fluss (incubating) and Apache Flink SQL.

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Streaming Storage | Apache Fluss | 0.9.0-incubating |
| Stream Processing | Apache Flink SQL | 1.20 |
| Coordination | Apache ZooKeeper | 3.9.2 |
| Data Generation | Flink Faker Connector | - |
| Deployment | Docker Compose | - |

---

## A. Logical Architecture

The platform is organized into six logical layers, each with a distinct responsibility.

```mermaid
graph TB
    subgraph "Operational Layer"
        OPS[Order Service / E-commerce Platform]
    end

    subgraph "Event Ingestion Layer"
        GEN[Flink Faker Generator<br/>50-5000 events/sec]
        RAW[orders_raw<br/>Log Table - Append Only]
    end

    subgraph "Processing Layer"
        ENR[Enrichment Job<br/>Temporal Lookup Joins]
        AGG[Aggregation Jobs<br/>Tumbling Windows]
        DET[Anomaly Detection<br/>Rule-Based Filters]
    end

    subgraph "Storage Layer"
        DIM[Dimension Tables<br/>customer_profile, product_catalog]
        FACT[orders_enriched<br/>PK Table]
        METRICS[revenue_5min, city_revenue_5min<br/>PK Tables]
        ALERTS[high_value_orders, suspicious_orders<br/>Log Tables]
    end

    subgraph "Serving Layer"
        SQL[Flink SQL Client<br/>Ad-hoc Queries]
        DASH[Dashboard Queries<br/>Point Lookups + Scans]
    end

    subgraph "Lakehouse Layer (Future)"
        LAKE[Apache Paimon / Iceberg<br/>Cold Storage & Historical Analytics]
    end

    OPS -->|"fire-and-forget events"| GEN
    GEN --> RAW
    RAW --> ENR
    DIM -->|"lookup"| ENR
    ENR --> FACT
    FACT --> AGG
    FACT --> DET
    AGG --> METRICS
    DET --> ALERTS
    FACT --> SQL
    METRICS --> SQL
    ALERTS --> SQL
    METRICS --> DASH
    FACT -->|"tiered storage"| LAKE
    METRICS -->|"tiered storage"| LAKE
```

---

## B. Data Flow

End-to-end data flow from event generation through enrichment, aggregation, and anomaly detection to queryable results.

```mermaid
flowchart LR
    subgraph "Source"
        FAKER[flink-faker<br/>Synthetic Orders]
    end

    subgraph "Raw Ingestion"
        OR[orders_raw<br/>4 buckets]
    end

    subgraph "Dimensions"
        CP[customer_profile<br/>20 customers]
        PC[product_catalog<br/>30 products]
    end

    subgraph "Enrichment"
        EJ[Temporal Lookup Join<br/>PROCTIME-based]
    end

    subgraph "Enriched Storage"
        OE[orders_enriched<br/>8 buckets]
    end

    subgraph "Analytics"
        W1[TUMBLE 5min<br/>by category]
        W2[TUMBLE 5min<br/>by city]
        HV[WHERE amount >= 12999]
        SUS[Multi-rule fraud filter]
    end

    subgraph "Output Tables"
        R5[revenue_5min]
        CR[city_revenue_5min]
        HVO[high_value_orders]
        SO[suspicious_orders]
    end

    FAKER --> OR
    OR --> EJ
    CP -.->|"FOR SYSTEM_TIME AS OF ptime"| EJ
    PC -.->|"FOR SYSTEM_TIME AS OF ptime"| EJ
    EJ --> OE
    OE --> W1 --> R5
    OE --> W2 --> CR
    OE --> HV --> HVO
    OE --> SUS --> SO
```

---

## C. Hot/Cold Storage Architecture

Fluss provides hot storage for real-time serving with sub-second latency. Cold storage (lakehouse) handles historical queries and long-term retention.

```mermaid
graph TB
    subgraph "Hot Path (Fluss - Milliseconds)"
        direction TB
        TS[Tablet Server<br/>In-Memory + Local Disk]
        KV[KV Snapshots<br/>PK Table State]
        LOG[Log Segments<br/>Append-Only Events]
    end

    subgraph "Tiered Storage (Future)"
        direction TB
        RS[Remote Storage<br/>/tmp/fluss/remote-data]
        SNAP[Periodic Snapshots<br/>Full KV State]
    end

    subgraph "Cold Path (Lakehouse - Future)"
        direction TB
        PAIMON[Apache Paimon<br/>Columnar Format]
        ICEBERG[Apache Iceberg<br/>Time-Travel Queries]
    end

    subgraph "Query Patterns"
        RT[Real-Time Queries<br/>Latest state, sub-second]
        HIST[Historical Queries<br/>Days/weeks/months back]
        BATCH[Batch Analytics<br/>Full table scans]
    end

    TS --> KV
    TS --> LOG
    KV -->|"kv.snapshot.interval"| RS
    LOG -->|"segment rotation"| RS
    RS -->|"compaction + export"| PAIMON
    RS -->|"compaction + export"| ICEBERG

    KV --> RT
    LOG --> RT
    PAIMON --> HIST
    ICEBERG --> BATCH

    style TS fill:#2d5f2d,color:#fff
    style PAIMON fill:#1a3a5c,color:#fff
    style ICEBERG fill:#1a3a5c,color:#fff
```

**Current Configuration:**
- `kv.snapshot.interval: 0s` (snapshots disabled for development)
- `remote.data.dir: /tmp/fluss/remote-data` (local tmpfs in Docker)
- Lakehouse integration is a planned future extension via Fluss tiered storage

---

## D. Failure Recovery Flow

Flink's checkpoint-based recovery combined with Fluss's log replay provides exactly-once semantics for stateful processing.

```mermaid
sequenceDiagram
    participant FM as Flink JobManager
    participant TM as Flink TaskManager
    participant FS as Fluss TabletServer
    participant CP as Checkpoint Storage

    Note over FM,CP: Normal Operation
    FM->>TM: Inject Checkpoint Barrier
    TM->>TM: Snapshot operator state
    TM->>CP: Persist state + offsets
    CP-->>FM: Checkpoint ACK

    Note over FM,CP: Failure Detected
    TM->>FM: TaskManager heartbeat lost
    FM->>FM: Trigger restart strategy<br/>(fixed-delay: 3 attempts, 10s delay)

    Note over FM,CP: Recovery
    FM->>CP: Load latest checkpoint
    CP-->>FM: State + Fluss offsets
    FM->>TM: Redeploy with restored state
    TM->>FS: Resume from saved offset
    FS-->>TM: Replay log from offset
    TM->>TM: Process replayed events
    TM->>FS: Write deduplicated output

    Note over FM,CP: Resumed - No Data Loss
```

**Configured Recovery Parameters:**
- Checkpointing interval: 30 seconds
- Minimum pause between checkpoints: 10 seconds
- Restart strategy: fixed-delay (3 attempts, 10-second delay)
- Checkpoint storage: `file:///tmp/flink-checkpoints`

---

## E. Scaling Architecture

The platform scales horizontally by adding Fluss Tablet Servers (storage parallelism) and Flink TaskManagers (compute parallelism).

```mermaid
graph TB
    subgraph "Coordination Tier"
        ZK[ZooKeeper Ensemble<br/>Metadata + Leader Election]
        CS[Fluss CoordinatorServer<br/>Cluster Management]
        JM[Flink JobManager<br/>Job Scheduling]
    end

    subgraph "Storage Tier - Horizontal Scale"
        TS1[TabletServer-0<br/>Buckets 0,1]
        TS2[TabletServer-1<br/>Buckets 2,3]
        TS3[TabletServer-N<br/>Buckets 4..N]
    end

    subgraph "Compute Tier - Horizontal Scale"
        TM1[TaskManager-1<br/>10 slots]
        TM2[TaskManager-2<br/>10 slots]
        TM3[TaskManager-N<br/>10 slots]
    end

    subgraph "Table Partitioning"
        B1[orders_raw: 4 buckets]
        B2[orders_enriched: 8 buckets]
        B3[revenue_5min: 4 buckets]
    end

    ZK --- CS
    ZK --- JM
    CS -->|"assign buckets"| TS1
    CS -->|"assign buckets"| TS2
    CS -->|"assign buckets"| TS3
    JM -->|"deploy tasks"| TM1
    JM -->|"deploy tasks"| TM2
    JM -->|"deploy tasks"| TM3

    TM1 -->|"read/write"| TS1
    TM2 -->|"read/write"| TS2
    TM3 -->|"read/write"| TS3

    B1 -.-> TS1
    B1 -.-> TS2
    B2 -.-> TS1
    B2 -.-> TS2
    B2 -.-> TS3
    B3 -.-> TS1
```

**Scaling Dimensions:**

| Dimension | Mechanism | Current | Scale Target |
|-----------|-----------|---------|--------------|
| Ingestion throughput | Add TabletServers + increase buckets | 1 TS, 4 buckets | N TS, 32+ buckets |
| Processing parallelism | Add TaskManagers + increase slots | 1 TM, 10 slots | N TM, 10 slots each |
| Storage capacity | Add TabletServers with local SSDs | 1 TS (tmpfs) | N TS (NVMe) |
| Query concurrency | Add TaskManagers for SQL client sessions | 1 TM | N TM |

**Scaling Procedure:**
1. Add new TabletServer instances to Docker Compose with unique `tablet-server.id`
2. Increase `bucket.num` on tables that need more parallelism (requires table recreation)
3. Add TaskManager replicas via `docker compose up --scale taskmanager=N`
4. CoordinatorServer automatically rebalances bucket assignments

---

## Notes on Apache Fluss Maturity

Apache Fluss is incubating software under the Apache Software Foundation. This means:

- The API surface may change between releases
- Production deployment patterns are still being established by the community
- Tiered storage and lakehouse integration are actively under development
- The 0.9.0-incubating release is the first public release suitable for evaluation and development

This architecture is designed for demonstration and evaluation purposes. Production deployments should track the Fluss release notes and community guidance for stability guarantees.

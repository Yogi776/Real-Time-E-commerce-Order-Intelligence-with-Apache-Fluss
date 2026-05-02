# System Design

This document presents the system design rationale, capacity planning, and bottleneck analysis for the Real-Time E-commerce Order Intelligence Platform.

---

## 1. Current Architecture Problems

### 1.1 Operational Database Overload

Traditional real-time analytics implementations query the operational order database directly:

```
[Dashboard] --> [Analytics Queries] --> [Operational DB (orders table)]
                                              ^
                                              |
                                    [Order Service (writes)]
```

**Problems:**
- Analytical queries (aggregations, joins, scans) compete with transactional writes for I/O and CPU
- Read replicas add replication lag (seconds to minutes) and operational complexity
- Index bloat: analytics-optimized indexes degrade write performance
- Connection pool exhaustion during peak query load
- Schema coupling: analytics requirements drive operational schema changes

### 1.2 Batch ETL Latency

The conventional alternative is periodic batch extraction:

```
[Operational DB] --ETL (hourly/daily)--> [Data Warehouse] --> [Dashboards]
```

**Problems:**
- Minimum 1-hour latency between event occurrence and dashboard visibility
- Fraud detection delayed by the batch window -- orders ship before alerts fire
- Revenue reporting is always stale; business decisions based on hours-old data
- ETL failures create data gaps that require manual backfill
- Peak load on source DB during extraction windows

### 1.3 Large Flink State with Kafka

A streaming approach using Kafka + Flink eliminates batch latency but introduces state management challenges:

```
[Kafka] --> [Flink (large state for dimension joins)] --> [External DB]
```

**Problems:**
- Dimension tables stored entirely in Flink operator state (RocksDB)
- State size grows with dimension cardinality; checkpoint size grows proportionally
- Checkpoint duration increases linearly with state size, risking timeout failures
- Dimension updates require job restart or complex state migration
- No native point-lookup serving; requires an external database for query results
- Kafka does not support key-value lookups natively, requiring a separate store

---

## 2. Proposed Architecture: Event-Driven with Flink + Fluss

### 2.1 Core Design Principle

Decouple analytics from operations via an event-driven architecture where:

1. The order service emits events (fire-and-forget)
2. Fluss stores events and dimension state independently
3. Flink processes streams without maintaining large local state
4. Query results are served directly from Fluss PK tables

```
[Order Service]
      |
      | fire-and-forget event
      v
[Fluss: orders_raw (Log Table)]
      |
      | stream consumption
      v
[Flink SQL: Enrichment]----lookup--->[Fluss: customer_profile (PK)]
      |                               [Fluss: product_catalog (PK)]
      |
      | enriched stream
      v
[Fluss: orders_enriched (PK Table)]
      |
      +-----> [Flink: Aggregation] -----> [Fluss: revenue_5min (PK)]
      |                                    [Fluss: city_revenue_5min (PK)]
      |
      +-----> [Flink: Anomaly Detection] -> [Fluss: high_value_orders (Log)]
                                             [Fluss: suspicious_orders (Log)]
```

### 2.2 Why Fluss Solves the State Problem

Apache Fluss provides both streaming log storage AND key-value state in a single system:

| Capability | Kafka | Fluss | Benefit |
|-----------|-------|-------|---------|
| Append-only log | Yes | Yes (Log tables) | Event replay, offset tracking |
| Key-value lookups | No | Yes (PK tables) | Dimension serving without external DB |
| Temporal join support | No (needs external store) | Native (FOR SYSTEM_TIME) | Dimensions stay in Fluss, not Flink state |
| Result serving | No | Yes (PK table reads) | Query results without additional infrastructure |
| Tiered storage | Limited | Planned (Paimon/Iceberg) | Hot/cold separation |

With Fluss, Flink operators perform **lookup joins** against Fluss PK tables rather than maintaining dimension copies in local state. This keeps Flink state small (only window aggregation buffers) and checkpoints fast.

---

## 3. Why This Design Prevents Operational System Impact

### 3.1 Fire-and-Forget Event Emission

The order service writes events to Fluss and immediately returns. There is no:
- Synchronous callback waiting for analytics processing
- Transactional coupling between order commit and event delivery
- Shared resource contention (different storage systems entirely)

### 3.2 Complete Physical Decoupling

| Concern | Operational System | Analytics Platform |
|---------|-------------------|-------------------|
| Storage | Order DB (PostgreSQL, MySQL, etc.) | Fluss (Log + KV) |
| Compute | Application servers | Flink cluster |
| Network | Production VPC | Analytics VPC (can be separate) |
| Failure domain | Order processing | Analytics pipelines |
| Schema | Normalized, write-optimized | Denormalized, read-optimized |

### 3.3 Backpressure Isolation

If the analytics platform is slow or unavailable:
- Events queue in Fluss log segments (bounded by retention)
- The order service is never blocked or slowed
- Analytics catch up automatically when capacity is restored
- No feedback loop from analytics failures to order processing

---

## 4. Data Volume Assumptions

### 4.1 Order Event Characteristics

| Parameter | Value | Notes |
|-----------|-------|-------|
| Average event size | ~1 KB | JSON-equivalent payload with all fields |
| Fields per event | 11 | order_id through event_time |
| Dimension lookup per event | 2 | customer_profile + product_catalog |
| Output records per input event | 5 | enriched + 2 aggregates + up to 2 alerts |

### 4.2 Volume Tiers

| Tier | Events/Day | Events/Sec (avg) | Peak (10x) | Storage/Day (raw) |
|------|-----------|------------------|------------|-------------------|
| Development | 4.3M | 50 | 500 | ~4 GB |
| Standard | 1M | ~12 | 120 | ~1 GB |
| Growth | 10M | ~116 | 1,160 | ~10 GB |
| Scale | 86M | 1,000 | 10,000 | ~86 GB |
| Peak (flash sale) | 432M | 5,000 | 50,000 | ~432 GB |

### 4.3 Retention Windows

| Table | Hot Retention (Fluss) | Cold Retention (Lakehouse) | Rationale |
|-------|----------------------|---------------------------|-----------|
| orders_raw | 1-3 days | 90 days | Replay window for reprocessing |
| orders_enriched | 3-7 days | 1 year | Operational queries + historical |
| revenue_5min | 7 days | 1 year | Dashboard lookback |
| city_revenue_5min | 7 days | 1 year | Geographic analysis |
| high_value_orders | 7 days | 1 year | Audit trail |
| suspicious_orders | 30 days | 2 years | Fraud investigation |
| customer_profile | Indefinite (PK) | N/A | Always current |
| product_catalog | Indefinite (PK) | N/A | Always current |

---

## 5. Capacity Planning

### 5.1 Compute Sizing

| Throughput Target | TaskManagers | Slots/TM | Total Slots | Memory/TM |
|-------------------|-------------|----------|-------------|-----------|
| 50 events/sec | 1 | 10 | 10 | 2 GB |
| 500 events/sec | 2 | 10 | 20 | 4 GB |
| 5,000 events/sec | 5 | 10 | 50 | 4 GB |
| 50,000 events/sec | 20 | 10 | 200 | 8 GB |

**Slot allocation per pipeline:**
- Order generation: 1 slot (source parallelism)
- Enrichment job: 4-8 slots (matching orders_raw bucket count)
- Revenue aggregation (category): 4 slots
- Revenue aggregation (city): 4 slots
- Anomaly detection: 4 slots
- Ad-hoc queries: 1-2 slots each

### 5.2 Storage Sizing

| Throughput | Buckets (orders_raw) | Buckets (orders_enriched) | TabletServers | Storage/Day |
|-----------|---------------------|--------------------------|---------------|-------------|
| 50/sec | 4 | 8 | 1 | ~4 GB |
| 500/sec | 8 | 16 | 2 | ~40 GB |
| 5,000/sec | 16 | 32 | 4 | ~400 GB |
| 50,000/sec | 64 | 128 | 16 | ~4 TB |

### 5.3 Checkpoint Sizing

| State Type | Size Driver | Approximate Size |
|-----------|-------------|-----------------|
| Enrichment job | Minimal (lookup join, no local state) | < 10 MB |
| Window aggregation (5 min) | Active window buffers | 50-500 MB depending on cardinality |
| Anomaly detection | Stateless filter | < 1 MB |

Checkpoint interval of 30 seconds with these state sizes results in:
- Checkpoint duration: < 5 seconds (with asynchronous barriers)
- Checkpoint storage per snapshot: < 1 GB
- Recovery time: < 30 seconds (state restore + log replay)

### 5.4 Partition and Parallelism Planning

| Table | Recommended Buckets | Rationale |
|-------|-------------------|-----------|
| orders_raw | 2x TabletServer count | Distribute write load evenly |
| orders_enriched | 4x TabletServer count | Serves multiple downstream consumers |
| Dimension tables | TabletServer count | Low write rate, lookup-heavy |
| Aggregate tables | TabletServer count | Low write rate, scan-heavy |
| Alert tables | TabletServer count | Low volume relative to main stream |

---

## 6. Bottlenecks and Mitigations

| Bottleneck | Symptom | Root Cause | Mitigation | Monitoring Signal |
|-----------|---------|-----------|------------|-------------------|
| Source ingestion saturation | Watermark lag increases; events/sec plateaus | TabletServer write throughput exceeded for assigned buckets | Add TabletServers; increase `bucket.num` on orders_raw | Flink: `numRecordsOutPerSecond` on source operator |
| Backpressure from enrichment | Source operator shows high backpressure ratio | Lookup join latency spikes due to TabletServer load | Scale TabletServers; increase enrichment parallelism; add lookup cache | Flink Web UI: backpressure indicators on enrichment task |
| Large join state (if stateful) | Checkpoint duration exceeds interval; timeout failures | Dimension data cached in Flink state grows unbounded | Use Fluss temporal lookup join (stateless); avoid regular joins | Flink: `lastCheckpointDuration`, `checkpointAlignmentBuffered` |
| Slow windowed aggregation | Window results delayed; output lag grows | High cardinality GROUP BY with many active windows | Reduce window size; pre-aggregate in enrichment step; increase parallelism | Flink: output watermark lag on aggregation operator |
| Dashboard query contention | Query latency spikes during peak ingestion | Reads and writes competing on same TabletServer buckets | Separate read replicas (future Fluss feature); cache query results | Query response time; TabletServer CPU utilization |
| Lakehouse small files | Compaction backlog; query performance degrades | Frequent flushes from streaming writes create many small files | Tune compaction intervals; batch writes before export; use Paimon's auto-compaction | File count metrics; compaction lag in lakehouse |
| ZooKeeper session timeout | Fluss cluster becomes unavailable; bucket reassignment storms | Network instability or ZooKeeper GC pauses | Tune session timeout; dedicate ZooKeeper nodes; monitor GC | ZooKeeper: outstanding requests, avg latency |
| Checkpoint storage exhaustion | Checkpoints fail; jobs cannot recover | Retained checkpoints accumulate without cleanup | Configure `state.checkpoints.num-retained`; use incremental checkpoints | Checkpoint storage directory size |
| Event time skew | Windows never close; no output produced | Source events have widely varying timestamps | Tighten watermark strategy; add idle source detection | Flink: watermark alignment across subtasks |
| Memory pressure on TaskManager | OOM kills; task failures | Too many concurrent operators sharing JVM heap | Increase `taskmanager.memory.process.size`; reduce slots per TM; enable off-heap state | JVM heap usage; GC pause duration |

---

## 7. Design Trade-offs

### 7.1 Chosen Trade-offs

| Decision | Trade-off | Rationale |
|----------|-----------|-----------|
| Fluss PK tables for dimensions | Adds network hop for each lookup vs. local state | Keeps Flink state small; dimension updates are immediately visible |
| 5-minute tumbling windows | Coarser granularity vs. per-event metrics | Reduces output volume; 5 minutes is sufficient for business dashboards |
| Rule-based fraud detection | Less accurate than ML models | Zero additional infrastructure; deterministic; easy to tune thresholds |
| LEFT JOIN for enrichment | Possible null dimensions in output | Never drops orders; missing dimension data is acceptable for analytics |
| Single TabletServer (dev) | No fault tolerance for storage | Simplifies development; production would use 3+ TabletServers |
| tmpfs for Fluss data | Data lost on container restart | Fast I/O for development; production uses persistent volumes |

### 7.2 Constraints Accepted

- **Fluss is incubating:** API and behavior may change. This design accepts the risk of breaking changes between versions in exchange for the architectural benefits of unified log + KV storage.
- **No exactly-once to external systems:** Within the Fluss + Flink boundary, exactly-once is maintained via checkpoints. External consumers must handle potential duplicates during recovery.
- **Bucket count is immutable:** Changing parallelism requires table recreation and data reload. Initial bucket counts must be sized for anticipated peak, not current load.

---

## 8. Comparison with Alternative Approaches

| Approach | Latency | State Size | Ops Complexity | Query Serving | Ops DB Impact |
|----------|---------|-----------|----------------|---------------|---------------|
| Direct DB polling | Seconds | N/A | Low | DB queries | High |
| Batch ETL | Hours | N/A | Medium | Warehouse | Low (extraction windows) |
| Kafka + Flink + External DB | Seconds | Large (dimensions in state) | High (3 systems) | External DB | None |
| **Fluss + Flink (this design)** | **Seconds** | **Small (lookup joins)** | **Medium (2 systems)** | **Native (PK tables)** | **None** |

The Fluss + Flink approach provides the latency of a streaming architecture with the simplicity of fewer moving parts. Fluss serves as both the streaming transport AND the serving layer, eliminating the need for a separate database to serve query results.

---

## 9. Operational Runbook Summary

### 9.1 Scaling Up

1. Observe: sustained backpressure > 50% on source operators for > 5 minutes
2. Add TaskManager: `docker compose up --scale taskmanager=N`
3. If storage-bound: add TabletServer with new `tablet-server.id`
4. If bucket-bound: recreate table with higher `bucket.num`, replay from source

### 9.2 Handling Job Failures

1. Check Flink Web UI (port 8083) for job status and exception
2. If restart attempts exhausted: diagnose root cause from TaskManager logs
3. Fix and resubmit SQL job; Flink resumes from last checkpoint automatically
4. If checkpoint is corrupted: cancel job, reset offsets, resubmit (data replayed from Fluss log)

### 9.3 Data Freshness Degradation

1. Check watermark lag on Flink Web UI
2. If lag is growing: likely backpressure upstream
3. Identify bottleneck operator (highest backpressure ratio)
4. Scale the bottleneck: add parallelism or reduce per-record processing cost

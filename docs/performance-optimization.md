# Performance Optimization Guide

This document covers tuning strategies for Apache Flink 1.20 and Apache Fluss 0.9.0-incubating in the context of the Real-Time E-commerce Order Intelligence Platform.

---

## Table of Contents

1. [Flink Runtime Tuning](#flink-runtime-tuning)
2. [Fluss Table Design](#fluss-table-design)
3. [Query Tuning](#query-tuning)
4. [Storage Tuning](#storage-tuning)

---

## Flink Runtime Tuning

### Parallelism

Parallelism is the primary lever for throughput. The current configuration provides 10 task slots per TaskManager.

```yaml
# docker-compose.yml (current)
taskmanager.numberOfTaskSlots: 10
taskmanager.memory.process.size: 2048m
```

**Tuning guidelines:**

- Set parallelism per job, not globally. Use `SET 'parallelism.default' = '8';` in SQL client before submitting a job.
- Source parallelism should be a divisor of `bucket.num` on the source table. If `orders_raw` has `bucket.num = 4`, use parallelism 1, 2, or 4.
- Over-parallelism wastes memory on idle subtasks. Under-parallelism causes backpressure.
- For lookup joins against PK tables (`customer_profile`, `product_catalog`), the join side inherits the parallelism of the driving stream.

### Checkpoint Interval and Timeout

Checkpointing is the tension point between recovery speed and runtime overhead.

```yaml
# Current configuration
execution.checkpointing.interval: 30s
execution.checkpointing.min-pause: 10s
state.checkpoints.dir: file:///tmp/flink-checkpoints
state.savepoints.dir: file:///tmp/flink-savepoints
```

| Parameter | Impact of Lower Value | Impact of Higher Value |
|---|---|---|
| `checkpointing.interval` | Faster recovery, more I/O overhead | Slower recovery, less overhead |
| `checkpointing.min-pause` | More frequent checkpoints possible | Prevents checkpoint storms |
| `checkpointing.timeout` | Fails fast on slow checkpoints | Tolerates slow storage |

**Recommendations by workload:**

| Workload | Interval | Min-Pause | Timeout |
|:--------:|:--------:|:---------:|:-------:|
| Development (50 evt/s) | 30s | 10s | 10min |
| Production (1K evt/s) | 30s | 15s | 5min |
| High-throughput (10K+ evt/s) | 60-120s | 30-60s | 10min |

### Restart Strategy

The platform uses a fixed-delay restart strategy:

```yaml
restart-strategy.type: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10s
```

This means: on failure, Flink retries up to 3 times with a 10-second pause between attempts. After 3 consecutive failures, the job enters a `FAILED` state and requires manual intervention.

**Production adjustments:**

| Parameter | Development | Production |
|---|:---:|:---:|
| `fixed-delay.attempts` | 3 | 10 |
| `fixed-delay.delay` | 10s | 30s |

For long-running production jobs, consider `failure-rate` strategy instead:

```yaml
restart-strategy.type: failure-rate
restart-strategy.failure-rate.max-failures-per-interval: 10
restart-strategy.failure-rate.failure-rate-interval: 5min
restart-strategy.failure-rate.delay: 15s
```

This tolerates transient bursts of failures without exhausting retry budget.

### State Backend

| Backend | Memory Model | Checkpoint Style | Best For |
|---------|:------------:|:----------------:|----------|
| HashMap (default) | On-heap | Full snapshot | State < 1 GB, low latency |
| RocksDB | Off-heap + disk | Incremental | State > 1 GB, large windows |

Switch to RocksDB for production:

```yaml
state.backend: rocksdb
state.backend.incremental: true
state.backend.rocksdb.localdir: /mnt/ssd/rocksdb
```

RocksDB incremental checkpoints only persist the delta since the last checkpoint, dramatically reducing checkpoint size and duration at high state volumes.

### Watermark Strategy

The platform uses bounded-out-of-orderness watermarks:

```sql
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

This means events arriving more than 5 seconds late are dropped from window aggregations.

**Tuning trade-offs:**

| Watermark Delay | Completeness | Latency |
|:---------------:|:------------:|:-------:|
| 2 seconds | Lower (drops more late events) | Lower (windows close faster) |
| 5 seconds (current) | Moderate | Moderate |
| 15 seconds | Higher (accepts more late events) | Higher (windows stay open longer) |

For an Indian e-commerce platform where network variability is high, 5 seconds is a reasonable default. Increase to 10-15 seconds if you observe a high late-event drop rate in production.

### Mini-Batch Aggregation

Mini-batching amortizes per-record overhead in aggregation operators by buffering records and processing them in batches.

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';
```

| Parameter | Effect |
|---|---|
| `allow-latency` | Maximum time a record waits in the buffer before being processed |
| `size` | Maximum number of records buffered before a flush |

Mini-batching is most effective for the `revenue_5min` and `city_revenue_5min` aggregation jobs where many records share the same group key. It reduces state access frequency at the cost of added latency.

**When to enable:**
- Aggregation throughput exceeds 5K events/sec
- State backend is RocksDB (where per-access cost is higher)

**When to avoid:**
- Latency-sensitive alert pipelines (`high_value_orders`, `suspicious_orders`)

---

## Fluss Table Design

### Log Tables vs PK Tables

Apache Fluss supports two table types. Choosing correctly is critical for both performance and correctness.

| Characteristic | Log Table | PK Table |
|---|---|---|
| Write model | Append-only | Upsert (insert or update by key) |
| Read model | Sequential scan | Point lookup by primary key |
| Use case | Event streams, audit logs | Dimension tables, materialized views |
| Deduplication | Not built-in | Automatic by primary key |
| Storage growth | Unbounded (time-based retention) | Bounded by unique key count |
| Read latency | High (scan) | Low (O(1) lookup) |

### Table Type Assignments in This Platform

| Table | Type | Justification |
|---|---|---|
| `orders_raw` | Log | Immutable event stream. Must preserve every event for replay. |
| `customer_profile` | PK | Dimension table. Updates in place when customer data changes. |
| `product_catalog` | PK | Dimension table. Updates in place when product data changes. |
| `orders_enriched` | PK | Enriched fact table. Keyed by `order_id` for upsert semantics -- if an order is enriched twice (e.g., late dimension update), the latest version wins. |
| `revenue_5min` | PK | Windowed aggregate. Composite key `(window_start, window_end, category)` ensures each window+category combination is upserted, not duplicated. |
| `city_revenue_5min` | PK | Same pattern as `revenue_5min` with city dimension. |
| `high_value_orders` | Log | Alert stream. Append-only for audit trail. |
| `suspicious_orders` | Log | Alert stream. Append-only for audit and compliance. |

### bucket.num Sizing

The `bucket.num` parameter determines:

1. **Write parallelism**: Maximum number of concurrent writers to the table.
2. **Read parallelism**: Maximum number of concurrent readers (Flink source subtasks).
3. **Data distribution**: Records are hash-distributed across buckets by primary key (PK tables) or round-robin (Log tables).

**Sizing formula:**

```
bucket.num = max(expected_write_parallelism, ceil(events_per_sec / 2000))
```

Round up to the next power of 2 for even distribution.

**Anti-patterns:**

- `bucket.num = 1` on a high-throughput table: creates a single-writer bottleneck.
- `bucket.num = 256` on a 30-row dimension table: wastes metadata and creates many nearly empty buckets.
- Changing `bucket.num` after table creation: requires table recreation and data migration.

### Avoiding Wide Hot Tables

Wide tables with many columns increase serialization/deserialization cost per record. The `orders_enriched` table has 17 columns, which is approaching the practical limit for a hot table.

**Strategies:**
- Keep dimension tables narrow. Only include columns needed for downstream joins and queries.
- If `orders_enriched` performance degrades, split it into a core fact table (IDs, amounts, timestamps) and an extended attributes table.
- Avoid adding computed columns to PK tables unless they are frequently queried. Compute them at query time instead.

---

## Query Tuning

### Query Aggregates, Not Raw

The single most impactful query optimization is querying pre-computed aggregate tables instead of running ad-hoc aggregations on raw data.

**Correct approach:**

```sql
-- Dashboard query: revenue by category for the last hour
SELECT category, total_orders, total_revenue, avg_order_value
FROM revenue_5min
WHERE window_start >= NOW() - INTERVAL '1' HOUR
ORDER BY total_revenue DESC;
```

**Incorrect approach (avoid):**

```sql
-- Ad-hoc aggregation on raw data: slow and resource-intensive
SELECT category, COUNT(*) AS total_orders, SUM(order_amount) AS total_revenue
FROM orders_enriched
WHERE event_time >= NOW() - INTERVAL '1' HOUR
GROUP BY category;
```

The first query reads from a bounded, pre-aggregated PK table. The second forces a full scan and aggregation of potentially millions of records.

### Filter by Time

Always include time predicates when querying log tables:

```sql
-- Good: bounded time range
SELECT * FROM orders_raw
WHERE event_time >= TIMESTAMP '2025-05-01 00:00:00'
  AND event_time <  TIMESTAMP '2025-05-02 00:00:00';

-- Bad: full table scan
SELECT * FROM orders_raw;
```

### Limit Results

Use `LIMIT` for exploratory and debugging queries:

```sql
-- Good: bounded result set
SELECT * FROM orders_raw ORDER BY event_time DESC LIMIT 100;

-- Bad: unbounded result set from a streaming table
SELECT * FROM orders_raw;
```

### Avoid Full Scans on PK Tables

PK tables support efficient point lookups. Use them:

```sql
-- Good: point lookup
SELECT * FROM orders_enriched WHERE order_id = 'abc-123';

-- Bad: full scan on a PK table
SELECT * FROM orders_enriched WHERE order_amount > 10000;
```

For range queries and analytical scans, use the pre-computed aggregate tables or export to a query engine optimized for OLAP (e.g., Trino, ClickHouse).

### Separate Operational from Historical Queries

| Query Type | Target | Execution Mode |
|---|---|---|
| Real-time dashboard | `revenue_5min`, `city_revenue_5min` | Streaming (continuous query) |
| Single order lookup | `orders_enriched` | Point query |
| Alert monitoring | `high_value_orders`, `suspicious_orders` | Streaming or recent batch |
| Historical analysis | Lakehouse / data warehouse | Batch (Flink batch or Trino) |
| Ad-hoc exploration | `orders_raw` with time filter + LIMIT | Batch |

Never run historical analytical queries against the hot Fluss tables. Export data to a lakehouse or data warehouse for historical analysis.

---

## Storage Tuning

### Hot Retention

Control how long data stays in the Fluss hot tier before being eligible for deletion or tiering.

**Log table retention:**

```
log.retention.time = 72h
```

Retains 3 days of raw events in the hot tier. Older segments are deleted (or tiered to cold storage if lakehouse tiering is configured).

**Segment sizing:**

```
log.segment.bytes = 134217728  # 128 MB
```

Larger segments reduce metadata overhead but increase the granularity of retention (you cannot delete half a segment). 128 MB is a good default for 1K-10K events/sec.

### Lakehouse Tiering

Fluss supports tiering cold data to lakehouse formats (Apache Paimon, Apache Iceberg). This enables:

1. Cost-effective long-term storage on object storage (S3, GCS, Azure Blob)
2. Batch query access via standard query engines
3. Automatic compaction and optimization by the lakehouse engine

Configure the remote data directory to point to object storage:

```yaml
remote.data.dir: s3://your-bucket/fluss/remote-data
```

The current `docker-compose.yml` uses a local tmpfs volume as a placeholder:

```yaml
remote.data.dir: /tmp/fluss/remote-data
```

### Compaction

PK tables in Fluss maintain a change log and periodically compact it into the base KV state. Compaction is critical for:

- Preventing unbounded storage growth on PK tables
- Maintaining consistent read performance
- Reducing checkpoint size

The `kv.snapshot.interval` parameter controls how often the KV state is snapshotted:

```yaml
kv.snapshot.interval: 0s  # Disabled in current dev config
```

For production, enable periodic snapshots:

```yaml
kv.snapshot.interval: 300s  # Every 5 minutes
```

### Object Storage for Cold Data

For production deployments, configure object storage as the remote data tier:

| Cloud Provider | Configuration |
|---|---|
| AWS S3 | `remote.data.dir: s3://bucket/fluss/remote-data` |
| Google Cloud Storage | `remote.data.dir: gs://bucket/fluss/remote-data` |
| Azure Blob Storage | `remote.data.dir: abfs://container@account/fluss/remote-data` |

### Storage Sizing Estimates

| Throughput | Raw Events/Day | Raw Storage/Day (est.) | Enriched Storage/Day (est.) |
|:----------:|:--------------:|:----------------------:|:---------------------------:|
| 50 evt/s | 4.3M | ~860 MB | ~1.7 GB |
| 1K evt/s | 86.4M | ~17 GB | ~35 GB |
| 10K evt/s | 864M | ~170 GB | ~350 GB |
| 100K evt/s | 8.6B | ~1.7 TB | ~3.5 TB |

Estimates assume ~200 bytes per raw event and ~400 bytes per enriched record (with customer and product attributes). Actual sizes depend on compression and encoding.

### Compression

Enable compression for both Flink state and Fluss storage to reduce I/O and storage costs:

```yaml
# Flink state compression
execution.checkpointing.snapshot-compression: true
```

For Fluss log segments, compression is applied at the segment level. The default codec provides a good balance between compression ratio and CPU overhead.

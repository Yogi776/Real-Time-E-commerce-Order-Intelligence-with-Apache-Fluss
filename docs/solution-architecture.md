# Solution Architecture

Detailed solution architecture for the Real-Time E-commerce Order Intelligence Platform.

---

## 1. Business Context

Indian e-commerce platforms process millions of orders daily across diverse payment methods (UPI, cards, wallets, COD, netbanking), device types, and geographies. Business teams require real-time visibility into order flow, revenue trends, and fraud signals without impacting the operational order processing systems.

Traditional approaches -- polling operational databases or running batch ETL with hourly/daily lag -- create unacceptable trade-offs between freshness and operational stability. This platform provides a streaming-first alternative.

## 2. Problem Statement

Design a real-time analytics platform that:

1. Ingests high-volume order events (50 to 5000+ events/second) without impacting the order processing system
2. Enriches raw events with customer and product dimensions in real-time
3. Computes rolling revenue metrics with sub-minute freshness
4. Detects high-value orders and fraud-like patterns within seconds
5. Serves analytical queries with millisecond-level point lookups
6. Supports future historical analysis through lakehouse integration

---

## 3. Functional Requirements

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-1 | Ingest order events at sustained 50-5000 events/sec | Flink Faker -> orders_raw (Log table, 4 buckets) |
| FR-2 | Enrich orders with customer name, loyalty tier, product details | Temporal lookup join against customer_profile and product_catalog |
| FR-3 | Compute 5-minute revenue by category | TUMBLE window aggregation -> revenue_5min |
| FR-4 | Compute 5-minute revenue by city | TUMBLE window aggregation -> city_revenue_5min |
| FR-5 | Detect high-value orders (>= 12,999 INR) | Filter on orders_enriched -> high_value_orders |
| FR-6 | Detect suspicious/fraudulent patterns | Multi-rule CASE expression -> suspicious_orders |
| FR-7 | Support ad-hoc SQL queries on all tables | Flink SQL Client with point lookups and scans |
| FR-8 | Future: query historical data beyond retention window | Lakehouse tiered storage (Paimon/Iceberg) |

## 4. Non-Functional Requirements

| ID | Requirement | Target | Mechanism |
|----|-------------|--------|-----------|
| NFR-1 | End-to-end latency (event to queryable) | < 5 seconds | Streaming mode, no micro-batch |
| NFR-2 | Sustained throughput | 5,000 events/sec | Horizontal scaling of buckets + TaskManagers |
| NFR-3 | Fault tolerance | Zero data loss on single-node failure | Flink checkpointing + Fluss log replay |
| NFR-4 | Horizontal scalability | Linear with added resources | Bucket-based partitioning, stateless enrichment |
| NFR-5 | No operational DB impact | Complete decoupling | Fire-and-forget event emission |
| NFR-6 | Backpressure handling | Graceful slowdown, no data loss | Flink credit-based flow control |
| NFR-7 | Observability | Job-level metrics | Flink Web UI (port 8083) |
| NFR-8 | Maintainability | Pure SQL pipelines, no custom code | All logic in Flink SQL, declarative |

---

## 5. System Components

### 5.1 Apache ZooKeeper

**Responsibility:** Cluster coordination, leader election, metadata storage for Fluss.

- Stores Fluss cluster membership and bucket assignments
- Provides distributed consensus for CoordinatorServer failover (multi-node deployments)
- Single instance for development; 3-node ensemble for production

### 5.2 Fluss CoordinatorServer

**Responsibility:** Cluster management, catalog operations, bucket assignment, client routing.

- Manages table metadata (schema, bucket count, table type)
- Assigns buckets to TabletServers and rebalances on topology changes
- Serves as the Flink catalog backend via `bootstrap.servers`
- Exposes port 9123 for client connections

### 5.3 Fluss TabletServer

**Responsibility:** Data storage and retrieval for assigned buckets.

- Stores Log table segments (append-only event streams)
- Maintains KV state for PK tables (point lookups, upserts)
- Handles read/write requests from Flink operators
- Manages local data directory and remote storage synchronization

### 5.4 Flink JobManager

**Responsibility:** Job scheduling, checkpoint coordination, failure recovery.

- Parses and optimizes SQL job graphs
- Coordinates distributed checkpoints across TaskManagers
- Implements restart strategy (fixed-delay: 3 attempts, 10-second delay)
- Exposes Web UI on port 8083 for job monitoring

### 5.5 Flink TaskManager

**Responsibility:** Parallel execution of streaming operators.

- Executes source, join, aggregation, and sink operators
- Manages operator state and participates in checkpointing
- Configured with 10 task slots and 2GB process memory
- Reads from and writes to Fluss TabletServers

### 5.6 Flink SQL Client

**Responsibility:** Interactive query interface for ad-hoc analytics.

- Connects to JobManager for job submission
- Supports both streaming (continuous) and batch (bounded) queries
- Used for table creation, pipeline deployment, and result inspection

---

## 6. Data Flow Patterns

### 6.1 Event Ingestion Pattern

```
[Faker Source] --INSERT INTO--> [orders_raw (Log Table)]
```

- Events are append-only and immutable once written
- Watermarks are embedded at source: `event_time - INTERVAL '5' SECOND`
- Processing-time column (`ptime`) is generated for lookup join semantics
- 4 buckets provide initial parallelism for ingestion

### 6.2 Stream Enrichment Pattern

```
[orders_raw] --temporal lookup join--> [customer_profile, product_catalog] --> [orders_enriched]
```

- Uses `FOR SYSTEM_TIME AS OF o.ptime` syntax
- Lookups are point reads against PK tables (KV store in Fluss)
- LEFT JOIN ensures orders are never dropped if dimension data is missing
- Enriched output is an upsert stream keyed by `order_id`

### 6.3 Windowed Aggregation Pattern

```
[orders_enriched] --TUMBLE(5 min)--> [revenue_5min, city_revenue_5min]
```

- Tumbling window with 5-minute granularity
- Composite primary key ensures upsert semantics per window slot
- Late events (within 5-second watermark tolerance) are included; later arrivals are dropped

### 6.4 Anomaly Detection Pattern

```
[orders_enriched] --WHERE/CASE filter--> [high_value_orders, suspicious_orders]
```

- Stateless per-event filtering (no windowing required)
- Multiple fraud rules evaluated via CASE expression with reason classification
- Results are append-only Log tables for audit trail

---

## 7. Storage Design

### 7.1 Table Classification

| Table | Type | Purpose | Buckets | Key |
|-------|------|---------|---------|-----|
| orders_raw | Log | Raw event stream | 4 | None (append-only) |
| customer_profile | PK | Customer dimension | 4 | customer_id |
| product_catalog | PK | Product dimension | 4 | product_id |
| orders_enriched | PK | Enriched fact | 8 | order_id |
| revenue_5min | PK | Category revenue aggregate | 4 | (window_start, window_end, category) |
| city_revenue_5min | PK | City revenue aggregate | 4 | (window_start, window_end, city) |
| high_value_orders | Log | High-value alert stream | 4 | None (append-only) |
| suspicious_orders | Log | Fraud alert stream | 4 | None (append-only) |

### 7.2 Storage Semantics

**Log Tables** store an immutable, ordered sequence of events. They are consumed from a specific offset and support replay. Ideal for raw events and audit trails.

**PK Tables** maintain the latest state for each primary key. They support point lookups (key-value reads) and range scans. Updates are applied via upsert semantics. Ideal for dimensions, enriched facts, and aggregates.

### 7.3 Bucket Sizing Rationale

- `orders_raw` (4 buckets): Matches initial ingestion parallelism
- `orders_enriched` (8 buckets): Higher parallelism for downstream consumers (aggregation + detection)
- Dimension/aggregate tables (4 buckets): Lower cardinality, fewer concurrent writers

---

## 8. Query Design Patterns

### 8.1 Point Lookup (PK Tables)

```sql
SELECT * FROM orders_enriched WHERE order_id = 'abc-123';
SELECT * FROM revenue_5min WHERE window_start = TIMESTAMP '...' AND category = 'electronics';
```

Served directly from Fluss KV store with single-digit millisecond latency.

### 8.2 Scan Query

```sql
SELECT * FROM revenue_5min ORDER BY window_start DESC LIMIT 20;
SELECT city, SUM(total_revenue) FROM city_revenue_5min
WHERE window_start >= TIMESTAMP '...' GROUP BY city;
```

Scans PK table state across all buckets.

### 8.3 Streaming Query (Continuous)

```sql
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM high_value_orders; -- Continuously prints new alerts
```

Tails the Log table from the current offset, printing new events as they arrive.

---

## 9. Scalability Design

### 9.1 Storage Scalability

| Scaling Action | Effect |
|---------------|--------|
| Add TabletServer | Buckets rebalanced across more nodes; increases aggregate storage I/O |
| Increase `bucket.num` | More parallel readers/writers per table; improves throughput ceiling |
| Enable tiered storage | Offloads cold data to remote object store; extends effective retention |

### 9.2 Compute Scalability

| Scaling Action | Effect |
|---------------|--------|
| Add TaskManager | More task slots available; allows higher job parallelism |
| Increase `taskmanager.numberOfTaskSlots` | More operators per JVM; trades isolation for density |
| Increase job parallelism | Flink distributes operators across available slots |

### 9.3 Scaling Bottlenecks

The primary scaling constraint is the number of buckets, which is set at table creation time. Changing bucket count requires table recreation. Plan bucket counts based on peak expected throughput, not current load.

---

## 10. Failure Handling

### 10.1 Flink Job Failure

- Restart strategy: fixed-delay with 3 attempts and 10-second delay
- On restart, state is restored from the latest successful checkpoint
- Fluss log offsets are part of the checkpoint; replay ensures no data loss
- If all 3 attempts fail, the job enters FAILED state and requires manual intervention

### 10.2 TabletServer Failure

- CoordinatorServer detects heartbeat loss
- Buckets are reassigned to surviving TabletServers (multi-server deployments)
- KV state is rebuilt from remote snapshots + log replay
- Single-server development setup: full outage until TabletServer restarts

### 10.3 ZooKeeper Failure

- Single-node: complete cluster unavailability
- Ensemble (3+ nodes): tolerates minority failures via quorum

### 10.4 Network Partition

- Flink uses TCP with configurable timeouts between JobManager and TaskManagers
- Fluss client retries with exponential backoff on transient connection failures
- Split-brain scenarios are prevented by ZooKeeper-based leader election

---

## 11. Security Considerations (Future)

The current deployment is designed for development and evaluation. Production deployments should address:

| Concern | Approach |
|---------|----------|
| Network encryption | TLS between all Fluss and Flink components |
| Authentication | Kerberos or mutual TLS for inter-service communication |
| Authorization | Table-level ACLs in Fluss catalog |
| Data encryption at rest | Filesystem-level or storage-level encryption |
| Audit logging | Fluss catalog operation logging |
| Secret management | External secret store for credentials |

---

## 12. Deployment Considerations

### 12.1 Current (Development)

- Single Docker Compose file with 6 services
- tmpfs volume for Fluss data (ephemeral, fast)
- All services on a single host
- Flink Web UI exposed on port 8083

### 12.2 Production Recommendations

| Aspect | Recommendation |
|--------|---------------|
| Orchestration | Kubernetes with StatefulSets for Fluss, Flink Kubernetes Operator |
| Storage | Persistent volumes (SSD) for TabletServers; S3/GCS for remote data |
| ZooKeeper | 3-node ensemble on dedicated nodes |
| Monitoring | Prometheus metrics export + Grafana dashboards |
| Log aggregation | Structured logging to ELK or similar |
| Resource isolation | Dedicated node pools for storage vs compute |
| Networking | Service mesh or network policies for inter-service communication |

---

## 13. Cost Considerations

### 13.1 Resource Requirements (Development)

| Service | CPU | Memory | Storage |
|---------|-----|--------|---------|
| ZooKeeper | 0.5 vCPU | 512 MB | Minimal |
| CoordinatorServer | 0.5 vCPU | 512 MB | Minimal |
| TabletServer | 1 vCPU | 1 GB | tmpfs (RAM-backed) |
| JobManager | 1 vCPU | 1 GB | Checkpoint storage |
| TaskManager | 2 vCPU | 2 GB | State backends |
| SQL Client | 0.5 vCPU | 512 MB | None |

**Total development footprint:** ~6 vCPU, ~6 GB RAM

### 13.2 Production Cost Drivers

- **Compute:** Flink TaskManagers dominate cost; scale linearly with throughput
- **Storage:** Fluss retention window determines hot storage cost; lakehouse for cold
- **Network:** Cross-AZ traffic between Flink and Fluss if not co-located
- **Coordination:** ZooKeeper and CoordinatorServer are lightweight relative to data plane

---

## 14. Future Extensions

| Extension | Description | Dependency |
|-----------|-------------|------------|
| Lakehouse integration | Tiered storage to Apache Paimon for historical queries | Fluss tiered storage GA |
| Real-time dashboards | Grafana or custom UI polling PK tables | REST/JDBC gateway |
| ML-based fraud detection | Replace rule-based with model scoring | Feature store + model serving |
| Multi-region deployment | Active-passive or active-active for DR | Fluss replication (future) |
| Schema evolution | Add fields to order events without downtime | Fluss schema registry |
| Customer segmentation | Real-time RFM scoring using session windows | Additional Flink jobs |
| Inventory sync | Decrement product_catalog.inventory_count on order | CDC from order service |
| SLA monitoring | Track order-to-delivery time distributions | Event correlation jobs |

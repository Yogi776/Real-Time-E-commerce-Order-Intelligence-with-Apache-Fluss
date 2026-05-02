# Production Readiness Checklist

A three-tier checklist for taking the Real-Time E-commerce Order Intelligence Platform from a local demo to a production deployment. Each tier builds on the previous one.

---

## Tier 1: Local Demo

The baseline: everything works on a single machine via Docker Compose. This is the current state of the project.

### Infrastructure

- [ ] `docker compose up` starts all services without errors (ZooKeeper, Fluss Coordinator, Fluss Tablet, Flink JobManager, Flink TaskManager, SQL Client)
- [ ] All healthchecks pass within 60 seconds of startup
- [ ] Services restart cleanly after `docker compose down && docker compose up`
- [ ] Docker resource requirements documented (minimum 4GB RAM, recommended 8GB)

### Data Pipeline

- [ ] `01_create_catalog.sql` creates the Fluss catalog and `ecommerce` database successfully
- [ ] `02_create_tables.sql` creates all 8 tables without errors
- [ ] `03_seed_data.sql` populates `customer_profile` (20 records) and `product_catalog` (30 records)
- [ ] `04_generate_orders.sql` starts streaming synthetic orders at 50 events/second
- [ ] `05_enrich_orders.sql` produces enriched orders with all dimension fields populated
- [ ] `06_revenue_aggregates.sql` populates `revenue_5min` and `city_revenue_5min` after the first 5-minute window closes
- [ ] `08_anomaly_detection.sql` detects and routes high-value and suspicious orders
- [ ] All streaming jobs visible and RUNNING in Flink Web UI (`localhost:8083`)

### Queries and Validation

- [ ] `09_demo_queries.sql` returns non-empty results for all 12 query categories
- [ ] Enrichment joins produce non-null `customer_name`, `product_name`, and `category` for all orders
- [ ] Revenue aggregates match expected distributions (electronics > grocery in average order value)
- [ ] High-value orders all have `order_amount >= 12999`
- [ ] Suspicious orders all have a non-null `reason` field

### Documentation

- [ ] README covers setup, prerequisites, and step-by-step execution
- [ ] SQL files are numbered and documented with execution order
- [ ] `datagen/generate_seed_data.py` usage documented (flags, output format)
- [ ] Known limitations and troubleshooting section present

---

## Tier 2: Pre-Production

The system runs reliably in a staging environment with persistent storage, monitoring, and basic security. Suitable for internal dashboards and non-critical analytics.

### Load Testing

- [ ] Load test completed at 500 events/second for 1 hour with no Flink job failures
- [ ] Load test completed at 5,000 events/second for 15 minutes with scaled TaskManagers
- [ ] Flink backpressure stays below HIGH under sustained 500 events/second load
- [ ] Checkpoint duration remains under 30 seconds under load
- [ ] Checkpoint size growth rate documented and projected for 30-day retention
- [ ] End-to-end latency measured: event_time to queryable in `orders_enriched`
- [ ] Memory usage profiled for each container under load (`docker stats` baseline)

### Persistent Storage

- [ ] Fluss data directory mapped to a persistent Docker volume (not tmpfs)
- [ ] Flink checkpoint directory mapped to a persistent volume
- [ ] ZooKeeper data directory mapped to a persistent volume
- [ ] Volume backup procedure documented and tested
- [ ] Recovery tested: stop all containers, restart, verify data survives
- [ ] Recovery tested: kill a single service, verify it rejoins the cluster

### Monitoring and Alerts

- [ ] Flink metrics exported to Prometheus (via Flink metrics reporter)
- [ ] Grafana dashboard configured with key panels:
  - Flink job status (running/failed/restarting)
  - Checkpoint duration and failure count
  - Records processed per second (per job)
  - Backpressure ratio per operator
  - TaskManager memory and CPU utilization
- [ ] Alerts configured for:
  - Flink job failure (any streaming job transitions to FAILED)
  - Checkpoint failure (2 consecutive failures)
  - Backpressure HIGH sustained for more than 5 minutes
  - Container restart (any service restarts unexpectedly)
- [ ] Log aggregation configured (stdout/stderr from all containers to a central location)
- [ ] Fluss CoordinatorServer and TabletServer logs accessible and rotated

### Security

- [ ] Default passwords changed (ZooKeeper, object storage if using lakehouse tiering)
- [ ] Flink Web UI access restricted (not exposed to public network)
- [ ] Docker network isolation: services communicate on an internal network, only necessary ports exposed
- [ ] SQL client access restricted to authorized users
- [ ] No secrets stored in plain text in version-controlled files (use Docker secrets or environment variable injection)

### Data Management

- [ ] Fluss log retention configured (`table.log.ttl`) for all Log tables
- [ ] Data volume growth rate calculated and storage provisioned for 30+ days
- [ ] Seed data generation reproducible (same script, same output with fixed seed)
- [ ] Schema change procedure documented (how to add a field, how to add a table)
- [ ] Backfill procedure documented (how to reprocess historical data after schema change)

### CI Pipeline

- [ ] Docker Compose starts successfully in CI (GitHub Actions, GitLab CI, or equivalent)
- [ ] SQL file syntax validated (at minimum, files parse without error)
- [ ] Smoke test: seed data loads, faker generates events, enriched orders appear within 2 minutes
- [ ] CI pipeline completes in under 5 minutes
- [ ] Docker image versions pinned (no `latest` tags in `docker-compose.yml`)

---

## Tier 3: Production

The system runs in a production environment with high availability, multi-node scaling, access control, and disaster recovery. Suitable for customer-facing dashboards, business-critical alerting, and compliance workloads.

### High Availability

- [ ] Fluss deployed with 3+ TabletServers for data replication
- [ ] Fluss replication factor configured to 3 (no single point of data loss)
- [ ] ZooKeeper deployed as a 3-node ensemble (tolerates 1 node failure)
- [ ] Flink deployed with 2+ JobManagers in HA mode (ZooKeeper-based leader election)
- [ ] Flink deployed with 3+ TaskManagers for workload distribution
- [ ] Failover tested: kill one TabletServer, verify reads and writes continue
- [ ] Failover tested: kill the active Flink JobManager, verify jobs recover on standby
- [ ] Failover tested: kill one ZooKeeper node, verify cluster remains operational

### Multi-Node Fluss Cluster

- [ ] Each TabletServer runs on a separate physical or virtual host
- [ ] Network latency between Fluss nodes measured and within acceptable bounds (<5ms p99)
- [ ] Disk I/O benchmarked on each TabletServer host (SSD recommended for hot data)
- [ ] Bucket count (`bucket.num`) tuned based on cluster size and expected throughput
- [ ] CoordinatorServer runs on a dedicated host (not co-located with TabletServer)

### Autoscaling

- [ ] Flink TaskManagers autoscale based on backpressure or CPU utilization (if running on Kubernetes)
- [ ] Reactive mode or adaptive scheduler configured in Flink for dynamic slot allocation
- [ ] Scaling thresholds documented (at what event rate to add a TaskManager)
- [ ] Scale-up tested: add TaskManagers, verify Flink redistributes work
- [ ] Scale-down tested: remove TaskManagers gracefully, verify no data loss

### Disaster Recovery

- [ ] Recovery Point Objective (RPO) defined and documented
- [ ] Recovery Time Objective (RTO) defined and documented
- [ ] Full cluster restore procedure documented and tested from backups
- [ ] Flink savepoints taken before any planned maintenance or upgrade
- [ ] Savepoints stored in durable storage (S3, GCS), not local filesystem
- [ ] Cross-region backup configured if RPO requires it
- [ ] Runbook: complete cluster failure and recovery from savepoints + Fluss snapshots

### Access Control

- [ ] Role-based access implemented for Flink SQL client (read-only analysts vs. pipeline operators)
- [ ] Network policies restrict which services can communicate (principle of least privilege)
- [ ] Audit logging enabled for all DDL and DML operations
- [ ] Service accounts used for inter-service communication (no shared credentials)
- [ ] Secrets managed via a secrets manager (HashiCorp Vault, AWS Secrets Manager, or equivalent)

### Encryption

- [ ] TLS enabled for Fluss client-to-server communication
- [ ] TLS enabled for Flink internal communication (RPC, blob transfer)
- [ ] TLS enabled for ZooKeeper client connections
- [ ] Data at rest encrypted on Fluss TabletServer storage volumes
- [ ] Data at rest encrypted in lakehouse object storage (if tiering enabled)
- [ ] Certificate rotation procedure documented

### Cost Monitoring

- [ ] Compute cost tracked per service (CPU, memory allocation)
- [ ] Storage cost tracked (Fluss hot storage, lakehouse cold storage, checkpoint storage)
- [ ] Network egress cost tracked (especially if querying from external engines)
- [ ] Cost-per-event calculated at current and projected scale
- [ ] Alerts configured for unexpected cost spikes (e.g., storage growth exceeding projections)
- [ ] Right-sizing review scheduled quarterly (are resources over-provisioned?)

### Operational Runbooks

- [ ] Runbook: Flink job fails and does not auto-recover
- [ ] Runbook: Fluss TabletServer goes offline
- [ ] Runbook: ZooKeeper ensemble loses quorum
- [ ] Runbook: Checkpoint size grows unbounded
- [ ] Runbook: Backpressure causes event processing delay exceeding SLA
- [ ] Runbook: Schema migration (adding/removing fields from streaming tables)
- [ ] Runbook: Rolling upgrade of Fluss, Flink, or ZooKeeper versions
- [ ] Runbook: Emergency data purge (PII deletion, compliance request)
- [ ] Runbook: Lakehouse tiering falls behind (if tiering enabled)
- [ ] All runbooks tested in staging environment at least once

---

## Progress Tracking

Use this table to track overall readiness across tiers:

| Tier | Total Items | Completed | Percentage | Status |
|---|---|---|---|---|
| Tier 1: Local Demo | 18 | _ | _% | Not started |
| Tier 2: Pre-Production | 32 | _ | _% | Not started |
| Tier 3: Production | 38 | _ | _% | Not started |

Update this table as items are completed. Do not skip tiers -- each builds on the foundation of the previous one.

# RustLakeDB v9.0.0 — Production SRE Incident Runbooks

Every Prometheus alert in [`deploy/observability/prometheus-alerts.yaml`](../deploy/observability/prometheus-alerts.yaml) and [`deploy/helm/rustlakedb/templates/monitoring.yaml`](../deploy/helm/rustlakedb/templates/monitoring.yaml) links directly to its corresponding section below.

---

## `#high-error-burn` — `RustLakeDBQueryErrorBurnRateCritical` / `Warning`

- **Trigger:** Query error rate burning 30-day 99.9% SLO error budget at `14.4x` (page) or `6x` (warning).
- **Immediate Triage (< 2 min):**
  1. Check `/v1/cluster/status` and `/metrics` on the coordinator NLB:
     ```bash
     curl -s http://rustlakedb-coordinator:8181/v1/cluster/status | jq
     curl -s http://rustlakedb-coordinator:8181/metrics | grep -E "query_errors|rpc_timeouts|raft_"
     ```
  2. Inspect recent audit/error entries via PGWire or HTTP:
     ```sql
     SELECT * FROM system.audit_log ORDER BY timestamp DESC LIMIT 25;
     ```
  3. Distinguish client SQL/RBAC errors (`42501` permission denied, `23502`/`23514` schema guardrail rejections) from infrastructure errors (S3 503 throttling, worker OOMs, Raft quorum loss).
- **Remediation:**
  - If caused by a bad ETL deployment corrupting data or violating schema: pause the offending DAG and roll back the affected table instantaneously via `RESTORE TABLE <tbl> TO VERSION <last_good_snap>`.
  - If caused by S3 throttling (`SlowDown` 503): verify S3 prefix partitioning (`PARTITIONED BY`) and reduce concurrent ingest fan-out.

---

## `#worker-rpc-timeouts` — `RustLakeDBWorkerRpcTimeoutSurge`

- **Trigger:** `rate(rustlakedb_worker_rpc_timeouts_total[5m]) > 0.5/s` for 2 minutes.
- **Immediate Triage:**
  1. Inspect worker pods for `OOMKilled` (`Exit 137`) or CPU throttling:
     ```bash
     kubectl get pods -l app=rustlakedb-worker -o wide
     kubectl top pods -l app=rustlakedb-worker
     ```
  2. Check active long-running queries:
     ```bash
     curl -s http://rustlakedb-coordinator:8181/v1/queries/active | jq
     ```
- **Remediation:**
  - Cancel runaway un-filtered Cartesian queries via `POST /v1/queries/{query_id}/cancel`.
  - Scale worker StatefulSet out (`kubectl scale sts rustlakedb-worker --replicas=<N>`) — new workers self-register to all 3 coordinators automatically within 2 seconds.
  - If cluster mTLS is enabled, verify certificate expiry in secret `rustlakedb-cluster-mtls`.

---

## `#raft-no-leader` — `RustLakeDBRaftNoLeader`

- **Trigger:** `sum(rustlakedb_raft_is_leader) != 1` for 30 seconds across the 3-node coordinator quorum.
- **Immediate Triage:**
  1. Query `/healthz` on each coordinator pod directly via headless DNS:
     ```bash
     for i in 0 1 2; do
       kubectl exec rustlakedb-coordinator-$i -- curl -s http://127.0.0.1:8181/healthz; echo
     done
     ```
  2. Check if >= 2 coordinator pods are `Running` (Raft requires majority quorum `floor(N/2)+1 = 2`).
  3. Check network connectivity on Raft port `9001` between `rustlakedb-coordinator-{0,1,2}.rustlakedb-coordinators`.
- **Remediation:**
  - If 2+ pods were terminated simultaneously: verify `PodDisruptionBudget` (`rustlakedb-coordinator-pdb`, `minAvailable: 2`) and restore node capacity immediately.
  - If a single coordinator's WAL PVC is corrupted: delete only that single pod + PVC; once recreated it will replicate the authoritative log from the 2 healthy quorum peers.

---

## `#raft-leader-churn` — `RustLakeDBRaftLeaderChurn`

- **Trigger:** `>= 3` Raft leadership changes in 15 minutes.
- **Immediate Triage:**
  1. Check coordinator CPU saturation, disk fsync latency on `raft-wal` PVC, and inter-AZ network RTT.
  2. Verify coordinator pod anti-affinity is keeping all 3 coordinators on dedicated non-preemptible nodes (never Spot/Preemptible instances!).
- **Remediation:** Move `raft-wal` PVCs to `gp3` / `io2` / `pd-ssd` low-latency storage and ensure coordinators have guaranteed CPU QoS (`requests == limits`).

---

## `#nvme-spill-pressure` — `RustLakeDBNvmeSpillPressure`

- **Trigger:** `max(rustlakedb_nvme_spill_bytes) > 50 GiB` for 5 minutes.
- **Immediate Triage:**
  1. Check active large `ORDER BY` / Grace Hash Join queries via `GET /v1/queries/active`.
  2. Check worker `/tmp` spill directory for orphaned `arrow_*_spill*.arw1` files if a pod suffered an unclean crash.
- **Remediation:** Increase worker memory limits or `workers.nvmeSpillSizeLimit`, and optimize skewed join keys using `ALTER TABLE <tbl> CLUSTER BY (...)` + `OPTIMIZE TABLE <tbl>`.

---

## `#snapshot-backlog` — `RustLakeDBSnapshotBacklogHigh`

- **Trigger:** `max(rustlakedb_catalog_max_snapshots) > 250` for 15 minutes.
- **Immediate Triage:**
  1. Check `rustlakedb-maintainer` Deployment logs (`kubectl logs deploy/rustlakedb-maintainer --tail=100`).
  2. Identify tables with high snapshot counts via `SHOW SNAPSHOTS ON <table>`.
- **Remediation:**
  - Restart `rustlakedb-maintainer` if stalled.
  - Manually run `VACUUM TABLE <table> RETAIN 10` (safe under concurrent writes via Iceberg `ExpireSnapshots` delta GC).

---

## `#worker-down` — `RustLakeDBWorkerDown`

- **Trigger:** `min(rustlakedb_workers_healthy) < max(rustlakedb_workers_total)` for 1 minute.
- **Remediation:** Coordinator circuit-breakers and task retries automatically route around failed workers with zero user query errors. Inspect `kubectl describe pod <worker>` for Spot preemption or OOM; Kubernetes StatefulSet automatically recreates and re-registers the pod.

---

## `#high-latency` — `RustLakeDBHighQueryLatency`

- **Trigger:** 5-minute `p99` query latency (`rustlakedb_query_duration_ms_bucket`) `> 2,500 ms` for 3 minutes.
- **Remediation:**
  1. Open the Unified Control Console (`http://<coordinator>:8181/`) -> **Workload & Traces** to inspect slow queries and distributed execution DAG profiles.
  2. Check `rustlakedb_cache_hits_total`, `rustlakedb_zonemap_pruned_total`, and `rustlakedb_puffin_pruned_total`.
  3. Run `ANALYZE TABLE <table>` + `OPTIMIZE TABLE <table>` on hot fact tables to tighten zone-maps and bin-pack small streaming files.
  4. Scale worker HPA `maxReplicas` if CPU > 70%.

---

## `#auth-denied-spike` — `RustLakeDBAuthDeniedSpike`

- **Trigger:** `rate(rustlakedb_auth_denied_total[5m]) > 1/s` for 2 minutes.
- **Immediate Triage:**
  1. Open Unified Control Console -> **Governance & Audit** -> **Audit Log** with **Denials Only** enabled (or query `GET /v1/governance/audit`).
  2. Check whether a misconfigured BI/Spark OIDC token expired, a role lacks a required schema/column grant, or a credential brute-force probe is occurring.
- **Remediation:** Verify OIDC issuer/JWKS rotation (`--oidc-issuer`), grant least-privilege roles via `GRANT`, or block abusive client IPs at the L4/L7 ingress.

---

## `#small-file-backlog` — `RustLakeDBSmallFileAccumulation`

- **Trigger:** `max(rustlakedb_small_files_total) > 1000` for 15 minutes.
- **Immediate Triage:**
  1. Open Unified Control Console -> **Catalog & Iceberg** (or `GET /v1/tables/{table}/health`) to inspect file-size distribution histograms and identify affected tables.
  2. Check `rustlakedb_compaction_runs_total` and `rustlakedb-maintainer` logs.
- **Remediation:** Ensure streaming producers use `?buffered=true` micro-batching and run `COMPACT TABLE <table>` (or verify `RUSTLAKEDB_AUTO_MAINTAIN=true` / `rustlakedb-maintainer` is healthy).

---

## Day-2 Maintenance: Graceful Node Drain & Verified Catalog DR Backup

- **Graceful Rolling Upgrade / Maintenance Drain:**
  ```bash
  curl -X POST http://<coord>:8181/v1/admin/drain -H 'Content-Type: application/json' -d '{"user":"postgres","timeout_ms":15000}'
  ```
  Marks `/readyz` as `503 Service Unavailable`, flushes all streaming ingest micro-batch buffers, drains active queries, and cleanly steps down Raft leadership. Also runs automatically on `SIGTERM` / `SIGINT`.
- **On-Demand Verified Catalog DR Backup:**
  ```bash
  curl -X POST http://<coord>:8181/v1/admin/backup-catalog -H 'Content-Type: application/json' -d '{"user":"postgres"}'
  ```
  Writes a consistent `FullCatalogSnapshot` to object storage and reads it back to verify SHA-256 checksum & JSON integrity.

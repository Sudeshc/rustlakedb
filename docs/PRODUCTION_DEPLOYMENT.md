# RustLakeDB Production Deployment Playbook (`v6.0.0`)

## 1. AWS EKS + S3 + Real IAM STS SigV4 Credential Vending

```bash
helm install rustlakedb ./deploy/helm/rustlakedb \
  --set image.repository=ghcr.io/sudeshc/rustlakedb \
  --set image.tag=6.0.0 \
  --set warehouse.uri=s3://my-lakehouse-bucket \
  --set warehouse.awsRegion=us-east-1 \
  --set workers.replicas=6 \
  --set workers.maxReplicas=32 \
  --set mtls.enabled=true \
  --set auth.oidcJwksUrl=https://my-idp.example.com/.well-known/jwks.json
```

To enable real AWS STS `AssumeRole` SigV4 credential vending on `/v1/namespaces/{ns}/tables/{table}/credentials`, set on the coordinator:
- `RUSTLAKEDB_STS_ROLE_ARN=arn:aws:iam::<account>:role/rustlakedb-table-reader`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`

## 2. Cluster-Internal Mutual TLS (mTLS)

Provision a cluster CA + server/client certs (e.g., via `cert-manager`) into Kubernetes Secret `rustlakedb-cluster-mtls` containing `ca.crt`, `server.crt`, `server.key`, `client.crt`, `client.key`, and set `mtls.enabled=true`.
- Workers automatically serve HTTPS mTLS via `RUSTLAKEDB_MTLS_CA`, `RUSTLAKEDB_MTLS_CERT`, `RUSTLAKEDB_MTLS_KEY` and reject any peer lacking a valid CA-signed client certificate.
- Coordinators automatically connect over HTTPS mTLS using the same environment variables.

## 3. Enterprise OIDC / JWKS (`RS256`) Authentication

Set `RUSTLAKEDB_OIDC_JWKS_URL=https://<idp>/.well-known/jwks.json`. The coordinator spawns a background JWKS poller (every 5 minutes) and verifies incoming `Authorization: Bearer <jwt>` headers cryptographically with `RSA_PKCS1_2048_8192_SHA256`, checking `kid` and `exp`, and mapping `sub`/`role`/`groups[0]` into RBAC + Row-Level Security + Dynamic Masking policies.

## 4. High-Throughput Client Protocols

- **PostgreSQL v3 Wire (`:5432`):** DBeaver, `psql`, JDBC, `dbt-postgres`, SQLAlchemy (supports TLS 1.3 + SCRAM-SHA-256).
- **Apache Arrow Flight SQL (`:8815`):** `adbc-driver-flightsql`, PyArrow, Polars, JDBC Flight SQL, Tableau, Power BI (supports `CommandStatementQuery`, `CommandStatementUpdate`, `CommandStatementIngest` bulk `DoPut`, and Prepared Statements).
- **Bi-Directional Iceberg v3 REST Catalog (`:8181`):** PyIceberg, Apache Spark, Trino, DuckDB, Flink (`/v1/config`, `/v1/oauth/tokens`, `/v1/namespaces`, `/v1/namespaces/{ns}/tables`, OCC commits, STS `/credentials`).

## 5. Streaming CDC from Kafka / Redpanda

```bash
rustlakedb --mode kafka-ingest \
  --kafka-broker redpanda:9092 \
  --kafka-topic dbserver1.public.orders \
  --kafka-partition 0 \
  --table orders \
  --storage-uri s3://my-lakehouse-bucket \
  --data-dir /var/lib/rustlakedb
```

Speaks pure-Rust Kafka `Metadata v1`, `ListOffsets v1`, and `Fetch v4` RecordBatch v2 (`magic=2`, uncompressed / Snappy / Zstd) with atomic Iceberg v3 Deletion Vectors and exactly-once offset checkpoints.

## 6. Observability (Prometheus + Grafana)

Import [deploy/observability/grafana-dashboard.json](../deploy/observability/grafana-dashboard.json) and load [deploy/observability/prometheus-alerts.yaml](../deploy/observability/prometheus-alerts.yaml) scraping `http://<coordinator>:8181/metrics` and `GET /v1/traces`.

# Multi-stage cloud-anywhere build for RustLakeDB v8.0.0 (AWS S3 / GCP GCS / Azure / On-Prem)
FROM rust:1-bookworm AS builder
WORKDIR /usr/src/rustlakedb
COPY Cargo.toml Cargo.lock README.md ./
COPY crates ./crates
COPY tests ./tests
RUN cargo build --release --locked --bin rustlakedb

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/rustlakedb/target/release/rustlakedb /usr/local/bin/rustlakedb

# PostgreSQL v3 Wire Protocol (5432), Worker RPC (7001), Iceberg v3 REST (8181), Arrow Flight gRPC (8815)
EXPOSE 5432 7001 8181 8815
VOLUME ["/warehouse"]

ENTRYPOINT ["rustlakedb"]
CMD ["--mode", "gateway", "--workers", "4", "--data-dir", "/warehouse", "--pg-addr", "0.0.0.0:5432", "--rest-addr", "0.0.0.0:8181"]

<#
.SYNOPSIS
  Unified Release QA test runner for RustLakeDB v7.0.0.
.EXAMPLE
  .\scripts\run-tests.ps1
  .\scripts\run-tests.ps1 -Suite live
  .\scripts\run-tests.ps1 -Suite bench
  .\scripts\run-tests.ps1 -StartMinio
#>
param(
    [ValidateSet("all", "unit", "sql", "chaos", "acid", "pgwire", "dbeaver", "network", "persistence", "production", "wave1", "wave2", "wave3", "wave4", "wave5", "wave6", "wave7", "wave8", "wave9", "wave10", "wave11", "wave12", "wave13", "wave14", "wave15", "wave16", "wave17", "wave18", "wave19", "wave20", "wave21", "wave22", "wave23", "wave24", "wave25", "wave26", "wave28", "tpch", "live", "bench")]
    [string]$Suite = "all",
    [switch]$Offline,
    [switch]$StartMinio
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
$Manifest = Join-Path $RepoRoot "Cargo.toml"

if ($StartMinio) {
    Write-Host "Starting MinIO S3-compatible test warehouse via Docker Compose..." -ForegroundColor Cyan
    docker compose -f (Join-Path $RepoRoot "docker\docker-compose.test.yml") up -d minio minio-init
}

# Ensure the `rustlakedb` CLI binary is freshly built so live multi-process OS tests can spawn it!
$BuildArgs = @("build", "--manifest-path", $Manifest, "--bin", "rustlakedb")
if ($Offline) { $BuildArgs += "--offline" }
& cargo @BuildArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$CommonArgs = @("test", "--manifest-path", $Manifest)
if ($Offline) { $CommonArgs += "--offline" }

switch ($Suite) {
    "all"         { & cargo @CommonArgs }
    "unit"        { & cargo @CommonArgs --lib }
    "sql"         { & cargo @CommonArgs -p rustlakedb-testkit --test sql_e2e }
    "chaos"       { & cargo @CommonArgs -p rustlakedb-testkit --test distributed_chaos }
    "acid"        { & cargo @CommonArgs -p rustlakedb-testkit --test iceberg_acid }
    "pgwire"      { & cargo @CommonArgs -p rustlakedb-testkit --test pgwire_protocol }
    "dbeaver"     { & cargo @CommonArgs -p rustlakedb-testkit --test dbeaver_and_regression }
    "network"     { & cargo @CommonArgs -p rustlakedb-testkit --test network_cluster }
    "persistence" { & cargo @CommonArgs -p rustlakedb-testkit --test persistence_and_maintenance }
    "production"  { & cargo @CommonArgs -p rustlakedb-testkit --test production_qa }
    "wave1"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave1_vectorized_and_cache -- --nocapture }
    "wave2"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave2_ingestion_and_sql -- --nocapture }
    "wave3"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave3_governance_and_observability -- --nocapture }
    "wave4"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave4_lakehouse_intelligence -- --nocapture }
    "wave5"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave5_deep_engine_ha_and_avro -- --nocapture }
    "wave6"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave6_sql_and_acid_tx -- --nocapture }
    "wave7"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave7_ha_discovery_and_auth -- --nocapture }
    "wave8"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave8_topn_imv_hnsw_bm25 -- --nocapture }
    "wave9"       { & cargo @CommonArgs -p rustlakedb-testkit --test wave9_vectorized_topn_dbt_autoloader -- --nocapture }
    "wave10"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave10_tls_sts_udfs -- --nocapture }
    "wave11"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave11_ha_snapshots_s3_interop -- --nocapture }
    "wave12"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave12_vectorized_spill_and_sql -- --nocapture }
    "wave13"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave13_cdc_tracing_and_tpch -- --nocapture }
    "wave14"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave14_native_types_partitioning_rollup -- --nocapture }
    "wave15"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave15_arrow_flight_grpc_and_kafka_cdc -- --nocapture }
    "wave16"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave16_mtls_real_sts_and_interop -- --nocapture }
    "wave17"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave17_iceberg_rest_rw_and_kafka_v2 -- --nocapture }
    "wave18"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave18_arrow_flight_sql_and_doput -- --nocapture }
    "wave19"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave19_tpch_22_and_cbo -- --nocapture }
    "wave20"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave20_columnar_arrow_pipeline -- --nocapture }
    "wave21"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave21_oidc_jwks_cancel_and_helm -- --nocapture }
    "wave22"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave22_field_id_evolution_cancel_oidc_arrow -- --nocapture }
    "wave23"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave23_jepsen_raft_occ_storm_and_chaos -- --nocapture }
    "wave24"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave24_schema_enforcement_time_travel_governance -- --nocapture }
    "wave25"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave25_vectorized_boost_aliasing_and_dpp -- --nocapture }
    "wave26"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave26_copy_analyze_and_memory_spill -- --nocapture }
    "wave28"      { & cargo @CommonArgs -p rustlakedb-testkit --test wave28_rolling_upgrade_chaos_and_prod_stack -- --nocapture }
    "tpch"        { & cargo @CommonArgs -p rustlakedb-testkit --test wave19_tpch_22_and_cbo -- --nocapture }
    "live"        { & cargo @CommonArgs -p rustlakedb-testkit --test live_multiprocess }
    "bench"       { & cargo @CommonArgs -p rustlakedb-testkit --test benchmarks -- --nocapture }
}

if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "`nRustLakeDB test suite '$Suite' PASSED." -ForegroundColor Green

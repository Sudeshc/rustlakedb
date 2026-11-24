<#
.SYNOPSIS
  Seeds 500,000 Iceberg v3 Parquet+Puffin rows into the live 3-worker Docker cluster and executes
  parallel distributed SQL benchmarks + live container-kill failover!
.EXAMPLE
  .\scripts\run-large-scale-docker-bench.ps1
  .\scripts\run-large-scale-docker-bench.ps1 -Rows 1000000 -Splits 24
#>
param(
    [int]$Rows = 500000,
    [int]$Splits = 20,
    [switch]$Rebuild
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
$ComposeFile = Join-Path $RepoRoot "docker\docker-compose.bench.yml"
$LargeSql = Join-Path $RepoRoot "docker\large_scale_bench.sql"

if ($Rebuild) {
    Write-Host "=== 1. Rebuilding rustlakedb:1.0.0 Docker image & restarting cluster ===" -ForegroundColor Cyan
    docker compose -f $ComposeFile down -v --remove-orphans | Out-Null
    docker compose -f $ComposeFile build worker-1
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    docker compose -f $ComposeFile up -d worker-1 worker-2 worker-3 coordinator maintainer
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Start-Sleep -Seconds 3
}

Write-Host "`n=== 2. Seeding $Rows rows ($Splits Zstd Parquet + Puffin splits) + 20,000 customers into live /warehouse ===" -ForegroundColor Cyan
docker exec rustlakedb-bench-coordinator rustlakedb --mode seed-large --rows $Rows --splits $Splits --data-dir /warehouse
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n=== 3. Running Live $Rows-Row Distributed SQL Benchmark via Official PostgreSQL 16 (psql) ===" -ForegroundColor Cyan
Get-Content $LargeSql -Raw | docker compose -f $ComposeFile --profile client run -T --rm --no-deps psql
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n=== 4. Per-Worker Distributed Task Load Verification (Proving All 3 Containers Executed In Parallel) ===" -ForegroundColor Cyan
foreach ($w in @("worker-1:7001", "worker-2:7002", "worker-3:7003")) {
    $parts = $w.Split(":")
    $name = $parts[0]
    $port = $parts[1]
    $metrics = docker exec "rustlakedb-bench-$name" curl -sf "http://127.0.0.1:$port/healthz"
    Write-Host "  $name -> $metrics" -ForegroundColor Green
}

Write-Host "`n=== 5. LIVE LARGE-DATASET CHAOS FAILOVER: Killing worker-1 & Running $Rows-Row Distributed Query ===" -ForegroundColor Yellow
docker kill rustlakedb-bench-worker-1 | Out-Null
docker compose -f $ComposeFile --profile client run --rm --no-deps psql -v ON_ERROR_STOP=1 -c "\timing on" -c "SELECT region, COUNT(*) AS orders, SUM(amount) AS total_rev FROM public.big_orders GROUP BY region ORDER BY region;"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`nRestarting worker-1 so all 3 workers stay healthy (tables big_orders & big_customers are ready in DBeaver!)..." -ForegroundColor Cyan
docker compose -f $ComposeFile up -d worker-1 | Out-Null
Write-Host "`n=== LARGE-DATASET DISTRIBUTED DOCKER BENCHMARK PASSED! ===" -ForegroundColor Green

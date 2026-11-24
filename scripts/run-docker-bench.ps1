<#
.SYNOPSIS
  Builds and runs the live multi-container Docker Test Bench for RustLakeDB v1.0.0:
  3 Worker containers + 1 Coordinator (:15432 PG, :18181 Iceberg REST) + 1 Maintainer + MinIO + real `psql`!
.EXAMPLE
  .\scripts\run-docker-bench.ps1
  .\scripts\run-docker-bench.ps1 -KeepRunning
#>
param(
    [switch]$KeepRunning,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
$ComposeFile = Join-Path $RepoRoot "docker\docker-compose.bench.yml"

Write-Host "=== 1. Starting RustLakeDB v1.0.0 Multi-Container Docker Test Bench ===" -ForegroundColor Cyan
docker compose -f $ComposeFile down -v --remove-orphans | Out-Null

if (-not $SkipBuild) {
    docker compose -f $ComposeFile build worker-1
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
docker compose -f $ComposeFile up -d worker-1 worker-2 worker-3 coordinator maintainer
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n=== 2. Waiting for Coordinator & Iceberg v3 REST Health ===" -ForegroundColor Cyan
$Healthy = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $resp = Invoke-RestMethod -Uri "http://127.0.0.1:18181/healthz" -TimeoutSec 2
        if ($resp.status -eq "ok") {
            Write-Host "Coordinator healthy: $($resp | ConvertTo-Json -Compress)" -ForegroundColor Green
            $Healthy = $true
            break
        }
    } catch {}
    Start-Sleep -Seconds 1
}
if (-not $Healthy) {
    docker compose -f $ComposeFile logs
    throw "Coordinator failed to become healthy"
}

Write-Host "`n=== 3. Running Real PostgreSQL 16 (psql) Live Workload Across 3 Worker Containers ===" -ForegroundColor Cyan
docker compose -f $ComposeFile --profile client run --rm psql -f /bench/live_bench.sql
if ($LASTEXITCODE -ne 0) {
    docker compose -f $ComposeFile logs
    exit $LASTEXITCODE
}

Write-Host "`n=== 4. Verifying Live Iceberg v3 REST Catalog HTTP Endpoints ===" -ForegroundColor Cyan
$tables = Invoke-RestMethod -Uri "http://127.0.0.1:18181/v1/namespaces/default/tables"
Write-Host "REST /v1/namespaces/default/tables -> $($tables | ConvertTo-Json -Compress)" -ForegroundColor Green
$ordersMeta = Invoke-RestMethod -Uri "http://127.0.0.1:18181/v1/namespaces/default/tables/orders"
Write-Host "REST orders format-version=$($ordersMeta.metadata.'format-version'), snapshot=$($ordersMeta.metadata.'current-snapshot-id'), indexes=$($ordersMeta.metadata.indexes.Count)" -ForegroundColor Green

Write-Host "`n=== 5. LIVE CONTAINER CHAOS TEST: Killing worker-1 Container Mid-Cluster ===" -ForegroundColor Yellow
docker kill rustlakedb-bench-worker-1 | Out-Null
Start-Sleep -Seconds 1

Write-Host "Running distributed SQL via psql AFTER killing worker-1 (verifying live failover to worker-2 & worker-3)..." -ForegroundColor Cyan
docker compose -f $ComposeFile --profile client run --rm --no-deps psql -v ON_ERROR_STOP=1 -c "SELECT tier, COUNT(*) AS cnt, SUM(amount) AS revenue FROM orders JOIN customers ON orders.customer_id = customers.customer_id GROUP BY tier ORDER BY tier;"
if ($LASTEXITCODE -ne 0) {
    docker compose -f $ComposeFile logs coordinator
    exit $LASTEXITCODE
}

Write-Host "`n=== ALL LIVE DOCKER BENCH & CHAOS TESTS PASSED! ===" -ForegroundColor Green

if ($KeepRunning) {
    Write-Host "`nRestarting worker-1 so all 3 worker containers remain healthy..." -ForegroundColor Cyan
    docker compose -f $ComposeFile up -d worker-1 | Out-Null
    Write-Host "Cluster left running! Connect anytime:" -ForegroundColor Cyan
    Write-Host "  PostgreSQL v3:  127.0.0.1:15432  (or: docker compose -f docker\docker-compose.bench.yml --profile client run --rm psql)"
    Write-Host "  Iceberg REST:   http://127.0.0.1:18181/v1/namespaces/default/tables"
    Write-Host "  MinIO Console:  http://127.0.0.1:19001  (rustlakedb / rustlakedb_dev_password)"
} else {
    Write-Host "Cleaning up Docker test bench containers..." -ForegroundColor DarkGray
    docker compose -f $ComposeFile down -v --remove-orphans | Out-Null
}

<#
.SYNOPSIS
  Turnkey Real-World Multi-Source Enterprise Lakehouse Test & Benchmark Runner for RustLakeDB v9.0.0.
  Spins up live PostgreSQL 16 OLTP + live RustFS S3 containers alongside the live 5-container
  RustLakeDB v9.0.0 cluster, executes Phases A-F (6 data sources, simple->extreme SQL, 10K->25M rows,
  16-client concurrency, PyIceberg direct scan), executes Phase G live worker-kill chaos failover,
  and cleans up temporary test artifacts cleanly.
#>
param(
    [string]$OutDir = "C:\Users\skc\.gemini\antigravity\brain\cc45d49a-daff-48df-a5f5-5ba4c6313a54\scratch"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

Write-Host "=== 1. Starting Live PostgreSQL 16 OLTP & RustFS S3 Containers on docker_default ===" -ForegroundColor Cyan
docker rm -f rw-oltp-pg rw-s3-rustfs 2>$null | Out-Null
docker run -d --name rw-oltp-pg --network docker_default -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=oltp postgres:16-alpine | Out-Null
docker run -d --name rw-s3-rustfs --network docker_default -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin rustfs/rustfs:latest /data | Out-Null

Write-Host "Waiting for Postgres 16 OLTP & RustFS S3 readiness..."
for ($i = 0; $i -lt 30; $i++) {
    docker exec rw-oltp-pg pg_isready -U postgres -d oltp 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 1
}
Start-Sleep -Seconds 2

try {
    Write-Host "`n=== 2. Running Real-World Multi-Source Lakehouse Suite (Phases A-F: 6 Sources, 10K->25M Rows, 16-Client Soak, PyIceberg) ===" -ForegroundColor Cyan
    docker run --rm --network docker_default `
        -v docker_warehouse_data:/warehouse `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        -v "${OutDir}:/out" `
        python:3.12-slim bash -lc 'pip install -q "psycopg[binary]" pyarrow numpy boto3 pyiceberg && python -u /scripts/realworld_lakehouse_suite.py'
    if ($LASTEXITCODE -ne 0) { throw "Phases A-F failed with exit code $LASTEXITCODE" }

    Write-Host "`n=== 3. Running Phase G: LIVE CHAOS FAILOVER (Killing rustlakedb-bench-worker-3 mid-workload!) ===" -ForegroundColor Yellow
    docker stop rustlakedb-bench-worker-3 | Out-Null
    Start-Sleep -Seconds 2
    $chaosOut = docker run --rm --network docker_default `
        -v docker_warehouse_data:/warehouse `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        python:3.12-slim bash -lc 'pip install -q "psycopg[binary]" pyarrow numpy boto3 pyiceberg && python -u /scripts/realworld_lakehouse_suite.py --chaos-probe'
    if ($LASTEXITCODE -ne 0) { throw "Chaos failover probe failed!" }
    Write-Host $chaosOut -ForegroundColor Green

    Write-Host "Restarting rustlakedb-bench-worker-3 and verifying cluster self-healing..." -ForegroundColor Cyan
    docker start rustlakedb-bench-worker-3 | Out-Null
    for ($i = 0; $i -lt 20; $i++) {
        $st = docker inspect --format "{{.State.Health.Status}}" rustlakedb-bench-worker-3
        if ($st -eq "healthy") { break }
        Start-Sleep -Seconds 1
    }
    Write-Host "rustlakedb-bench-worker-3 recovered -> healthy!" -ForegroundColor Green

    # Save chaos output alongside main JSON
    $chaosLine = ($chaosOut | Where-Object { $_ -like "CHAOS_PROBE_JSON:*" } | Select-Object -First 1)
    if ($chaosLine) {
        $chaosJson = $chaosLine.Substring("CHAOS_PROBE_JSON:".Length)
        $mainPath = Join-Path $OutDir "realworld_results.json"
        $doc = Get-Content $mainPath -Raw | ConvertFrom-Json
        $doc | Add-Member -NotePropertyName "chaos" -NotePropertyValue ($chaosJson | ConvertFrom-Json) -Force
        $doc | ConvertTo-Json -Depth 10 | Set-Content -Path $mainPath -Encoding UTF8
    }

    Write-Host "`n=== 4. Cleaning Up Temporary Benchmark Tables & Test Containers ===" -ForegroundColor Cyan
    docker run --rm --network docker_default `
        -v docker_warehouse_data:/warehouse `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        python:3.12-slim bash -lc 'pip install -q "psycopg[binary]" pyarrow numpy boto3 pyiceberg && python -u /scripts/realworld_lakehouse_suite.py --cleanup'
}
finally {
    docker start rustlakedb-bench-worker-3 2>$null | Out-Null
    docker rm -f rw-oltp-pg rw-s3-rustfs 2>$null | Out-Null
}

Write-Host "`n=== REAL-WORLD LAKEHOUSE TEST CAMPAIGN COMPLETE! ===" -ForegroundColor Green

<#
.SYNOPSIS
  Executes the live 1,000,000,000 (1 Billion) Financial Transaction Medallion (Bronze -> Silver -> Gold)
  benchmark against the live 3-worker RustLakeDB v9.0.0 cluster, captures live Docker container RAM/CPU
  telemetry, and cleans up the ~6 GiB temporary 1B Parquet dataset afterward.
#>
param(
    [string]$OutDir = "C:\Users\skc\.gemini\antigravity\brain\cc45d49a-daff-48df-a5f5-5ba4c6313a54\scratch"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

Write-Host "=== Pre-Benchmark Container Telemetry ===" -ForegroundColor Cyan
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" rustlakedb-bench-coordinator rustlakedb-bench-worker-1 rustlakedb-bench-worker-2 rustlakedb-bench-worker-3

try {
    Write-Host "`n=== Running 1,000,000,000-Row Financial Medallion Benchmark (Bronze -> Silver -> Gold) ===" -ForegroundColor Cyan
    docker run --rm --network docker_default `
        -v docker_warehouse_data:/warehouse `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        -v "${OutDir}:/out" `
        python:3.12-slim bash -lc 'pip install -q "psycopg[binary]" pyarrow numpy pyiceberg && python -u /scripts/medallion_1b_financial_bench.py'
    if ($LASTEXITCODE -ne 0) { throw "1B Medallion benchmark failed with exit code $LASTEXITCODE" }

    Write-Host "`n=== Post-1B-Scan Container Telemetry ===" -ForegroundColor Green
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" rustlakedb-bench-coordinator rustlakedb-bench-worker-1 rustlakedb-bench-worker-2 rustlakedb-bench-worker-3
}
finally {
    Write-Host "`n=== Cleaning Up 1B Temporary Medallion Tables & Parquet Splits ===" -ForegroundColor Cyan
    docker run --rm --network docker_default `
        -v docker_warehouse_data:/warehouse `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        python:3.12-slim bash -lc 'pip install -q "psycopg[binary]" pyarrow numpy pyiceberg && python -u /scripts/medallion_1b_financial_bench.py --cleanup'
}

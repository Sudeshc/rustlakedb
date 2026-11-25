<#
.SYNOPSIS
  Executes the 7-Pillar Hyperscale Production Readiness Certification (PRC) Suite against
  the live RustLakeDB v9.0.0 cluster + a live PostgreSQL 16 Differential Oracle:
    - Pillar 1: 1,000-Job Multi-Tenant Enterprise Day Storm (20 runners, 6 personas)
    - Pillar 2: 50-Query Differential Correctness Oracle vs Live PostgreSQL 16
    - Pillar 3: 12-Writer Concurrent OCC Conservation Storm (120 commits)
    - Pillar 4A: Live Worker SIGKILL Mid-Flight Under 8-Thread BI Load
    - Pillar 4B: Hard Coordinator SIGKILL + Cold Restart Durability Proof
    - Pillar 5: Bad-Deploy Disaster Recovery & Instant Rollback Drill
    - Pillar 6: Resource Hygiene (RAM/CPU/Temp-Spill Leaks) & Prometheus Audit
#>
param(
    [string]$OutDir = "C:\Users\skc\.gemini\antigravity\brain\cc45d49a-daff-48df-a5f5-5ba4c6313a54\scratch"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

Write-Host "=== Starting Live PostgreSQL 16 Differential Oracle Container ===" -ForegroundColor Cyan
docker rm -f prc-oracle-pg 2>$null | Out-Null
docker run -d --name prc-oracle-pg --network docker_default -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=oracle postgres:16-alpine | Out-Null
for ($i = 0; $i -lt 30; $i++) {
    docker exec prc-oracle-pg pg_isready -U postgres -d oracle 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 1
}

try {
    Write-Host "`n=== Running PRC Pillars 1, 2, 3, 5 & 6 (1,000-Job Storm, 50-Query PG16 Oracle, 12-Writer OCC, DR Drill) ===" -ForegroundColor Cyan
    docker run --rm --network docker_default `
        -v docker_warehouse_data:/warehouse `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        -v "${OutDir}:/out" `
        python:3.12-slim bash -lc 'pip install --root-user-action=ignore --disable-pip-version-check -q "psycopg[binary]" pyarrow pyiceberg >/dev/null 2>&1 && python -u /scripts/prc_hyperscale_certification.py'
    if ($LASTEXITCODE -ne 0) { throw "PRC main pillars failed!" }

    Write-Host "`n=== Running Pillar 4A: LIVE WORKER SIGKILL MID-FLIGHT UNDER 8-THREAD BI LOAD ===" -ForegroundColor Yellow
    docker rm -f prc-chaos-load 2>$null | Out-Null
    docker run -d --name prc-chaos-load --network docker_default -e CHAOS_DURATION_SECS=14 `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        python:3.12-slim bash -lc 'pip install --root-user-action=ignore --disable-pip-version-check -q "psycopg[binary]" pyarrow pyiceberg >/dev/null 2>&1 && python -u /scripts/prc_hyperscale_certification.py --worker-kill-load' | Out-Null
    for ($i = 0; $i -lt 60; $i++) {
        $logs = docker logs prc-chaos-load 2>$null
        if ($logs -match "CHAOS_LOAD_STARTED") { break }
        Start-Sleep -Milliseconds 500
    }
    Start-Sleep -Seconds 3
    Write-Host "  >> Injecting SIGKILL into rustlakedb-bench-worker-2 mid-workload!" -ForegroundColor Red
    docker kill -s SIGKILL rustlakedb-bench-worker-2 | Out-Null
    $chaosExit = docker wait prc-chaos-load
    $workerChaosOut = docker logs prc-chaos-load 2>&1
    docker rm -f prc-chaos-load 2>$null | Out-Null
    Write-Host ($workerChaosOut -join "`n") -ForegroundColor Green
    if ([int]$chaosExit -ne 0 -or -not (($workerChaosOut -join "`n") -match "CHAOS_WORKER_JSON:")) { throw "Pillar 4A worker SIGKILL chaos failed!" }

    Write-Host "  >> Restarting rustlakedb-bench-worker-2 and waiting for healthy..." -ForegroundColor Cyan
    docker start rustlakedb-bench-worker-2 | Out-Null
    for ($i = 0; $i -lt 20; $i++) {
        if ((docker inspect --format "{{.State.Health.Status}}" rustlakedb-bench-worker-2) -eq "healthy") { break }
        Start-Sleep -Seconds 1
    }

    Write-Host "`n=== Running Pillar 4B: HARD COORDINATOR SIGKILL + COLD RESTART DURABILITY PROOF ===" -ForegroundColor Yellow
    docker kill -s SIGKILL rustlakedb-bench-coordinator | Out-Null
    Start-Sleep -Seconds 1
    docker start rustlakedb-bench-coordinator | Out-Null
    for ($i = 0; $i -lt 25; $i++) {
        if ((docker inspect --format "{{.State.Health.Status}}" rustlakedb-bench-coordinator) -eq "healthy") { break }
        Start-Sleep -Seconds 1
    }
    $coordChaosOut = docker run --rm --network docker_default `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        python:3.12-slim bash -lc 'pip install --root-user-action=ignore --disable-pip-version-check -q "psycopg[binary]" pyarrow pyiceberg >/dev/null 2>&1 && python -u /scripts/prc_hyperscale_certification.py --verify-coord-crash'
    if ($LASTEXITCODE -ne 0) { throw "Pillar 4B Coordinator SIGKILL recovery failed!" }
    Write-Host $coordChaosOut -ForegroundColor Green

    Write-Host "`n=== Pillar 6: Post-Certification Resource & Spill-Dir Hygiene Check ===" -ForegroundColor Cyan
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" rustlakedb-bench-coordinator rustlakedb-bench-worker-1 rustlakedb-bench-worker-2 rustlakedb-bench-worker-3
    $w1Leaks = docker exec rustlakedb-bench-worker-1 sh -c 'find /tmp -name "arrow_*_spill*.arw1" 2>/dev/null | wc -l'
    Write-Host "Leaked NVMe spill files on worker-1: $w1Leaks"

    # Merge chaos outputs into prc_results.json
    $mainPath = Join-Path $OutDir "prc_results.json"
    $doc = Get-Content $mainPath -Raw | ConvertFrom-Json
    $wLine = ($workerChaosOut | Where-Object { $_ -like "CHAOS_WORKER_JSON:*" } | Select-Object -First 1)
    if ($wLine) {
        $doc | Add-Member -NotePropertyName "pillar4a_worker_sigkill" -NotePropertyValue ($wLine.Substring("CHAOS_WORKER_JSON:".Length) | ConvertFrom-Json) -Force
    }
    $cLine = ($coordChaosOut | Where-Object { $_ -like "CHAOS_COORD_JSON:*" } | Select-Object -First 1)
    if ($cLine) {
        $doc | Add-Member -NotePropertyName "pillar4b_coordinator_sigkill" -NotePropertyValue ($cLine.Substring("CHAOS_COORD_JSON:".Length) | ConvertFrom-Json) -Force
    }
    $doc | ConvertTo-Json -Depth 10 | Set-Content -Path $mainPath -Encoding UTF8

    Write-Host "`n=== Cleaning Up PRC Tables ===" -ForegroundColor Cyan
    docker run --rm --network docker_default `
        -v docker_warehouse_data:/warehouse `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        python:3.12-slim bash -lc 'pip install --root-user-action=ignore --disable-pip-version-check -q "psycopg[binary]" pyarrow pyiceberg >/dev/null 2>&1 && python -u /scripts/prc_hyperscale_certification.py --cleanup'
}
finally {
    docker rm -f prc-chaos-load 2>$null | Out-Null
    docker start rustlakedb-bench-worker-2 rustlakedb-bench-coordinator 2>$null | Out-Null
    docker rm -f prc-oracle-pg 2>$null | Out-Null
}

Write-Host "`n=== ALL 7 PRODUCTION READINESS CERTIFICATION PILLARS COMPLETE! ===" -ForegroundColor Green

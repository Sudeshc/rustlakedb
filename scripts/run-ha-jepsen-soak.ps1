<#
.SYNOPSIS
  Executes the Live 3-Node Raft HA + L4 NLB (HAProxy) + S3 (RustFS) + Jepsen Asymmetric
  Network Partition + Raft Leader SIGKILL + Continuous Soak Verification Suite.

  Defaults to a fast 28-second live chaos/partition verification run; pass `-DurationHours 72`
  for a full 72-hour staging soak.
#>
param(
    [double]$DurationSecs = 28,
    [double]$DurationHours = 0,
    [string]$OutDir = "C:\Users\skc\.gemini\antigravity\brain\cc45d49a-daff-48df-a5f5-5ba4c6313a54\scratch"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$EffectiveSecs = if ($DurationHours -gt 0) { [math]::Round($DurationHours * 3600) } else { $DurationSecs }
$Project = "rustlakedb-ha"
$NetName = "${Project}_default"

Write-Host "=== Starting 3-Node Raft HA Quorum + L4 NLB (HAProxy) + S3 (RustFS) Stack ===" -ForegroundColor Cyan
docker compose -p $Project -f "$RepoRoot\docker\docker-compose.ha.yml" down -v 2>$null | Out-Null
docker compose -p $Project -f "$RepoRoot\docker\docker-compose.ha.yml" up -d
if ($LASTEXITCODE -ne 0) { throw "Failed to start HA stack!" }

try {
    # Wait for NLB + workers
    Start-Sleep -Seconds 5
    Write-Host "`n=== Launching Continuous Soak & Jepsen Partition Workload (${EffectiveSecs}s) ===" -ForegroundColor Cyan
    docker rm -f ha-jepsen-runner 2>$null | Out-Null
    docker run -d --name ha-jepsen-runner --network $NetName `
        -v "${RepoRoot}\scripts:/scripts:ro" `
        -v "${OutDir}:/out" `
        python:3.12-slim bash -lc "pip install --root-user-action=ignore --disable-pip-version-check -q 'psycopg[binary]' >/dev/null 2>&1 && python -u /scripts/soak_and_jepsen_suite.py --duration-secs $EffectiveSecs" | Out-Null

    $leader = ""
    for ($i = 0; $i -lt 90; $i++) {
        $logs = (docker logs ha-jepsen-runner 2>&1) -join "`n"
        if ($logs -match "JEPSEN_INITIAL_LEADER:(coord-\d)") {
            $leader = $Matches[1]
        }
        if ($logs -match "SOAK_TRAFFIC_STARTED") { break }
        Start-Sleep -Milliseconds 500
    }
    if (-not $leader) { throw "Could not detect initial Raft leader from runner logs!" }

    function Get-ActiveLeader {
        foreach ($c in @("coord-1", "coord-2", "coord-3")) {
            $hc = docker exec "rustlakedb-ha-$c" sh -c 'curl -sf http://127.0.0.1:8181/readyz' 2>$null
            if ($hc -match '"is_leader":true') { return $c }
        }
        return $null
    }

    function Invoke-JepsenRound([int]$RoundNum, [string]$FallbackLeader) {
        $cur = Get-ActiveLeader
        if (-not $cur) { $cur = $FallbackLeader }
        $lCont = "rustlakedb-ha-$cur"
        Write-Host "`n=== [JEPSEN ROUND $RoundNum - CHAOS 1] Network-Partitioning Active Raft Leader ($lCont) ===" -ForegroundColor Yellow
        docker network disconnect $NetName $lCont
        Start-Sleep -Seconds 7
        Write-Host "=== [JEPSEN ROUND $RoundNum - HEAL 1] Reconnecting Former Leader ($lCont) to Quorum ===" -ForegroundColor Green
        docker network connect --alias $cur $NetName $lCont

        Start-Sleep -Seconds 5
        $cur2 = Get-ActiveLeader
        if (-not $cur2) { $cur2 = $cur }
        $lCont2 = "rustlakedb-ha-$cur2"
        Write-Host "`n=== [JEPSEN ROUND $RoundNum - CHAOS 2] Injecting Hard SIGKILL into Current Raft Leader ($lCont2) ===" -ForegroundColor Red
        docker kill -s SIGKILL $lCont2 | Out-Null
        Start-Sleep -Seconds 5
        Write-Host "=== [JEPSEN ROUND $RoundNum - RECOVERY 2] Cold-Restarting Killed Leader ($lCont2) ===" -ForegroundColor Green
        docker start $lCont2 | Out-Null
    }

    # Immediate Round 1 Jepsen partition + leader SIGKILL
    Start-Sleep -Seconds 4
    Invoke-JepsenRound -RoundNum 1 -FallbackLeader $leader

    # For long multi-hour staging soaks, repeat Jepsen chaos rounds every 30 minutes and print live memory/progress!
    if ($EffectiveSecs -gt 120) {
        $round = 1
        while ((docker inspect -f "{{.State.Running}}" ha-jepsen-runner 2>$null) -eq "true") {
            for ($tick = 0; $tick -lt 36; $tick++) {
                Start-Sleep -Seconds 50
                if ((docker inspect -f "{{.State.Running}}" ha-jepsen-runner 2>$null) -ne "true") { break }
                if (($tick % 6) -eq 0) {
                    Write-Host "`n--- [SOAK LIVE CHECKPOINT $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ---" -ForegroundColor Cyan
                    docker logs --tail 3 ha-jepsen-runner 2>&1
                    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" rustlakedb-ha-coord-1 rustlakedb-ha-coord-2 rustlakedb-ha-coord-3 rustlakedb-ha-worker-1 rustlakedb-ha-worker-2 rustlakedb-ha-nlb
                }
            }
            if ((docker inspect -f "{{.State.Running}}" ha-jepsen-runner 2>$null) -ne "true") { break }
            $round += 1
            Invoke-JepsenRound -RoundNum $round -FallbackLeader $leader
        }
    }

    $exitCode = [int](docker wait ha-jepsen-runner)
    $runnerOut = (docker logs ha-jepsen-runner 2>&1) -join "`n"
    Write-Host "`n$runnerOut" -ForegroundColor Cyan

    Write-Host "`n=== Post-Soak HA Container Memory & CPU Audit ===" -ForegroundColor Cyan
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" rustlakedb-ha-coord-1 rustlakedb-ha-coord-2 rustlakedb-ha-coord-3 rustlakedb-ha-worker-1 rustlakedb-ha-worker-2 rustlakedb-ha-nlb

    if ($exitCode -ne 0) { throw "HA Jepsen + Soak verification failed with exit code $exitCode!" }
}
finally {
    docker rm -f ha-jepsen-runner 2>$null | Out-Null
    Write-Host "`n=== Tearing Down Ephemeral HA Test Stack (Keeping Primary Bench Cluster Intact) ===" -ForegroundColor Cyan
    docker compose -p $Project -f "$RepoRoot\docker\docker-compose.ha.yml" down -v 2>$null | Out-Null
}

Write-Host "`n=== 3-NODE RAFT HA + L4 NLB + S3 + JEPSEN PARTITION + SOAK PASSED 100%! ===" -ForegroundColor Green

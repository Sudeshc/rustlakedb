<#
.SYNOPSIS
  Reproducible RustLakeDB Benchmark Harness (10M-Row Analytical + TPC-H 22/22 Suite).
.EXAMPLE
  .\scripts\run-tpch-bench.ps1
  .\scripts\run-tpch-bench.ps1 -Rows 10000000 -Splits 40 -Workers 4
#>
param(
    [int]$Rows = 10000000,
    [int]$Splits = 40,
    [int]$Workers = 4,
    [string]$Image = "rustlakedb:8.0.0"
)

$ErrorActionPreference = "Stop"

Write-Host "=== 1. Running Full 22-Query TPC-H Decision-Support Suite ===" -ForegroundColor Cyan
cargo test --manifest-path "$PSScriptRoot\..\Cargo.toml" -p rustlakedb-testkit --test wave19_tpch_22_and_cbo -- --nocapture
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n=== 2. Running Reproducible $Rows-Row Analytical Benchmark on $Image ($Workers workers, $Splits splits) ===" -ForegroundColor Cyan
$benchSh = @"
set -e
rustlakedb --mode seed-large --rows $Rows --splits $Splits --data-dir /tmp/bench_run
rustlakedb --mode gateway --workers $Workers --data-dir /tmp/bench_run --pg-addr 127.0.0.1:5432 --rest-addr 127.0.0.1:8181 > /tmp/server.log 2>&1 &
PID=`$!
for i in `$(seq 1 40); do
  if curl -sf http://127.0.0.1:8181/healthz > /dev/null 2>&1; then break; fi
  sleep 0.5
done
run_q() {
  LABEL="`$1"; SQL="`$2"
  T0=`$(date +%s%3N)
  OUT=`$(curl -sS -X POST http://127.0.0.1:8181/v1/sql -H 'Content-Type: application/json' -d "{\"sql\":\"`$SQL\"}")
  T1=`$(date +%s%3N)
  echo "[`$LABEL] `$((T1 - T0)) ms | `$(echo "`$OUT" | head -c 220)"
}
run_q "Q1-FullScan-GroupBy" "SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue FROM big_orders GROUP BY region ORDER BY region"
run_q "Q2-Filter-GroupBy-Avg" "SELECT region, COUNT(*) AS high_val, AVG(amount) AS avg_amt FROM big_orders WHERE amount > 450.0 GROUP BY region ORDER BY region"
run_q "Q3-TopN-Limit10" "SELECT order_id, region, amount FROM big_orders ORDER BY amount DESC LIMIT 10"
run_q "Q4-HashJoin-GroupBy" "SELECT c.tier, COUNT(*) AS orders, SUM(o.amount) AS revenue FROM big_orders o JOIN big_customers c ON o.customer_id = c.customer_id GROUP BY c.tier ORDER BY revenue DESC"
run_q "Q5-ZeroRewrite-Rename" "ALTER TABLE big_orders RENAME COLUMN amount TO gross_amount"
run_q "Q6-PostEvolution-GroupBy" "SELECT region, SUM(gross_amount) AS gross_rev FROM big_orders GROUP BY region ORDER BY region"
kill `$PID 2>/dev/null || true
"@
$b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($benchSh -replace "`r`n", "`n")))
docker run --rm --entrypoint sh $Image -c "echo '$b64' | base64 -d | sh -"

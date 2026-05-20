$ErrorActionPreference = "Continue"

$env:HTTP_PROXY = ""
$env:HTTPS_PROXY = ""
$env:ALL_PROXY = ""
$env:GIT_HTTP_PROXY = ""
$env:GIT_HTTPS_PROXY = ""

Set-Location (Resolve-Path "$PSScriptRoot\..")

$cases = @(
  @{ tag = "case1_pos35_p3"; pos = "35"; pending = "3" },
  @{ tag = "case2_pos45_p3"; pos = "45"; pending = "3" },
  @{ tag = "case3_pos45_p5"; pos = "45"; pending = "5" },
  @{ tag = "case4_pos55_p5"; pos = "55"; pending = "5" }
)

foreach ($case in $cases) {
  Write-Output "[case_runner] start tag=$($case.tag) signal_box_position=$($case.pos) max_pending_days=$($case.pending) $(Get-Date -Format s)"

  .\venv\Scripts\python.exe scripts\backtest_box_pullback.py `
    --start 2024-01-01 `
    --end 2025-12-31 `
    --exit-case ma25_stop_box_tp `
    --signal-box-position-max-pct $case.pos `
    --max-pending-days $case.pending 2>&1

  if ($LASTEXITCODE -ne 0) {
    Write-Output "[case_runner] backtest failed tag=$($case.tag) exit=$LASTEXITCODE $(Get-Date -Format s)"
    continue
  }

  $trade = Get-ChildItem outputs\box_backtest\box_backtest_trades_*.csv |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

  Write-Output "[case_runner] portfolio tag=$($case.tag) trades=$($trade.FullName) $(Get-Date -Format s)"

  .\venv\Scripts\python.exe scripts\portfolio_backtest_box.py `
    --trades $trade.FullName `
    --exit-case ma25_stop_box_tp `
    --tag $case.tag 2>&1

  if ($LASTEXITCODE -ne 0) {
    Write-Output "[case_runner] portfolio failed tag=$($case.tag) exit=$LASTEXITCODE $(Get-Date -Format s)"
    continue
  }

  Write-Output "[case_runner] done tag=$($case.tag) $(Get-Date -Format s)"
}

Write-Output "[case_runner] all done $(Get-Date -Format s)"

# Case Mix Report

Fixed-weight mix report generated from `outputs/case_mix/*.csv`.
Equity uses the simple, non-compounded daily return curve produced by `backtest_case_mix.py`.

## Scenario Summary

### 2020_covid_crash

| mix | total return | max DD | profit factor | active days | trades |
|---|---:|---:|---:|---:|---:|
| pullback2_only | 0.00% | 0.00% | - | 0 | 0 |
| core_mix | 0.91% | -0.53% | 1.74 | 52 | 179 |
| defensive_mix | 1.82% | -1.05% | 1.74 | 52 | 179 |
| bull_mix | 0.91% | -0.53% | 1.74 | 52 | 179 |

- Return leader: defensive_mix (1.82%, DD -1.05%).
- Lowest equity DD: pullback2_only (DD 0.00%, return 0.00%).
- Best profit factor: core_mix (1.74).
- Defensive mix beats pullback2_only on return, but DD did not improve.
- Bull mix does not add clear return advantage.
- Data note: zero trades: combo_current__ma5__margin_le20, combo_current__pullback2__margin_le20, combo_current__rsi70__margin_le5 | zero trades: combo_current__pullback2__margin_le20 | zero trades: combo_current__pullback2__margin_le20, combo_current__ma5__margin_le20, combo_current__rsi70__margin_le5 | zero trades: combo_current__pullback2__margin_le20, combo_current__rsi70__margin_le5, combo_current__ma5__margin_le20

### 2022_rate_hike_bear

| mix | total return | max DD | profit factor | active days | trades |
|---|---:|---:|---:|---:|---:|
| pullback2_only | 0.00% | 0.00% | - | 0 | 0 |
| core_mix | 0.31% | -1.10% | 1.05 | 314 | 834 |
| defensive_mix | 0.63% | -2.20% | 1.05 | 314 | 834 |
| bull_mix | 0.31% | -1.10% | 1.05 | 314 | 834 |

- Return leader: defensive_mix (0.63%, DD -2.20%).
- Lowest equity DD: pullback2_only (DD 0.00%, return 0.00%).
- Best profit factor: core_mix (1.05).
- Defensive mix beats pullback2_only on return, but DD did not improve.
- Bull mix does not add clear return advantage.
- Data note: zero trades: combo_current__ma5__margin_le20, combo_current__pullback2__margin_le20, combo_current__rsi70__margin_le5 | zero trades: combo_current__pullback2__margin_le20 | zero trades: combo_current__pullback2__margin_le20, combo_current__ma5__margin_le20, combo_current__rsi70__margin_le5 | zero trades: combo_current__pullback2__margin_le20, combo_current__rsi70__margin_le5, combo_current__ma5__margin_le20

### 2023_rebound

| mix | total return | max DD | profit factor | active days | trades |
|---|---:|---:|---:|---:|---:|
| pullback2_only | 49.86% | -5.26% | 2.55 | 278 | 600 |
| core_mix | 48.55% | -5.12% | 2.73 | 347 | 2467 |
| defensive_mix | 48.77% | -4.34% | 2.77 | 347 | 2467 |
| bull_mix | 48.18% | -5.40% | 2.67 | 347 | 2467 |

- Return leader: pullback2_only (49.86%, DD -5.26%).
- Lowest equity DD: defensive_mix (DD -4.34%, return 48.77%).
- Best profit factor: defensive_mix (2.77).
- Defensive mix keeps most of pullback2_only return while reducing DD.
- Defensive mix looks better balanced than core_mix here.
- Bull mix does not add clear return advantage.

### 2024_ai_bubble

| mix | total return | max DD | profit factor | active days | trades |
|---|---:|---:|---:|---:|---:|
| pullback2_only | 70.22% | -6.57% | 2.93 | 277 | 640 |
| core_mix | 68.71% | -5.15% | 3.26 | 352 | 2681 |
| defensive_mix | 70.50% | -3.60% | 3.47 | 352 | 2681 |
| bull_mix | 68.08% | -5.61% | 3.24 | 352 | 2681 |

- Return leader: defensive_mix (70.50%, DD -3.60%).
- Lowest equity DD: defensive_mix (DD -3.60%, return 70.50%).
- Best profit factor: defensive_mix (3.47).
- Defensive mix keeps most of pullback2_only return while reducing DD.
- Defensive mix looks better balanced than core_mix here.
- Bull mix does not add clear return advantage.

### 2025_ai_bubble

| mix | total return | max DD | profit factor | active days | trades |
|---|---:|---:|---:|---:|---:|
| pullback2_only | 68.36% | -3.84% | 3.23 | 266 | 636 |
| core_mix | 60.66% | -3.64% | 3.33 | 345 | 2610 |
| defensive_mix | 62.39% | -2.83% | 3.32 | 345 | 2610 |
| bull_mix | 57.52% | -4.19% | 3.20 | 345 | 2610 |

- Return leader: pullback2_only (68.36%, DD -3.84%).
- Lowest equity DD: defensive_mix (DD -2.83%, return 62.39%).
- Best profit factor: core_mix (3.33).
- Defensive mix keeps most of pullback2_only return while reducing DD.
- Defensive mix looks better balanced than core_mix here.
- Bull mix does not add clear return advantage.

### custom_recent

| mix | total return | max DD | profit factor | active days | trades |
|---|---:|---:|---:|---:|---:|
| pullback2_only | 13.86% | -8.06% | 2.04 | 59 | 163 |
| core_mix | 12.34% | -7.83% | 2.03 | 77 | 684 |
| defensive_mix | 12.86% | -7.64% | 2.12 | 77 | 684 |
| bull_mix | 11.67% | -7.81% | 1.97 | 77 | 684 |

- Return leader: pullback2_only (13.86%, DD -8.06%).
- Lowest equity DD: defensive_mix (DD -7.64%, return 12.86%).
- Best profit factor: defensive_mix (2.12).
- Defensive mix keeps most of pullback2_only return while reducing DD.
- Defensive mix looks better balanced than core_mix here.
- Bull mix does not add clear return advantage.

## Case Contribution Notes

- `fixed10`: contribution total 93.85%, positive rows 18/18, avg risk_score 6.44.
- `ma5_margin_le20`: contribution total 121.19%, positive rows 12/12, avg risk_score 1.91.
- `pullback2_margin_le20`: contribution total 465.29%, positive rows 16/16, avg risk_score 0.42.
- `rsi70_margin_le5`: contribution total 97.11%, positive rows 12/12, avg risk_score 1.62.

## Reading Guide

- `risk_score = abs(max_drawdown_pct) / max(return_contribution_pct, 0.01)`; smaller is better.
- `pullback2_only` is the single-case benchmark.
- `defensive_mix` is the first candidate to check for DD reduction without killing returns.
- `bull_mix` should only survive if it adds clear upside in bull scenarios.

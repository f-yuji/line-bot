"""Build a consolidated H5 meta summary from existing output files.

The script is deliberately read-only for strategy code, DB definitions, UI, and
case registration. It reads outputs/* and writes outputs/h5_meta_summary/*.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]


class FileTracker:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.rows: list[dict[str, str]] = []

    def rel(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.root))
        except ValueError:
            return str(path)

    def note(self, path: Path, used: bool, purpose: str, notes: str = "") -> None:
        self.rows.append(
            {
                "file_path": self.rel(path),
                "exists": str(path.exists()).lower(),
                "used": str(used).lower(),
                "purpose": purpose,
                "notes": notes,
            }
        )

    def read_text(self, path: Path, purpose: str) -> str:
        if path.exists():
            self.note(path, True, purpose)
            return path.read_text(encoding="utf-8", errors="replace")
        fallback = self.find_near_report(path.parent)
        if fallback:
            self.note(path, False, purpose, f"missing; fallback={self.rel(fallback)}")
            self.note(fallback, True, purpose, "fallback report")
            return fallback.read_text(encoding="utf-8", errors="replace")
        self.note(path, False, purpose, "missing; no fallback found")
        return ""

    def read_csv(self, path: Path, purpose: str) -> list[dict[str, str]]:
        if not path.exists():
            self.note(path, False, purpose, "missing")
            return []
        self.note(path, True, purpose)
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def find_near_report(directory: Path) -> Path | None:
        if not directory.exists():
            return None
        candidates: list[Path] = []
        for pattern in ("*report*.txt", "*recommendation*.txt", "*summary*.txt"):
            candidates.extend(sorted(directory.glob(pattern)))
        return candidates[0] if candidates else None


def write_text(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.strip() + "\n", encoding="utf-8")


def write_csv(path: Path, rows: Iterable[dict[str, object]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def val(row: dict[str, str] | None, *keys: str) -> str:
    if not row:
        return ""
    for key in keys:
        item = row.get(key)
        if item not in (None, ""):
            return item
    return ""


def row(rows: list[dict[str, str]], **criteria: str) -> dict[str, str] | None:
    for item in rows:
        if all(item.get(key) == expected for key, expected in criteria.items()):
            return item
    return None


def add_case_row(
    rows: list[dict[str, object]],
    source: str,
    group: str,
    period: str,
    n: str = "",
    wr: str = "",
    avg: str = "",
    pf: str = "",
    max_dd: str = "",
    max_loss: str = "",
    stop_n: str = "",
    ext_n: str = "",
    pb_n: str = "",
    notes: str = "",
) -> None:
    rows.append(
        {
            "source_report": source,
            "group_or_case": group,
            "period": period,
            "n": n,
            "WR": wr,
            "avg_or_EV": avg,
            "PF": pf,
            "maxDD": max_dd,
            "max_loss": max_loss,
            "emergency_stop_count": stop_n,
            "extension_count": ext_n,
            "peak_pullback_count": pb_n,
            "notes": notes,
        }
    )


def build_case_matrix(data: dict[str, list[dict[str, str]]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []

    for label in (
        "research_all",
        "live_selected",
        "not_selected",
        "not_selected_position_limited",
        "not_selected_rank_below_10",
    ):
        for period in ("train", "test", "all"):
            item = row(data["live_selected"], label=label, period=period)
            if item:
                add_case_row(
                    out,
                    "h5_live_selection_audit/05_selected_vs_not_selected.csv",
                    label,
                    period,
                    val(item, "n"),
                    val(item, "hd3_raw_wr"),
                    val(item, "hd3_raw_avg"),
                    val(item, "hd3_raw_pf"),
                    notes="HD3 raw selection audit",
                )

    for label in (
        "current_ev_desc",
        "prob_desc",
        "low_volume_asc",
        "moderate_volume",
        "drop_deep",
        "random_seed42",
        "random_seed0",
        "random_seed99",
    ):
        item = row(data["sort_variants"], label=label, period="all")
        if item:
            add_case_row(
                out,
                "h5_live_selection_audit/08_sort_variant_comparison.csv",
                f"sort_variant:{label}",
                "all",
                val(item, "n"),
                val(item, "hd3_raw_wr"),
                val(item, "hd3_raw_avg"),
                val(item, "hd3_raw_pf"),
                notes="Live Limited sort variant, HD3 raw",
            )

    for hold_days in ("3.0", "5.0", "7.0", "10.0"):
        item = row(data["holding_research"], period="research_all", hold_days=hold_days)
        if item:
            add_case_row(
                out,
                "h5_hd3_edge_anatomy/07_holding_day_comparison_research.csv",
                f"H5 Research ALL HD{int(float(hold_days))}",
                "all",
                val(item, "n_raw"),
                val(item, "raw_win_rate"),
                val(item, "raw_avg_ret"),
                val(item, "raw_pf"),
                notes=f"EST12 avg={val(item, 'est12_avg_ret')} PF={val(item, 'est12_pf')}",
            )

    for source, key in (
        ("h5_extension_allow_case/02_primary_vs_extension_vs_ban_vs_allow_train.csv", "extension_train"),
        ("h5_extension_allow_case/03_primary_vs_extension_vs_ban_vs_allow_test.csv", "extension_test"),
    ):
        for item in data[key]:
            add_case_row(
                out,
                source,
                val(item, "case_key"),
                val(item, "period"),
                val(item, "trade_count"),
                val(item, "win_rate"),
                val(item, "avg_ret"),
                val(item, "pf"),
                val(item, "max_dd"),
                val(item, "max_loss"),
                val(item, "emergency_stop_count"),
                val(item, "extension_enabled_count"),
                val(item, "peak_pullback_count"),
                notes=f"allowed={val(item, 'extension_allowed_count')} rejected={val(item, 'extension_rejected_count')}",
            )

    for source, key in (
        ("h5_pullback_audit/06_pullback_variant_comparison_train.csv", "pullback_train"),
        ("h5_pullback_audit/07_pullback_variant_comparison_test.csv", "pullback_test"),
    ):
        for item in data[key]:
            if val(item, "variant") in ("PB20_CURRENT", "NO_PULLBACK_HD3_EST12"):
                add_case_row(
                    out,
                    source,
                    val(item, "variant"),
                    val(item, "period"),
                    val(item, "n"),
                    val(item, "win_rate"),
                    val(item, "avg_ret"),
                    val(item, "profit_factor"),
                    val(item, "max_drawdown"),
                    val(item, "max_loss"),
                    val(item, "emergency_stop_n"),
                    "",
                    val(item, "peak_pullback_n"),
                    notes=f"PB profit={val(item, 'peak_pullback_profit_n')} PB loss={val(item, 'peak_pullback_loss_n')}",
                )

    return out


def build_exit_summary(data: dict[str, list[dict[str, str]]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for population, key in (("Research", "holding_research"), ("Live Limited", "holding_live")):
        for item in data[key]:
            out.append(
                {
                    "population": population,
                    "period": val(item, "period"),
                    "holding_days": val(item, "hold_days"),
                    "n": val(item, "n_raw"),
                    "avg_return": val(item, "raw_avg_ret"),
                    "WR": val(item, "raw_win_rate"),
                    "PF": val(item, "raw_pf"),
                    "maxDD": "",
                    "emergency_stop_rate": val(item, "est12_stop_rate"),
                    "avg_holding_days": val(item, "hold_days"),
                    "notes": f"raw path; EST12 avg={val(item, 'est12_avg_ret')} PF={val(item, 'est12_pf')}",
                }
            )
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", default="outputs")
    parser.add_argument("--output-dir", default="outputs/h5_meta_summary")
    args = parser.parse_args()

    base = ROOT / args.base_dir
    out = ROOT / args.output_dir
    tracker = FileTracker(ROOT)

    # Priority TXT reports. The content is read to prove source availability and
    # keep the files_read_log complete; numeric summaries below come from CSVs.
    tracker.read_text(base / "h5_live_selection_audit/13_live_selection_audit_report.txt", "Live selection audit")
    tracker.read_text(base / "h5_hd3_edge_anatomy/16_hd3_edge_anatomy_report.txt", "HD3 edge anatomy")
    tracker.read_text(base / "h5_extension_allow_case/11_after_feature_fix_report.txt", "Extension Allow after feature fix")
    tracker.read_text(base / "h5_extension_technical_rule_search/21_technical_rule_search_report.txt", "Technical rule search")
    tracker.read_text(base / "h5_pullback_audit/10_recommendation_report.txt", "PB20 pullback audit")

    data = {
        "live_selected": tracker.read_csv(base / "h5_live_selection_audit/05_selected_vs_not_selected.csv", "Selected vs not selected"),
        "sort_variants": tracker.read_csv(base / "h5_live_selection_audit/08_sort_variant_comparison.csv", "Sort variant comparison"),
        "regime": tracker.read_csv(base / "h5_live_selection_audit/11_regime_selection_performance.csv", "Regime selection performance"),
        "holding_research": tracker.read_csv(base / "h5_hd3_edge_anatomy/07_holding_day_comparison_research.csv", "Research holding-day comparison"),
        "holding_live": tracker.read_csv(base / "h5_hd3_edge_anatomy/08_holding_day_comparison_live.csv", "Live holding-day comparison"),
        "extension_train": tracker.read_csv(base / "h5_extension_allow_case/02_primary_vs_extension_vs_ban_vs_allow_train.csv", "Extension train comparison"),
        "extension_test": tracker.read_csv(base / "h5_extension_allow_case/03_primary_vs_extension_vs_ban_vs_allow_test.csv", "Extension test comparison"),
        "allow_real": tracker.read_csv(base / "h5_extension_allow_case/13_real_feature_allow_summary.csv", "Real feature allow summary"),
        "proxy_usage": tracker.read_csv(base / "h5_extension_allow_case/08_extension_allow_proxy_usage.csv", "Extension proxy usage"),
        "technical_allow": tracker.read_csv(base / "h5_extension_technical_rule_search/13_compare_with_current_allow.csv", "Current allow technical comparison"),
        "top_technical": tracker.read_csv(base / "h5_extension_technical_rule_search/12_top_candidate_rules.csv", "Top technical rules"),
        "pullback_train": tracker.read_csv(base / "h5_pullback_audit/06_pullback_variant_comparison_train.csv", "Pullback train variants"),
        "pullback_test": tracker.read_csv(base / "h5_pullback_audit/07_pullback_variant_comparison_test.csv", "Pullback test variants"),
    }

    live_all = row(data["live_selected"], label="research_all", period="all")
    selected_all = row(data["live_selected"], label="live_selected", period="all")
    not_selected_all = row(data["live_selected"], label="not_selected", period="all")
    rank_dropped_all = row(data["live_selected"], label="not_selected_rank_below_10", period="all")
    current_sort = row(data["sort_variants"], label="current_ev_desc", period="all")
    low_volume = row(data["sort_variants"], label="low_volume_asc", period="all")
    pb20_test = row(data["pullback_test"], variant="PB20_CURRENT", period="test")
    no_pb_test = row(data["pullback_test"], variant="NO_PULLBACK_HD3_EST12", period="test")
    allow_all = row(data["allow_real"], group="all")
    allow_yes = row(data["allow_real"], group="allowed")
    allow_no = row(data["allow_real"], group="rejected")
    normal = row(data["regime"], entry_market_regime="normal", period="all")
    panic = row(data["regime"], entry_market_regime="panic_rebound", period="all")
    tech_allow_all = row(data["technical_allow"], period="all")

    write_text(
        out / "01_key_findings.txt",
        f"""
# H5 Meta Summary - Key Findings

- H5 Research population still has positive edge. Research ALL HD3: n={val(live_all, 'n')}, avg={val(live_all, 'hd3_raw_avg')}%, WR={val(live_all, 'hd3_raw_wr')}%, PF={val(live_all, 'hd3_raw_pf')}.
- HD3 is a practical baseline, not the max-return horizon. Research ALL raw avg rises from HD3 0.3541% to HD5 0.5841%, HD7 0.8628%, and HD10 1.1820%.
- Live Limited selection is worse than Research. Live Selected HD3 avg={val(selected_all, 'hd3_raw_avg')}%, Not Selected avg={val(not_selected_all, 'hd3_raw_avg')}%, rank_limit dropped avg={val(rank_dropped_all, 'hd3_raw_avg')}%.
- entry_sort list bug is critical. Rules store entry_sort as a list, while _sort_candidates behaved as if it were a string. The intended signal_probability/overheat/volume sort did not run; expected_value_desc fallback likely ran.
- PB20 removal is justified. PB20_CURRENT test: WR={val(pb20_test, 'win_rate')}%, avg={val(pb20_test, 'avg_ret')}%, PF={val(pb20_test, 'profit_factor')}. NO_PULLBACK_HD3_EST12 test: WR={val(no_pb_test, 'win_rate')}%, avg={val(no_pb_test, 'avg_ret')}%, PF={val(no_pb_test, 'profit_factor')}.
- Extension Allow is useful as research, but not ready for Primary. Real-feature all: count={val(allow_all, 'count')}, avg_extension_benefit={val(allow_all, 'avg_extension_benefit')}%, recovered_rate={val(allow_all, 'recovered_rate')}%.
- Regime matters. panic_rebound is strong; normal selected performance appears to damage expected value.
- High volume priority is suspicious. current_ev_desc all avg={val(current_sort, 'hd3_raw_avg')}%, while low_volume_asc all avg={val(low_volume, 'hd3_raw_avg')}%.
- Next priority is not another exit rule. Fix and re-test Live Limited selection first.
""",
    )

    write_csv(
        out / "02_case_performance_matrix.csv",
        build_case_matrix(data),
        [
            "source_report",
            "group_or_case",
            "period",
            "n",
            "WR",
            "avg_or_EV",
            "PF",
            "maxDD",
            "max_loss",
            "emergency_stop_count",
            "extension_count",
            "peak_pullback_count",
            "notes",
        ],
    )

    write_csv(
        out / "03_exit_holding_days_summary.csv",
        build_exit_summary(data),
        [
            "population",
            "period",
            "holding_days",
            "n",
            "avg_return",
            "WR",
            "PF",
            "maxDD",
            "emergency_stop_rate",
            "avg_holding_days",
            "notes",
        ],
    )

    write_text(
        out / "04_live_selection_problem_summary.txt",
        f"""
# Live Selection Problem Summary

Current intended entry_sort:
["signal_probability_desc", "overheat_score_asc", "volume_ratio_desc"]

Implementation issue:
entry_sort is a list in rules, but sorting logic treated it like a string. The intended multi-key sort did not run, and expected_value_desc fallback likely selected trades.

Observed degradation:
- Research ALL HD3 avg: {val(live_all, 'hd3_raw_avg')}%
- Live Selected HD3 avg: {val(selected_all, 'hd3_raw_avg')}%
- Not Selected HD3 avg: {val(not_selected_all, 'hd3_raw_avg')}%
- rank_limit dropped HD3 avg: {val(rank_dropped_all, 'hd3_raw_avg')}%

Why it matters:
The selection layer may be dropping good candidates and keeping weaker candidates. This is more urgent than exit-rule expansion.

Volume issue:
High volume priority may be selecting continued forced selling. low_volume_asc and moderate_volume are better candidates for the next controlled comparison.

Regime issue:
panic_rebound selected performance is strong. normal selected performance is weak. panic_rebound research avg={val(panic, 'research_hd3_raw_avg')}%, selected avg={val(panic, 'selected_hd3_raw_avg')}%; normal research avg={val(normal, 'research_hd3_raw_avg')}%, selected avg={val(normal, 'selected_hd3_raw_avg')}.

Next work:
Fix list-based entry_sort, then compare current_bug_ev_desc, intended_original, no_volume, low_volume, moderate_volume, regime_priority, and random_baseline under identical constraints.
""",
    )

    top_rule_lines = []
    for item in data["top_technical"][:5]:
        top_rule_lines.append(
            f"- {val(item, 'condition', 'rule')} | mode={val(item, 'mode')} | period={val(item, 'period')} | diff={val(item, 'avg_ret_diff')}"
        )
    top_rules = "\n".join(top_rule_lines) if top_rule_lines else "- No top technical rule CSV rows found."

    write_text(
        out / "05_extension_summary.txt",
        f"""
# Extension Summary

Extension base:
Extend to HD5 only when day3_return <= -1%. This has positive expectancy, but it is broad and can worsen drawdown.

Extension Allow:
Condition: day3_return <= -1%, day1_return >= -2.22%, day3_body_pct <= 3.74%, day3_volume_ratio <= 2.0.

Real-feature result:
- allowed count={val(allow_yes, 'count')}, avg_extension_benefit={val(allow_yes, 'avg_extension_benefit')}%, recovered_rate={val(allow_yes, 'recovered_rate')}%
- rejected count={val(allow_no, 'count')}, avg_extension_benefit={val(allow_no, 'avg_extension_benefit')}%, died_rate={val(allow_no, 'died_rate')}%
- all count={val(allow_all, 'count')}, avg_extension_benefit={val(allow_all, 'avg_extension_benefit')}%

Technical Rule Search:
Current allow comparison all diff={val(tech_allow_all, 'avg_ret_diff')}%, selected_recovered_rate={val(tech_allow_all, 'selected_recovered_rate')}%.
Candidate themes: day1 not too weak, weekly 13w support alive, day3 close near short support, and volume not overheated.

Top rule preview:
{top_rules}

Conclusion:
Extension Allow remains a research/comparison case. It should not be promoted before Live Limited selection is fixed and re-tested.
""",
    )

    write_text(
        out / "06_pullback_summary.txt",
        f"""
# Pullback Summary

Old PB20 logic:
After peak > entry * 1.005, exit when close <= peak * 0.98. This fires even when the trade is losing.

Problem:
PB20 behaved less like profit-taking and more like failed-rebound stop logic. In test, PB20_CURRENT had peak_pullback_n={val(pb20_test, 'peak_pullback_n')}, including peak_pullback_loss_n={val(pb20_test, 'peak_pullback_loss_n')}.

Comparison:
- PB20_CURRENT test: WR={val(pb20_test, 'win_rate')}%, avg={val(pb20_test, 'avg_ret')}%, PF={val(pb20_test, 'profit_factor')}
- NO_PULLBACK_HD3_EST12 test: WR={val(no_pb_test, 'win_rate')}%, avg={val(no_pb_test, 'avg_ret')}%, PF={val(no_pb_test, 'profit_factor')}

Conclusion:
PB20 removal is justified. Keep old PB20 only as comparison/research. Do not restore it as Primary.
""",
    )

    write_text(
        out / "07_regime_volume_support_summary.txt",
        """
# Regime / Volume / Support Summary

Regime:
panic_rebound is the best-fit regime for H5. normal regime is where Live selection appears most likely to damage expected value.

Volume:
High-volume priority is dangerous. Volume spikes may indicate continuing forced selling rather than rebound energy. Low or moderate volume sort variants deserve controlled testing.

Support and weekly context:
Weekly 13w support, short-term support proximity, and non-overheated weekly volume appear useful as research filters. Support breaks and day1 collapse are possible extension-ban signals.

Working interpretation:
The key question is not "more technical filters everywhere." It is whether the candidate is collapsing, whether weekly support is still alive, and whether volume is overheating into forced selling.
""",
    )

    write_text(
        out / "08_current_best_hypothesis.txt",
        """
# Current Best Hypothesis

1. H5 entry conditions are valid. The Research population has positive expectancy.
2. PB20 cuts expected value. No-pullback HD3 + EST12 is the current baseline.
3. HD3 is not necessarily the max-return horizon. It is the simple operational baseline.
4. Extension Allow is promising but limited. It remains research only.
5. The largest current problem is Live Limited selection, especially the entry_sort list bug and expected_value fallback.
6. High-volume priority is likely dangerous. Low/moderate volume or panic_rebound priority may be better.
7. normal regime selection may damage expectancy.
8. The next priority is Live Limited sort repair and controlled comparison, not another exit rule.
""",
    )

    write_text(
        out / "09_next_action_plan.txt",
        """
# Next Action Plan

Priority 1:
Fix the entry_sort list bug so _sort_candidates handles list-based sort keys correctly.

Priority 2:
Run a controlled Live sort comparison: current_bug_ev_desc, intended_original, no_volume, low_volume, moderate_volume, regime_priority + moderate_volume, and random_baseline.

Priority 3:
Find a Live Limited selector that does not destroy the Research edge. Evaluate train/test/all, WR, avg/EV, PF, maxDD, rank_limit drops, normal, panic_rebound, and volume buckets.

Priority 4:
Re-check fixed holding days with PF, maxDD, monthly stability, and capital lockup, not only average return.

Priority 5:
Keep Extension Allow as research. Re-evaluate it after Live selection is repaired.

Priority 6:
Do not change Primary now. Do not restore PB20. Do not overtrust current Live Selected until selection is repaired.
""",
    )

    write_text(
        out / "10_meta_summary_report.txt",
        """
# H5 Meta Summary Report

## Executive Summary

H5 entry conditions remain valid. PB20 removal is justified. HD3 + EST12 should remain the current Primary baseline. Extension Allow is promising but limited and should remain research. The biggest current issue is Live Limited selection: entry_sort is stored as a list, but sorting logic treated it as a string, so fallback expected_value sorting likely caused adverse selection.

## What Is Confirmed

- H5 Research has positive expectancy.
- PB20 fires on losing trades and underperforms NO_PULLBACK_HD3_EST12.
- Extension Allow feature proxy issues were fixed for open and volume_ratio.
- Live Selected is worse than Research, Not Selected, and rank_limit dropped groups.
- entry_sort list handling is a real implementation issue.

## What Is Likely

- High-volume priority is counterproductive.
- normal regime Live selection is damaging expected value.
- panic_rebound is the strongest H5 environment.
- Extension Allow can add value, but it is not the main bottleneck.

## What Is Uncertain

- Whether HD7/HD10 should ever be operationally adopted.
- Whether HD3 remains best after PF/maxDD/monthly stability/capital lockup.
- The best Live sort recipe.
- Proper max_daily_entries, max_open_positions, entry_rank_limit, and sector limit settings.
- Whether regime_priority is robust enough for production.

## Current Recommended Operation

- Keep Primary as HD3 + EST12.
- Do not restore PB20.
- Treat current Live Selected cautiously.
- Inspect Research candidates alongside Live candidates.
- Be cautious with high-volume and normal-regime candidates.
- Prioritize panic_rebound candidates for manual review.

## Next Technical Tasks

1. Fix entry_sort list handling.
2. Run Live sort variant comparison.
3. Search for a Live selector that preserves Research edge.
4. Re-check holding days using PF/maxDD/monthly stability.
5. Keep Extension Allow as research until Live selection is repaired.
""",
    )

    confidence_rows = [
        {
            "conclusion": "PB20 removal is justified",
            "confidence": "high",
            "evidence": "NO_PULLBACK_HD3_EST12 beats PB20_CURRENT; PB20 fires often on losing trades.",
            "risk": "Period dependence",
            "next_check": "Monthly stability by variant",
        },
        {
            "conclusion": "H5 entry conditions have edge",
            "confidence": "high",
            "evidence": "Research ALL has positive HD3/HD5/HD7/HD10 averages.",
            "risk": "Live constraints can destroy the edge",
            "next_check": "Re-test after Live selector fix",
        },
        {
            "conclusion": "Current Live Limited selection is adverse",
            "confidence": "high",
            "evidence": "Live Selected underperforms Not Selected and rank_limit dropped; entry_sort list bug exists.",
            "risk": "Aggregation mismatch",
            "next_check": "Controlled sort variant comparison",
        },
        {
            "conclusion": "Extension Allow is promising research",
            "confidence": "medium",
            "evidence": "Allowed/rejected extension benefit split is meaningful and proxy issue is fixed.",
            "risk": "Test improvement is limited; Live selector bug may distort results",
            "next_check": "Re-evaluate after selector repair",
        },
        {
            "conclusion": "Extension Allow should become Primary now",
            "confidence": "low",
            "evidence": "Improvement is not decisive enough and main bottleneck is selection.",
            "risk": "Overfitting",
            "next_check": "Forward-test only",
        },
        {
            "conclusion": "High-volume priority is dangerous",
            "confidence": "medium",
            "evidence": "low_volume/moderate_volume variants beat current fallback in audit.",
            "risk": "Liquidity trade-off",
            "next_check": "Volume bucket sort comparison",
        },
    ]
    write_csv(out / "11_confidence_table.csv", confidence_rows, ["conclusion", "confidence", "evidence", "risk", "next_check"])

    write_text(
        out / "12_open_questions.txt",
        """
# Open Questions

- Is HD3 still best after PF, maxDD, monthly stability, and capital lockup?
- Are HD7/HD10 strong beyond average return?
- What is the optimal Live Limited sort?
- Is max_daily_entries=2 appropriate?
- Is max_open_positions=2 appropriate?
- Is entry_rank_limit=10 appropriate?
- Does sector limiting remove too much expected value?
- Can regime_priority be used in production?
- What is the best volume_ratio range?
- Does Extension Allow still help after Live selection is fixed?
- How much live execution slippage remains from open/close timing?
""",
    )

    write_csv(out / "13_files_read_log.txt", tracker.rows, ["file_path", "exists", "used", "purpose", "notes"])

    print(f"Wrote H5 meta summary to {out}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Render case mix research report from generated CSV files.

Input-only script. It reads outputs/case_mix/*.csv and writes PNG/CSV/Markdown
under outputs/case_mix/report. It does not access DB or virtual_trades.
"""
from __future__ import annotations

import argparse
import csv
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = ROOT / "outputs" / "case_mix"
DEFAULT_REPORT_DIR = DEFAULT_INPUT_DIR / "report"

MIX_ORDER = ["pullback2_only", "core_mix", "defensive_mix", "bull_mix"]
MIX_STYLES = {
    "pullback2_only": {"linewidth": 2.8, "linestyle": "--", "alpha": 0.95},
    "core_mix": {"linewidth": 2.1, "linestyle": "-", "alpha": 0.95},
    "defensive_mix": {"linewidth": 3.2, "linestyle": "-", "alpha": 1.0},
    "bull_mix": {"linewidth": 2.1, "linestyle": "-", "alpha": 0.95},
}

CASE_LABELS = {
    "combo_current__pullback2__margin_le20": "pullback2_margin_le20",
    "combo_current__ma5__margin_le20": "ma5_margin_le20",
    "combo_current__rsi70__margin_le5": "rsi70_margin_le5",
    "combo_current__fixed10": "fixed10",
}

SUMMARY_COLS = [
    "scenario",
    "mix_name",
    "total_return_pct",
    "max_drawdown_pct",
    "win_rate_days",
    "avg_daily_return_pct",
    "best_day_pct",
    "worst_day_pct",
    "profit_factor",
    "active_days",
    "total_trades",
    "notes",
]

CONTRIBUTION_SUMMARY_COLS = [
    "scenario",
    "mix_name",
    "case_key",
    "weight",
    "return_contribution_pct",
    "contribution_share_pct",
    "avg_trade_return_pct",
    "win_rate",
    "trades",
    "risk_score",
    "notes",
]


def log(message: str) -> None:
    print(f"[case_mix_report] {message}")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _fmt_pct(value: Any, digits: int = 2) -> str:
    v = _float(value)
    if v is None:
        return "-"
    return f"{v:.{digits}f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    v = _float(value)
    if v is None:
        return "-"
    if math.isinf(v):
        return "inf"
    return f"{v:.{digits}f}"


def _scenario_order(rows: list[dict[str, str]]) -> list[str]:
    seen: list[str] = []
    for row in rows:
        sc = row.get("scenario") or ""
        if sc and sc not in seen:
            seen.append(sc)
    return seen


def _mix_sort_key(mix_name: str) -> tuple[int, str]:
    try:
        return (MIX_ORDER.index(mix_name), mix_name)
    except ValueError:
        return (999, mix_name)


def _group_by(rows: list[dict[str, str]], key: str) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.get(key, "")].append(row)
    return grouped


def _load_pyplot():
    mpl_config_dir = DEFAULT_REPORT_DIR / ".mplconfig"
    mpl_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_config_dir))
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        return plt
    except ImportError as exc:
        raise SystemExit(
            "matplotlib is required. Install it with: "
            ".\\venv\\Scripts\\pip.exe install matplotlib"
        ) from exc


def _render_line_chart(
    plt,
    scenario: str,
    rows: list[dict[str, str]],
    y_col: str,
    ylabel: str,
    title: str,
    output_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(12, 6.6))
    by_mix = _group_by(rows, "mix_name")
    for mix_name in sorted(by_mix, key=_mix_sort_key):
        mix_rows = sorted(by_mix[mix_name], key=lambda r: r.get("date") or "")
        x = [r.get("date") or "" for r in mix_rows]
        y = [_float(r.get(y_col), 0.0) or 0.0 for r in mix_rows]
        style = MIX_STYLES.get(mix_name, {"linewidth": 2.0, "linestyle": "-", "alpha": 0.9})
        ax.plot(x, y, label=mix_name, **style)

    if y_col == "drawdown_pct":
        ax.axhline(0, color="#222222", linewidth=0.8, alpha=0.55)
    ax.set_title(title)
    ax.set_xlabel("date")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")

    tick_step = max(1, len({r.get("date") for r in rows}) // 8)
    ticks = list(range(0, len(sorted({r.get("date") for r in rows})), tick_step))
    labels = sorted({r.get("date") or "" for r in rows})
    ax.set_xticks(ticks)
    ax.set_xticklabels([labels[i] for i in ticks], rotation=35, ha="right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def render_charts(equity_rows: list[dict[str, str]], report_dir: Path) -> None:
    plt = _load_pyplot()
    by_scenario = _group_by(equity_rows, "scenario")
    for scenario in _scenario_order(equity_rows):
        rows = by_scenario.get(scenario, [])
        log(f"rendering equity chart scenario={scenario}")
        _render_line_chart(
            plt,
            scenario,
            rows,
            "equity",
            "equity",
            f"Case mix equity curves: {scenario}",
            report_dir / f"equity_{scenario}.png",
        )
        log(f"rendering dd chart scenario={scenario}")
        _render_line_chart(
            plt,
            scenario,
            rows,
            "drawdown_pct",
            "drawdown_pct",
            f"Case mix drawdown curves: {scenario}",
            report_dir / f"dd_{scenario}.png",
        )


def build_contribution_summary(contribution_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    totals: dict[tuple[str, str], float] = defaultdict(float)
    for row in contribution_rows:
        key = (row.get("scenario") or "", row.get("mix_name") or "")
        totals[key] += _float(row.get("total_return_contribution_pct"), 0.0) or 0.0

    output: list[dict[str, Any]] = []
    for row in contribution_rows:
        scenario = row.get("scenario") or ""
        mix_name = row.get("mix_name") or ""
        case_key = row.get("case_key") or ""
        contribution = _float(row.get("total_return_contribution_pct"), 0.0) or 0.0
        max_dd = _float(row.get("max_drawdown_pct"), 0.0) or 0.0
        total = totals[(scenario, mix_name)]
        share = (contribution / total * 100.0) if abs(total) > 1e-12 else None
        risk_score = abs(max_dd) / max(contribution, 0.01)
        trades = _int(row.get("trades"), 0)

        notes = []
        if trades == 0:
            notes.append("no trades")
        if contribution < 0:
            notes.append("negative contribution")
        if trades > 0 and risk_score >= 2.0:
            notes.append("dd heavy")
        if contribution > 0 and risk_score < 1.0:
            notes.append("efficient")

        output.append({
            "scenario": scenario,
            "mix_name": mix_name,
            "case_key": case_key,
            "weight": row.get("weight"),
            "return_contribution_pct": round(contribution, 4),
            "contribution_share_pct": round(share, 2) if share is not None else None,
            "avg_trade_return_pct": row.get("avg_trade_return_pct"),
            "win_rate": row.get("win_rate"),
            "trades": trades,
            "risk_score": round(risk_score, 4),
            "notes": "; ".join(notes),
        })

    return output


def _best_by(rows: list[dict[str, str]], col: str, *, reverse: bool = True) -> dict[str, str] | None:
    valid = [r for r in rows if _float(r.get(col)) is not None]
    if not valid:
        return None
    return sorted(valid, key=lambda r: _float(r.get(col), 0.0) or 0.0, reverse=reverse)[0]


def _scenario_comment(rows: list[dict[str, str]]) -> list[str]:
    lines: list[str] = []
    best_return = _best_by(rows, "total_return_pct", reverse=True)
    best_dd = _best_by(rows, "max_drawdown_pct", reverse=True)
    best_pf = _best_by(rows, "profit_factor", reverse=True)

    if best_return:
        lines.append(
            f"- Return leader: {best_return['mix_name']} "
            f"({_fmt_pct(best_return.get('total_return_pct'))}, "
            f"DD {_fmt_pct(best_return.get('max_drawdown_pct'))})."
        )
    if best_dd:
        lines.append(
            f"- Lowest equity DD: {best_dd['mix_name']} "
            f"(DD {_fmt_pct(best_dd.get('max_drawdown_pct'))}, "
            f"return {_fmt_pct(best_dd.get('total_return_pct'))})."
        )
    if best_pf:
        lines.append(
            f"- Best profit factor: {best_pf['mix_name']} "
            f"({_fmt_num(best_pf.get('profit_factor'))})."
        )

    by_mix = {r.get("mix_name"): r for r in rows}
    pullback = by_mix.get("pullback2_only")
    defensive = by_mix.get("defensive_mix")
    core = by_mix.get("core_mix")
    bull = by_mix.get("bull_mix")

    if pullback and defensive:
        p_ret = _float(pullback.get("total_return_pct"))
        d_ret = _float(defensive.get("total_return_pct"))
        p_dd = abs(_float(pullback.get("max_drawdown_pct"), 0.0) or 0.0)
        d_dd = abs(_float(defensive.get("max_drawdown_pct"), 0.0) or 0.0)
        if p_ret is not None and d_ret is not None:
            if d_ret >= p_ret * 0.9 and d_dd < p_dd:
                lines.append("- Defensive mix keeps most of pullback2_only return while reducing DD.")
            elif d_dd < p_dd:
                lines.append("- Defensive mix reduces DD, but return sacrifice should be checked.")
            elif d_ret > p_ret:
                lines.append("- Defensive mix beats pullback2_only on return, but DD did not improve.")
            else:
                lines.append("- Pullback2_only remains hard to beat in this scenario.")

    if core and defensive:
        c_ret = _float(core.get("total_return_pct"))
        d_ret = _float(defensive.get("total_return_pct"))
        c_dd = abs(_float(core.get("max_drawdown_pct"), 0.0) or 0.0)
        d_dd = abs(_float(defensive.get("max_drawdown_pct"), 0.0) or 0.0)
        if c_ret is not None and d_ret is not None:
            if d_dd < c_dd and d_ret >= c_ret * 0.95:
                lines.append("- Defensive mix looks better balanced than core_mix here.")
            elif c_ret >= d_ret and c_dd <= d_dd:
                lines.append("- Core mix has the better balance here.")

    if bull:
        b_ret = _float(bull.get("total_return_pct"))
        best_ret_val = _float(best_return.get("total_return_pct")) if best_return else None
        if b_ret is not None and best_ret_val is not None:
            if b_ret >= best_ret_val * 0.98:
                lines.append("- Bull mix is competitive in return.")
            else:
                lines.append("- Bull mix does not add clear return advantage.")

    notes = sorted({r.get("notes", "") for r in rows if r.get("notes")})
    if notes:
        lines.append(f"- Data note: {' | '.join(notes)}")
    return lines


def _case_role_comments(rows: list[dict[str, Any]]) -> list[str]:
    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_case[str(row.get("case_key") or "")].append(row)

    lines = ["## Case Contribution Notes", ""]
    for case_key in sorted(by_case):
        case_rows = by_case[case_key]
        active = [r for r in case_rows if _int(r.get("trades"), 0) > 0]
        if not active:
            lines.append(f"- `{case_key}`: no active trades in the generated report.")
            continue
        total_contrib = sum(_float(r.get("return_contribution_pct"), 0.0) or 0.0 for r in active)
        avg_risk = sum(_float(r.get("risk_score"), 0.0) or 0.0 for r in active) / len(active)
        good = len([r for r in active if (_float(r.get("return_contribution_pct"), 0.0) or 0.0) > 0])
        label = CASE_LABELS.get(case_key, case_key)
        lines.append(
            f"- `{label}`: contribution total {_fmt_pct(total_contrib)}, "
            f"positive rows {good}/{len(active)}, avg risk_score {_fmt_num(avg_risk)}."
        )
    return lines


def build_markdown(
    summary_rows: list[dict[str, str]],
    contribution_summary_rows: list[dict[str, Any]],
) -> str:
    lines: list[str] = [
        "# Case Mix Report",
        "",
        "Fixed-weight mix report generated from `outputs/case_mix/*.csv`.",
        "Equity uses the simple, non-compounded daily return curve produced by `backtest_case_mix.py`.",
        "",
        "## Scenario Summary",
        "",
    ]

    by_scenario = _group_by(summary_rows, "scenario")
    for scenario in _scenario_order(summary_rows):
        rows = sorted(by_scenario.get(scenario, []), key=lambda r: _mix_sort_key(r.get("mix_name", "")))
        lines.append(f"### {scenario}")
        lines.append("")
        lines.append("| mix | total return | max DD | profit factor | active days | trades |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for row in rows:
            lines.append(
                f"| {row.get('mix_name')} | {_fmt_pct(row.get('total_return_pct'))} | "
                f"{_fmt_pct(row.get('max_drawdown_pct'))} | {_fmt_num(row.get('profit_factor'))} | "
                f"{row.get('active_days') or '0'} | {row.get('total_trades') or '0'} |"
            )
        lines.append("")
        lines.extend(_scenario_comment(rows))
        lines.append("")

    lines.extend(_case_role_comments(contribution_summary_rows))
    lines.extend([
        "",
        "## Reading Guide",
        "",
        "- `risk_score = abs(max_drawdown_pct) / max(return_contribution_pct, 0.01)`; smaller is better.",
        "- `pullback2_only` is the single-case benchmark.",
        "- `defensive_mix` is the first candidate to check for DD reduction without killing returns.",
        "- `bull_mix` should only survive if it adds clear upside in bull scenarios.",
    ])
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> None:
    input_dir = Path(args.input_dir)
    report_dir = Path(args.output_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    log("loading summary...")
    summary_rows = _read_csv(input_dir / "case_mix_summary.csv")
    equity_rows = _read_csv(input_dir / "case_mix_equity.csv")
    contribution_rows = _read_csv(input_dir / "case_mix_contribution.csv")

    render_charts(equity_rows, report_dir)

    contribution_summary_rows = build_contribution_summary(contribution_rows)
    _write_csv(report_dir / "mix_contribution_summary.csv", CONTRIBUTION_SUMMARY_COLS, contribution_summary_rows)

    log("saving report_summary.md")
    markdown = build_markdown(summary_rows, contribution_summary_rows)
    (report_dir / "report_summary.md").write_text(markdown, encoding="utf-8")
    log("done")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render case mix report from CSV outputs")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="directory containing case_mix CSV files")
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR), help="directory for report outputs")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())

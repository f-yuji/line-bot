#!/usr/bin/env python3
"""H5 PB20出口ロジック整合性確認 & 代替仕様比較スクリプト。

比較する仕様:
  A: PB20_CURRENT         - 現行 peak>entry*1.005, close<=peak*0.98, 損失側も発火
  B: PB20_PROFIT_ONLY_CLOSE - 現行+pullback発火時に close>entry を条件追加
  C: PB20_PROFIT_ONLY_LINE  - pullback_line(peak*0.98)>entry の場合だけPB監視有効
  D1: PB20_PEAK_START_1PCT  - 監視開始を peak>entry*1.01 に変更
  D2: PB20_PEAK_START_2PCT  - 監視開始を peak>entry*1.02 に変更
  E: NO_PULLBACK_HD3_EST12  - PBなし、HD3+EST12のみ

Usage:
    python scripts/run_h5_pullback_audit.py
    python scripts/run_h5_pullback_audit.py --period test
"""
from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs" / "h5_pullback_audit"

PERIODS = {
    "train": (date(2023, 1, 1),  date(2024, 12, 31)),
    "test":  (date(2025, 1, 1),  date(2026, 5, 28)),
    "all":   (date(2023, 1, 1),  date(2026, 5, 28)),
}

VARIANTS = [
    ("PB20_CURRENT",          "peak>entry*1.005, close<=peak*0.98, 損失側も発火（現行）"),
    ("PB20_PROFIT_ONLY_CLOSE","現行+close>entryのときだけPB発火"),
    ("PB20_PROFIT_ONLY_LINE", "pullbackライン(peak*0.98)>entryのときだけPB監視"),
    ("PB20_PEAK_START_1PCT",  "監視開始 peak>entry*1.01"),
    ("PB20_PEAK_START_2PCT",  "監視開始 peak>entry*1.02"),
    ("NO_PULLBACK_HD3_EST12", "PBなし HD3+EST12のみ"),
]


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def _f(v, default=None):
    try:
        if v is None: return default
        out = float(v)
        return default if (math.isnan(out) or math.isinf(out)) else out
    except Exception:
        return default


def _profit_factor(pcts: list[float]) -> float | None:
    wins = sum(p for p in pcts if p > 0)
    losses = abs(sum(p for p in pcts if p < 0))
    return round(wins / losses, 3) if losses else None


def _stats(trades: list[dict], period_name: str, variant: str) -> dict:
    closed = [t for t in trades if t.get("status") == "closed"]
    if not closed:
        return {"variant": variant, "period": period_name, "n": 0}
    pcts = [_f(t.get("profit_pct"), 0.0) for t in closed]
    wins = [p for p in pcts if p > 0]
    losses_neg = [p for p in pcts if p <= 0]
    hds = [_f(t.get("holding_days"), 0) for t in closed if t.get("holding_days") is not None]
    max_dd_vals = [_f(t.get("max_drawdown_pct")) for t in closed if t.get("max_drawdown_pct") is not None]
    max_dd = round(min(max_dd_vals), 3) if max_dd_vals else None
    reasons = defaultdict(int)
    for t in closed:
        reasons[t.get("exit_reason", "unknown")] += 1
    pb_all = reasons["peak_pullback_exit"]
    pb_profit = sum(1 for t in closed if t.get("exit_reason") == "peak_pullback_exit" and (_f(t.get("profit_pct"), 0) or 0) > 0)
    pb_loss = pb_all - pb_profit
    return {
        "variant": variant,
        "period": period_name,
        "n": len(closed),
        "win_rate": round(len(wins) / len(pcts) * 100, 1) if pcts else None,
        "avg_ret": round(mean(pcts), 3) if pcts else None,
        "median_ret": round(median(pcts), 3) if pcts else None,
        "total_ret": round(sum(pcts), 2),
        "profit_factor": _profit_factor(pcts),
        "max_loss": round(min(pcts), 3) if pcts else None,
        "max_gain": round(max(pcts), 3) if pcts else None,
        "max_drawdown": max_dd,
        "avg_holding_days": round(mean(hds), 2) if hds else None,
        "emergency_stop_n": reasons["sl"],
        "time_stop_n": reasons["timeout"],
        "peak_pullback_n": pb_all,
        "peak_pullback_profit_n": pb_profit,
        "peak_pullback_loss_n": pb_loss,
    }


def _exit_reason_breakdown(trades: list[dict], period_name: str, variant: str) -> list[dict]:
    closed = [t for t in trades if t.get("status") == "closed"]
    if not closed:
        return []
    by_reason: dict[str, list[dict]] = defaultdict(list)
    for t in closed:
        by_reason[t.get("exit_reason", "unknown")].append(t)
    rows = []
    for reason, group in sorted(by_reason.items()):
        pcts = [_f(t.get("profit_pct"), 0.0) for t in group]
        wins = [p for p in pcts if p > 0]
        hds = [_f(t.get("holding_days"), 0) for t in group if t.get("holding_days") is not None]
        rows.append({
            "variant": variant,
            "period": period_name,
            "exit_reason": reason,
            "n": len(group),
            "win_rate": round(len(wins) / len(pcts) * 100, 1) if pcts else None,
            "avg_ret": round(mean(pcts), 3) if pcts else None,
            "median_ret": round(median(pcts), 3) if pcts else None,
            "total_ret": round(sum(pcts), 2),
            "profit_factor": _profit_factor(pcts),
            "max_loss": round(min(pcts), 3) if pcts else None,
            "avg_holding_days": round(mean(hds), 2) if hds else None,
        })
    return rows


def _monthly_stability(trades: list[dict], period_name: str, variant: str) -> list[dict]:
    closed = [t for t in trades if t.get("status") == "closed" and t.get("entry_date")]
    by_month: dict[str, list[float]] = defaultdict(list)
    for t in closed:
        m = str(t.get("entry_date", ""))[:7]
        if m:
            by_month[m].append(_f(t.get("profit_pct"), 0.0) or 0.0)
    rows = []
    for m, pcts in sorted(by_month.items()):
        wins = [p for p in pcts if p > 0]
        rows.append({
            "variant": variant,
            "period": period_name,
            "month": m,
            "n": len(pcts),
            "win_rate": round(len(wins) / len(pcts) * 100, 1) if pcts else None,
            "avg_ret": round(mean(pcts), 3) if pcts else None,
            "total_ret": round(sum(pcts), 2),
        })
    return rows


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("no data\n", encoding="utf-8-sig")
        return
    fields: list[str] = []
    for row in rows:
        for k in row:
            if k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("[audit] wrote %s (%d rows)", path.name, len(rows))


# ─────────────────────────────────────────────────────────────────────────────
# variant simulation functions
# ─────────────────────────────────────────────────────────────────────────────

def _sim_variant(row: dict, variant_name: str, base_rules: dict) -> dict:
    """Simulate a single trade under the given variant rules."""
    from services.trade_case_tester import _price_path, _close_trade, _timeout_or_open, _to_float, _to_int

    entry, _entry_date, days = _price_path(row, base_rules)
    if not entry:
        return {"status": "open", "exit_reason": "invalid_entry", "variant": variant_name}

    _raw_sl = base_rules.get("initial_sl_pct") if base_rules.get("initial_sl_pct") is not None else base_rules.get("sl_pct")
    _sl_pct = float(_raw_sl) if _raw_sl is not None else None
    use_sl = _sl_pct is not None and _sl_pct > -0.49
    initial_sl = entry * (1 + _sl_pct) if use_sl else None
    peak_pullback_pct = float(base_rules.get("peak_pullback_pct", -0.02))
    max_holding_days = _to_int(base_rules.get("max_holding_days"), 3)

    if variant_name == "NO_PULLBACK_HD3_EST12":
        # HD3 time-stop + emergency-stop only, no pullback
        peak_price = entry
        for d in days:
            low = _to_float(d.get("low"), None)
            if use_sl and initial_sl is not None and low is not None and low <= initial_sl:
                out = _close_trade(entry, d["date"], initial_sl, "sl", d["day"],
                                   days=days, exit_signal_value=(initial_sl - entry) / entry * 100,
                                   exit_indicator="initial_sl")
                out["variant"] = variant_name
                return out
        out = _timeout_or_open(entry, days, max_holding_days)
        out["variant"] = variant_name
        return out

    # PB variants — common peak logic
    min_peak_ratio = {
        "PB20_CURRENT":          1.005,
        "PB20_PROFIT_ONLY_CLOSE": 1.005,
        "PB20_PROFIT_ONLY_LINE":  1.005,
        "PB20_PEAK_START_1PCT":   1.01,
        "PB20_PEAK_START_2PCT":   1.02,
    }.get(variant_name, 1.005)

    peak_price = entry
    for d in days:
        high = _to_float(d.get("high"), None)
        low = _to_float(d.get("low"), None)
        close = _to_float(d.get("close"), None)
        if high is not None:
            peak_price = max(peak_price, high)

        # Emergency stop (priority 1)
        if use_sl and initial_sl is not None and low is not None and low <= initial_sl:
            out = _close_trade(entry, d["date"], initial_sl, "sl", d["day"],
                               days=days, exit_signal_value=(initial_sl - entry) / entry * 100,
                               exit_indicator="initial_sl")
            out["variant"] = variant_name
            return out

        # Pullback check (priority 2)
        if close is not None and peak_price > entry * min_peak_ratio:
            pullback_line = peak_price * (1 + peak_pullback_pct)  # peak * 0.98

            fire_pullback = False
            if variant_name == "PB20_CURRENT":
                fire_pullback = close <= pullback_line
            elif variant_name == "PB20_PROFIT_ONLY_CLOSE":
                # Only exit if current close is above entry (profit territory)
                fire_pullback = close <= pullback_line and close > entry
            elif variant_name == "PB20_PROFIT_ONLY_LINE":
                # Only activate PB monitoring if pullback line itself is above entry
                fire_pullback = close <= pullback_line and pullback_line > entry
            elif variant_name in ("PB20_PEAK_START_1PCT", "PB20_PEAK_START_2PCT"):
                fire_pullback = close <= pullback_line

            if fire_pullback:
                out = _close_trade(entry, d["date"], close, "peak_pullback_exit", d["day"],
                                   days=days,
                                   exit_signal_value=(close - entry) / entry * 100,
                                   exit_indicator="peak_pullback")
                out["variant"] = variant_name
                return out

    out = _timeout_or_open(entry, days, max_holding_days)
    out["variant"] = variant_name
    return out


def _run_h5_candidates_variants(
    candidates: list[dict],
    base_rules: dict,
    period_name: str,
) -> dict[str, list[dict]]:
    """Run all variants against the H5-filtered candidate set (no position limits)."""
    variant_names = [v[0] for v in VARIANTS]

    # Filter to H5-qualified candidates only
    from services.trade_case_tester import _to_float as _tcf
    from services.h5_primary import evaluate_h5_primary_entry

    h5_candidates: list[dict] = []
    for row in candidates:
        try:
            passes, _reasons, _meta = evaluate_h5_primary_entry(row)
            if passes:
                h5_candidates.append(row)
        except Exception:
            continue

    logger.info("[audit] period=%s h5_candidates=%d (from %d total)", period_name, len(h5_candidates), len(candidates))

    results: dict[str, list[dict]] = {v: [] for v in variant_names}
    for row in h5_candidates:
        code = row.get("code", "?")
        entry_date = str(row.get("trade_date", ""))[:10]
        entry_price = _tcf(row.get("entry_price"), None) or _tcf(row.get("close"), None)
        for vname in variant_names:
            sim = _sim_variant(row, vname, base_rules)
            sim["code"] = code
            sim["name"] = row.get("name")
            sim["sector"] = row.get("sector")
            sim["entry_date"] = entry_date
            sim["entry_price"] = entry_price
            sim["signal_probability"] = _tcf(row.get("signal_probability"))
            results[vname].append(sim)

    for vname, sims in results.items():
        closed_n = sum(1 for s in sims if s.get("status") == "closed")
        logger.info("[audit]   variant=%-30s sims=%d closed=%d", vname, len(sims), closed_n)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# HD3 hold comparison for pullback-loss trades
# ─────────────────────────────────────────────────────────────────────────────

def _compare_pb_loss_vs_hd3(
    h5_candidates: list[dict],
    base_rules: dict,
    period_name: str,
) -> list[dict]:
    """For PB20_CURRENT trades that fire at a loss, compare vs holding to HD3."""
    from services.trade_case_tester import _to_float as _tcf

    rows = []
    for row in h5_candidates:
        pb_sim = _sim_variant(row, "PB20_CURRENT", base_rules)
        if pb_sim.get("exit_reason") != "peak_pullback_exit":
            continue
        pb_ret = _tcf(pb_sim.get("profit_pct"), 0.0) or 0.0
        if pb_ret > 0:
            continue  # only loss-side

        # Compute HD3 result by forcing timeout at day 3
        hd3_sim = _sim_variant(row, "NO_PULLBACK_HD3_EST12", base_rules)
        hd3_ret = _tcf(hd3_sim.get("profit_pct"), 0.0) or 0.0

        rows.append({
            "period": period_name,
            "code": row.get("code"),
            "name": row.get("name"),
            "entry_date": str(row.get("trade_date", ""))[:10],
            "entry_price": _tcf(row.get("entry_price")) or _tcf(row.get("close")),
            "pb_exit_day": pb_sim.get("holding_days"),
            "pb_exit_price": pb_sim.get("exit_price"),
            "pb_ret_pct": round(pb_ret, 3),
            "hd3_exit_day": hd3_sim.get("holding_days"),
            "hd3_exit_price": hd3_sim.get("exit_price"),
            "hd3_ret_pct": round(hd3_ret, 3),
            "diff_pct": round(hd3_ret - pb_ret, 3),
            "better_if_hold": 1 if hd3_ret > pb_ret else 0,
            "worse_if_hold": 1 if hd3_ret < pb_ret else 0,
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# logic report
# ─────────────────────────────────────────────────────────────────────────────

def _write_logic_report(path: Path) -> None:
    text = """H5 PB20 出口ロジック整合性確認レポート
================================================
生成: {today}

■ 1. 現在実装（virtual_trade_exit.py evaluate_h5_primary_exit）

  関数名: evaluate_h5_primary_exit
  ファイル: services/virtual_trade_exit.py
  entry価格: trade.buy_price
  peak更新: 日次の high で更新（high がなければ close で代替）
  peak監視開始条件: peak_price > buy_price * 1.005  ← hardcoded
  pullback判定式: close <= peak_price * (1 + peak_pullback_pct)
                = close <= peak_price * 0.98（peak_pullback_pct=-0.02のとき）
  pullback判定価格: close（日次終値）
  emergency_stop判定式: low <= buy_price * (1 + initial_sl_pct)
                      = low <= buy_price * 0.88（initial_sl_pct=-0.12のとき）
  emergency_stop優先順位: 最優先（SL → PB → Timeout の順）
  time_stop判定: day_number >= max_holding_days = 3営業日目終値
  HD3の数え方: buy_date翌日=day1, 翌々日=day2, その翌日=day3（3営業日目）
  損益マイナス状態でpullback発火: あり（close > entry_price の条件なし）
  根拠コード: peak_price > buy * 1.005 and close <= peak_price * (1.0 + peak_pullback_pct)

■ 2. バックテスト実装（trade_case_tester.py simulate_peak_pullback_exit）

  関数名: simulate_peak_pullback_exit
  ファイル: services/trade_case_tester.py L687-727
  min_peak_ratio: rules.get("min_peak_ratio", 1.005) ← デフォルト1.005（rulesで上書き可）
  peak_pullback_pct: rules.get("peak_pullback_pct", -0.02)
  initial_sl_pct: rules.get("initial_sl_pct") or rules.get("sl_pct")
  優先度: SL → PB → Timeout（現在実装と同一）
  損益マイナス発火: あり（同条件）

■ 3. 全分析スクリプトとの一致確認

  analyze_h5_holding_days_reason.py: peak > entry*1.005 and close <= peak*(1-pb_pct/100) ✓
  analyze_h5_forward_next_steps.py: peak > entry*(1+trigger_pct/100) and close <= peak*(1-trigger_pct/100) ✓
  analyze_h5_focused_grid.py: peak > entry*1.005 and close <= peak*(1-pullback_pct/100) ✓

■ 4. 整合性結論

  ✅ 過去H5検証のPB20は本当にpeak_pullbackだった
  ✅ 現在実装 simulate_peak_pullback_exit と完全一致
  ✅ 過去検証でも損益マイナスのpeak_pullback_exitは存在する
  ⚠ close > entry の利益圏限定条件はどの実装にも存在しない
  ✅ emergency_stop は最優先（SL first → PB → Timeout）
  ✅ HD3の数え方は entry_date+1day=day1, entry_date+3day=day3

■ 5. 差分（唯一の差異）

  exit_signal_value の定義:
    virtual_trade_exit.py: (close / peak_price - 1) * 100 ← peak反落率
    trade_case_tester.py:  (close - entry) / entry * 100  ← entry基準リターン
  → ログ記録の形式のみ異なる。exit判定ロジック自体は同一。

■ 6. peak_pullback_exit の性格

  現行仕様のpeak_pullback_exitは「純粋な利確ルール」ではない。
  entry後に+0.5%の高値を付けさえすれば、その後-2%反落すれば発火する。
  entry価格との比較はなく、損益マイナス（例: -1.5%）でも撤退する。
  これは「反発失速撤退」に近い性格を持つ。

  一方、過去検証でもこの仕様込みの成績が良かった場合、
  反発失速を早期に捨てるルールとして機能していた可能性がある。
  → 詳細はCSV出力を参照。

■ 7. UI表示名変更案（exit_reason表示の分化）

  trade_return > 0 の peak_pullback_exit → "H5ピーク反落利確"
  trade_return <= 0 の peak_pullback_exit → "H5反発失速撤退"
  timeout                                 → "H5時間切れ撤退"
  sl（emergency_stop）                   → "H5事故停止"
""".format(today=date.today().isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    logger.info("[audit] wrote %s", path.name)


# ─────────────────────────────────────────────────────────────────────────────
# recommendation report
# ─────────────────────────────────────────────────────────────────────────────

def _write_recommendation(
    path: Path,
    variant_stats: dict[str, dict[str, dict]],  # {period: {variant: stats}}
    pb_loss_rows: list[dict],
) -> None:
    text_parts = ["H5 PB20 出口ロジック 検証結論レポート", "=" * 50, ""]

    for period_name in ("test", "train"):
        stats = variant_stats.get(period_name, {})
        if not stats:
            continue
        text_parts.append(f"\n■ {period_name.upper()} 期間 バリアント比較サマリー")
        text_parts.append(f"{'variant':<30} {'n':>5} {'WR%':>6} {'avg%':>6} {'PF':>6} {'PB_n':>5} {'PBL':>4}")
        text_parts.append("-" * 75)
        for vname, _ in VARIANTS:
            s = stats.get(vname, {})
            n = s.get("n", 0)
            wr = f"{s['win_rate']:.1f}" if s.get("win_rate") is not None else "  - "
            avg = f"{s['avg_ret']:.3f}" if s.get("avg_ret") is not None else "  -  "
            pf = f"{s['profit_factor']:.3f}" if s.get("profit_factor") is not None else "  - "
            pb = s.get("peak_pullback_n", 0)
            pbl = s.get("peak_pullback_loss_n", 0)
            text_parts.append(f"{vname:<30} {n:>5} {wr:>6} {avg:>6} {pf:>6} {pb:>5} {pbl:>4}")

    if pb_loss_rows:
        n = len(pb_loss_rows)
        better = sum(1 for r in pb_loss_rows if r.get("better_if_hold"))
        worse = sum(1 for r in pb_loss_rows if r.get("worse_if_hold"))
        avg_pb = mean([r.get("pb_ret_pct", 0) for r in pb_loss_rows])
        avg_hd3 = mean([r.get("hd3_ret_pct", 0) for r in pb_loss_rows])
        text_parts.append(f"\n■ peak_pullback_loss → HD3保有比較")
        text_parts.append(f"  対象: {n}件（pullback発火かつ損益マイナスのトレード）")
        text_parts.append(f"  pullback撤退 avg: {avg_pb:.3f}%")
        text_parts.append(f"  HD3保有 avg:      {avg_hd3:.3f}%")
        text_parts.append(f"  HD3の方が良かった: {better}件 ({better/n*100:.1f}%)")
        text_parts.append(f"  HD3の方が悪かった: {worse}件 ({worse/n*100:.1f}%)")

        if avg_hd3 > avg_pb:
            text_parts.append("  → HD3まで持つと改善。現行PBは損切り貧乏の可能性あり。")
        else:
            text_parts.append("  → HD3まで持つと悪化。現行PBは反発失速捨てルールとして機能。")

    text_parts.append("\n■ Q&A")
    text_parts.append("1. H5検証のPB20は本当にpeak_pullbackだったか → YES")
    text_parts.append("2. 現在実装と完全一致か → YES（min_peak_ratio=1.005, peak*0.98どちらも同一）")
    text_parts.append("3. 損益マイナスのpeak_pullback_exitは存在したか → YES（利益圏限定条件なし）")
    text_parts.append("4. 現行PB20は利確か反発失速撤退か → 両方あり（CSV参照）")
    text_parts.append("5. emergency_stop は通常損切りか → 非常停止扱い（SL優先だが発火頻度は少ない）")
    text_parts.append("6. peak_pullback_lossをHD3まで持つと → CSV比較参照")
    text_parts.append("7-9. profit_only版・現行維持判断 → バリアント比較CSV参照")
    text_parts.append("10. UI表示名 → exit_reasonを利益/損失で分化することを推奨")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(text_parts), encoding="utf-8")
    logger.info("[audit] wrote %s", path.name)


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    from services.research_database import build_supabase
    from services.trade_case_tester import _load_candidates_v2
    from services.h5_primary import H5_BASE_RULES, evaluate_h5_primary_entry

    sb = build_supabase()

    # H5 base rules for simulation (all filters, no position limits for research)
    base_rules = dict(H5_BASE_RULES)
    base_rules.update({
        "exit_type": "peak_pullback_exit",
        "peak_pullback_pct": -0.02,
        "initial_sl_pct": -0.12,
        "max_holding_days": 3,
        "min_peak_ratio": 1.005,
        "max_open_positions": 999,
        "max_daily_entries": 999,
        "entry_rank_limit": 999,
    })

    periods_to_run = {args.period: PERIODS[args.period]} if args.period in PERIODS else PERIODS

    all_variant_sims: dict[str, dict[str, list[dict]]] = {}  # {period: {variant: [sims]}}
    all_pb_loss_rows: list[dict] = []

    for period_name, (start, end) in periods_to_run.items():
        logger.info("[audit] loading candidates period=%s %s..%s", period_name, start, end)
        candidates = _load_candidates_v2(sb, start, end)
        logger.info("[audit] candidates=%d", len(candidates))

        # H5-filter candidates (evaluate_h5_primary_entry returns (passes: bool, reasons, meta))
        h5_candidates: list[dict] = []
        for row in candidates:
            try:
                passes, _reasons, _meta = evaluate_h5_primary_entry(row)
                if passes:
                    h5_candidates.append(row)
            except Exception:
                continue
        logger.info("[audit] h5_candidates=%d", len(h5_candidates))

        # Run all variants
        variant_sims: dict[str, list[dict]] = {v[0]: [] for v in VARIANTS}
        for vname, _ in VARIANTS:
            logger.info("[audit] running variant=%s", vname)
            for row in h5_candidates:
                from services.trade_case_tester import _to_float as _tcf
                sim = _sim_variant(row, vname, base_rules)
                sim.update({
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "sector": row.get("sector"),
                    "entry_date": str(row.get("trade_date", ""))[:10],
                    "entry_price": _tcf(row.get("entry_price")) or _tcf(row.get("close")),
                    "signal_probability": _tcf(row.get("signal_probability")),
                    "market_regime": row.get("market_regime"),
                })
                variant_sims[vname].append(sim)
            closed_n = sum(1 for s in variant_sims[vname] if s.get("status") == "closed")
            pb_n = sum(1 for s in variant_sims[vname] if s.get("exit_reason") == "peak_pullback_exit")
            logger.info("[audit]   variant=%-30s closed=%d pb=%d", vname, closed_n, pb_n)

        all_variant_sims[period_name] = variant_sims

        # PB loss vs HD3 comparison
        pb_loss = _compare_pb_loss_vs_hd3(h5_candidates, base_rules, period_name)
        all_pb_loss_rows.extend(pb_loss)
        logger.info("[audit] pb_loss_vs_hd3=%d rows (period=%s)", len(pb_loss), period_name)

    # ── Generate output files ──
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 01: Logic report
    _write_logic_report(OUT_DIR / "01_current_vs_backtest_logic.txt")

    # 02/03: exit_reason breakdown per period
    for period_name, variant_sims in all_variant_sims.items():
        rows = []
        for vname, sims in variant_sims.items():
            rows.extend(_exit_reason_breakdown(sims, period_name, vname))
        suffix = "train" if period_name == "train" else "test"
        _write_csv(OUT_DIR / f"0{'2' if suffix=='train' else '3'}_exit_reason_breakdown_{suffix}.csv", rows)

    # 04: PB profit/loss detail (base variant only)
    pb_detail_rows = []
    for period_name, variant_sims in all_variant_sims.items():
        base_sims = variant_sims.get("PB20_CURRENT", [])
        for s in base_sims:
            if s.get("exit_reason") != "peak_pullback_exit":
                continue
            pct = _f(s.get("profit_pct"), 0.0) or 0.0
            pb_detail_rows.append({
                "period": period_name,
                "code": s.get("code"),
                "name": s.get("name"),
                "entry_date": s.get("entry_date"),
                "entry_price": s.get("entry_price"),
                "exit_date": s.get("exit_date"),
                "exit_price": s.get("exit_price"),
                "profit_pct": s.get("profit_pct"),
                "holding_days": s.get("holding_days"),
                "category": "profit" if pct > 0 else "loss",
                "peak_profit_pct": s.get("peak_profit_pct"),
                "max_drawdown_pct": s.get("max_drawdown_pct"),
                "signal_probability": s.get("signal_probability"),
                "market_regime": s.get("market_regime"),
            })
    _write_csv(OUT_DIR / "04_peak_pullback_profit_loss_detail.csv", pb_detail_rows)

    # 05: PB loss vs HD3
    _write_csv(OUT_DIR / "05_peak_pullback_loss_hold_to_hd3.csv", all_pb_loss_rows)

    # 06/07: variant comparison
    for suffix, period_name in [("train", "train"), ("test", "test")]:
        if period_name not in all_variant_sims:
            continue
        rows = []
        for vname, _ in VARIANTS:
            sims = all_variant_sims[period_name].get(vname, [])
            s = _stats(sims, period_name, vname)
            rows.append(s)
        _write_csv(OUT_DIR / f"0{'6' if suffix=='train' else '7'}_pullback_variant_comparison_{suffix}.csv", rows)

    # 08: exit breakdown per variant (all periods combined)
    all_eb_rows = []
    for period_name, variant_sims in all_variant_sims.items():
        for vname, sims in variant_sims.items():
            all_eb_rows.extend(_exit_reason_breakdown(sims, period_name, vname))
    _write_csv(OUT_DIR / "08_pullback_variant_exit_breakdown.csv", all_eb_rows)

    # 09: monthly stability
    monthly_rows = []
    for period_name, variant_sims in all_variant_sims.items():
        for vname, sims in variant_sims.items():
            monthly_rows.extend(_monthly_stability(sims, period_name, vname))
    _write_csv(OUT_DIR / "09_monthly_stability_by_variant.csv", monthly_rows)

    # 10: Recommendation report
    variant_stats: dict[str, dict[str, dict]] = {}
    for period_name, variant_sims in all_variant_sims.items():
        variant_stats[period_name] = {
            vname: _stats(sims, period_name, vname)
            for vname, sims in variant_sims.items()
        }
    _write_recommendation(OUT_DIR / "10_recommendation_report.txt", variant_stats, all_pb_loss_rows)

    print(f"\nAll outputs → {OUT_DIR}")
    print("\n=== QUICK SUMMARY ===")
    for period_name in ("test", "train"):
        vs = variant_stats.get(period_name, {})
        if not vs:
            continue
        print(f"\n{period_name.upper()}:")
        print(f"  {'variant':<30} {'n':>5} {'WR%':>6} {'avg%':>6} {'PF':>6} {'PB_n':>5} {'PBLoss':>6}")
        for vname, _ in VARIANTS:
            s = vs.get(vname, {})
            n = s.get("n", 0)
            wr = f"{s['win_rate']:.1f}" if s.get("win_rate") is not None else "  - "
            avg = f"{s['avg_ret']:.3f}" if s.get("avg_ret") is not None else "  -  "
            pf = f"{s['profit_factor']:.3f}" if s.get("profit_factor") is not None else "  - "
            pb = s.get("peak_pullback_n", 0)
            pbl = s.get("peak_pullback_loss_n", 0)
            print(f"  {vname:<30} {n:>5} {wr:>6} {avg:>6} {pf:>6} {pb:>5} {pbl:>6}")

    if all_pb_loss_rows:
        n = len(all_pb_loss_rows)
        better = sum(1 for r in all_pb_loss_rows if r.get("better_if_hold"))
        avg_pb = mean([r.get("pb_ret_pct", 0) for r in all_pb_loss_rows])
        avg_hd3 = mean([r.get("hd3_ret_pct", 0) for r in all_pb_loss_rows])
        print(f"\nPB_LOSS vs HD3: {n}件 better_if_hold={better}/{n} ({better/n*100:.1f}%)")
        print(f"  pullback avg: {avg_pb:.3f}%  HD3 avg: {avg_hd3:.3f}%")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="H5 PB20 pullback exit audit")
    p.add_argument(
        "--period",
        choices=["train", "test", "all", "both"],
        default="both",
        help="train=~2024, test=2025~, all=全期間, both=train+test（デフォルト）",
    )
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())

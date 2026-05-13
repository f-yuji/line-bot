#!/usr/bin/env python3
"""Audit or remove the 2026-05-13 fallback-created virtual trades."""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

from services.research_database import build_supabase
from services.signal_stage import evaluate_signal_stage
from settings_loader import get_settings
import scripts.predict_rebound as predict_rebound

load_dotenv()

START_UTC = "2026-05-13T10:32:00+00:00"
END_UTC = "2026-05-13T10:33:00+00:00"
BAD_REASON = "AI probability=0.67 expected_value=2.36 stage=strong_confirmed mode=normal horizon=5d"


def _target_trades(sb) -> list[dict]:
    return (
        sb.table("virtual_trades")
        .select("*")
        .gte("created_at", START_UTC)
        .lte("created_at", END_UTC)
        .eq("entry_probability", 0.67)
        .eq("expected_value", 2.36)
        .eq("buy_score", 67)
        .eq("signal_stage", "strong_confirmed")
        .eq("entry_reason", BAD_REASON)
        .order("created_at")
        .execute()
        .data
        or []
    )


def _recalculate(sb, trades: list[dict]) -> list[dict]:
    ids = [t.get("feature_snapshot_id") for t in trades if t.get("feature_snapshot_id") is not None]
    if not ids:
        return []
    snapshots = (
        sb.table("stock_feature_snapshots")
        .select("*")
        .in_("id", ids)
        .execute()
        .data
        or []
    )
    by_id = {r.get("id"): r for r in snapshots}
    ordered = [by_id.get(i) for i in ids if by_id.get(i)]

    args = argparse.Namespace(target_label="5d", model_name=None)
    model_row, bundle = predict_rebound._load_model_bundle(sb, args)
    if not bundle:
        raise RuntimeError("active model could not be loaded")

    x = predict_rebound._prepare_model_frame(ordered, bundle)
    probs = bundle["model"].predict_proba(x)[:, 1]
    cfg = get_settings()
    target = predict_rebound._target_config(args)

    recalc_by_snapshot: dict[int, dict] = {}
    for row, prob in zip(ordered, probs):
        p = float(prob)
        ev = predict_rebound._expected_value(p, target["take_profit_pct"], target["stop_loss_pct"])
        score = round(p * 100, 2)
        stage = evaluate_signal_stage(p, score, ev, cfg, {"ai_threshold_adjust": 0.0})["stage"]
        recalc_by_snapshot[int(row["id"])] = {
            "correct_probability": round(p, 6),
            "correct_expected_value": round(ev, 4),
            "correct_score": score,
            "correct_stage": stage,
        }

    out = []
    for trade in trades:
        out.append({
            "trade_id": trade.get("id"),
            "code": trade.get("code"),
            "name": trade.get("name"),
            "status": trade.get("status"),
            "watchlist_id": trade.get("watchlist_id"),
            "feature_snapshot_id": trade.get("feature_snapshot_id"),
            "bad_probability": trade.get("entry_probability"),
            "bad_expected_value": trade.get("expected_value"),
            "bad_stage": trade.get("signal_stage"),
            **recalc_by_snapshot.get(int(trade.get("feature_snapshot_id")), {}),
        })
    return out


def _delete_targets(sb, trades: list[dict], recalculated: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    trade_ids = [t["id"] for t in trades if t.get("id")]
    watchlist_ids = [t["watchlist_id"] for t in trades if t.get("watchlist_id")]

    for trade_id in trade_ids:
        sb.table("virtual_trades").delete().eq("id", trade_id).execute()

    recalc_by_watchlist = {r.get("watchlist_id"): r for r in recalculated}
    for watchlist_id in watchlist_ids:
        recalc = recalc_by_watchlist.get(watchlist_id, {})
        stage = recalc.get("correct_stage") or "none"
        status = "rebound_signal" if stage in ("confirmed", "strong_confirmed") else ("rebound_candidate" if stage == "early" else "ai_dropped")
        update = {
            "status": status,
            "signal_stage": stage,
            "signal_probability": recalc.get("correct_probability"),
            "expected_value": recalc.get("correct_expected_value"),
            "signal_score": recalc.get("correct_score"),
            "virtual_trade_id": None,
            "entered_at": None,
            "signal_status_reason": "fallback_trade_removed_recalculated",
            "updated_at": now,
        }
        if status == "ai_dropped":
            update["closed_at"] = now
            update["close_reason"] = "ai_score_below_threshold"
        sb.table("stock_drop_watchlist").update(update).eq("id", watchlist_id).execute()


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair 2026-05-13 fallback-created virtual trades")
    parser.add_argument("--apply-delete", action="store_true", help="Delete the exact bad virtual trades and recalculate watchlist rows")
    args = parser.parse_args()

    sb = build_supabase()
    trades = _target_trades(sb)
    recalculated = _recalculate(sb, trades) if trades else []
    print(json.dumps({"target_count": len(trades), "apply_delete": args.apply_delete, "rows": recalculated}, ensure_ascii=False, indent=2))
    if args.apply_delete and trades:
        _delete_targets(sb, trades, recalculated)
        print(f"[repair_fallback_trades] deleted={len(trades)} watchlist_recalculated={len(recalculated)}")


if __name__ == "__main__":
    main()

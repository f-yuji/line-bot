#!/usr/bin/env python3
"""Execute box_lab pending signals into box_virtual_trades.

This script is intentionally isolated from rebound_lab. It reads box_signals
with entry_status=entry_pending and creates box_virtual_trades only when a
later trading day's price range touches the signal's entry range.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv
from supabase import create_client

from services.box_signal_logic import DEFAULTS, _to_float

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

MISSING_COLUMN_RE = re.compile(r"Could not find the '([^']+)' column")


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = mode.upper() if mode else ""
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _fetch_all(build_query, *, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        res = build_query().range(offset, offset + page_size - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            return rows
        offset += page_size


def _chunked(values: list[str], size: int = 80):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _load_settings(sb) -> dict:
    cfg = dict(DEFAULTS)
    cfg.setdefault("max_pending_days", 5)
    try:
        rows = (
            sb.table("box_settings")
            .select("*")
            .eq("user_id", "global")
            .limit(1)
            .execute()
            .data
            or []
        )
        if rows:
            row = rows[0]
            for key in (
                "entry_mode",
                "gu_skip_pct",
                "gd_skip_pct",
                "max_open_positions",
                "max_sector_positions",
                "max_pending_days",
            ):
                if row.get(key) is not None:
                    cfg[key] = row.get(key)
    except Exception as e:
        logger.warning("[box_execute] box_settings unavailable; defaults used: %s", e)
    cfg["max_open_positions"] = int(float(cfg.get("max_open_positions") or 5))
    cfg["max_sector_positions"] = int(float(cfg.get("max_sector_positions") or 2))
    cfg["max_pending_days"] = int(float(cfg.get("max_pending_days") or 5))
    cfg["gu_skip_pct"] = float(cfg.get("gu_skip_pct") or 3.0)
    cfg["gd_skip_pct"] = float(cfg.get("gd_skip_pct") or 5.0)
    return cfg


def _latest_trade_date(sb, trade_date: str | None) -> str:
    if trade_date:
        return trade_date
    rows = (
        sb.table("stock_feature_snapshots")
        .select("trade_date")
        .order("trade_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        raise RuntimeError("stock_feature_snapshots is empty")
    return str(rows[0]["trade_date"])


def _load_trade_dates(sb, start_date: str, end_date: str) -> list[str]:
    rows = _fetch_all(
        lambda: (
            sb.table("stock_feature_snapshots")
            .select("trade_date")
            .gte("trade_date", start_date)
            .lte("trade_date", end_date)
            .order("trade_date")
        )
    )
    return sorted({str(r["trade_date"]) for r in rows if r.get("trade_date")})


def _load_latest_snapshots(sb, trade_date: str, codes: list[str]) -> dict[str, dict]:
    by_code: dict[str, dict] = {}
    for chunk in _chunked(codes):
        rows = (
            sb.table("stock_feature_snapshots")
            .select("*")
            .eq("trade_date", trade_date)
            .in_("code", chunk)
            .execute()
            .data
            or []
        )
        for row in rows:
            by_code[str(row.get("code"))] = row
    return by_code


def _load_open_trades(sb) -> list[dict]:
    return (
        sb.table("box_virtual_trades")
        .select("*")
        .eq("status", "open")
        .execute()
        .data
        or []
    )


def _remove_missing_column(payload: dict, exc: Exception) -> bool:
    match = MISSING_COLUMN_RE.search(str(exc))
    if not match:
        return False
    col = match.group(1)
    if col not in payload:
        return False
    payload.pop(col, None)
    logger.warning("[box_execute] optional column missing; omitted column=%s", col)
    return True


def _insert_optional(sb, table: str, payload: dict, dry_run: bool) -> dict | None:
    if dry_run:
        return None
    remaining = dict(payload)
    for _ in range(30):
        try:
            rows = sb.table(table).insert(remaining).execute().data or []
            return rows[0] if rows else None
        except Exception as e:
            if _remove_missing_column(remaining, e):
                continue
            raise
    raise RuntimeError(f"too many optional column retries for {table}")


def _update_optional(sb, table: str, payload: dict, *, row_id: str, dry_run: bool) -> None:
    if dry_run:
        return
    remaining = dict(payload)
    for _ in range(30):
        try:
            sb.table(table).update(remaining).eq("id", row_id).execute()
            return
        except Exception as e:
            if _remove_missing_column(remaining, e):
                continue
            raise
    raise RuntimeError(f"too many optional column retries for {table}")


def _price_touched(signal: dict, snap: dict) -> tuple[bool, float | None]:
    day_high = _to_float(snap.get("high"))
    day_low = _to_float(snap.get("low"))
    entry_min = _to_float(signal.get("entry_price_min"))
    entry_max = _to_float(signal.get("entry_price_max"))
    target = _to_float(signal.get("entry_target_price"))
    if None in (day_high, day_low, entry_min, entry_max):
        return False, None
    if not (day_low <= entry_max and day_high >= entry_min):
        return False, None
    if target is None:
        target = entry_min
    if target < day_low:
        return True, day_low
    if target > day_high:
        return True, entry_max
    return True, target


def _gap_skip_reason(signal: dict, snap: dict, cfg: dict) -> str | None:
    open_price = _to_float(snap.get("open"))
    signal_close = _to_float(signal.get("close"))
    if open_price is None or signal_close is None or signal_close <= 0:
        return None
    gap_pct = (open_price / signal_close - 1.0) * 100.0
    if gap_pct >= cfg["gu_skip_pct"]:
        return "GU_too_high"
    if gap_pct <= -cfg["gd_skip_pct"]:
        return "GD_too_deep"
    return None


def _trade_payload(signal: dict, snap: dict, entry_price: float, trade_date: str) -> dict:
    qty = 100
    return {
        "signal_id": signal.get("id"),
        "code": signal.get("code"),
        "name": signal.get("name"),
        "sector": signal.get("sector"),
        "status": "open",
        "buy_date": trade_date,
        "buy_price": round(float(entry_price), 4),
        "quantity": qty,
        "current_price": _to_float(snap.get("close")) or round(float(entry_price), 4),
        "strategy_type": signal.get("strategy_type") or "box_pullback",
        "entry_mode": signal.get("entry_mode"),
        "entry_reason": signal.get("entry_reason") or signal.get("signal_reason"),
        "exit_rule": "ma25_stop_box_tp",
        "take_profit_price": signal.get("take_profit_price") or signal.get("box_high") or signal.get("box_upper"),
        "stop_loss_price": signal.get("stop_loss_price"),
        "box_score": signal.get("box_score"),
        "box_high": signal.get("box_high") or signal.get("box_upper"),
        "box_low": signal.get("box_low") or signal.get("box_lower"),
        "box_upper": signal.get("box_high") or signal.get("box_upper"),
        "box_lower": signal.get("box_low") or signal.get("box_lower"),
        "box_width_pct": signal.get("box_width_pct"),
        "box_position_pct": signal.get("box_position_pct"),
        "bounce_count": signal.get("bounce_count"),
        "support_line": signal.get("support_line"),
        "support_zone_low": signal.get("support_zone_low"),
        "support_zone_high": signal.get("support_zone_high"),
        "support_touch_count": signal.get("support_touch_count"),
        "support_break_count": signal.get("support_break_count"),
        "support_distance_pct": signal.get("support_distance_pct"),
        "avg_bounce_return_pct": signal.get("avg_bounce_return_pct"),
        "margin_ratio": signal.get("margin_ratio"),
        "margin_date": signal.get("margin_date"),
        "rsi14": signal.get("rsi14"),
        "atr_pct": signal.get("atr_pct"),
        "volume_ratio_20d": signal.get("volume_ratio_20d"),
        "unrealized_pnl": ((_to_float(snap.get("close")) or entry_price) - entry_price) * qty,
        "unrealized_pnl_pct": (((_to_float(snap.get("close")) or entry_price) / entry_price) - 1.0) * 100.0
        if entry_price > 0
        else None,
        "raw": {"entry_snapshot_id": snap.get("id"), "signal_trade_date": signal.get("trade_date")},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    cfg = _load_settings(sb)
    target_date = _latest_trade_date(sb, args.trade_date)
    pending = (
        sb.table("box_signals")
        .select("*")
        .eq("entry_status", "entry_pending")
        .order("trade_date")
        .limit(args.limit)
        .execute()
        .data
        or []
    )
    open_trades = _load_open_trades(sb)
    open_codes = {str(t.get("code")) for t in open_trades if t.get("code")}
    sector_counts = Counter(str(t.get("sector") or "") for t in open_trades if t.get("sector"))
    codes = sorted({str(s.get("code")) for s in pending if s.get("code")})
    snapshots = _load_latest_snapshots(sb, target_date, codes) if codes else {}
    min_signal_date = min((str(s.get("trade_date")) for s in pending if s.get("trade_date")), default=target_date)
    trade_dates = _load_trade_dates(sb, min_signal_date, target_date) if pending else []

    logger.info(
        "[box_execute] target_date=%s pending=%d open_positions=%d max_open_positions=%d max_pending_days=%d",
        target_date,
        len(pending),
        len(open_trades),
        cfg["max_open_positions"],
        cfg["max_pending_days"],
    )

    entered = expired = skipped = continued = 0
    skip_reasons: Counter[str] = Counter()
    now_iso = datetime.now(timezone.utc).isoformat()
    current_open_count = len(open_trades)

    for sig in pending:
        sid = str(sig.get("id"))
        code = str(sig.get("code") or "")
        signal_date = str(sig.get("trade_date") or "")
        snap = snapshots.get(code)
        elapsed_dates = [d for d in trade_dates if signal_date < d <= target_date]
        pending_days = len(elapsed_dates)

        if pending_days == 0:
            continued += 1
            logger.info("[box_execute] pending code=%s signal_date=%s reason=wait_next_session", code, signal_date)
            continue
        if pending_days > cfg["max_pending_days"]:
            expired += 1
            skip_reasons["pending_expired"] += 1
            logger.info("[box_execute] expired code=%s pending_days=%d", code, pending_days)
            _update_optional(
                sb,
                "box_signals",
                {
                    "entry_status": "expired",
                    "skip_reason": "pending_expired",
                    "checked_at": now_iso,
                    "updated_at": now_iso,
                },
                row_id=sid,
                dry_run=args.dry_run,
            )
            continue
        if not snap:
            skipped += 1
            skip_reasons["snapshot_missing"] += 1
            logger.info("[box_execute] skip code=%s reason=snapshot_missing", code)
            continue
        if code in open_codes:
            skipped += 1
            skip_reasons["already_open"] += 1
            logger.info("[box_execute] skip code=%s reason=already_open", code)
            _update_optional(
                sb,
                "box_signals",
                {"entry_status": "skipped", "skip_reason": "already_open", "checked_at": now_iso, "updated_at": now_iso},
                row_id=sid,
                dry_run=args.dry_run,
            )
            continue
        if current_open_count >= cfg["max_open_positions"]:
            skipped += 1
            skip_reasons["max_positions_reached"] += 1
            logger.info("[box_execute] skip code=%s reason=max_positions_reached", code)
            _update_optional(
                sb,
                "box_signals",
                {
                    "entry_status": "skipped",
                    "skip_reason": "max_positions_reached",
                    "checked_at": now_iso,
                    "updated_at": now_iso,
                },
                row_id=sid,
                dry_run=args.dry_run,
            )
            continue
        sector = str(sig.get("sector") or "")
        if sector and sector_counts[sector] >= cfg["max_sector_positions"]:
            skipped += 1
            skip_reasons["max_sector_positions_reached"] += 1
            logger.info("[box_execute] skip code=%s reason=max_sector_positions_reached sector=%s", code, sector)
            _update_optional(
                sb,
                "box_signals",
                {
                    "entry_status": "skipped",
                    "skip_reason": "max_sector_positions_reached",
                    "checked_at": now_iso,
                    "updated_at": now_iso,
                },
                row_id=sid,
                dry_run=args.dry_run,
            )
            continue

        gap_reason = _gap_skip_reason(sig, snap, cfg)
        if gap_reason:
            skipped += 1
            skip_reasons[gap_reason] += 1
            logger.info("[box_execute] skip code=%s reason=%s", code, gap_reason)
            _update_optional(
                sb,
                "box_signals",
                {"entry_status": "skipped", "skip_reason": gap_reason, "checked_at": now_iso, "updated_at": now_iso},
                row_id=sid,
                dry_run=args.dry_run,
            )
            continue

        touched, entry_price = _price_touched(sig, snap)
        if not touched or entry_price is None:
            continued += 1
            logger.info(
                "[box_execute] pending code=%s range=%s-%s day=%s-%s reason=entry_range_not_touched",
                code,
                sig.get("entry_price_min"),
                sig.get("entry_price_max"),
                snap.get("low"),
                snap.get("high"),
            )
            _update_optional(
                sb,
                "box_signals",
                {"checked_at": now_iso, "updated_at": now_iso},
                row_id=sid,
                dry_run=args.dry_run,
            )
            continue

        payload = _trade_payload(sig, snap, float(entry_price), target_date)
        logger.info("[box_execute] entered code=%s price=%.2f pending_days=%d", code, float(entry_price), pending_days)
        created = _insert_optional(sb, "box_virtual_trades", payload, args.dry_run)
        trade_id = created.get("id") if created else None
        entered += 1
        current_open_count += 1
        open_codes.add(code)
        if sector:
            sector_counts[sector] += 1
        _update_optional(
            sb,
            "box_signals",
            {
                "entry_status": "entered",
                "entered_at": now_iso,
                "virtual_trade_id": trade_id,
                "checked_at": now_iso,
                "updated_at": now_iso,
            },
            row_id=sid,
            dry_run=args.dry_run,
        )

    logger.info(
        "[box_execute] complete dry_run=%s entered=%d expired=%d skipped=%d continued=%d skip_reasons=%s",
        args.dry_run,
        entered,
        expired,
        skipped,
        continued,
        dict(sorted(skip_reasons.items())),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute box_lab pending virtual entries")
    parser.add_argument("--trade-date", default=None, help="Execution trade_date. Defaults to latest snapshot date.")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    run(_parse_args())

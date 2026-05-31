"""H5 Screenshot Assist: read SBI broker screenshots via AI Vision."""

from __future__ import annotations

import csv
import json
import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

H5_STORED_FORWARD_DIR = Path("outputs") / "h5_stored_forward_test"
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "webp"}
MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB

VALID_SIDES = {"buy", "sell"}
VALID_ORDER_TYPES = {"market", "limit", "close_market", "close_limit", "unknown"}
VALID_ORDER_STATUSES = {"filled", "pending", "not_filled", "partial", "unknown"}

_SYSTEM_PROMPT = (
    "あなたはSBI証券の注文・約定・保有スクリーンショットから実弾売買記録に必要な情報を抽出する。"
    "売買判断や推奨は行わない。"
    "読み取れない項目はnullまたはunknownにする。推測で補完しない。"
    "銘柄コードは4桁数字のみ有効。数字と英字が混ざる場合はwarningsに記録しcode=nullにする。"
    "口座番号・残高・余力は読み取っても出力に含めない。"
    "JSONのみ返す。コードブロックや説明文は不要。"
)

_JSON_SCHEMA_HINT = """{
  "broker": "SBI|unknown",
  "screen_type": "buy_order|sell_order|execution|position|order_confirm|unknown",
  "side": "buy|sell|unknown",
  "code": "4桁数字 or null",
  "name": "銘柄名 or null",
  "price": 数値 or null,
  "quantity": 整数 or null,
  "order_type": "market|limit|close_market|close_limit|unknown",
  "order_status": "filled|pending|not_filled|partial|unknown",
  "trade_datetime": "YYYY-MM-DD HH:MM or null",
  "currency": "JPY",
  "confidence": 0.0~1.0,
  "warnings": [],
  "raw_notes": ""
}"""


def _read_csv_dicts(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        with path.open(newline="", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        logger.warning("csv read failed %s: %s", path, e)
        return []


def _csv_bool(v) -> bool:
    return str(v).strip().lower() in {"true", "1", "yes"}


def allowed_file(filename: str) -> bool:
    ext = Path(filename).suffix.lstrip(".").lower()
    return ext in ALLOWED_EXTENSIONS


def analyze_sbi_screenshot_with_ai(image_path: Path, side: str, openai_client) -> dict:
    """Send screenshot to OpenAI Vision and return extracted JSON dict."""
    import base64

    side_hint = {
        "buy": "この画像は買いスクショとして送られた。画像内のsideが売りに見える場合はside=sellとして警告する。",
        "sell": "この画像は売りスクショとして送られた。画像内のsideが買いに見える場合はside=buyとして警告する。",
    }.get(side, "")

    user_msg = (
        f"{side_hint}\n"
        f"以下のJSONスキーマで返してください：\n{_JSON_SCHEMA_HINT}"
    )

    with image_path.open("rb") as f:
        img_b64 = base64.b64encode(f.read()).decode()

    ext = image_path.suffix.lstrip(".").lower()
    media_type = "image/jpeg" if ext in {"jpg", "jpeg"} else f"image/{ext}"

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            max_tokens=1024,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_msg},
                        {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{img_b64}", "detail": "high"}},
                    ],
                },
            ],
        )
        raw = response.choices[0].message.content or ""
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("AI Vision JSON parse failed: %s", e)
        return {"error": f"JSON parse failed: {e}", "raw": raw[:500], "warnings": [], "confidence": 0.0}
    except Exception as e:
        logger.exception("AI Vision call failed")
        return {"error": str(e), "warnings": [], "confidence": 0.0}


def normalize_screenshot_extract(raw: dict) -> dict:
    """Clean types: code→str, price→float, quantity→int."""
    out = dict(raw)

    code = str(out.get("code") or "").strip()
    out["code"] = code if re.match(r"^\d{4}$", code) else None

    price = out.get("price")
    try:
        price_str = str(price).replace(",", "").replace("円", "").strip()
        out["price"] = float(price_str) if price_str not in ("", "None", "null") else None
    except (ValueError, TypeError):
        out["price"] = None

    qty = out.get("quantity")
    try:
        out["quantity"] = int(str(qty).replace(",", "").strip()) if qty is not None else None
    except (ValueError, TypeError):
        out["quantity"] = None

    ot = str(out.get("order_type") or "unknown").strip().lower()
    out["order_type"] = ot if ot in VALID_ORDER_TYPES else "unknown"

    os_ = str(out.get("order_status") or "unknown").strip().lower()
    out["order_status"] = os_ if os_ in VALID_ORDER_STATUSES else "unknown"

    si = str(out.get("side") or "unknown").strip().lower()
    out["side"] = si if si in VALID_SIDES else "unknown"

    try:
        out["confidence"] = float(out.get("confidence") or 0.0)
    except (ValueError, TypeError):
        out["confidence"] = 0.0

    if not isinstance(out.get("warnings"), list):
        out["warnings"] = []

    return out


def validate_screenshot_extract(result: dict, side: str) -> tuple[list[str], list[str]]:
    """Return (errors, warnings). errors block registration; warnings allow with confirmation."""
    errors: list[str] = []
    warnings: list[str] = []

    if result.get("error"):
        errors.append(f"AI読み取りエラー: {result['error']}")
        return errors, warnings

    code = result.get("code")
    if not code:
        errors.append("銘柄コードが読み取れません（4桁数字が必要）")
    elif not re.match(r"^\d{4}$", str(code)):
        errors.append(f"銘柄コードが不正です: {code}（4桁数字のみ有効）")

    price = result.get("price")
    if price is None:
        errors.append("価格が読み取れません")
    elif float(price) <= 0:
        errors.append(f"価格が不正です: {price}")

    qty = result.get("quantity")
    if qty is None:
        errors.append("数量が読み取れません")
    elif int(qty) <= 0:
        errors.append(f"数量が不正です: {qty}")
    elif int(qty) % 100 != 0:
        warnings.append(f"数量が100株単位ではありません: {qty}（S株の場合は無視してください）")

    ai_side = result.get("side", "unknown")
    if ai_side == "unknown":
        errors.append("売買区分が読み取れません（buy/sellが必要）")
    elif ai_side != side:
        errors.append(f"売買区分が不一致: 画面={ai_side} / フォーム={side}（スクショを確認してください）")

    os_ = result.get("order_status", "unknown")
    if os_ in ("pending", "not_filled"):
        warnings.append(f"注文状態が {os_} です。約定済みのスクショを推奨します。")
    elif os_ == "unknown":
        warnings.append("注文状態が読み取れません。約定済みか確認してください。")

    conf = float(result.get("confidence") or 0.0)
    if conf < 0.5:
        errors.append(f"AI信頼度が低すぎます: {conf:.2f}（0.5未満は登録不可）")
    elif conf < 0.7:
        warnings.append(f"AI信頼度がやや低いです: {conf:.2f}。読み取り内容を確認してください。")

    for w in result.get("warnings") or []:
        warnings.append(f"AI警告: {w}")

    return errors, warnings


def match_buy_h5_candidate(result: dict, base_dir: Path | None = None) -> dict:
    """Match buy result against latest H5 full candidates CSV."""
    base = base_dir or H5_STORED_FORWARD_DIR
    candidates = _read_csv_dicts(base / "latest_h5_full_candidates.csv")
    code = str(result.get("code") or "")
    for row in candidates:
        if str(row.get("code") or "") == code:
            if (
                str(row.get("score_source") or "") == "stored_predictions"
                and not _csv_bool(row.get("score_fallback_used"))
                and _csv_bool(row.get("H5_full"))
            ):
                return {"matched": True, "candidate": row}
    return {"matched": False, "candidate": None}


def match_sell_open_position(result: dict, actual_trade_logs: list[dict]) -> dict:
    """Match sell result against open actual_trade_logs positions."""
    code = str(result.get("code") or "")
    for row in actual_trade_logs:
        if (
            str(row.get("code") or "") == code
            and row.get("actual_exit_status") in (None, "holding", "")
            and not row.get("actual_exit_date")
            and row.get("actual_entry_price")
        ):
            return {"matched": True, "position": row}
    return {"matched": False, "position": None}


def compute_entry_gap(actual_price: float, candidate: dict) -> dict:
    """Compute actual entry price gap vs candidate reference price.

    Returns dict with gap_pct, reference_price, reference_field, warning.
    warning values: None | "warning" (>=2%) | "strong_warning" (>=3%) | "check_material" (<=-3%)
    """
    for field in ("entry_price", "close", "signal_price"):
        ref_raw = candidate.get(field)
        try:
            ref_f = float(str(ref_raw or "").replace(",", "").replace("円", "").strip())
            if ref_f > 0:
                gap = round((actual_price / ref_f - 1.0) * 100.0, 3)
                warning = None
                if gap >= 3.0:
                    warning = "strong_warning"
                elif gap >= 2.0:
                    warning = "warning"
                elif gap <= -3.0:
                    warning = "check_material"
                return {"gap_pct": gap, "reference_price": ref_f, "reference_field": field, "warning": warning}
        except (ValueError, TypeError):
            continue
    return {"gap_pct": None, "reference_price": None, "reference_field": None, "warning": "no_reference"}


def build_entry_prefill(result: dict, match: dict, screenshot_filename: str | None = None) -> dict:
    """Build prefill dict for actual entry form."""
    candidate = match.get("candidate") or {}
    matched = match.get("matched", False)
    actual_price = result.get("price")

    entry_gap: dict = {}
    if matched and actual_price:
        try:
            entry_gap = compute_entry_gap(float(actual_price), candidate)
        except (ValueError, TypeError):
            entry_gap = {}

    strategy_group = candidate.get("strategy_group") or ("H5_full" if matched else "manual_unmatched")
    note_parts = [
        "H5 Stored screenshot entry",
        f"candidate_trade_date={candidate.get('trade_date') or ''}",
        f"score_source={candidate.get('score_source') or ''}",
        f"model_version={candidate.get('model_version') or ''}",
        f"signal_probability={candidate.get('signal_probability') or ''}",
        f"strategy_group={strategy_group}",
        f"ai_confidence={result.get('confidence', 0):.2f}",
    ]
    if entry_gap.get("gap_pct") is not None:
        note_parts.append(f"actual_entry_gap_pct={entry_gap['gap_pct']:+.3f}")
    if screenshot_filename:
        note_parts.append(f"screenshot={screenshot_filename}")

    return {
        "code": result.get("code") or "",
        "name": result.get("name") or candidate.get("name") or "",
        "actual_entry_price": result.get("price") or "",
        "quantity": result.get("quantity") or "",
        "actual_order_type": result.get("order_type") or "market",
        "actual_fill_status": result.get("order_status") or "filled",
        "actual_entry_date": (result.get("trade_datetime") or "")[:10] or "",
        "case_key": "h5_ai65_hd3_est12_cm_range330_live_limited",
        "actual_entry_model": "H5_stored_predictions",
        "signal_price": candidate.get("close") or candidate.get("signal_price") or "",
        "note": "\n".join(note_parts),
        "_entry_gap": entry_gap,
    }


def build_exit_prefill(result: dict, match: dict, screenshot_filename: str | None = None) -> dict:
    """Build prefill dict for actual exit form."""
    position = match.get("position") or {}
    entry_price = float(position.get("actual_entry_price") or 0) or None
    exit_price = float(result.get("price") or 0) or None
    pnl_pct = None
    if entry_price and exit_price:
        pnl_pct = round((exit_price / entry_price - 1.0) * 100.0, 3)
    note_parts = [
        "H5 Stored screenshot exit",
        f"matched_open_position={match.get('matched', False)}",
        f"ai_confidence={result.get('confidence', 0):.2f}",
    ]
    if screenshot_filename:
        note_parts.append(f"screenshot={screenshot_filename}")
    return {
        "actual_trade_id": position.get("id") or "",
        "code": result.get("code") or "",
        "name": result.get("name") or position.get("name") or "",
        "actual_exit_price": result.get("price") or "",
        "quantity": result.get("quantity") or position.get("quantity") or "",
        "actual_exit_date": (result.get("trade_datetime") or "")[:10] or "",
        "actual_entry_price": position.get("actual_entry_price") or "",
        "actual_exit_reason": "manual_exit",
        "estimated_pnl_pct": pnl_pct,
        "exit_note": "\n".join(note_parts),
    }

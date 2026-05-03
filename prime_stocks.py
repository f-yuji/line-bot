"""
TSE プライム銘柄リスト管理。
J-Quants API で取得して Supabase にキャッシュ。
取得失敗時は Nikkei225 にフォールバック。
"""
import logging
import os
from datetime import datetime, timedelta, timezone

import requests as _req

logger = logging.getLogger(__name__)

JQUANTS_TOKEN_URL = "https://api.jquants.com/v2/token/auth_refresh"
JQUANTS_LISTED_URL = "https://api.jquants.com/v2/listed/info"
CACHE_TTL_DAYS = 7


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _get_id_token(refresh_token: str) -> str | None:
    try:
        r = _req.post(
            JQUANTS_TOKEN_URL,
            params={"refreshtoken": refresh_token},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json().get("idToken")
        logger.warning("J-Quants id_token取得失敗: status=%s body=%s", r.status_code, r.text[:200])
    except Exception as e:
        logger.warning("J-Quants auth error: %s", e)
    return None


def _fetch_from_jquants() -> list[dict]:
    api_key = _opt("JQUANTS_API_KEY") or _opt("JQUANTS_REFRESH_TOKEN")
    if not api_key:
        logger.info("JQUANTS_API_KEY 未設定 → フォールバック")
        return []

    id_token = _get_id_token(api_key)
    if not id_token:
        return []

    try:
        r = _req.get(
            JQUANTS_LISTED_URL,
            headers={"Authorization": f"Bearer {id_token}"},
            timeout=30,
        )
        if r.status_code != 200:
            logger.warning("J-Quants listed/info エラー: status=%s", r.status_code)
            return []

        stocks = r.json().get("info", [])
        prime = [
            {
                "code": s.get("Code", "")[:4],
                "name": s.get("CompanyName", ""),
                "sector": s.get("Sector17CodeName", "") or s.get("Sector33CodeName", ""),
            }
            for s in stocks
            if s.get("MarketCodeName") in ("プライム", "Prime")
            and len(s.get("Code", "")) >= 4
        ]
        logger.info("J-Quants: %d銘柄（TSEプライム）取得", len(prime))
        return prime
    except Exception as e:
        logger.warning("J-Quants listed/info 取得エラー: %s", e)
        return []


def _save_to_supabase(supabase, records: list[dict]) -> None:
    if not records:
        return
    try:
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            {"code": r["code"], "name": r["name"], "sector": r.get("sector", ""), "updated_at": now}
            for r in records
            if r.get("code")
        ]
        # 1000件ずつ upsert
        for i in range(0, len(rows), 1000):
            supabase.table("prime_stocks_cache").upsert(rows[i:i + 1000]).execute()
        logger.info("prime_stocks_cache: %d件保存", len(rows))
    except Exception as e:
        logger.warning("prime_stocks_cache保存エラー: %s", e)


def _load_from_supabase(supabase) -> list[dict] | None:
    """TTL 内のキャッシュがあれば返す。古い or 空なら None。"""
    try:
        res = supabase.table("prime_stocks_cache").select("code, name, sector, updated_at").limit(2000).execute()
        rows = res.data or []
        if not rows:
            return None
        latest = max(r.get("updated_at", "") for r in rows)
        if latest:
            dt = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - dt < timedelta(days=CACHE_TTL_DAYS):
                return [{"code": r["code"], "name": r["name"], "sector": r.get("sector", "")} for r in rows]
    except Exception as e:
        logger.warning("prime_stocks_cache読み込みエラー: %s", e)
    return None


def get_prime_tickers(supabase=None, *, force_refresh: bool = False) -> list[dict]:
    """
    TSEプライム銘柄リストを返す。
    Returns: [{"code": str, "name": str, "sector": str}]
    失敗時は Nikkei225 にフォールバック。
    """
    if supabase and not force_refresh:
        cached = _load_from_supabase(supabase)
        if cached:
            logger.info("prime_stocks_cache: %d銘柄（キャッシュ使用）", len(cached))
            return cached

    stocks = _fetch_from_jquants()

    if stocks:
        if supabase:
            _save_to_supabase(supabase, stocks)
        return stocks

    logger.warning("プライム銘柄取得失敗 → Nikkei225 フォールバック")
    from nikkei_alert import NIKKEI225
    return [{"code": k, "name": v, "sector": ""} for k, v in NIKKEI225.items()]

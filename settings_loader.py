"""
strategy_settings テーブルから設定を読み込む。
テーブルが存在しない / レコードがない場合は既存定数をデフォルト値として返す。
クラッシュしないことを最優先にする。
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ─── デフォルト値（nikkei_alert.py の既存定数と同値）───
DEFAULTS: dict = {
    "drop_list_threshold":      -2.0,
    "alert_threshold":          -9.0,
    "index_gap_threshold":      -1.5,
    "daily_rebound_threshold":   3.0,
    "drop_rebound_threshold":    5.0,
    "volume_ratio_threshold":    1.5,
    "rsi_low_threshold":        30.0,
    "rsi_recover_threshold":    35.0,
    "ma5_cross_enabled":        True,
    "watch_days_limit":         10,
    "technical_score_weight":   50.0,
    "fundamental_score_weight": 30.0,
    "market_score_weight":      20.0,
    "strong_watch_score":       80.0,
    "watch_score":              70.0,
    "ignore_score":             60.0,
    "drop_notify_enabled":      True,
    "rebound_notify_enabled":   True,
    "morning_summary_enabled":  True,
    "portfolio_notify_enabled": True,
}

_cache: dict | None = None


def _build_supabase():
    """nikkei_alert.py と同じ env var 解決ロジック。"""
    from supabase import create_client

    def _opt(name: str) -> str:
        return os.getenv(name, "").strip()

    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()

    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("SUPABASE_URL / SUPABASE_KEY が設定されていません")
    return create_client(url, key)


def get_settings(*, force_reload: bool = False) -> dict:
    """
    strategy_settings テーブルの global レコードから設定を返す。

    - DB 取得成功: DB の値を優先し、未設定カラムはデフォルト値で補完
    - DB 取得失敗（テーブル未作成含む）: DEFAULTS をそのまま返す
    - force_reload=True: cron の都度リロードに使用
    """
    global _cache
    if _cache is not None and not force_reload:
        return _cache

    try:
        sb = _build_supabase()
        res = (
            sb.table("strategy_settings")
            .select("*")
            .eq("user_id", "global")
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            row = res.data[0]
            # DB の値を優先しつつ、None のカラムはデフォルト値で補完
            settings = {
                k: (row[k] if row.get(k) is not None else v)
                for k, v in DEFAULTS.items()
            }
            logger.info("strategy_settings: DB から読み込み完了")
            _cache = settings
            return settings
        else:
            logger.info("strategy_settings: レコードなし → デフォルト値使用")
    except Exception as e:
        logger.warning("strategy_settings 読み込み失敗（デフォルト値使用）: %s", e)

    _cache = dict(DEFAULTS)
    return _cache

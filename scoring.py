"""
投資スコアリング: テクニカル(50) + ファンダメンタル(30) + 市場(20) = 100点満点
"""
import logging

logger = logging.getLogger(__name__)


def _rsi_series(closes: "pd.Series", period: int = 14) -> "pd.Series":
    import pandas as pd  # noqa: F401
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return (100 - 100 / (1 + rs)).fillna(0)


def technical_score(closes, volumes, price_at_drop: float | None, cfg: dict) -> float:
    """テクニカルスコア（0〜50点）"""
    try:
        if len(closes) < 2:
            return 0.0

        current = float(closes.iloc[-1])
        prev = float(closes.iloc[-2])
        score = 0.0

        # ① MA5上抜け (+10)
        if cfg.get("ma5_cross_enabled", True) and len(closes) >= 5:
            ma5 = float(closes.tail(5).mean())
            if prev <= ma5 < current:
                score += 10.0

        # ② 前日比 (0〜10)
        if prev > 0:
            day_pct = (current - prev) / prev * 100
            thr = float(cfg.get("daily_rebound_threshold", 3.0))
            if day_pct > 0 and thr > 0:
                score += min(10.0, day_pct / thr * 10.0)

        # ③ 急落時比 (0〜15)
        if price_at_drop and price_at_drop > 0:
            from_drop = (current - price_at_drop) / price_at_drop * 100
            thr = float(cfg.get("drop_rebound_threshold", 5.0))
            if from_drop > 0 and thr > 0:
                score += min(15.0, from_drop / thr * 15.0)

        # ④ 出来高 (0〜10)
        if len(volumes) >= 21:
            vol_avg = float(volumes.iloc[-21:-1].mean())
            vol_now = float(volumes.iloc[-1])
            thr = float(cfg.get("volume_ratio_threshold", 1.5))
            if vol_avg > 0 and vol_now > vol_avg and thr > 1.0:
                ratio = vol_now / vol_avg
                score += min(10.0, (ratio - 1.0) / (thr - 1.0) * 10.0)

        # ⑤ RSI回復 (+5)
        rsi_s = _rsi_series(closes).dropna()
        if len(rsi_s) >= 6:
            rsi_now = float(rsi_s.iloc[-1])
            recent_prev = list(rsi_s.iloc[-6:-1])
            rsi_rec = float(cfg.get("rsi_recover_threshold", 35.0))
            rsi_low = float(cfg.get("rsi_low_threshold", 30.0))
            if rsi_now >= rsi_rec and any(r < rsi_low for r in recent_prev):
                score += 5.0

        return min(50.0, round(score, 1))
    except Exception as e:
        logger.debug("technical_score error: %s", e)
        return 0.0


def fundamental_score(
    is_deficit: bool | None = None,
    per: float | None = None,
    pbr: float | None = None,
    div_yield_pct: float | None = None,
) -> float:
    """ファンダメンタルスコア（0〜30点）。赤字企業は0点固定。"""
    if is_deficit:
        return 0.0

    score = 0.0

    # PER (0〜10)
    if per is not None:
        if per < 10:
            score += 10.0
        elif per < 15:
            score += 8.0
        elif per < 20:
            score += 5.0
        else:
            score += 2.0
    else:
        score += 5.0  # 情報なし → 中立

    # PBR (0〜10)
    if pbr is not None:
        if pbr < 1.0:
            score += 10.0
        elif pbr < 1.5:
            score += 8.0
        elif pbr < 2.0:
            score += 5.0
        else:
            score += 2.0
    else:
        score += 5.0  # 情報なし → 中立

    # 配当利回り (0〜10)
    if div_yield_pct is not None:
        if div_yield_pct >= 3.0:
            score += 10.0
        elif div_yield_pct >= 2.0:
            score += 7.0
        elif div_yield_pct >= 1.0:
            score += 4.0
        else:
            score += 1.0
    else:
        score += 3.0  # 情報なし → 中立

    return min(30.0, round(score, 1))


def market_score(nikkei_pct: float | None) -> float:
    """市場スコア（0〜20点）。市場全体の下げが大きいほど高得点（パニック売り→反発期待）"""
    if nikkei_pct is None:
        return 10.0
    if nikkei_pct <= -2.0:
        return 20.0
    if nikkei_pct <= -1.0:
        return 14.0
    if nikkei_pct <= 0.0:
        return 8.0
    return 4.0


def score_label(total: float, cfg: dict) -> str:
    if total >= float(cfg.get("strong_watch_score", 80.0)):
        return "強監視★★"
    if total >= float(cfg.get("watch_score", 70.0)):
        return "監視"
    if total >= float(cfg.get("ignore_score", 60.0)):
        return "観察"
    return "スルー"


def calculate_score(
    item: dict,
    closes,
    volumes,
    cfg: dict,
    *,
    per: float | None = None,
    pbr: float | None = None,
    div_yield_pct: float | None = None,
    is_deficit: bool | None = None,
    nikkei_pct: float | None = None,
) -> dict:
    """
    総合スコアを計算して返す。
    Returns: {total, technical, fundamental, market, label}
    """
    t = technical_score(closes, volumes, item.get("price_at_drop"), cfg)
    f = fundamental_score(is_deficit, per, pbr, div_yield_pct)
    nk = nikkei_pct if nikkei_pct is not None else item.get("nikkei_pct")
    m = market_score(nk)
    total = round(t + f + m, 1)
    return {
        "total": total,
        "technical": t,
        "fundamental": f,
        "market": m,
        "label": score_label(total, cfg),
    }

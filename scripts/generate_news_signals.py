#!/usr/bin/env python3
"""
Generate rule-based market_news_signals.

This is separate from the user-facing LINE news bot. It fetches lightweight RSS
items, classifies market/material signals, and can apply same-day max scores to
stock_feature_snapshots.
"""
import argparse
import hashlib
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import feedparser
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
RSS_SOURCES = [
    ("google_news", "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"),
    ("yahoo_top", "https://news.yahoo.co.jp/rss/topics/top-picks.xml"),
    ("nikkei", "https://assets.wor.jp/rss/rdf/nikkei/news.rdf"),
]

RULES = {
    "bad_strong": {
        "category": "company",
        "score": 100,
        "severity": "strong",
        "action": "exclude",
        "keywords": ["下方修正", "赤字転落", "上場廃止", "債務超過", "民事再生", "会社更生", "破産", "不祥事", "不正", "粉飾", "不正会計", "虚偽記載", "行政処分", "課徴金", "逮捕", "大幅減益", "経営危機", "特別損失", "巨額損失", "減損", "減配", "無配"],
    },
    "bad_medium": {
        "category": "earnings",
        "score": 60,
        "severity": "medium",
        "action": "penalize",
        "keywords": ["減収", "減益", "営業減益", "純利益減", "受注減", "需要減", "販売不振", "通期見通し", "業績予想", "ガイダンス", "未達", "下振れ", "延期", "中止", "撤退", "採算悪化", "コスト増", "原材料高", "物流費上昇"],
    },
    "bad_weak": {
        "category": "company",
        "score": 20,
        "severity": "weak",
        "action": "feature_only",
        "keywords": ["警戒感", "不透明感", "懸念", "軟調", "一服", "反落", "伸び悩み"],
    },
    "market_shock": {
        "category": "macro",
        "score": 70,
        "severity": "medium",
        "action": "watch",
        "keywords": ["全面安", "急落", "暴落", "リスクオフ", "パニック", "金融不安", "信用不安", "銀行不安", "世界株安", "米株急落", "日経平均急落", "TOPIX急落", "VIX急騰", "日経VI急騰", "ボラティリティ上昇"],
    },
    "fx_yen": {
        "category": "fx",
        "score": 55,
        "severity": "medium",
        "action": "feature_only",
        "keywords": ["円高", "急速な円高", "円買い", "為替介入", "介入観測", "ドル円急落", "日銀", "財務省", "為替市場", "輸出採算", "為替差損", "円高進行"],
    },
    "interest_rate": {
        "category": "interest_rate",
        "score": 55,
        "severity": "medium",
        "action": "feature_only",
        "keywords": ["金利上昇", "長期金利", "利上げ", "追加利上げ", "日銀", "FRB", "FOMC", "金融引き締め", "国債利回り", "住宅ローン金利", "不動産株", "REIT", "グロース株"],
    },
    "naphtha": {
        "category": "naphtha",
        "score": 60,
        "severity": "medium",
        "action": "feature_only",
        "keywords": ["ナフサ", "原油高", "原油急騰", "石化原料", "石油化学", "樹脂", "溶剤", "塗料", "接着剤", "防水材", "建材", "化学品", "供給停止", "受注停止", "原材料不足", "材料不足", "価格改定", "値上げ", "コスト増"],
    },
    "geopolitical": {
        "category": "geopolitical",
        "score": 60,
        "severity": "medium",
        "action": "watch",
        "keywords": ["戦争", "紛争", "中東", "ホルムズ海峡", "台湾有事", "制裁", "攻撃", "ミサイル", "地政学", "海上輸送", "物流混乱", "サプライチェーン", "紅海", "原油供給"],
    },
    "supply_chain": {
        "category": "supply_chain",
        "score": 55,
        "severity": "medium",
        "action": "feature_only",
        "keywords": ["供給停止", "出荷停止", "受注停止", "納期遅延", "物流混乱", "輸送遅延", "部品不足", "原材料不足", "在庫不足", "工場停止", "操業停止"],
    },
}

SECTOR_RULES = {
    "banks": ["銀行", "金融", "地銀", "メガバンク"],
    "real_estate": ["不動産", "REIT", "住宅", "マンション"],
    "construction": ["建設", "建材", "防水材"],
    "autos": ["自動車", "輸出", "円高", "為替"],
    "chemicals": ["化学", "石油化学", "樹脂", "塗料", "ナフサ"],
    "semiconductors": ["半導体", "AI", "データセンター"],
    "energy": ["資源", "原油", "天然ガス", "LNG"],
}


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_supabase():
    mode = _opt("SUPABASE_MODE") or _opt("ENV")
    mode_upper = (mode or "").upper()
    url = (_opt(f"SUPABASE_URL_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_URL")
    key = (_opt(f"SUPABASE_KEY_{mode_upper}") if mode_upper else "") or _opt("SUPABASE_KEY")
    if not url or not key:
        raise KeyError("SUPABASE_URL / SUPABASE_KEY is not set")
    return create_client(url, key)


def _target_dates(args: argparse.Namespace) -> list[date]:
    if args.today:
        return [datetime.now(JST).date()]
    if args.date:
        return [datetime.strptime(args.date, "%Y-%m-%d").date()]
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else datetime.now(JST).date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else start
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _hash_key(signal_date: str, title: str, url: str | None) -> str:
    raw = url or f"{signal_date}:{title}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_news(limit: int | None = None) -> list[dict]:
    items = []
    for source, url in RSS_SOURCES:
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "line_bot_news_signals/1.0"})
            r.raise_for_status()
            feed = feedparser.parse(r.content)
            for e in feed.entries:
                items.append({
                    "source": source,
                    "title": str(getattr(e, "title", "") or "").strip(),
                    "url": str(getattr(e, "link", "") or "").strip() or None,
                    "summary": str(getattr(e, "summary", "") or "").strip()[:1000],
                })
                if limit and len(items) >= limit:
                    return items
            logger.info("fetched rss: source=%s entries=%d", source, len(feed.entries))
        except Exception as e:
            logger.warning("rss fetch failed source=%s: %s", source, e)
    return items[:limit] if limit else items


def _matches(text: str, keywords: list[str]) -> list[str]:
    return [kw for kw in keywords if kw in text]


def classify(item: dict, signal_date: date) -> dict:
    text = f"{item.get('title') or ''} {item.get('summary') or ''}"
    matched: list[str] = []
    scores = {
        "market_shock_score": 0.0,
        "sector_risk_score": 0.0,
        "bad_news_score": 0.0,
        "fx_yen_score": 0.0,
        "energy_naphtha_score": 0.0,
        "interest_rate_score": 0.0,
        "geopolitical_score": 0.0,
        "supply_chain_score": 0.0,
    }
    category = "other"
    severity = "none"
    action = "feature_only"
    reasons = []

    for name, rule in RULES.items():
        hits = _matches(text, rule["keywords"])
        if not hits:
            continue
        matched.extend(hits)
        category = rule["category"]
        severity = _max_severity(severity, rule["severity"])
        action = _max_action(action, rule["action"])
        reasons.append(f"{name}: {', '.join(hits[:5])}")
        score = float(rule["score"])
        if name.startswith("bad"):
            scores["bad_news_score"] = max(scores["bad_news_score"], score)
        elif name == "market_shock":
            scores["market_shock_score"] = max(scores["market_shock_score"], score)
        elif name == "fx_yen":
            scores["fx_yen_score"] = max(scores["fx_yen_score"], score)
        elif name == "interest_rate":
            scores["interest_rate_score"] = max(scores["interest_rate_score"], score)
        elif name == "naphtha":
            scores["energy_naphtha_score"] = max(scores["energy_naphtha_score"], score)
            scores["sector_risk_score"] = max(scores["sector_risk_score"], 30.0)
            action = "penalize" if action != "exclude" else action
        elif name == "geopolitical":
            scores["geopolitical_score"] = max(scores["geopolitical_score"], score)
            scores["market_shock_score"] = max(scores["market_shock_score"], 40.0)
        elif name == "supply_chain":
            scores["supply_chain_score"] = max(scores["supply_chain_score"], score)
            scores["sector_risk_score"] = max(scores["sector_risk_score"], 40.0)

    # Naphtha/materials must never become exclude by itself.
    if scores["bad_news_score"] < 90 and scores["energy_naphtha_score"] > 0 and scores["market_shock_score"] == 0:
        action = "penalize"

    related_sectors = [sector for sector, kws in SECTOR_RULES.items() if _matches(text, kws)]
    related_codes = sorted(set(_matches(text, [str(n) for n in range(1000, 10000)])))[:20]
    row = {
        "signal_date": signal_date.isoformat(),
        "source": item.get("source"),
        "title": item.get("title") or "(no title)",
        "url": item.get("url"),
        "url_hash": _hash_key(signal_date.isoformat(), item.get("title") or "", item.get("url")),
        "summary": item.get("summary"),
        "category": category,
        "subcategory": None,
        "related_codes": related_codes,
        "related_sectors": related_sectors,
        **scores,
        "severity": severity,
        "action_type": action,
        "reason": " / ".join(reasons)[:1000],
        "matched_keywords": sorted(set(matched)),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    return row


def _max_severity(a: str, b: str) -> str:
    order = {"none": 0, "weak": 1, "medium": 2, "strong": 3}
    return a if order.get(a, 0) >= order.get(b, 0) else b


def _max_action(a: str, b: str) -> str:
    order = {"feature_only": 0, "watch": 1, "boost": 1, "penalize": 2, "exclude": 3}
    return a if order.get(a, 0) >= order.get(b, 0) else b


def _upsert(sb, rows: list[dict]) -> int:
    if not rows:
        return 0
    sb.table("market_news_signals").upsert(rows, on_conflict="signal_date,url_hash").execute()
    return len(rows)


def apply_to_features(sb, signal_date: date, rows: list[dict], dry_run: bool) -> int:
    if not rows:
        return 0
    cols = ["market_shock_score", "sector_risk_score", "bad_news_score", "fx_yen_score", "energy_naphtha_score", "interest_rate_score"]
    update = {c: max(float(r.get(c) or 0) for r in rows) for c in cols}
    update["updated_at"] = datetime.now(timezone.utc).isoformat()
    if dry_run:
        logger.info("DRYRUN apply_to_features: date=%s update=%s", signal_date, update)
        return 0
    res = sb.table("stock_feature_snapshots").update(update).eq("trade_date", signal_date.isoformat()).execute()
    for r in rows:
        sb.table("market_news_signals").update({
            "is_applied_to_features": True,
            "applied_at": datetime.now(timezone.utc).isoformat(),
        }).eq("signal_date", signal_date.isoformat()).eq("url_hash", r["url_hash"]).execute()
    return len(res.data or [])


def run(args: argparse.Namespace) -> None:
    sb = _build_supabase()
    for d in _target_dates(args):
        logger.info("start news signal generation: date=%s", d)
        raw = fetch_news(args.limit)
        logger.info("fetched news: %d", len(raw))
        rows = [classify(item, d) for item in raw if item.get("title")]
        for r in rows[:20]:
            logger.info(
                "classified: title=%s category=%s severity=%s action=%s bad=%s shock=%s fx=%s naphtha=%s rate=%s reason=%s",
                r["title"][:80],
                r["category"],
                r["severity"],
                r["action_type"],
                r["bad_news_score"],
                r["market_shock_score"],
                r["fx_yen_score"],
                r["energy_naphtha_score"],
                r["interest_rate_score"],
                r["reason"],
            )
        saved = 0 if args.dry_run else _upsert(sb, rows)
        if args.dry_run:
            logger.info("DRYRUN upsert market_news_signals: rows=%d", len(rows))
        else:
            logger.info("upsert market_news_signals: rows=%d", saved)
        if args.apply_to_features:
            updated = apply_to_features(sb, d, rows, args.dry_run)
            logger.info("apply_to_features: date=%s updated_rows=%d", d, updated)
        logger.info("complete: rows=%d errors=0", len(rows))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate market news signals")
    p.add_argument("--date")
    p.add_argument("--today", action="store_true")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--apply-to-features", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--limit", type=int)
    p.add_argument("--source")
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())

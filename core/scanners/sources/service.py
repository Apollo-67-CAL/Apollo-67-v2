from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from core.scanners.sources.connectors.google_news_rss import fetch_symbol as fetch_google_news_symbol
from core.scanners.sources.connectors.reddit_rss import fetch_symbol as fetch_reddit_symbol
from core.storage.db import get_connection


_SOCIAL_SOURCES = ["Reddit"]
_NEWS_SOURCES = ["Google News"]


def _ttl_minutes() -> int:
    raw = os.getenv("SCANNER_SOURCES_CACHE_TTL_MIN", "30").strip()
    try:
        return max(1, int(raw))
    except Exception:
        return 30


def _ensure_source_snapshots_table() -> None:
    with get_connection() as conn:
        if conn.backend == "postgres":
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS source_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    source TEXT NOT NULL,
                    posts INTEGER NOT NULL,
                    positive INTEGER NOT NULL,
                    negative INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_source_snapshots_symbol_agent_created
                ON source_snapshots(symbol, agent, created_at DESC)
                """
            )
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS source_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    source TEXT NOT NULL,
                    posts INTEGER NOT NULL,
                    positive INTEGER NOT NULL,
                    negative INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_source_snapshots_symbol_agent_created
                ON source_snapshots(symbol, agent, created_at)
                """
            )


def _parse_created_at(raw: Any) -> datetime:
    text = str(raw or "").strip()
    if not text:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        try:
            dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime.fromtimestamp(0, tz=timezone.utc)


def _cache_rows(symbol: str, agent: str) -> List[Dict[str, Any]]:
    _ensure_source_snapshots_table()
    with get_connection() as conn:
        if conn.backend == "postgres":
            rows = conn.execute(
                """
                SELECT source, posts, positive, negative, created_at
                FROM source_snapshots
                WHERE symbol = ? AND agent = ?
                ORDER BY created_at DESC
                LIMIT 200
                """,
                (symbol, agent),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT source, posts, positive, negative, created_at
                FROM source_snapshots
                WHERE symbol = ? AND agent = ?
                ORDER BY id DESC
                LIMIT 200
                """,
                (symbol, agent),
            ).fetchall()

    output = []
    for row in rows:
        output.append(
            {
                "source": row.get("source"),
                "posts": int(row.get("posts") or 0),
                "positive": int(row.get("positive") or 0),
                "negative": int(row.get("negative") or 0),
                "created_at": row.get("created_at"),
            }
        )
    return output


def _latest_by_source_within_ttl(symbol: str, agent: str) -> List[Dict[str, Any]]:
    ttl_cutoff = datetime.now(timezone.utc) - timedelta(minutes=_ttl_minutes())
    latest_by_source: Dict[str, Dict[str, Any]] = {}
    for row in _cache_rows(symbol, agent):
        source = str(row.get("source") or "").strip()
        if not source:
            continue
        created_at = _parse_created_at(row.get("created_at"))
        if created_at < ttl_cutoff:
            continue
        if source not in latest_by_source:
            latest_by_source[source] = row

    rows = list(latest_by_source.values())
    rows.sort(key=lambda x: str(x.get("source") or ""))
    return rows


def _insert_snapshot(symbol: str, agent: str, source: str, posts: int, positive: int, negative: int) -> None:
    _ensure_source_snapshots_table()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO source_snapshots(symbol, agent, source, posts, positive, negative)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (symbol, agent, source, int(posts), int(positive), int(negative)),
        )


def _fetch_live(symbol: str, agent: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    social_enabled = os.getenv("ENABLE_REDDIT_RSS", "1").strip().lower() in {"1", "true", "yes", "on"}
    news_enabled = os.getenv("ENABLE_GOOGLE_NEWS_RSS", "1").strip().lower() in {"1", "true", "yes", "on"}

    if agent in {"social", "overall"} and social_enabled:
        try:
            record = fetch_reddit_symbol(symbol, limit=25)
            rows.append(record)
        except Exception:
            pass

    if agent in {"news", "overall"} and news_enabled:
        try:
            record = fetch_google_news_symbol(symbol, limit=25, days=7)
            rows.append(record)
        except Exception:
            pass

    for record in rows:
        _insert_snapshot(
            symbol=symbol,
            agent=agent,
            source=str(record.get("source") or "Unknown"),
            posts=int(record.get("posts") or 0),
            positive=int(record.get("positive") or 0),
            negative=int(record.get("negative") or 0),
        )
    return rows


def get_sources_snapshot(symbol: str, agent: str) -> Dict[str, Any]:
    symbol_value = (symbol or "").strip().upper()
    agent_value = (agent or "social").strip().lower()

    cached_rows = _latest_by_source_within_ttl(symbol_value, agent_value)
    if cached_rows:
        sources = []
        for row in cached_rows:
            posts = int(row.get("posts") or 0)
            positive = int(row.get("positive") or 0)
            negative = int(row.get("negative") or 0)
            neutral = max(0, posts - positive - negative)
            sources.append(
                {
                    "id": str(row.get("source") or "unknown").lower().replace(" ", "_"),
                    "name": row.get("source") or "Unknown",
                    "origin": "rss_cache",
                    "mentions": posts,
                    "positive": positive,
                    "negative": negative,
                    "neutral": neutral,
                    "score": float(positive - negative),
                    "confidence": None,
                    "meta": {"cache": True},
                }
            )
        return {
            "symbol": symbol_value,
            "scanner_type": agent_value,
            "ts": datetime.now(timezone.utc).isoformat(),
            "sources": sources,
            "cache_hit": True,
        }

    live_rows = _fetch_live(symbol_value, agent_value)
    sources = []
    for row in live_rows:
        posts = int(row.get("posts") or 0)
        positive = int(row.get("positive") or 0)
        negative = int(row.get("negative") or 0)
        neutral = max(0, posts - positive - negative)
        sources.append(
            {
                "id": str(row.get("source") or "unknown").lower().replace(" ", "_"),
                "name": row.get("source") or "Unknown",
                "origin": "rss_live",
                "mentions": posts,
                "positive": positive,
                "negative": negative,
                "neutral": neutral,
                "score": float(positive - negative),
                "confidence": None,
                "meta": {"url": row.get("url")},
            }
        )

    return {
        "symbol": symbol_value,
        "scanner_type": agent_value,
        "ts": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
        "cache_hit": False,
    }

from __future__ import annotations

from typing import Any, Dict

from core.scanners.sources.rss import fetch_rss, q
from core.scanners.sources.sentiment import classify


def fetch_symbol(symbol: str, limit: int = 25, days: int = 7) -> Dict[str, Any]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"source": "Google News", "posts": 0, "positive": 0, "negative": 0, "items": []}

    query = f"{sym} stock when:{int(days)}d"
    url = f"https://news.google.com/rss/search?q={q(query)}&hl=en-US&gl=US&ceid=US:en"
    items = fetch_rss(url, limit=limit)

    positive = 0
    negative = 0
    for item in items:
        text = ((item.get("title") or "") + " " + (item.get("summary") or "")).strip()
        pos, neg = classify(text)
        positive += pos
        negative += neg

    return {
        "source": "Google News",
        "posts": len(items),
        "positive": positive,
        "negative": negative,
        "items": items,
        "url": url,
    }

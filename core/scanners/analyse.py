from __future__ import annotations

import json
import os
import urllib.request
from typing import Any, Dict, List

_POS = {
    "beat", "beats", "bull", "bullish", "upgrade", "surge", "growth", "strong", "outperform", "buy", "rally", "profit", "positive", "breakout",
}
_NEG = {
    "miss", "bear", "bearish", "downgrade", "plunge", "weak", "warning", "lawsuit", "fraud", "loss", "negative", "sell", "cut", "crash", "investigation",
}


def _classify_simple(text: str) -> str:
    lower = (text or "").lower()
    pos = sum(1 for token in _POS if token in lower)
    neg = sum(1 for token in _NEG if token in lower)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


def _heuristic(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    per_source: Dict[str, Dict[str, Any]] = {}
    pos = 0
    neg = 0
    neu = 0

    for item in items:
        source_id = str(item.get("source_id") or "unknown")
        source_label = str(item.get("source_label") or source_id)
        bucket = per_source.setdefault(
            source_id,
            {
                "source_id": source_id,
                "label": source_label,
                "posts": 0,
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "net": 0,
            },
        )
        bucket["posts"] += 1
        sentiment = _classify_simple(str(item.get("text") or item.get("title") or ""))
        if sentiment == "positive":
            bucket["positive"] += 1
            pos += 1
        elif sentiment == "negative":
            bucket["negative"] += 1
            neg += 1
        else:
            bucket["neutral"] += 1
            neu += 1

    for bucket in per_source.values():
        bucket["net"] = int(bucket["positive"]) - int(bucket["negative"])

    posts_total = len(items)
    net = pos - neg
    confidence = 0.0 if posts_total == 0 else min(1.0, max(0.1, abs(net) / float(posts_total)))
    score = 50
    if posts_total > 0:
        score = int(max(0, min(100, round(50 + (net / float(posts_total)) * 50))))

    recommendation = "WATCH"
    if score >= 65:
        recommendation = "BUY"
    elif score <= 35:
        recommendation = "AVOID"

    reasons = []
    if posts_total == 0:
        reasons.append("No recent source posts fetched")
    else:
        reasons.append(f"Analysed {posts_total} items across {len(per_source)} sources")
        reasons.append(f"Net sentiment {net} (positive {pos} vs negative {neg})")

    return {
        "posts_total": posts_total,
        "positive_count": pos,
        "negative_count": neg,
        "neutral_count": neu,
        "net": net,
        "confidence": round(confidence, 4),
        "per_source": per_source,
        "top_reasons": reasons,
        "recommendation": recommendation,
        "target_price": None,
        "score": score,
    }


def _openai_classify(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY missing")

    sample = []
    for item in items[:30]:
        sample.append(
            {
                "source_id": item.get("source_id"),
                "title": item.get("title"),
                "text": item.get("text"),
            }
        )

    payload = {
        "model": "gpt-4o-mini",
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": "You are a strict financial sentiment classifier. Output JSON only.",
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": "Return JSON with keys: posts_total, positive_count, negative_count, neutral_count, net, confidence(0..1), per_source map {source_id:{posts,positive,negative,neutral,net}}, top_reasons(list up to 3), recommendation(BUY|WATCH|AVOID), target_price(null), score(0..100)",
                        "items": sample,
                    }
                ),
            },
        ],
    }

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = json.loads(resp.read().decode("utf-8", errors="ignore"))

    content = (
        raw.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "{}")
    )
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise RuntimeError("Invalid OpenAI response")
    return parsed


def analyse_items_openai(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not items:
        return _heuristic(items)
    try:
        parsed = _openai_classify(items)
        # light validation + defaults
        base = _heuristic(items)
        base.update({k: parsed.get(k, base.get(k)) for k in base.keys()})
        per_source = parsed.get("per_source")
        if isinstance(per_source, dict):
            base["per_source"] = per_source
        return base
    except Exception:
        return _heuristic(items)

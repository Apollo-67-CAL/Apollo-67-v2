from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from core.repositories.scanner_connectors import ScannerConnectorsRepository
from core.repositories.scanner_sources import ScannerSourceBreakdownsRepository
from core.repositories.scanner_source_controls import normalize_source_key
from core.scanners.analyse import analyse_items_openai
from core.scanners.pipeline import fetch_items

_CACHE_TTL_SECONDS = 300
_CACHE_LOCK = Lock()
_EVIDENCE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_LIVE_EVIDENCE_LOOKUP = str(os.getenv("SCANNER_EVIDENCE_LIVE_LOOKUP", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

_SOURCES_REPO = ScannerSourceBreakdownsRepository()
_CONNECTORS_REPO = ScannerConnectorsRepository()

_ALIASES: Dict[str, List[str]] = {
    "LTR.AX": ["LTR", "$LTR", "LIONTOWN"],
    "PDN.AX": ["PDN", "$PDN", "PALADIN"],
    "RMS.AX": ["RMS", "$RMS", "RAMELIUS"],
    "RIVN": ["$RIVN", "RIVIAN"],
    "PLTR": ["$PLTR", "PALANTIR"],
    "SOFI": ["$SOFI", "SOFI"],
}


def _empty_evidence() -> Dict[str, int]:
    return {
        "posts": 0,
        "mentions": 0,
        "positive": 0,
        "negative": 0,
        "neutral": 0,
        "net": 0,
    }


def _empty_source_counts() -> Dict[str, int]:
    return {"social": 0, "news": 0, "institution": 0}


def _empty_source_breakdown() -> Dict[str, Dict[str, int]]:
    return {
        "social": {
            "reddit": 0,
            "x": 0,
            "hotcopper": 0,
            "youtube": 0,
            "facebook": 0,
            "tiktok": 0,
        },
        "news": {
            "articles": 0,
            "publishers": 0,
        },
        "institution": {
            "filings": 0,
            "upgrades": 0,
            "downgrades": 0,
            "unusual_volume": 0,
        },
    }


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        out = float(value or 0.0)
    except Exception:
        return 0.0
    return out if out == out else 0.0


def _count_non_zero(values: List[int]) -> int:
    return sum(1 for value in values if int(value or 0) > 0)


def _breakdown_totals_from_source_breakdown(source_breakdown: Dict[str, Any]) -> Dict[str, int]:
    social = source_breakdown.get("social") if isinstance(source_breakdown.get("social"), dict) else {}
    news = source_breakdown.get("news") if isinstance(source_breakdown.get("news"), dict) else {}
    institution = (
        source_breakdown.get("institution") if isinstance(source_breakdown.get("institution"), dict) else {}
    )
    return {
        "social": sum(_safe_int(v) for v in social.values()),
        "news": _safe_int(news.get("articles")) or sum(_safe_int(v) for v in news.values()),
        "institution": sum(_safe_int(v) for v in institution.values()),
    }


def normalize_symbol_for_evidence(symbol: str, market: str) -> Dict[str, Any]:
    symbol_u = str(symbol or "").strip().upper()
    market_u = str(market or ("AU" if symbol_u.endswith(".AX") else "US")).strip().upper()
    if market_u == "AU" and symbol_u and not symbol_u.endswith(".AX"):
        symbol_u = f"{symbol_u}.AX"
    base = symbol_u[:-3] if symbol_u.endswith(".AX") else symbol_u
    aliases = [symbol_u, base, f"${base}"]
    for extra in _ALIASES.get(symbol_u, []):
        norm = str(extra or "").strip().upper()
        if norm and norm not in aliases:
            aliases.append(norm)
    deduped: List[str] = []
    seen: set[str] = set()
    for token in aliases:
        if token and token not in seen:
            seen.add(token)
            deduped.append(token)
    return {"symbol": symbol_u, "market": market_u, "base": base, "aliases": deduped}


def _cache_key(symbol: str, market: str, lookback_days: int) -> str:
    return f"{symbol}|{market}|{int(lookback_days)}"


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _source_totals_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    posts = int(totals.get("posts") or totals.get("mentions") or 0)
    positive = int(totals.get("positive") or 0)
    negative = int(totals.get("negative") or 0)
    neutral = int(totals.get("neutral") or max(0, posts - positive - negative))
    net = int(totals.get("net") or (positive - negative))
    confidence = float(totals.get("confidence") or 0.0)
    return {
        "posts": posts,
        "mentions": int(totals.get("mentions") or posts),
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "net": net,
        "confidence": confidence,
    }


def _per_source_from_payload(payload: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    rows = payload.get("sources") if isinstance(payload.get("sources"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_key = normalize_source_key(str(row.get("source_key") or row.get("id") or row.get("name") or "unknown"))
        posts = int(row.get("posts") or row.get("mentions") or 0)
        pos = int(row.get("positive") or 0)
        neg = int(row.get("negative") or 0)
        neu = int(row.get("neutral") or max(0, posts - pos - neg))
        out[source_key] = {
            "posts": posts,
            "positive": pos,
            "negative": neg,
            "neutral": neu,
            "net": int(row.get("net") or (pos - neg)),
        }
    return out


def _runtime_group_analysis(symbol_query: str, group: str, lookback_days: int) -> Dict[str, Any]:
    del lookback_days  # current connectors use fixed source windows
    try:
        enabled_map = _CONNECTORS_REPO.get_all_enabled_map()
        items, _runtime = fetch_items(
            symbol=symbol_query,
            group=group,
            connectors_enabled=enabled_map,
            timeframe="1day",
        )
        analysis = analyse_items_openai(items)
    except Exception:
        return {
            "totals": {
                "posts": 0,
                "mentions": 0,
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "net": 0,
                "confidence": 0.0,
            },
            "per_source": {},
        }
    per_source = analysis.get("per_source") if isinstance(analysis.get("per_source"), dict) else {}
    per_source_norm: Dict[str, Dict[str, int]] = {}
    for key, row in per_source.items():
        if not isinstance(row, dict):
            continue
        source_key = normalize_source_key(str(key or "unknown"))
        posts = int(row.get("posts") or 0)
        pos = int(row.get("positive") or 0)
        neg = int(row.get("negative") or 0)
        neu = int(row.get("neutral") or max(0, posts - pos - neg))
        per_source_norm[source_key] = {
            "posts": posts,
            "positive": pos,
            "negative": neg,
            "neutral": neu,
            "net": int(row.get("net") or (pos - neg)),
        }
    return {
        "totals": {
            "posts": int(analysis.get("posts_total") or 0),
            "mentions": int(analysis.get("posts_total") or 0),
            "positive": int(analysis.get("positive_count") or 0),
            "negative": int(analysis.get("negative_count") or 0),
            "neutral": int(analysis.get("neutral_count") or 0),
            "net": int(analysis.get("net") or 0),
            "confidence": float(analysis.get("confidence") or 0.0),
        },
        "per_source": per_source_norm,
    }


def _group_evidence(symbol: str, market: str, group: str, lookback_days: int) -> Dict[str, Any]:
    symbol_meta = normalize_symbol_for_evidence(symbol=symbol, market=market)
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(lookback_days)))

    for token in symbol_meta["aliases"]:
        lookup_symbol = f"{token}.AX" if symbol_meta["market"] == "AU" and token.isalpha() and not token.endswith(".AX") else token
        try:
            latest = _SOURCES_REPO.get_latest_breakdown(symbol=lookup_symbol, scanner_type=group)
        except Exception:
            latest = None
        if not latest:
            continue
        created_at = _parse_iso(latest.get("created_at"))
        if created_at and created_at < cutoff:
            continue
        payload = latest.get("payload") if isinstance(latest.get("payload"), dict) else {}
        totals = _source_totals_from_payload(payload)
        return {"totals": totals, "per_source": _per_source_from_payload(payload), "source": "repo"}

    if not _LIVE_EVIDENCE_LOOKUP:
        return {
            "totals": {
                "posts": 0,
                "mentions": 0,
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "net": 0,
                "confidence": 0.0,
            },
            "per_source": {},
            "source": "none",
        }

    runtime = _runtime_group_analysis(symbol_query=symbol_meta["base"], group=group, lookback_days=lookback_days)
    return {"totals": runtime.get("totals") or _empty_evidence(), "per_source": runtime.get("per_source") or {}, "source": "runtime"}


def _evidence_score(group_totals: Dict[str, Dict[str, Any]]) -> Tuple[float, float]:
    weights = {"social": 0.25, "news": 0.35, "institution": 0.40}
    score = 0.0
    conf = 0.0
    for group, weight in weights.items():
        totals = group_totals.get(group) if isinstance(group_totals.get(group), dict) else {}
        posts = int(totals.get("posts") or 0)
        net = int(totals.get("net") or 0)
        if posts <= 0:
            continue
        sentiment = max(-1.0, min(1.0, float(net) / float(max(posts, 1))))
        volume = min(1.0, float(posts) / 30.0)
        group_score = (sentiment * 75.0) + (volume * 25.0)
        group_conf = max(0.05, min(1.0, float(totals.get("confidence") or 0.0) + (volume * 0.25)))
        score += group_score * weight
        conf += group_conf * weight
    return (max(-30.0, min(30.0, score)), max(0.0, min(1.0, conf)))


def build_normalized_evidence(
    source_counts: Dict[str, Any],
    source_breakdown: Dict[str, Any],
    raw_social_hits: Optional[Dict[str, Any]] = None,
    raw_news_hits: Optional[Dict[str, Any]] = None,
    raw_institution_hits: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    evidence = _empty_evidence()
    counts = _empty_source_counts()
    if isinstance(source_counts, dict):
        counts["social"] = _safe_int(source_counts.get("social"))
        counts["news"] = _safe_int(source_counts.get("news"))
        counts["institution"] = _safe_int(source_counts.get("institution"))

    breakdown_totals = _breakdown_totals_from_source_breakdown(
        source_breakdown if isinstance(source_breakdown, dict) else _empty_source_breakdown()
    )
    counts["social"] = max(counts["social"], breakdown_totals["social"])
    counts["news"] = max(counts["news"], breakdown_totals["news"])
    counts["institution"] = max(counts["institution"], breakdown_totals["institution"])

    group_hits = {
        "social": raw_social_hits if isinstance(raw_social_hits, dict) else {},
        "news": raw_news_hits if isinstance(raw_news_hits, dict) else {},
        "institution": raw_institution_hits if isinstance(raw_institution_hits, dict) else {},
    }

    sentiment_known = False
    for group_name, payload in group_hits.items():
        group_count = counts[group_name]
        posts = _safe_int(payload.get("posts") or payload.get("mentions"))
        mentions = _safe_int(payload.get("mentions") or payload.get("posts"))
        positive = _safe_int(payload.get("positive"))
        negative = _safe_int(payload.get("negative"))
        neutral = _safe_int(payload.get("neutral"))
        if posts <= 0 and group_count > 0:
            posts = group_count
        if mentions <= 0 and posts > 0:
            mentions = posts
        if posts > 0 and positive + negative + neutral <= 0:
            neutral = posts
        if positive > 0 or negative > 0:
            sentiment_known = True
        evidence["posts"] += posts
        evidence["mentions"] += mentions
        evidence["positive"] += positive
        evidence["negative"] += negative
        evidence["neutral"] += neutral

    if evidence["posts"] <= 0:
        # Fallback when only source counts/breakdowns are available.
        fallback_posts = counts["social"] + counts["news"] + counts["institution"]
        if fallback_posts > 0:
            evidence["posts"] = fallback_posts
            evidence["mentions"] = fallback_posts
            evidence["neutral"] = fallback_posts

    if evidence["mentions"] <= 0 and evidence["posts"] > 0:
        evidence["mentions"] = evidence["posts"]

    if evidence["positive"] + evidence["negative"] + evidence["neutral"] <= 0 and evidence["posts"] > 0:
        evidence["neutral"] = evidence["posts"]

    evidence["net"] = evidence["positive"] - evidence["negative"]
    if not sentiment_known and evidence["posts"] > 0 and evidence["positive"] == 0 and evidence["negative"] == 0:
        evidence["neutral"] = max(evidence["neutral"], evidence["posts"])
    return evidence


def compute_evidence_score(
    evidence: Dict[str, Any],
    source_counts: Dict[str, Any],
    source_breakdown: Dict[str, Any],
) -> Dict[str, Any]:
    counts = _empty_source_counts()
    if isinstance(source_counts, dict):
        counts["social"] = _safe_int(source_counts.get("social"))
        counts["news"] = _safe_int(source_counts.get("news"))
        counts["institution"] = _safe_int(source_counts.get("institution"))

    breakdown = source_breakdown if isinstance(source_breakdown, dict) else _empty_source_breakdown()
    breakdown_totals = _breakdown_totals_from_source_breakdown(breakdown)
    counts["social"] = max(counts["social"], breakdown_totals["social"])
    counts["news"] = max(counts["news"], breakdown_totals["news"])
    counts["institution"] = max(counts["institution"], breakdown_totals["institution"])

    total_presence = counts["social"] + counts["news"] + counts["institution"]
    if total_presence <= 0:
        return {"evidence_score_raw": 0.0, "evidence_confidence": 0.0, "evidence_state": "evidence_unavailable"}

    ev_posts = _safe_int(evidence.get("posts"))
    ev_positive = _safe_int(evidence.get("positive"))
    ev_negative = _safe_int(evidence.get("negative"))
    sentiment_known = (ev_positive + ev_negative) > 0
    polarity_score = (
        max(-1.0, min(1.0, float(ev_positive - ev_negative) / float(max(ev_posts, 1)))) if sentiment_known else 0.0
    )

    social_breakdown = breakdown.get("social") if isinstance(breakdown.get("social"), dict) else {}
    news_breakdown = breakdown.get("news") if isinstance(breakdown.get("news"), dict) else {}
    institution_breakdown = (
        breakdown.get("institution") if isinstance(breakdown.get("institution"), dict) else {}
    )
    social_div = _count_non_zero([_safe_int(v) for v in social_breakdown.values()]) / 6.0
    news_div = _count_non_zero([_safe_int(v) for v in news_breakdown.values()]) / 2.0
    institution_div = _count_non_zero([_safe_int(v) for v in institution_breakdown.values()]) / 4.0

    social_vol = min(1.0, counts["social"] / 20.0)
    news_vol = min(1.0, counts["news"] / 20.0)
    institution_vol = min(1.0, counts["institution"] / 20.0)

    social_component = 0.6 * social_vol + 0.4 * social_div + (0.5 * polarity_score if sentiment_known else 0.0)
    news_component = 0.6 * news_vol + 0.4 * news_div + (0.5 * polarity_score if sentiment_known else 0.0)
    institution_component = (
        0.6 * institution_vol + 0.4 * institution_div + (0.5 * polarity_score if sentiment_known else 0.0)
    )

    weighted = (0.25 * social_component) + (0.35 * news_component) + (0.40 * institution_component)
    evidence_score_raw = max(0.0, min(20.0, 10.0 * weighted))

    diversity_global = (
        _count_non_zero([counts["social"], counts["news"], counts["institution"]]) / 3.0
    )
    volume_conf = min(1.0, total_presence / 40.0)
    sentiment_conf = 0.20 if sentiment_known else 0.0
    evidence_confidence = max(0.05, min(0.95, (0.45 * volume_conf) + (0.35 * diversity_global) + sentiment_conf))

    evidence_state = "available" if sentiment_known else "evidence_present_unclassified"
    return {
        "evidence_score_raw": float(evidence_score_raw),
        "evidence_confidence": float(evidence_confidence),
        "evidence_state": evidence_state,
    }


def aggregate_evidence(social: Dict[str, Any], news: Dict[str, Any], institution: Dict[str, Any]) -> Dict[str, Any]:
    source_counts = _empty_source_counts()
    source_breakdown = _empty_source_breakdown()
    per_group_totals: Dict[str, Dict[str, Any]] = {
        "social": {},
        "news": {},
        "institution": {},
    }

    for group_name, group_payload in (("social", social), ("news", news), ("institution", institution)):
        totals = group_payload.get("totals") if isinstance(group_payload.get("totals"), dict) else _empty_evidence()
        per_source = group_payload.get("per_source") if isinstance(group_payload.get("per_source"), dict) else {}
        per_group_totals[group_name] = totals
        per_source_posts = sum(int(row.get("posts") or 0) for row in per_source.values())
        source_counts[group_name] = max(
            int(totals.get("posts") or totals.get("mentions") or 0),
            int(per_source_posts),
        )

        if group_name == "social":
            for source_key, row in per_source.items():
                posts = int(row.get("posts") or 0)
                mapped = source_key
                if "twitter" in source_key:
                    mapped = "x"
                elif "youtube" in source_key:
                    mapped = "youtube"
                elif "hotcopper" in source_key:
                    mapped = "hotcopper"
                elif "facebook" in source_key:
                    mapped = "facebook"
                elif "tiktok" in source_key:
                    mapped = "tiktok"
                elif "reddit" in source_key:
                    mapped = "reddit"
                else:
                    mapped = "x"
                source_breakdown["social"][mapped] += posts
        elif group_name == "news":
            article_count = 0
            publishers = 0
            for _source_key, row in per_source.items():
                posts = int(row.get("posts") or 0)
                article_count += posts
                if posts > 0:
                    publishers += 1
            source_breakdown["news"]["articles"] += article_count
            source_breakdown["news"]["publishers"] += publishers
        elif group_name == "institution":
            for source_key, row in per_source.items():
                posts = int(row.get("posts") or 0)
                positive = int(row.get("positive") or 0)
                negative = int(row.get("negative") or 0)
                if any(token in source_key for token in ("edgar", "13f", "holding", "insider", "filing")):
                    source_breakdown["institution"]["filings"] += posts
                if "analyst" in source_key or "rating" in source_key:
                    source_breakdown["institution"]["upgrades"] += positive
                    source_breakdown["institution"]["downgrades"] += negative
                if any(token in source_key for token in ("broker", "flow", "options", "short_interest", "etf")):
                    source_breakdown["institution"]["unusual_volume"] += posts

    evidence = build_normalized_evidence(
        source_counts=source_counts,
        source_breakdown=source_breakdown,
        raw_social_hits=per_group_totals.get("social"),
        raw_news_hits=per_group_totals.get("news"),
        raw_institution_hits=per_group_totals.get("institution"),
    )
    score_meta = compute_evidence_score(
        evidence=evidence,
        source_counts=source_counts,
        source_breakdown=source_breakdown,
    )

    return {
        "evidence": evidence,
        "evidence_summary": evidence,
        "source_counts": source_counts,
        "source_breakdown": source_breakdown,
        "evidence_score_raw": float(score_meta.get("evidence_score_raw") or 0.0),
        "evidence_confidence": float(score_meta.get("evidence_confidence") or 0.0),
        "evidence_state": str(score_meta.get("evidence_state") or "evidence_unavailable"),
    }


def get_social_evidence(symbol: str, market: str, lookback_days: int = 7) -> Dict[str, Any]:
    return _group_evidence(symbol=symbol, market=market, group="social", lookback_days=lookback_days)


def get_news_evidence(symbol: str, market: str, lookback_days: int = 7) -> Dict[str, Any]:
    return _group_evidence(symbol=symbol, market=market, group="news", lookback_days=lookback_days)


def get_institution_evidence(symbol: str, market: str, lookback_days: int = 30) -> Dict[str, Any]:
    return _group_evidence(symbol=symbol, market=market, group="institution", lookback_days=lookback_days)


def get_symbol_evidence(symbol: str, market: str, lookback_days: int = 7) -> Dict[str, Any]:
    symbol_u = str(symbol or "").strip().upper()
    market_u = str(market or ("AU" if symbol_u.endswith(".AX") else "US")).strip().upper()
    cache_key = _cache_key(symbol_u, market_u, lookback_days)
    with _CACHE_LOCK:
        cached = _EVIDENCE_CACHE.get(cache_key)
        if cached and (time.time() - cached[0]) <= _CACHE_TTL_SECONDS:
            return dict(cached[1])

    social = get_social_evidence(symbol=symbol_u, market=market_u, lookback_days=lookback_days)
    news = get_news_evidence(symbol=symbol_u, market=market_u, lookback_days=lookback_days)
    institution = get_institution_evidence(symbol=symbol_u, market=market_u, lookback_days=max(lookback_days, 30))
    aggregated = aggregate_evidence(social=social, news=news, institution=institution)

    with _CACHE_LOCK:
        _EVIDENCE_CACHE[cache_key] = (time.time(), dict(aggregated))
    return aggregated

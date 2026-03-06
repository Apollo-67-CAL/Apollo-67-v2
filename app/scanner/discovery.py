from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from app.providers.selector import get_quote_cached_first
from core.scanners.discovery import discover_market_segment

EvidenceLookup = Callable[[str, str], Dict[str, Any]]


def _as_float_or_none(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _normalise_symbol(symbol: Any) -> str:
    return str(symbol or "").strip().upper()


def _market_from_symbol(symbol: str, default_market: str) -> str:
    if symbol.endswith(".AX"):
        return "AU"
    return "US" if default_market == "ALL" else default_market


def _market_list(market: str) -> List[str]:
    market_u = str(market or "ALL").strip().upper()
    if market_u == "ALL":
        return ["US", "AU"]
    if market_u in {"US", "AU"}:
        return [market_u]
    return ["US"]


def get_top_movers(market: str, segment: str = "small", pool_size: int = 240, force_refresh: bool = False) -> List[Dict[str, Any]]:
    rows = discover_market_segment(
        market=str(market or "US").strip().upper(),
        segment=str(segment or "small").strip().lower(),
        limit=max(1, min(int(pool_size), 500)),
        force_refresh=bool(force_refresh),
    )
    return sorted(
        list(rows or []),
        key=lambda row: _as_float_or_none(row.get("change_pct")) or float("-inf"),
        reverse=True,
    )


def get_volume_surge(market: str, segment: str = "small", pool_size: int = 240, force_refresh: bool = False) -> List[Dict[str, Any]]:
    # Stage-1 deliberately avoids bar fetches. Use change magnitude as lightweight activity proxy.
    rows = get_top_movers(market=market, segment=segment, pool_size=pool_size, force_refresh=force_refresh)
    return sorted(
        rows,
        key=lambda row: abs(_as_float_or_none(row.get("change_pct")) or 0.0),
        reverse=True,
    )


def discover_candidates(
    market: str = "ALL",
    segment: str = "small",
    per_market_limit: int = 30,
    pool_size: int = 240,
    include_symbols: Optional[List[str]] = None,
    force_refresh: bool = False,
    quote_live_budget: int = 12,
    evidence_lookup: Optional[EvidenceLookup] = None,
) -> Dict[str, Any]:
    markets = _market_list(market)
    segment_value = str(segment or "small").strip().lower()
    top_n = max(1, min(int(per_market_limit), 50))
    pool_n = max(1, min(int(pool_size), 500))
    live_budget = max(0, min(int(quote_live_budget), 200))

    include_by_market: Dict[str, List[str]] = {"US": [], "AU": []}
    for raw in include_symbols or []:
        sym = _normalise_symbol(raw)
        if not sym:
            continue
        mk = _market_from_symbol(sym, str(market or "ALL").strip().upper())
        bucket = include_by_market.setdefault(mk, [])
        if sym not in bucket:
            bucket.append(sym)

    merged_candidates: List[Dict[str, Any]] = []
    per_market: Dict[str, List[Dict[str, Any]]] = {}
    scanned_by_market: Dict[str, int] = {}
    quote_ok_by_market: Dict[str, int] = {}
    universe_size_by_market: Dict[str, int] = {}

    for mk in markets:
        movers = get_top_movers(market=mk, segment=segment_value, pool_size=pool_n, force_refresh=force_refresh)
        active = get_volume_surge(market=mk, segment=segment_value, pool_size=pool_n, force_refresh=force_refresh)
        surge_set = {_normalise_symbol(row.get("symbol")) for row in active[: max(top_n * 4, 40)]}

        universe_size_by_market[mk] = len(movers)
        seen: set[str] = set()
        ordered_rows: List[Dict[str, Any]] = []
        for row in movers:
            sym = _normalise_symbol(row.get("symbol"))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            ordered_rows.append(dict(row))

        for sym in include_by_market.get(mk, []):
            if sym in seen:
                continue
            seen.add(sym)
            ordered_rows.append(
                {
                    "symbol": sym,
                    "display_symbol": sym,
                    "market": mk,
                    "segment": segment_value,
                    "price": None,
                    "change_pct": None,
                    "provider_used": "priority",
                }
            )

        scanned_by_market[mk] = 0
        quote_ok_by_market[mk] = 0
        market_candidates: List[Dict[str, Any]] = []
        market_live_budget = live_budget

        for base in ordered_rows:
            symbol = _normalise_symbol(base.get("symbol"))
            if not symbol:
                continue
            scanned_by_market[mk] += 1
            price = _as_float_or_none(base.get("price"))
            quote_provider = str(base.get("provider_used") or "")

            if price is None or price <= 0:
                try:
                    quote_result = get_quote_cached_first(
                        symbol=symbol,
                        max_age_seconds=2 * 60 * 60,
                        allow_live=market_live_budget > 0,
                        freshness_seconds=60,
                    )
                    quote_last = _as_float_or_none(getattr(quote_result.quote, "last", None))
                    if quote_last is not None and quote_last > 0:
                        price = quote_last
                        quote_provider = quote_result.provider
                        quote_ok_by_market[mk] += 1
                        if market_live_budget > 0:
                            market_live_budget -= 1
                except Exception:
                    pass
            else:
                quote_ok_by_market[mk] += 1

            if price is None or price <= 0:
                continue

            evidence = evidence_lookup(symbol, mk) if evidence_lookup else {}
            source_counts = evidence.get("source_counts") if isinstance(evidence.get("source_counts"), dict) else {}
            evidence_summary = evidence.get("evidence_summary") if isinstance(evidence.get("evidence_summary"), dict) else {}
            posts = int(evidence_summary.get("posts") or 0)
            net = int(evidence_summary.get("net") or 0)

            change_pct = _as_float_or_none(base.get("change_pct"))
            change_component = _clamp((change_pct or 0.0) * 4.0, -40.0, 40.0)
            activity_component = _clamp(min(abs(change_pct or 0.0), 12.0) * 1.8, 0.0, 18.0)
            if symbol in surge_set:
                activity_component += 4.0
            evidence_component = _clamp((net * 2.0) + (min(posts, 30) * 0.45), -24.0, 24.0)

            score_prelim = _clamp(change_component + activity_component + evidence_component, -100.0, 100.0)
            confidence_prelim = _clamp(
                0.28
                + min(abs(change_component) / 120.0, 0.30)
                + min(activity_component / 100.0, 0.20)
                + min(max(evidence_component, 0.0) / 120.0, 0.18),
                0.2,
                0.9,
            )

            market_candidates.append(
                {
                    "symbol": symbol,
                    "display_symbol": _normalise_symbol(base.get("display_symbol") or symbol),
                    "market": mk,
                    "segment": segment_value,
                    "price": price,
                    "change_pct": change_pct,
                    "score_prelim": score_prelim,
                    "confidence_prelim": confidence_prelim,
                    "provider_used": quote_provider or base.get("provider_used") or "cache",
                    "evidence_summary": {
                        "posts": posts,
                        "mentions": int(evidence_summary.get("mentions") or posts),
                        "positive": int(evidence_summary.get("positive") or 0),
                        "negative": int(evidence_summary.get("negative") or 0),
                        "neutral": int(evidence_summary.get("neutral") or max(0, posts - int(evidence_summary.get("positive") or 0) - int(evidence_summary.get("negative") or 0))),
                        "net": net,
                    },
                    "source_counts": source_counts,
                    "stage": "candidate",
                }
            )

        market_candidates.sort(
            key=lambda row: (
                _as_float_or_none(row.get("score_prelim")) or float("-inf"),
                _as_float_or_none(row.get("confidence_prelim")) or float("-inf"),
            ),
            reverse=True,
        )
        top_market = market_candidates[:top_n]
        per_market[mk] = top_market
        merged_candidates.extend(top_market)

    merged_candidates.sort(
        key=lambda row: (
            _as_float_or_none(row.get("score_prelim")) or float("-inf"),
            _as_float_or_none(row.get("confidence_prelim")) or float("-inf"),
        ),
        reverse=True,
    )

    return {
        "markets": per_market,
        "merged": merged_candidates,
        "universe_size_by_market": universe_size_by_market,
        "scanned_by_market": scanned_by_market,
        "quote_ok_by_market": quote_ok_by_market,
        "universe_size": sum(int(v or 0) for v in universe_size_by_market.values()),
        "scanned_count": sum(int(v or 0) for v in scanned_by_market.values()),
        "quote_ok_count": sum(int(v or 0) for v in quote_ok_by_market.values()),
    }

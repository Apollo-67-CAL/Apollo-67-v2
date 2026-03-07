from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.providers.massive import MassiveClient
from app.providers.selector import get_bars_cached_first
from app.providers.selector import get_quote_cached_first

EvidenceLookup = Callable[[str, str], Dict[str, Any]]
ProgressCallback = Callable[[Dict[str, Any]], None]
_ROOT_DIR = Path(__file__).resolve().parents[2]
_ROOT_DATA_DIR = _ROOT_DIR / "data"
_UNIVERSE_FILE_SMALL = {
    "US": _ROOT_DATA_DIR / "universe_us_small.csv",
    "AU": _ROOT_DATA_DIR / "universe_au_small.csv",
}
_UNIVERSE_ROTATE_SECONDS = 600
_MASSIVE_GROUPED_TTL_SECONDS = 5 * 60
_MASSIVE_GROUPED_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


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


def _massive_enabled() -> bool:
    raw = os.getenv("MASSIVE_ENABLED", "1")
    enabled = str(raw).strip().lower() in {"1", "true", "yes", "on"}
    return enabled and bool(os.getenv("MASSIVE_API_KEY", "").strip())


def _massive_get_grouped_daily(date: Optional[str] = None, refresh: bool = False) -> List[Dict[str, Any]]:
    cache_key = str(date or "latest").strip() or "latest"
    now = time.monotonic()
    if not refresh:
        cached = _MASSIVE_GROUPED_CACHE.get(cache_key)
        if cached and cached[0] >= now:
            return list(cached[1])
    if not _massive_enabled():
        return []
    try:
        client = MassiveClient()
        payload = client.get_grouped_daily(date=date, market="US")
    except Exception:
        return []
    rows = payload.get("results") if isinstance(payload.get("results"), list) else []
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = _normalise_symbol(row.get("symbol"))
        if not symbol or symbol.endswith(".AX"):
            continue
        normalized.append(
            {
                "symbol": symbol,
                "display_symbol": symbol,
                "market": "US",
                "segment": "small",
                "price": _as_float_or_none(row.get("close")),
                "change_pct": _as_float_or_none(row.get("change_pct")),
                "volume": _as_float_or_none(row.get("volume")),
                "provider_used": "massive_grouped_daily",
                "grouped_daily_provider_used": "massive",
            }
        )
    _MASSIVE_GROUPED_CACHE[cache_key] = (now + _MASSIVE_GROUPED_TTL_SECONDS, normalized)
    return list(normalized)


def _read_symbols_file(path: Path, market: str) -> List[str]:
    if not path.exists():
        return []
    symbols: List[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    if not rows:
        return []

    header = [str(col or "").strip().lower() for col in rows[0]]
    symbol_idx = None
    for key in ("symbol", "ticker", "code"):
        if key in header:
            symbol_idx = header.index(key)
            break
    start_idx = 1 if symbol_idx is not None else 0

    for row in rows[start_idx:]:
        if not row:
            continue
        raw = row[symbol_idx] if symbol_idx is not None and symbol_idx < len(row) else row[0]
        symbol = _normalise_symbol(raw)
        if not symbol or symbol.startswith("#"):
            continue
        if market == "AU" and not symbol.endswith(".AX"):
            symbol = f"{symbol}.AX"
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _load_universe_for_market(market: str, segment: str) -> Tuple[List[str], List[str]]:
    market_u = str(market or "US").strip().upper()
    segment_v = str(segment or "small").strip().lower()
    errors: List[str] = []
    if segment_v != "small":
        errors.append(f"segment {segment_v} not configured for file-backed universe")
        return [], errors

    path = _UNIVERSE_FILE_SMALL.get(market_u)
    if path is None:
        errors.append(f"market {market_u} not supported")
        return [], errors
    if not path.exists():
        errors.append(f"{path.name} missing")
        return [], errors

    symbols = _read_symbols_file(path, market_u)
    if not symbols:
        errors.append(f"{path.name} empty")
    return symbols, errors


def _load_universe_with_meta(market: str, segment: str) -> Tuple[List[str], Dict[str, int], List[str], List[str]]:
    markets = _market_list(market)
    merged: List[str] = []
    seen: set[str] = set()
    sources: Dict[str, int] = {}
    errors: List[str] = []
    for mk in markets:
        symbols, load_errors = _load_universe_for_market(mk, segment)
        sources[mk] = len(symbols)
        errors.extend(load_errors)
        for sym in symbols:
            if sym in seen:
                continue
            seen.add(sym)
            merged.append(sym)
    if str(market or "ALL").strip().upper() == "ALL" and not merged:
        errors.append("market ALL merge returned 0 symbols")
    return merged, sources, errors, merged[:10]


def load_universe(market: str, segment: str) -> List[str]:
    symbols, _, _, _ = _load_universe_with_meta(market=market, segment=segment)
    return symbols


def universe_health(market: str, segment: str) -> Dict[str, Any]:
    symbols, sources, errors, first_10 = _load_universe_with_meta(market=market, segment=segment)
    return {
        "universe_size": len(symbols),
        "universe_sources": sources,
        "universe_errors": errors,
        "first_10_symbols": first_10,
    }


def _sample_symbols(symbols: List[str], sample_size: int) -> List[str]:
    if not symbols:
        return []
    if len(symbols) <= sample_size:
        return symbols
    bucket = int(time.time() // _UNIVERSE_ROTATE_SECONDS)
    start = bucket % len(symbols)
    out: List[str] = []
    idx = start
    while len(out) < sample_size:
        out.append(symbols[idx % len(symbols)])
        idx += 1
    return out


def _resolve_change_pct(symbol: str) -> Optional[float]:
    try:
        bars_result = get_bars_cached_first(
            symbol=symbol,
            interval="1day",
            outputsize=2,
            max_age_seconds=12 * 60 * 60,
            allow_live=False,
        )
    except Exception:
        return None
    bars = bars_result.bars if hasattr(bars_result, "bars") else []
    if not isinstance(bars, list) or len(bars) < 2:
        return None
    try:
        prev = bars[-2]
        last = bars[-1]
        prev_close = float(prev.close if hasattr(prev, "close") else prev.get("close"))
        last_close = float(last.close if hasattr(last, "close") else last.get("close"))
    except Exception:
        return None
    if prev_close <= 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def _resolve_cached_last_close(symbol: str) -> Optional[float]:
    try:
        bars_result = get_bars_cached_first(
            symbol=symbol,
            interval="1day",
            outputsize=2,
            max_age_seconds=24 * 60 * 60,
            allow_live=False,
        )
    except Exception:
        return None
    bars = bars_result.bars if hasattr(bars_result, "bars") else []
    if not isinstance(bars, list) or not bars:
        return None
    try:
        last = bars[-1]
        close_val = float(last.close if hasattr(last, "close") else last.get("close"))
    except Exception:
        return None
    return close_val if close_val > 0 else None


def get_massive_us_daily_movers(limit: int = 120, force_refresh: bool = False) -> List[Dict[str, Any]]:
    rows = _massive_get_grouped_daily(refresh=force_refresh)
    out = sorted(
        rows,
        key=lambda row: _as_float_or_none(row.get("change_pct")) or float("-inf"),
        reverse=True,
    )
    return out[: max(1, min(int(limit), 500))]


def get_massive_us_active_names(limit: int = 120, force_refresh: bool = False) -> List[Dict[str, Any]]:
    rows = _massive_get_grouped_daily(refresh=force_refresh)
    out = sorted(
        rows,
        key=lambda row: _as_float_or_none(row.get("volume")) or float("-inf"),
        reverse=True,
    )
    return out[: max(1, min(int(limit), 500))]


def get_top_movers(market: str, segment: str = "small", pool_size: int = 240, force_refresh: bool = False) -> List[Dict[str, Any]]:
    market_u = str(market or "US").strip().upper()
    if market_u == "US":
        massive_rows = get_massive_us_daily_movers(limit=max(40, min(int(pool_size), 500)), force_refresh=force_refresh)
        if massive_rows:
            return massive_rows

    del force_refresh  # file-backed fallback path
    symbols = load_universe(market_u, segment)
    sampled = _sample_symbols(symbols, max(1, min(int(pool_size), 500)))
    rows: List[Dict[str, Any]] = []
    for symbol in sampled:
        rows.append(
            {
                "symbol": symbol,
                "display_symbol": symbol,
                "market": market_u,
                "segment": str(segment or "small").strip().lower(),
                "price": None,
                "change_pct": _resolve_change_pct(symbol),
                "provider_used": "universe",
            }
        )
    return sorted(
        rows,
        key=lambda row: _as_float_or_none(row.get("change_pct")) or float("-inf"),
        reverse=True,
    )


def get_volume_surge(market: str, segment: str = "small", pool_size: int = 240, force_refresh: bool = False) -> List[Dict[str, Any]]:
    market_u = str(market or "US").strip().upper()
    if market_u == "US":
        active_rows = get_massive_us_active_names(limit=max(40, min(int(pool_size), 500)), force_refresh=force_refresh)
        if active_rows:
            return active_rows

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
    on_progress: Optional[ProgressCallback] = None,
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
    grouped_daily_provider_by_market: Dict[str, Optional[str]] = {}
    universe_errors: List[str] = []
    first_10_symbols: List[str] = []

    _, universe_sources, load_errors, first_10 = _load_universe_with_meta(market=market, segment=segment_value)
    if load_errors:
        universe_errors.extend(load_errors)
    first_10_symbols = first_10
    progress_every = max(20, min(top_n, 40))

    def _emit_progress(extra_market_rows: Optional[List[Dict[str, Any]]] = None, market_key: Optional[str] = None) -> None:
        if on_progress is None:
            return
        try:
            merged_preview: List[Dict[str, Any]] = []
            for mk_key, rows in per_market.items():
                if isinstance(rows, list):
                    merged_preview.extend(rows[:top_n])
            if market_key and isinstance(extra_market_rows, list):
                merged_preview.extend(extra_market_rows[:top_n])
            merged_preview.sort(
                key=lambda row: (
                    _as_float_or_none(row.get("score_prelim")) or float("-inf"),
                    _as_float_or_none(row.get("confidence_prelim")) or float("-inf"),
                ),
                reverse=True,
            )
            on_progress(
                {
                    "markets": per_market,
                    "merged": merged_preview[: max(top_n * 2, 40)],
                    "universe_sources": universe_size_by_market,
                    "universe_errors": universe_errors,
                    "first_10_symbols": first_10_symbols,
                    "universe_size_by_market": universe_size_by_market,
                    "scanned_by_market": scanned_by_market,
                    "quote_ok_by_market": quote_ok_by_market,
                    "grouped_daily_provider_by_market": grouped_daily_provider_by_market,
                    "universe_size": sum(int(v or 0) for v in universe_size_by_market.values()),
                    "scanned_count": sum(int(v or 0) for v in scanned_by_market.values()),
                    "quote_ok_count": sum(int(v or 0) for v in quote_ok_by_market.values()),
                }
            )
        except Exception:
            pass

    for mk in markets:
        market_symbols, market_errors = _load_universe_for_market(mk, segment_value)
        if market_errors:
            universe_errors.extend(market_errors)
        market_symbol_set = {_normalise_symbol(sym) for sym in market_symbols}
        movers = get_top_movers(market=mk, segment=segment_value, pool_size=pool_n, force_refresh=force_refresh)
        active = get_volume_surge(market=mk, segment=segment_value, pool_size=pool_n, force_refresh=force_refresh)
        if mk == "US" and segment_value == "small" and market_symbol_set:
            movers = [row for row in movers if _normalise_symbol(row.get("symbol")) in market_symbol_set]
            active = [row for row in active if _normalise_symbol(row.get("symbol")) in market_symbol_set]
        surge_set = {_normalise_symbol(row.get("symbol")) for row in active[: max(top_n * 4, 40)]}

        universe_size_by_market[mk] = len(market_symbols)
        grouped_daily_provider_by_market[mk] = None
        seen: set[str] = set()
        ordered_rows: List[Dict[str, Any]] = []
        for row in movers:
            sym = _normalise_symbol(row.get("symbol"))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            row_payload = dict(row)
            grouped_provider = str(row_payload.get("grouped_daily_provider_used") or "").strip().lower() or None
            if grouped_provider:
                grouped_daily_provider_by_market[mk] = grouped_provider
            ordered_rows.append(row_payload)

        for sym in _sample_symbols(market_symbols, pool_n):
            sym_u = _normalise_symbol(sym)
            if not sym_u or sym_u in seen:
                continue
            seen.add(sym_u)
            ordered_rows.append(
                {
                    "symbol": sym_u,
                    "display_symbol": sym_u,
                    "market": mk,
                    "segment": segment_value,
                    "price": None,
                    "change_pct": None,
                    "provider_used": "universe",
                }
            )

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
                cached_close = _resolve_cached_last_close(symbol)
                if cached_close is not None and cached_close > 0:
                    price = cached_close
                    quote_provider = quote_provider or "bars_cache"
                    quote_ok_by_market[mk] += 1

            if price is None or price <= 0:
                continue

            evidence = evidence_lookup(symbol, mk) if evidence_lookup else {}
            source_counts = (
                evidence.get("source_counts")
                if isinstance(evidence.get("source_counts"), dict)
                else {"social": 0, "news": 0, "institution": 0}
            )
            source_breakdown = (
                evidence.get("source_breakdown")
                if isinstance(evidence.get("source_breakdown"), dict)
                else {
                    "social": {"reddit": 0, "x": 0, "hotcopper": 0, "youtube": 0, "facebook": 0, "tiktok": 0},
                    "news": {"articles": 0, "publishers": 0},
                    "institution": {"filings": 0, "upgrades": 0, "downgrades": 0, "unusual_volume": 0},
                }
            )
            evidence_summary = (
                evidence.get("evidence_summary")
                if isinstance(evidence.get("evidence_summary"), dict)
                else (evidence.get("evidence") if isinstance(evidence.get("evidence"), dict) else {})
            )
            evidence_score_raw = _as_float_or_none(evidence.get("evidence_score_raw")) or 0.0
            evidence_confidence = _as_float_or_none(evidence.get("evidence_confidence")) or 0.0
            evidence_state = str(evidence.get("evidence_state") or "").strip().lower() or "evidence_unavailable"
            posts = int(evidence_summary.get("posts") or 0)
            net = int(evidence_summary.get("net") or 0)

            change_pct = _as_float_or_none(base.get("change_pct"))
            change_component = _clamp((change_pct or 0.0) * 4.0, -40.0, 40.0)
            activity_component = _clamp(min(abs(change_pct or 0.0), 12.0) * 1.8, 0.0, 18.0)
            if symbol in surge_set:
                activity_component += 4.0
            evidence_component_fallback = _clamp((net * 2.0) + (min(posts, 30) * 0.45), -24.0, 24.0)
            evidence_component = _clamp(evidence_score_raw if abs(evidence_score_raw) > 0 else evidence_component_fallback, -30.0, 30.0)

            score_prelim = _clamp(change_component + activity_component + evidence_component, -100.0, 100.0)
            confidence_prelim = _clamp(
                0.28
                + min(abs(change_component) / 120.0, 0.30)
                + min(activity_component / 100.0, 0.20)
                + min(max(evidence_component, 0.0) / 120.0, 0.18),
                0.2,
                0.9,
            )
            confidence_prelim = _clamp(max(confidence_prelim, evidence_confidence), 0.2, 0.92)

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
                    "grouped_daily_provider_used": base.get("grouped_daily_provider_used"),
                    "evidence_summary": {
                        "posts": posts,
                        "mentions": int(evidence_summary.get("mentions") or posts),
                        "positive": int(evidence_summary.get("positive") or 0),
                        "negative": int(evidence_summary.get("negative") or 0),
                        "neutral": int(evidence_summary.get("neutral") or max(0, posts - int(evidence_summary.get("positive") or 0) - int(evidence_summary.get("negative") or 0))),
                        "net": net,
                    },
                    "source_counts": source_counts,
                    "source_breakdown": source_breakdown,
                    "evidence_score_raw": evidence_score_raw,
                    "evidence_confidence": evidence_confidence,
                    "evidence_state": evidence_state,
                    "stage": "candidate",
                }
            )
            if scanned_by_market[mk] % progress_every == 0:
                snapshot = sorted(
                    market_candidates,
                    key=lambda row: (
                        _as_float_or_none(row.get("score_prelim")) or float("-inf"),
                        _as_float_or_none(row.get("confidence_prelim")) or float("-inf"),
                    ),
                    reverse=True,
                )
                _emit_progress(extra_market_rows=snapshot, market_key=mk)

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
        _emit_progress()

    merged_candidates.sort(
        key=lambda row: (
            _as_float_or_none(row.get("score_prelim")) or float("-inf"),
            _as_float_or_none(row.get("confidence_prelim")) or float("-inf"),
        ),
        reverse=True,
    )
    _emit_progress()

    return {
        "markets": per_market,
        "merged": merged_candidates,
        "universe_sources": universe_size_by_market,
        "universe_errors": universe_errors,
        "first_10_symbols": first_10_symbols,
        "universe_size_by_market": universe_size_by_market,
        "scanned_by_market": scanned_by_market,
        "quote_ok_by_market": quote_ok_by_market,
        "grouped_daily_provider_by_market": grouped_daily_provider_by_market,
        "universe_size": sum(int(v or 0) for v in universe_size_by_market.values()),
        "scanned_count": sum(int(v or 0) for v in scanned_by_market.values()),
        "quote_ok_count": sum(int(v or 0) for v in quote_ok_by_market.values()),
    }

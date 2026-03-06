from __future__ import annotations

import time
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.providers.selector import get_bars_cached_first, get_quote_with_fallback

_BASE_DIR = Path(__file__).resolve().parents[2]
_DATA_DIR = _BASE_DIR / "app" / "data"
_ROOT_DATA_DIR = _BASE_DIR / "data"

_CACHE_TTL_SECONDS = 600
_DISCOVERY_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_symbols(filename: str) -> List[str]:
    path = Path(filename)
    if not path.is_absolute():
        path = _DATA_DIR / filename
    if not path.exists():
        return []
    out: List[str] = []
    seen = set()
    if path.suffix.lower() == ".csv":
        try:
            with path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw = row.get("symbol") or row.get("ticker") or row.get("code")
                    symbol = str(raw or "").strip().upper()
                    if not symbol or symbol.startswith("#") or symbol in seen:
                        continue
                    seen.add(symbol)
                    out.append(symbol)
            return out
        except Exception:
            return []
    for raw in path.read_text().splitlines():
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol.startswith("#") or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _cache_get(key: str) -> Optional[List[Dict[str, Any]]]:
    item = _DISCOVERY_CACHE.get(key)
    if not item:
        return None
    ts, rows = item
    if (time.time() - ts) > _CACHE_TTL_SECONDS:
        _DISCOVERY_CACHE.pop(key, None)
        return None
    return rows


def _cache_set(key: str, rows: List[Dict[str, Any]]) -> None:
    _DISCOVERY_CACHE[key] = (time.time(), rows)


def _sample_symbols(symbols: List[str], sample_size: int) -> List[str]:
    if not symbols:
        return []
    if len(symbols) <= sample_size:
        return symbols
    bucket = int(time.time() // _CACHE_TTL_SECONDS)
    start = bucket % len(symbols)
    out = []
    idx = start
    while len(out) < sample_size:
        out.append(symbols[idx % len(symbols)])
        idx += 1
    return out


def _resolve_change_pct(symbol: str, last: float) -> Optional[float]:
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

    prev_close = None
    try:
        prev = bars[-2]
        prev_close = float(prev.close if hasattr(prev, "close") else prev.get("close"))
    except Exception:
        prev_close = None

    if prev_close is None or prev_close <= 0:
        return None
    return ((float(last) - prev_close) / prev_close) * 100.0


def _discover_segment(market: str, segment: str, filename: str, limit: int = 80, force_refresh: bool = False) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    cache_key = f"discover:{market}:{segment}:{safe_limit}"
    cached = _cache_get(cache_key)
    if cached is not None and not force_refresh:
        return cached

    symbols = _load_symbols(filename)
    sampled = _sample_symbols(symbols, sample_size=min(len(symbols), max(60, safe_limit * 3)))

    rows: List[Dict[str, Any]] = []
    for symbol in sampled:
        try:
            quote_result = get_quote_with_fallback(symbol=symbol, freshness_seconds=300)
            last = float(quote_result.quote.last)
            if last <= 0:
                continue
            change_pct = _resolve_change_pct(symbol, last)
            rows.append(
                {
                    "symbol": symbol,
                    "display_symbol": symbol,
                    "market": market,
                    "segment": segment,
                    "price": last,
                    "change_pct": change_pct,
                    "provider_used": quote_result.provider,
                    "discovered_at": _now_iso(),
                }
            )
        except Exception:
            continue

    rows.sort(key=lambda x: float(x.get("change_pct") or -999999.0), reverse=True)
    trimmed = rows[:safe_limit]
    _cache_set(cache_key, trimmed)
    return trimmed


def discover_us_movers(limit: int = 80, force_refresh: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    us_small_csv = _ROOT_DATA_DIR / "universe_us_small.csv"
    return {
        "large": _discover_segment("US", "large", "us_universe_large.txt", limit=limit, force_refresh=force_refresh),
        "small": _discover_segment(
            "US",
            "small",
            str(us_small_csv if us_small_csv.exists() else (_DATA_DIR / "us_universe_small.txt")),
            limit=limit,
            force_refresh=force_refresh,
        ),
    }


def discover_au_movers(limit: int = 80, force_refresh: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    au_small_csv = _ROOT_DATA_DIR / "universe_au_small.csv"
    return {
        "large": _discover_segment("AU", "large", "asx_universe_large.txt", limit=limit, force_refresh=force_refresh),
        "mid": _discover_segment("AU", "mid", "asx_universe_mid.txt", limit=limit, force_refresh=force_refresh),
        "small": _discover_segment(
            "AU",
            "small",
            str(au_small_csv if au_small_csv.exists() else (_DATA_DIR / "asx_universe_small.txt")),
            limit=limit,
            force_refresh=force_refresh,
        ),
    }


def discover_market_segment(market: str, segment: str, limit: int = 80, force_refresh: bool = False) -> List[Dict[str, Any]]:
    market_u = str(market or "US").strip().upper()
    seg = str(segment or "large").strip().lower()
    if market_u == "AU":
        return discover_au_movers(limit=limit, force_refresh=force_refresh).get(seg, [])
    return discover_us_movers(limit=limit, force_refresh=force_refresh).get(seg, [])

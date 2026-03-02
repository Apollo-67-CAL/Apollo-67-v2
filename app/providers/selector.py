# app/providers/selector.py

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.providers.twelvedata import ProviderError, TwelveDataClient
from app.providers.finnhub import FinnhubClient
from app.validation.market_data import validate_bars, validate_quote


@dataclass
class BarsResult:
    provider: str
    bars: List[Any]


@dataclass
class QuoteResult:
    provider: str
    quote: Any


# Simple in memory TTL caches
# Keyed by symbol or (symbol, interval, outputsize)
_QUOTE_CACHE: Dict[str, Tuple[float, QuoteResult]] = {}
_BARS_CACHE: Dict[Tuple[str, str, int], Tuple[float, BarsResult]] = {}

# Tunable TTLs
_QUOTE_TTL_SECONDS = 20
_BARS_TTL_SECONDS = 60

# When rate limited or provider down, allow serving slightly stale data
_STALE_GRACE_SECONDS = 10 * 60


def _now() -> float:
    return time.monotonic()


def _get_cached_quote(symbol: str) -> Optional[QuoteResult]:
    entry = _QUOTE_CACHE.get(symbol)
    if not entry:
        return None
    expires_at, payload = entry
    if expires_at >= _now():
        return payload
    return None


def _get_cached_quote_allow_stale(symbol: str) -> Optional[QuoteResult]:
    entry = _QUOTE_CACHE.get(symbol)
    if not entry:
        return None
    expires_at, payload = entry
    # Allow stale within grace window
    if expires_at + _STALE_GRACE_SECONDS >= _now():
        return payload
    return None


def _set_cached_quote(symbol: str, payload: QuoteResult) -> None:
    _QUOTE_CACHE[symbol] = (_now() + _QUOTE_TTL_SECONDS, payload)


def _get_cached_bars(key: Tuple[str, str, int]) -> Optional[BarsResult]:
    entry = _BARS_CACHE.get(key)
    if not entry:
        return None
    expires_at, payload = entry
    if expires_at >= _now():
        return payload
    return None


def _get_cached_bars_allow_stale(key: Tuple[str, str, int]) -> Optional[BarsResult]:
    entry = _BARS_CACHE.get(key)
    if not entry:
        return None
    expires_at, payload = entry
    if expires_at + _STALE_GRACE_SECONDS >= _now():
        return payload
    return None


def _set_cached_bars(key: Tuple[str, str, int], payload: BarsResult) -> None:
    _BARS_CACHE[key] = (_now() + _BARS_TTL_SECONDS, payload)


def _has_twelvedata_key() -> bool:
    return bool(os.getenv("TWELVEDATA_API_KEY", "").strip())


def _has_finnhub_key() -> bool:
    return bool(os.getenv("FINNHUB_API_KEY", "").strip())


def get_quote_with_fallback(symbol: str, freshness_seconds: int = 60) -> QuoteResult:
    symbol_u = (symbol or "").strip().upper()
    if not symbol_u:
        raise ProviderError("Missing symbol")

    cached = _get_cached_quote(symbol_u)
    if cached:
        return cached

    if not (_has_twelvedata_key() or _has_finnhub_key()):
        raise ProviderError("No API keys configured. Set TWELVEDATA_API_KEY (preferred) or FINNHUB_API_KEY.")

    errors: List[str] = []

    # Prefer TwelveData for quote
    if _has_twelvedata_key():
        try:
            td = TwelveDataClient()
            res = td.fetch_quote(symbol_u)
            # validate_quote expects quote object or mapping
            validate_quote(res.quote if hasattr(res, "quote") else res)
            payload = QuoteResult(provider="twelvedata", quote=res.quote if hasattr(res, "quote") else res)
            _set_cached_quote(symbol_u, payload)
            return payload
        except Exception as exc:
            errors.append(f"TwelveData quote failed for {symbol_u}: {exc}")

    # Fallback Finnhub for quote
    if _has_finnhub_key():
        try:
            fh = FinnhubClient()
            res = fh.fetch_quote(symbol_u)
            validate_quote(res.quote if hasattr(res, "quote") else res)
            payload = QuoteResult(provider="finnhub", quote=res.quote if hasattr(res, "quote") else res)
            _set_cached_quote(symbol_u, payload)
            return payload
        except Exception as exc:
            errors.append(f"Finnhub quote failed for {symbol_u}: {exc}")

    # If we got rate limited, try serving stale cache
    stale = _get_cached_quote_allow_stale(symbol_u)
    if stale:
        return stale

    raise ProviderError(" | ".join(errors) if errors else "Quote provider failed")


def get_bars_with_fallback(symbol: str, interval: str = "1day", outputsize: int = 500) -> BarsResult:
    symbol_u = (symbol or "").strip().upper()
    interval_v = (interval or "1day").strip()
    size_v = int(outputsize)

    if not symbol_u:
        raise ProviderError("Missing symbol")
    if size_v <= 0:
        raise ProviderError("outputsize must be > 0")

    key = (symbol_u, interval_v, size_v)
    cached = _get_cached_bars(key)
    if cached:
        return cached

    if not (_has_twelvedata_key() or _has_finnhub_key()):
        raise ProviderError("No API keys configured. Set TWELVEDATA_API_KEY (preferred) or FINNHUB_API_KEY.")

    errors: List[str] = []

    # Prefer TwelveData for bars
    if _has_twelvedata_key():
        try:
            td = TwelveDataClient()
            res = td.fetch_bars(symbol_u, interval=interval_v, outputsize=size_v)
            bars = res.bars if hasattr(res, "bars") else res
            validate_bars(bars)
            payload = BarsResult(provider="twelvedata", bars=bars)
            _set_cached_bars(key, payload)
            return payload
        except Exception as exc:
            errors.append(f"TwelveData bars failed for {symbol_u}: {exc}")

    # Finnhub candles are often restricted, so treat as best effort only
    if _has_finnhub_key():
        try:
            fh = FinnhubClient()
            res = fh.fetch_bars(symbol_u, interval=interval_v, outputsize=size_v)
            bars = res.bars if hasattr(res, "bars") else res
            validate_bars(bars)
            payload = BarsResult(provider="finnhub", bars=bars)
            _set_cached_bars(key, payload)
            return payload
        except Exception as exc:
            errors.append(f"Finnhub bars failed for {symbol_u}: {exc}")

    # If rate limited, serve stale bars if we have them
    stale = _get_cached_bars_allow_stale(key)
    if stale:
        return stale

    raise ProviderError(" | ".join(errors) if errors else "Bars provider failed")
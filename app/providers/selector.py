# app/providers/selector.py

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.providers.finnhub import FinnhubClient
from app.providers.twelvedata import BarModel, ProviderError, QuoteOutModel, TwelveDataClient
from app.providers.yahoo import fetch_bars as fetch_yahoo_bars
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
_BARS_TTL_SECONDS = 300

# When rate limited or provider down, allow serving slightly stale data
_STALE_GRACE_SECONDS = 10 * 60
_PROVIDER_COOLDOWN_SECONDS_RATE_LIMIT = 12 * 60 * 60
_PROVIDER_COOLDOWN_SECONDS_AUTH = 24 * 60 * 60
_PROVIDER_COOLDOWN_UNTIL: Dict[str, float] = {}
_PROVIDER_COOLDOWN_REASON: Dict[str, str] = {}

logger = logging.getLogger(__name__)
_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


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


def _cooldown_key(provider: str, kind: str) -> str:
    return f"{provider.lower()}:{kind}"


def _provider_in_cooldown(provider: str, kind: str) -> bool:
    key = _cooldown_key(provider, kind)
    until = _PROVIDER_COOLDOWN_UNTIL.get(key, 0.0)
    if until <= _now():
        _PROVIDER_COOLDOWN_UNTIL.pop(key, None)
        _PROVIDER_COOLDOWN_REASON.pop(key, None)
        return False
    return True


def _set_provider_cooldown(provider: str, kind: str, seconds: int, reason: str) -> None:
    key = _cooldown_key(provider, kind)
    _PROVIDER_COOLDOWN_UNTIL[key] = _now() + float(seconds)
    _PROVIDER_COOLDOWN_REASON[key] = reason
    logger.warning("Provider cooldown set: %s for %ss (%s)", key, seconds, reason)


def _provider_cooldown_reason(provider: str, kind: str) -> str:
    return _PROVIDER_COOLDOWN_REASON.get(_cooldown_key(provider, kind), "cooldown")


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return (
        "[rate_limit]" in msg
        or "run out of api credits" in msg
        or "too many requests" in msg
        or "http 429" in msg
    )


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return (
        "[auth]" in msg
        or "http 401" in msg
        or "http 403" in msg
    )


def _has_twelvedata_key() -> bool:
    return bool(os.getenv("TWELVEDATA_API_KEY", "").strip())


def _has_finnhub_key() -> bool:
    return bool(os.getenv("FINNHUB_API_KEY", "").strip())


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _quote_payload(res: Any) -> Any:
    if res is None:
        return None
    return res.quote if hasattr(res, "quote") else res


def _bars_payload(res: Any) -> List[Any]:
    if res is None:
        return []
    bars = res.bars if hasattr(res, "bars") else res
    if bars is None:
        return []
    if isinstance(bars, list):
        return bars
    return list(bars)


def _quote_last(quote: Any) -> Any:
    if quote is None:
        return None
    if isinstance(quote, dict):
        return quote.get("last")
    return getattr(quote, "last", None)


def _bar_required_value(bar: Any, field: str) -> Any:
    if isinstance(bar, dict):
        return bar.get(field)
    return getattr(bar, field, None)


def _validate_quote_or_raise(quote: Any, symbol: str, provider: str) -> None:
    if quote is None:
        raise ProviderError(f"{provider} quote empty for {symbol}")
    validate_quote(quote)
    if _quote_last(quote) is None:
        raise ProviderError(f"{provider} quote missing last for {symbol}")


def _validate_bars_or_raise(bars: List[Any], symbol: str, provider: str) -> None:
    if not bars:
        raise ProviderError(f"{provider} bars empty for {symbol}")
    validate_bars(bars)
    for bar in bars:
        for field in ("open", "high", "low", "close"):
            if _bar_required_value(bar, field) in (None, ""):
                raise ProviderError(f"{provider} bars missing {field} for {symbol}")


def _yahoo_interval(interval: str) -> str:
    iv = (interval or "1day").strip().lower()
    if iv in ("1day", "1d", "day", "d"):
        return "1d"
    if iv in ("1h", "60m", "60min"):
        return "60m"
    if iv in ("30m", "30min"):
        return "30m"
    if iv in ("15m", "15min"):
        return "15m"
    if iv in ("5m", "5min"):
        return "5m"
    if iv in ("1m", "1min"):
        return "1m"
    if iv in ("1week", "1w", "week", "w"):
        return "1wk"
    if iv in ("1month", "1mo", "month", "m"):
        return "1mo"
    return "1d"


def _yahoo_range(interval: str, outputsize: int) -> str:
    iv = _yahoo_interval(interval)
    n = max(1, int(outputsize))
    if iv in ("1m", "5m", "15m", "30m", "60m"):
        if n <= 200:
            return "1mo"
        if n <= 800:
            return "3mo"
        return "6mo"
    if iv == "1wk":
        if n <= 104:
            return "5y"
        return "10y"
    if iv == "1mo":
        return "10y"
    if n <= 30:
        return "1mo"
    if n <= 120:
        return "6mo"
    if n <= 260:
        return "2y"
    if n <= 780:
        return "5y"
    return "10y"


def _fetch_yahoo_quote(symbol: str, timeout: int = 20) -> QuoteResult:
    symbol_u = symbol.strip().upper()
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol_u}"
    params = {"interval": "1d", "range": "5d"}
    try:
        response = requests.get(url, params=params, headers=_YAHOO_HEADERS, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        raise ProviderError(f"Yahoo quote request failed: {exc}") from exc

    result = ((payload or {}).get("chart") or {}).get("result") or []
    if not result:
        raise ProviderError(f"Yahoo quote empty for {symbol_u}")
    data = result[0] or {}
    meta = data.get("meta") or {}
    indicators = data.get("indicators") or {}
    quote_rows = indicators.get("quote") or []
    quote_row = quote_rows[0] if quote_rows else {}
    closes = quote_row.get("close") or []
    timestamps = data.get("timestamp") or []

    last = meta.get("regularMarketPrice")
    if last in (None, ""):
        for value in reversed(closes):
            if value not in (None, ""):
                last = value
                break
    if last in (None, ""):
        raise ProviderError(f"Yahoo quote missing last for {symbol_u}")

    ts_event = None
    if timestamps:
        try:
            ts_event = datetime.fromtimestamp(int(timestamps[-1]), tz=timezone.utc)
        except Exception:
            ts_event = None

    quote = QuoteOutModel(
        instrument_id=f"YAHOO:{symbol_u}",
        ts_event=ts_event,
        ts_ingest=_utc_now(),
        last=float(last),
        bid=None,
        ask=None,
        source_provider="yahoo",
        quality_flags=[],
    )
    return QuoteResult(provider="yahoo", quote=quote)


def get_quote_with_fallback(symbol: str, freshness_seconds: int = 60) -> QuoteResult:
    symbol_u = (symbol or "").strip().upper()
    if not symbol_u:
        raise ProviderError("Missing symbol")

    cached = _get_cached_quote(symbol_u)
    if cached:
        return cached

    errors: List[str] = []

    try:
        if _has_twelvedata_key():
            td = TwelveDataClient()
            res = td.fetch_quote(symbol_u)
            quote = _quote_payload(res)
            _validate_quote_or_raise(quote, symbol_u, "TwelveData")
            payload = QuoteResult(provider="twelvedata", quote=quote)
            _set_cached_quote(symbol_u, payload)
            return payload
        raise ProviderError("TWELVEDATA_API_KEY is not set")
    except Exception as exc:
        msg = f"TwelveData quote failed for {symbol_u}: {exc}"
        errors.append(msg)
        logger.warning(msg)

    try:
        if _has_finnhub_key():
            fh = FinnhubClient()
            res = fh.fetch_quote(symbol_u)
            quote = _quote_payload(res)
            _validate_quote_or_raise(quote, symbol_u, "Finnhub")
            payload = QuoteResult(provider="finnhub", quote=quote)
            _set_cached_quote(symbol_u, payload)
            return payload
        raise ProviderError("FINNHUB_API_KEY is not set")
    except Exception as exc:
        msg = f"Finnhub quote failed for {symbol_u}: {exc}"
        errors.append(msg)
        logger.warning(msg)

    try:
        res = _fetch_yahoo_quote(symbol_u)
        quote = _quote_payload(res)
        _validate_quote_or_raise(quote, symbol_u, "Yahoo")
        payload = QuoteResult(provider="yahoo", quote=quote)
        _set_cached_quote(symbol_u, payload)
        return payload
    except Exception as exc:
        msg = f"Yahoo quote failed for {symbol_u}: {exc}"
        errors.append(msg)
        logger.warning(msg)

    stale = _get_cached_quote_allow_stale(symbol_u)
    if stale:
        return stale

    raise ProviderError(f"All providers failed for symbol {symbol_u}")


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

    errors: List[str] = []

    try:
        res = fetch_yahoo_bars(symbol_u, interval=interval_v, outputsize=size_v)
        bars = _bars_payload(res)
        _validate_bars_or_raise(bars, symbol_u, "Yahoo")
        payload = BarsResult(provider="yahoo", bars=bars)
        _set_cached_bars(key, payload)
        return payload
    except Exception as exc:
        msg = f"Yahoo bars failed for {symbol_u}: {exc}"
        errors.append(msg)
        logger.warning(msg)

    if _provider_in_cooldown("finnhub", "bars"):
        reason = _provider_cooldown_reason("finnhub", "bars")
        msg = f"Finnhub bars skipped for {symbol_u}: cooldown ({reason})"
        errors.append(msg)
        logger.warning(msg)
    else:
        try:
            if _has_finnhub_key():
                fh = FinnhubClient()
                res = fh.fetch_bars(symbol_u, interval=interval_v, outputsize=size_v)
                bars = _bars_payload(res)
                _validate_bars_or_raise(bars, symbol_u, "Finnhub")
                payload = BarsResult(provider="finnhub", bars=bars)
                _set_cached_bars(key, payload)
                return payload
            raise ProviderError("FINNHUB_API_KEY is not set")
        except Exception as exc:
            if _is_auth_error(exc):
                _set_provider_cooldown(
                    "finnhub",
                    "bars",
                    _PROVIDER_COOLDOWN_SECONDS_AUTH,
                    str(exc),
                )
            msg = f"Finnhub bars failed for {symbol_u}: {exc}"
            errors.append(msg)
            logger.warning(msg)

    if _provider_in_cooldown("twelvedata", "bars"):
        reason = _provider_cooldown_reason("twelvedata", "bars")
        msg = f"TwelveData bars skipped for {symbol_u}: cooldown ({reason})"
        errors.append(msg)
        logger.warning(msg)
    else:
        try:
            if _has_twelvedata_key():
                td = TwelveDataClient()
                res = td.fetch_bars(symbol_u, interval=interval_v, outputsize=size_v)
                bars = _bars_payload(res)
                _validate_bars_or_raise(bars, symbol_u, "TwelveData")
                payload = BarsResult(provider="twelvedata", bars=bars)
                _set_cached_bars(key, payload)
                return payload
            raise ProviderError("TWELVEDATA_API_KEY is not set")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                _set_provider_cooldown(
                    "twelvedata",
                    "bars",
                    _PROVIDER_COOLDOWN_SECONDS_RATE_LIMIT,
                    str(exc),
                )
            msg = f"TwelveData bars failed for {symbol_u}: {exc}"
            errors.append(msg)
            logger.warning(msg)

    stale = _get_cached_bars_allow_stale(key)
    if stale:
        return stale

    raise ProviderError(f"All providers failed for symbol {symbol_u}: {'; '.join(errors)}")

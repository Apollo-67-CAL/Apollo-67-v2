# app/providers/selector.py

from __future__ import annotations

import logging
import os
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.providers.finnhub import FinnhubClient
from app.providers.twelvedata import BarModel, ProviderError, QuoteOutModel, TwelveDataClient
from app.providers.yahoo import fetch_bars as fetch_yahoo_bars
from app.ws.twelvedata_ws import get_ws_client
from app.validation.market_data import validate_bars, validate_quote
from core.storage.db import get_connection


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
_PROVIDER_CALLS_MINUTE: Dict[str, List[float]] = {}
_PROVIDER_CALLS_DAY: Dict[str, Tuple[str, int]] = {}
_PROVIDER_CALLS_DAY_LIMIT = 10000

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


def _seconds_until_next_utc_day() -> int:
    now_dt = datetime.now(timezone.utc)
    next_day = datetime(
        year=now_dt.year,
        month=now_dt.month,
        day=now_dt.day,
        tzinfo=timezone.utc,
    ) + timedelta(days=1)
    delta = next_day - now_dt
    return max(60, int(delta.total_seconds()))


def _provider_call_allowed(provider: str, kind: str, per_minute_limit: int) -> bool:
    key = _cooldown_key(provider, kind)
    now_ts = _now()
    bucket = _PROVIDER_CALLS_MINUTE.get(key) or []
    bucket = [ts for ts in bucket if ts >= (now_ts - 60.0)]
    if len(bucket) >= max(1, int(per_minute_limit)):
        return False
    _PROVIDER_CALLS_MINUTE[key] = bucket

    day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_state = _PROVIDER_CALLS_DAY.get(key)
    day_count = 0
    if day_state and day_state[0] == day_key:
        day_count = int(day_state[1])
    if day_count >= _PROVIDER_CALLS_DAY_LIMIT:
        return False
    return True


def _provider_minute_limit(default_value: int) -> int:
    raw = os.getenv("PROVIDER_CALLS_PER_MINUTE_LIMIT")
    if raw is None or not str(raw).strip():
        return max(1, int(default_value))
    try:
        return max(1, int(str(raw).strip()))
    except Exception:
        return max(1, int(default_value))


def _record_provider_call(provider: str, kind: str) -> None:
    key = _cooldown_key(provider, kind)
    now_ts = _now()
    bucket = _PROVIDER_CALLS_MINUTE.get(key) or []
    bucket.append(now_ts)
    _PROVIDER_CALLS_MINUTE[key] = bucket

    day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_state = _PROVIDER_CALLS_DAY.get(key)
    day_count = 0
    if day_state and day_state[0] == day_key:
        day_count = int(day_state[1])
    _PROVIDER_CALLS_DAY[key] = (day_key, day_count + 1)


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


def _ws_quote(symbol: str, max_age_seconds: int = 15) -> Optional[QuoteResult]:
    try:
        client = get_ws_client()
        row = client.get_price(symbol=symbol, max_age_seconds=max_age_seconds)
    except Exception:
        return None
    if not isinstance(row, dict):
        return None
    price = row.get("price")
    try:
        last = float(price)
    except Exception:
        return None
    if last <= 0:
        return None
    ts_event = row.get("ts")
    if not isinstance(ts_event, datetime):
        ts_event = _utc_now()
    quote = QuoteOutModel(
        instrument_id=f"TWELVEDATA_WS:{(symbol or '').strip().upper()}",
        ts_event=ts_event,
        ts_ingest=_utc_now(),
        last=last,
        bid=None,
        ask=None,
        source_provider="twelvedata_ws",
        quality_flags=["ws"],
    )
    return QuoteResult(provider="twelvedata_ws", quote=quote)


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


def _db_latest_quote(symbol: str, max_age_seconds: int) -> Optional[QuoteResult]:
    symbol_u = (symbol or "").strip().upper()
    if not symbol_u:
        return None
    now_dt = datetime.now(timezone.utc)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT payload, source, created_at
            FROM events
            WHERE event_type = ?
            ORDER BY id DESC
            LIMIT 2000
            """,
            ("worker.quote",),
        ).fetchall()

    for row in rows:
        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            continue
        sym = str(payload.get("symbol", "")).strip().upper()
        if sym != symbol_u:
            continue
        quote_payload = payload.get("quote")
        if not isinstance(quote_payload, dict):
            continue
        created_raw = row.get("created_at") or quote_payload.get("ts_ingest")
        created_dt = None
        if isinstance(created_raw, datetime):
            created_dt = created_raw.astimezone(timezone.utc) if created_raw.tzinfo else created_raw.replace(tzinfo=timezone.utc)
        elif isinstance(created_raw, str) and created_raw.strip():
            try:
                created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                else:
                    created_dt = created_dt.astimezone(timezone.utc)
            except Exception:
                created_dt = None
        if created_dt and (now_dt - created_dt).total_seconds() > max(1, int(max_age_seconds)):
            continue
        quote = QuoteOutModel(**quote_payload)
        return QuoteResult(provider=str(payload.get("provider") or row.get("source") or "cache"), quote=quote)
    return None


def _db_recent_bars(symbol: str, interval: str, outputsize: int, max_age_seconds: int) -> Optional[BarsResult]:
    symbol_u = (symbol or "").strip().upper()
    if not symbol_u:
        return None
    now_dt = datetime.now(timezone.utc)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT instrument_id, timeframe, ts_event, ts_ingest, open, high, low, close, volume, source_provider
            FROM canonical_price_bars
            WHERE timeframe = ? AND instrument_id LIKE ?
            ORDER BY ts_event DESC
            LIMIT ?
            """,
            (interval, f"%:{symbol_u}", max(10, int(outputsize))),
        ).fetchall()

    if not rows:
        return None
    latest_ingest = None
    bars: List[BarModel] = []
    for row in rows:
        ts_ingest = row.get("ts_ingest")
        parsed_ingest = None
        if isinstance(ts_ingest, datetime):
            parsed_ingest = ts_ingest.astimezone(timezone.utc) if ts_ingest.tzinfo else ts_ingest.replace(tzinfo=timezone.utc)
        elif isinstance(ts_ingest, str):
            try:
                parsed_ingest = datetime.fromisoformat(ts_ingest.replace("Z", "+00:00"))
                if parsed_ingest.tzinfo is None:
                    parsed_ingest = parsed_ingest.replace(tzinfo=timezone.utc)
            except Exception:
                parsed_ingest = None
        if parsed_ingest and (latest_ingest is None or parsed_ingest > latest_ingest):
            latest_ingest = parsed_ingest
        bars.append(
            BarModel(
                ts_event=row.get("ts_event"),
                open=float(row.get("open")),
                high=float(row.get("high")),
                low=float(row.get("low")),
                close=float(row.get("close")),
                volume=float(row.get("volume")) if row.get("volume") is not None else None,
                instrument_id=row.get("instrument_id"),
                ts_ingest=row.get("ts_ingest"),
                source_provider=row.get("source_provider"),
                quality_flags=[],
            )
        )
    if latest_ingest and (now_dt - latest_ingest).total_seconds() > max(1, int(max_age_seconds)):
        return None
    bars.reverse()
    return BarsResult(provider="cache", bars=bars)


def get_bars_cached_first(
    symbol: str,
    interval: str = "1day",
    outputsize: int = 500,
    max_age_seconds: int = 21600,
    allow_live: bool = False,
) -> BarsResult:
    cached = _db_recent_bars(symbol=symbol, interval=interval, outputsize=outputsize, max_age_seconds=max_age_seconds)
    if cached:
        return cached
    if not allow_live:
        raise ProviderError("No recent cached bars")
    return get_bars_with_fallback(symbol=symbol, interval=interval, outputsize=outputsize)


def get_quote_cached_first(
    symbol: str,
    max_age_seconds: int = 900,
    allow_live: bool = False,
    freshness_seconds: int = 60,
) -> QuoteResult:
    symbol_u = (symbol or "").strip().upper()
    # Prefer live WS ticks when available.
    ws_hit = _ws_quote(symbol_u, max_age_seconds=15)
    if ws_hit:
        _set_cached_quote(symbol_u, ws_hit)
        return ws_hit
    cached = _db_latest_quote(symbol=symbol_u, max_age_seconds=max_age_seconds)
    if cached:
        return cached
    if not allow_live:
        raise ProviderError("No recent cached quote")
    errors: List[str] = []
    try:
        if not _provider_call_allowed("yahoo", "quote", per_minute_limit=_provider_minute_limit(40)):
            raise ProviderError("[RATE_LIMIT] Yahoo quote local rate limit reached")
        _record_provider_call("yahoo", "quote")
        payload = _fetch_yahoo_quote(symbol_u)
        _set_cached_quote(symbol_u, payload)
        return payload
    except Exception as exc:
        errors.append(f"yahoo:{exc}")

    try:
        if _has_finnhub_key():
            if not _provider_call_allowed("finnhub", "quote", per_minute_limit=_provider_minute_limit(20)):
                raise ProviderError("[RATE_LIMIT] Finnhub quote local rate limit reached")
            _record_provider_call("finnhub", "quote")
            fh = FinnhubClient()
            res = fh.fetch_quote(symbol_u)
            quote = _quote_payload(res)
            _validate_quote_or_raise(quote, symbol_u, "Finnhub")
            payload = QuoteResult(provider="finnhub", quote=quote)
            _set_cached_quote(symbol_u, payload)
            return payload
    except Exception as exc:
        errors.append(f"finnhub:{exc}")

    try:
        if _has_twelvedata_key() and not _provider_in_cooldown("twelvedata", "quote"):
            if not _provider_call_allowed("twelvedata", "quote", per_minute_limit=_provider_minute_limit(20)):
                raise ProviderError("[RATE_LIMIT] Twelvedata quote local rate limit reached")
            _record_provider_call("twelvedata", "quote")
            td = TwelveDataClient()
            res = td.fetch_quote(symbol_u)
            quote = _quote_payload(res)
            _validate_quote_or_raise(quote, symbol_u, "TwelveData")
            payload = QuoteResult(provider="twelvedata", quote=quote)
            _set_cached_quote(symbol_u, payload)
            return payload
    except Exception as exc:
        if _is_rate_limit_error(exc):
            _set_provider_cooldown("twelvedata", "quote", _seconds_until_next_utc_day(), str(exc))
        errors.append(f"twelvedata:{exc}")

    raise ProviderError("All providers failed for symbol %s: %s" % (symbol_u, "; ".join(errors)))


def get_quote_with_fallback(symbol: str, freshness_seconds: int = 60) -> QuoteResult:
    symbol_u = (symbol or "").strip().upper()
    if not symbol_u:
        raise ProviderError("Missing symbol")

    # WS first so consumers (scanner/paper engine/quote endpoint) prefer live price.
    ws_hit = _ws_quote(symbol_u, max_age_seconds=15)
    if ws_hit:
        _set_cached_quote(symbol_u, ws_hit)
        return ws_hit
    cached = _get_cached_quote(symbol_u)
    if cached:
        return cached

    errors: List[str] = []

    try:
        if _has_twelvedata_key():
            if not _provider_call_allowed("twelvedata", "quote", per_minute_limit=_provider_minute_limit(20)):
                raise ProviderError("[RATE_LIMIT] Twelvedata quote local rate limit reached")
            _record_provider_call("twelvedata", "quote")
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
            if not _provider_call_allowed("finnhub", "quote", per_minute_limit=_provider_minute_limit(20)):
                raise ProviderError("[RATE_LIMIT] Finnhub quote local rate limit reached")
            _record_provider_call("finnhub", "quote")
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
        if not _provider_call_allowed("yahoo", "quote", per_minute_limit=_provider_minute_limit(40)):
            raise ProviderError("[RATE_LIMIT] Yahoo quote local rate limit reached")
        _record_provider_call("yahoo", "quote")
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
        if not _provider_call_allowed("yahoo", "bars", per_minute_limit=_provider_minute_limit(40)):
            raise ProviderError("[RATE_LIMIT] Yahoo bars local rate limit reached")
        _record_provider_call("yahoo", "bars")
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

    if _provider_in_cooldown("twelvedata", "bars"):
        reason = _provider_cooldown_reason("twelvedata", "bars")
        msg = f"TwelveData bars skipped for {symbol_u}: cooldown ({reason})"
        errors.append(msg)
        logger.warning(msg)
    else:
        try:
            if _has_twelvedata_key():
                if not _provider_call_allowed("twelvedata", "bars", per_minute_limit=_provider_minute_limit(20)):
                    raise ProviderError("[RATE_LIMIT] TwelveData bars local rate limit reached")
                _record_provider_call("twelvedata", "bars")
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
                if os.getenv("PROVIDER_TWELVEDATA_COOLDOWN_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}:
                    _set_provider_cooldown(
                        "twelvedata",
                        "bars",
                        _seconds_until_next_utc_day(),
                        str(exc),
                    )
            msg = f"TwelveData bars failed for {symbol_u}: {exc}"
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
                if not _provider_call_allowed("finnhub", "bars", per_minute_limit=_provider_minute_limit(20)):
                    raise ProviderError("[RATE_LIMIT] Finnhub bars local rate limit reached")
                _record_provider_call("finnhub", "bars")
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

    stale = _get_cached_bars_allow_stale(key)
    if stale:
        return stale

    raise ProviderError(f"All providers failed for symbol {symbol_u}: {'; '.join(errors)}")

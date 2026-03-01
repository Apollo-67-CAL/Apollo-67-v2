import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any

from app.contracts.market_data import CanonicalBar, CanonicalQuote
from app.providers.alphavantage import AlphaVantageClient
from app.providers.twelvedata import ProviderError, TwelveDataClient
from app.validation.market_data import ValidationError, validate_bars, validate_quote

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 60
_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, Any]] = {}


@dataclass
class QuoteResult:
    provider: str
    quote: CanonicalQuote


@dataclass
class BarsResult:
    provider: str
    bars: list[CanonicalBar]


def get_quote_with_fallback(symbol: str, freshness_seconds: int) -> QuoteResult:
    key = f"quote:{symbol.upper()}"
    cached = _cache_get(key)
    if cached is not None:
        provider, quote = cached
        try:
            validate_quote(quote, freshness_seconds=freshness_seconds)
            logger.info(
                "market_data.quote served provider=%s symbol=%s source=cache",
                provider,
                symbol.upper(),
            )
            return QuoteResult(provider=provider, quote=quote)
        except ValidationError:
            _cache_delete(key)

    td = TwelveDataClient()
    try:
        quote = td.fetch_quote(symbol=symbol)
        validate_quote(quote, freshness_seconds=freshness_seconds)
        _cache_set(key, ("twelvedata", quote))
        logger.info("market_data.quote served provider=twelvedata symbol=%s source=live", symbol.upper())
        return QuoteResult(provider="twelvedata", quote=quote)
    except Exception as exc:
        if not _should_fallback(exc):
            raise
        logger.warning(
            "market_data.quote twelvedata_failed symbol=%s reason=%s; using alphavantage fallback",
            symbol.upper(),
            str(exc),
        )

    av = AlphaVantageClient()
    quote = av.fetch_quote(symbol=symbol)
    validate_quote(quote, freshness_seconds=max(freshness_seconds, _CACHE_TTL_SECONDS))
    _cache_set(key, ("alphavantage", quote))
    logger.info("market_data.quote served provider=alphavantage symbol=%s source=live", symbol.upper())
    return QuoteResult(provider="alphavantage", quote=quote)


def get_bars_with_fallback(symbol: str, interval: str, outputsize: int) -> BarsResult:
    key = f"bars:{symbol.upper()}:{interval}:{outputsize}"
    cached = _cache_get(key)
    if cached is not None:
        provider, bars = cached
        logger.info("market_data.bars served provider=%s symbol=%s source=cache", provider, symbol.upper())
        return BarsResult(provider=provider, bars=bars)

    td = TwelveDataClient()
    try:
        bars = td.fetch_bars(symbol=symbol, interval=interval, outputsize=outputsize)
        validate_bars(bars)
        _cache_set(key, ("twelvedata", bars))
        logger.info("market_data.bars served provider=twelvedata symbol=%s source=live", symbol.upper())
        return BarsResult(provider="twelvedata", bars=bars)
    except Exception as exc:
        if not _should_fallback(exc):
            raise
        logger.warning(
            "market_data.bars twelvedata_failed symbol=%s interval=%s outputsize=%s reason=%s; using alphavantage fallback",
            symbol.upper(),
            interval,
            outputsize,
            str(exc),
        )

    av = AlphaVantageClient()
    bars = av.fetch_bars(symbol=symbol, interval=interval, outputsize=outputsize)
    validate_bars(bars)
    _cache_set(key, ("alphavantage", bars))
    logger.info("market_data.bars served provider=alphavantage symbol=%s source=live", symbol.upper())
    return BarsResult(provider="alphavantage", bars=bars)


def _should_fallback(exc: Exception) -> bool:
    if isinstance(exc, ValidationError):
        return True
    if isinstance(exc, ProviderError):
        message = str(exc).lower()
        markers = (
            "429",
            "503",
            "timed out",
            "timeout",
            "read timed out",
            "connect timeout",
            "connection reset",
            "missing symbol",
            "symbol not found",
            "invalid symbol",
            "no bars returned",
            "validation",
        )
        return any(marker in message for marker in markers)
    return False


def _cache_get(key: str):
    now = time.monotonic()
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry is None:
            return None
        expires_at, payload = entry
        if expires_at <= now:
            _CACHE.pop(key, None)
            return None
        return payload


def _cache_set(key: str, payload: Any) -> None:
    expires_at = time.monotonic() + _CACHE_TTL_SECONDS
    with _CACHE_LOCK:
        _CACHE[key] = (expires_at, payload)


def _cache_delete(key: str) -> None:
    with _CACHE_LOCK:
        _CACHE.pop(key, None)

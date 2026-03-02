# app/providers/selector.py

import os
from typing import Optional

from app.providers.twelvedata import TwelveDataClient, ProviderError, QuoteResult, BarsResult
from app.providers.finnhub import FinnhubClient
from app.validation.market_data import validate_quote, validate_bars


def get_quote_with_fallback(symbol: str, freshness_seconds: int) -> QuoteResult:
    """
    Provider order:
    1) TwelveData (preferred)
    2) Finnhub (fallback)

    Note: your validate_quote() does NOT accept freshness_seconds, so we do not pass it.
    """
    symbol_u = (symbol or "").strip().upper()

    twelvedata_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()

    if not twelvedata_key and not finnhub_key:
        raise ProviderError("No API keys configured. Set TWELVEDATA_API_KEY (preferred) or FINNHUB_API_KEY.")

    errors = []

    # 1) TwelveData
    if twelvedata_key:
        try:
            td = TwelveDataClient()
            res = td.fetch_quote(symbol_u)
            validate_quote(res.quote)
            return res
        except Exception as exc:
            errors.append(f"TwelveData quote failed for {symbol_u}: {exc}")

    # 2) Finnhub
    if finnhub_key:
        try:
            fh = FinnhubClient()
            res = fh.fetch_quote(symbol_u)
            validate_quote(res.quote)
            return res
        except Exception as exc:
            errors.append(f"Finnhub quote failed for {symbol_u}: {exc}")

    raise ProviderError(" | ".join(errors) if errors else f"No valid provider available for quote: {symbol_u}")


def get_bars_with_fallback(symbol: str, interval: str, outputsize: int) -> BarsResult:
    """
    Provider order:
    1) TwelveData (preferred)
    2) Finnhub (fallback)
    """
    symbol_u = (symbol or "").strip().upper()

    twelvedata_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()

    if not twelvedata_key and not finnhub_key:
        raise ProviderError("No API keys configured. Set TWELVEDATA_API_KEY (preferred) or FINNHUB_API_KEY.")

    errors = []

    # 1) TwelveData
    if twelvedata_key:
        try:
            td = TwelveDataClient()
            res = td.fetch_bars(symbol=symbol_u, interval=interval, outputsize=outputsize)
            validate_bars(res.bars)
            return res
        except Exception as exc:
            errors.append(f"TwelveData bars failed for {symbol_u}: {exc}")

    # 2) Finnhub
    if finnhub_key:
        try:
            fh = FinnhubClient()
            res = fh.fetch_bars(symbol=symbol_u, interval=interval, outputsize=outputsize)
            validate_bars(res.bars)
            return res
        except Exception as exc:
            errors.append(f"Finnhub bars failed for {symbol_u}: {exc}")

    raise ProviderError(" | ".join(errors) if errors else f"No valid provider available for bars: {symbol_u}")
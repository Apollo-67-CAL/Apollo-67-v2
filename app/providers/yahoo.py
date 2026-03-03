from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List

import requests

from app.providers.twelvedata import BarModel, BarsResult, ProviderError

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def fetch_bars(symbol: str, interval: str = "1day", outputsize: int = 500, timeout: int = 20) -> BarsResult:
    symbol_u = symbol.strip().upper()
    y_interval = _yahoo_interval(interval)
    y_range = _yahoo_range(interval, outputsize)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol_u}"
    params = {"interval": y_interval, "range": y_range}

    try:
        response = requests.get(url, params=params, headers=_YAHOO_HEADERS, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        raise ProviderError(f"Yahoo bars request failed: {exc}") from exc

    result = ((payload or {}).get("chart") or {}).get("result") or []
    if not result:
        raise ProviderError(f"Yahoo bars empty for {symbol_u}")
    data = result[0] or {}
    indicators = data.get("indicators") or {}
    quote_rows = indicators.get("quote") or []
    quote_row = quote_rows[0] if quote_rows else {}

    timestamps = data.get("timestamp") or []
    opens = quote_row.get("open") or []
    highs = quote_row.get("high") or []
    lows = quote_row.get("low") or []
    closes = quote_row.get("close") or []
    volumes = quote_row.get("volume") or []

    bars: List[Any] = []
    n = min(len(timestamps), len(opens), len(highs), len(lows), len(closes))
    ts_ingest = _utc_now()
    for i in range(n):
        o = opens[i]
        h = highs[i]
        l = lows[i]
        c = closes[i]
        if o in (None, "") or h in (None, "") or l in (None, "") or c in (None, ""):
            continue
        try:
            ts_event = datetime.fromtimestamp(int(timestamps[i]), tz=timezone.utc)
            vol = None
            if i < len(volumes) and volumes[i] not in (None, ""):
                vol = float(volumes[i])
            bars.append(
                BarModel(
                    instrument_id=f"YAHOO:{symbol_u}",
                    ts_event=ts_event,
                    ts_ingest=ts_ingest,
                    open=float(o),
                    high=float(h),
                    low=float(l),
                    close=float(c),
                    volume=vol,
                    source_provider="yahoo",
                    quality_flags=[],
                )
            )
        except Exception:
            continue

    if outputsize > 0:
        bars = bars[-int(outputsize):]

    return BarsResult(provider="yahoo", bars=bars)

# app/providers/finnhub.py

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel, Field

from app.providers.twelvedata import ProviderError, QuoteOutModel, BarModel, QuoteResult, BarsResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _unix_to_utc(ts: Any) -> Optional[datetime]:
    try:
        if ts is None:
            return None
        t = int(ts)
        return datetime.fromtimestamp(t, tz=timezone.utc)
    except Exception:
        return None


def _interval_to_resolution(interval: str) -> str:
    """
    Finnhub candle resolution:
    1, 5, 15, 30, 60, D, W, M
    We'll map common strings used by your API:
    1day -> D
    1week -> W
    1month -> M
    1min / 1m -> 1
    5min -> 5, etc
    """
    iv = (interval or "").strip().lower()
    if iv in ("1day", "1d", "day", "d"):
        return "D"
    if iv in ("1week", "1w", "week", "w"):
        return "W"
    if iv in ("1month", "1mo", "month", "mth", "m"):
        return "M"
    if iv in ("1min", "1m", "min", "minute"):
        return "1"
    if iv.endswith("min"):
        n = iv.replace("min", "").strip()
        if n.isdigit():
            return n
    if iv.endswith("m"):
        n = iv.replace("m", "").strip()
        if n.isdigit():
            return n
    # default
    return "D"


class FinnhubClient:
    def __init__(self, api_key: Optional[str] = None, timeout: int = 20):
        self.api_key = (api_key or os.getenv("FINNHUB_API_KEY", "")).strip()
        if not self.api_key:
            raise ProviderError("FINNHUB_API_KEY is not set")
        self.timeout = timeout
        self.base_url = "https://finnhub.io/api/v1"
        self.session = requests.Session()

    def fetch_quote(self, symbol: str) -> QuoteResult:
        symbol_u = (symbol or "").strip().upper()
        url = f"{self.base_url}/quote"
        params = {"symbol": symbol_u, "token": self.api_key}

        try:
            r = self.session.get(url, params=params, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            raise ProviderError(f"Finnhub request failed: {exc}") from exc

        # Finnhub quote keys:
        # c current, d change, dp percent, h high, l low, o open, pc prev close, t timestamp
        last = data.get("c")
        if last is None:
            raise ProviderError("Quote missing price")

        ts_event = _unix_to_utc(data.get("t"))
        quote = QuoteOutModel(
            instrument_id=f"FINNHUB:{symbol_u}",
            ts_event=ts_event,
            ts_ingest=_utc_now(),
            last=float(last),
            bid=None,
            ask=None,
            source_provider="finnhub",
            quality_flags=[],
        )
        return QuoteResult(provider="finnhub", quote=quote)

    def fetch_bars(self, symbol: str, interval: str = "1day", outputsize: int = 500) -> BarsResult:
        symbol_u = (symbol or "").strip().upper()
        resolution = _interval_to_resolution(interval)

        # Finnhub candle needs UNIX seconds range: from, to.
        # We'll estimate outputsize units back from now.
        now = int(time.time())
        # Rough seconds per bar
        if resolution == "D":
            step = 86400
        elif resolution == "W":
            step = 86400 * 7
        elif resolution == "M":
            step = 86400 * 30
        else:
            # minutes as integer
            try:
                mins = int(resolution)
                step = mins * 60
            except Exception:
                step = 86400

        count = max(10, int(outputsize))
        frm = now - (count * step)

        url = f"{self.base_url}/stock/candle"
        params = {
            "symbol": symbol_u,
            "resolution": resolution,
            "from": frm,
            "to": now,
            "token": self.api_key,
        }

        try:
            r = self.session.get(url, params=params, timeout=self.timeout)
            if r.status_code in (401, 403):
                if r.status_code == 403:
                    raise ProviderError("[AUTH] Finnhub 403 Forbidden: check FINNHUB_API_KEY or plan")
                raise ProviderError("[AUTH] Finnhub 401 Unauthorized: check FINNHUB_API_KEY")
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            if isinstance(exc, ProviderError):
                raise
            raise ProviderError(f"Finnhub request failed: {exc}") from exc

        # Finnhub candle response:
        # {"c":[...], "h":[...], "l":[...], "o":[...], "t":[...], "v":[...], "s":"ok"}
        if not isinstance(data, dict) or data.get("s") != "ok":
            raise ProviderError(f"Finnhub candle error: {data}")

        t = data.get("t") or []
        o = data.get("o") or []
        h = data.get("h") or []
        l = data.get("l") or []
        c = data.get("c") or []
        v = data.get("v") or []

        bars: List[BarModel] = []
        n = min(len(t), len(o), len(h), len(l), len(c))
        for i in range(n):
            ts = _unix_to_utc(t[i]) or t[i]
            vol = None
            if i < len(v) and v[i] is not None:
                try:
                    vol = float(v[i])
                except Exception:
                    vol = None

            bars.append(
                BarModel(
                    instrument_id=f"FINNHUB:{symbol_u}",
                    ts_event=ts,
                    ts_ingest=_utc_now(),
                    open=float(o[i]),
                    high=float(h[i]),
                    low=float(l[i]),
                    close=float(c[i]),
                    volume=vol,
                    source_provider="finnhub",
                    quality_flags=[],
                )
            )

        # Finnhub returns ascending time; your UI usually wants latest first or consistent.
        # We’ll keep as-is and let callers sort if needed.
        return BarsResult(provider="finnhub", bars=bars)

# app/providers/twelvedata.py

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel, Field


class ProviderError(Exception):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_twelvedata_date(s: Any) -> Optional[datetime]:
    """
    TwelveData quote returns "datetime":"YYYY-MM-DD" for daily,
    and time_series returns values with "datetime":"YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
    """
    if not s:
        return None
    if isinstance(s, datetime):
        return s.astimezone(timezone.utc) if s.tzinfo else s.replace(tzinfo=timezone.utc)
    try:
        txt = str(s).strip()
        # YYYY-MM-DD
        if len(txt) == 10:
            dt = datetime.strptime(txt, "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        # YYYY-MM-DD HH:MM:SS
        dt = datetime.strptime(txt, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


class QuoteOutModel(BaseModel):
    instrument_id: str
    ts_event: Optional[datetime] = None
    ts_ingest: datetime
    last: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    source_provider: str
    quality_flags: List[str] = Field(default_factory=list)


class BarModel(BaseModel):
    ts_event: Any
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    instrument_id: Optional[str] = None
    ts_ingest: Optional[Any] = None
    source_provider: Optional[str] = None
    quality_flags: Optional[List[str]] = None


class QuoteResult(BaseModel):
    provider: str
    quote: QuoteOutModel


class BarsResult(BaseModel):
    provider: str
    bars: List[BarModel]


class TwelveDataClient:
    def __init__(self, api_key: Optional[str] = None, timeout: int = 20):
        self.api_key = (api_key or os.getenv("TWELVEDATA_API_KEY", "")).strip()
        if not self.api_key:
            raise ProviderError("TWELVEDATA_API_KEY is not set")
        self.timeout = timeout
        self.base_url = "https://api.twelvedata.com"
        self.session = requests.Session()

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        params = dict(params or {})
        params["apikey"] = self.api_key

        try:
            r = self.session.get(url, params=params, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            raise ProviderError(f"TwelveData request failed: {exc}") from exc

        # TwelveData sometimes returns {"status":"error", "message":"..."}
        if isinstance(data, dict) and data.get("status") == "error":
            raise ProviderError(f"TwelveData error: {data.get('message') or data}")
        return data

    def fetch_quote(self, symbol: str) -> QuoteResult:
        symbol_u = (symbol or "").strip().upper()
        data = self._get("/quote", {"symbol": symbol_u})

        # TwelveData: "close" is effectively last for daily
        last = data.get("close") or data.get("price") or data.get("last")
        if last is None:
            raise ProviderError(f"TwelveData quote missing price for {symbol_u}")

        ts_event = _parse_twelvedata_date(data.get("datetime"))
        # If we only have date, set typical US close time is unknown; we keep date in UTC midnight.
        # That’s fine for our current use.
        quote = QuoteOutModel(
            instrument_id=f"TWELVEDATA:{symbol_u}",
            ts_event=ts_event,
            ts_ingest=_utc_now(),
            last=float(last),
            bid=None,
            ask=None,
            source_provider="twelvedata",
            quality_flags=[],
        )
        return QuoteResult(provider="twelvedata", quote=quote)

    def fetch_bars(self, symbol: str, interval: str = "1day", outputsize: int = 500) -> BarsResult:
        symbol_u = (symbol or "").strip().upper()

        # TwelveData uses: time_series?symbol=...&interval=1day&outputsize=...&order=DESC
        data = self._get(
            "/time_series",
            {"symbol": symbol_u, "interval": interval, "outputsize": int(outputsize), "order": "DESC"},
        )

        values = data.get("values") if isinstance(data, dict) else None
        if not isinstance(values, list):
            raise ProviderError(f"TwelveData bars missing values for {symbol_u}")

        bars: List[BarModel] = []
        for v in values:
            if not isinstance(v, dict):
                continue
            ts = _parse_twelvedata_date(v.get("datetime")) or v.get("datetime")
            bars.append(
                BarModel(
                    instrument_id=f"TWELVEDATA:{symbol_u}",
                    ts_event=ts if ts is not None else v.get("datetime"),
                    ts_ingest=_utc_now(),
                    open=float(v.get("open")) if v.get("open") is not None else 0.0,
                    high=float(v.get("high")) if v.get("high") is not None else 0.0,
                    low=float(v.get("low")) if v.get("low") is not None else 0.0,
                    close=float(v.get("close")) if v.get("close") is not None else 0.0,
                    volume=float(v.get("volume")) if v.get("volume") not in (None, "") else None,
                    source_provider="twelvedata",
                    quality_flags=[],
                )
            )

        return BarsResult(provider="twelvedata", bars=bars)

    def search_symbols(self, q: str) -> List[Dict[str, Any]]:
        # TwelveData: /symbol_search?symbol=xxx
        q = (q or "").strip()
        if not q:
            return []
        data = self._get("/symbol_search", {"symbol": q})
        items = data.get("data") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []
        out: List[Dict[str, Any]] = []
        for it in items:
            if isinstance(it, dict):
                out.append(it)
        return out
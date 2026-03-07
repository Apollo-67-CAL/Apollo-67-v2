from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.providers.twelvedata import BarModel, BarsResult, ProviderError, QuoteOutModel, QuoteResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        raw = int(float(value))
    except Exception:
        return None
    # Massive timestamps can come in ns/us/ms/s depending on endpoint.
    if raw > 10_000_000_000_000_000:
        raw = int(raw / 1_000_000_000)  # ns -> s
    elif raw > 10_000_000_000_000:
        raw = int(raw / 1_000_000)  # us -> s
    elif raw > 10_000_000_000:
        raw = int(raw / 1_000)  # ms -> s
    return datetime.fromtimestamp(raw, tz=timezone.utc)


def _market_for_symbol(symbol: str, market: Optional[str]) -> str:
    market_u = str(market or "").strip().upper()
    if market_u in {"US", "AU"}:
        return market_u
    if str(symbol or "").strip().upper().endswith(".AX"):
        return "AU"
    return "US"


def _interval_to_massive(interval: str) -> Tuple[int, str, int]:
    iv = str(interval or "1day").strip().lower()
    if iv in {"1min", "1m"}:
        return 1, "minute", 60
    if iv in {"5min", "5m"}:
        return 5, "minute", 5 * 60
    if iv in {"15min", "15m"}:
        return 15, "minute", 15 * 60
    if iv in {"30min", "30m"}:
        return 30, "minute", 30 * 60
    if iv in {"1h", "60m", "60min"}:
        return 60, "minute", 60 * 60
    if iv in {"1week", "1w", "week", "w"}:
        return 1, "week", 7 * 24 * 60 * 60
    if iv in {"1month", "1mo", "month", "m"}:
        return 1, "month", 30 * 24 * 60 * 60
    return 1, "day", 24 * 60 * 60


def _range_bounds(interval: str, outputsize: int) -> Tuple[str, str]:
    now_dt = _utc_now()
    multiplier, timespan, seconds_per_bar = _interval_to_massive(interval)
    bars = max(10, int(outputsize))
    lookback_seconds = int(seconds_per_bar * max(multiplier, 1) * bars * 1.6)
    start_dt = now_dt - timedelta(seconds=max(86_400, lookback_seconds))
    if timespan in {"day", "week", "month"}:
        return start_dt.strftime("%Y-%m-%d"), now_dt.strftime("%Y-%m-%d")
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(now_dt.timestamp() * 1000)
    return str(start_ms), str(end_ms)


class MassiveClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 20,
        base_url: Optional[str] = None,
    ):
        self.api_key = (api_key or os.getenv("MASSIVE_API_KEY", "")).strip()
        if not self.api_key:
            raise ProviderError("MASSIVE_API_KEY is not set")
        self.timeout = timeout
        self.base_url = (base_url or os.getenv("MASSIVE_BASE_URL", "https://api.massive.com")).strip().rstrip("/")
        self.session = requests.Session()

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        query = dict(params or {})
        query["apiKey"] = self.api_key
        try:
            response = self.session.get(url, params=query, timeout=self.timeout)
        except Exception as exc:
            raise ProviderError(f"Massive request failed: {exc}") from exc

        if response.status_code == 429:
            raise ProviderError("[RATE_LIMIT] Massive HTTP 429 Too Many Requests")
        if response.status_code in {401, 403}:
            raise ProviderError(f"[AUTH] Massive HTTP {response.status_code}")
        if response.status_code >= 400:
            body_preview = response.text[:200] if isinstance(response.text, str) else ""
            raise ProviderError(f"Massive request failed: HTTP {response.status_code} {body_preview}")
        try:
            data = response.json()
        except Exception as exc:
            raise ProviderError(f"Massive JSON parse failed: {exc}") from exc
        if isinstance(data, dict) and data.get("status") == "ERROR":
            raise ProviderError(f"Massive error: {data.get('error') or data.get('message') or data}")
        return data if isinstance(data, dict) else {}

    def get_quote(self, symbol: str, market: Optional[str] = "US") -> QuoteResult:
        symbol_u = str(symbol or "").strip().upper()
        if not symbol_u:
            raise ProviderError("Missing symbol")
        if _market_for_symbol(symbol_u, market) != "US":
            raise ProviderError("Massive supports US market only")

        payload = self._get(f"/v2/last/trade/{symbol_u}")
        result = payload.get("results") if isinstance(payload.get("results"), dict) else {}
        price = result.get("p")
        ts_value = result.get("t")

        if price in (None, ""):
            snapshot = self._get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol_u}")
            ticker_payload = snapshot.get("ticker") if isinstance(snapshot.get("ticker"), dict) else {}
            last_trade = ticker_payload.get("lastTrade") if isinstance(ticker_payload.get("lastTrade"), dict) else {}
            day_payload = ticker_payload.get("day") if isinstance(ticker_payload.get("day"), dict) else {}
            minute_payload = ticker_payload.get("min") if isinstance(ticker_payload.get("min"), dict) else {}
            price = last_trade.get("p")
            if price in (None, ""):
                price = minute_payload.get("c")
            if price in (None, ""):
                price = day_payload.get("c")
            ts_value = last_trade.get("t") or minute_payload.get("t")

        if price in (None, ""):
            raise ProviderError(f"Massive quote missing price for {symbol_u}")

        ts_event = _to_utc_datetime(ts_value) or _utc_now()
        quote = QuoteOutModel(
            instrument_id=f"MASSIVE:{symbol_u}",
            ts_event=ts_event,
            ts_ingest=_utc_now(),
            last=float(price),
            bid=None,
            ask=None,
            source_provider="massive",
            quality_flags=[],
        )
        return QuoteResult(provider="massive", quote=quote)

    def get_bars(
        self,
        symbol: str,
        interval: str = "1day",
        outputsize: int = 500,
        market: Optional[str] = "US",
    ) -> BarsResult:
        symbol_u = str(symbol or "").strip().upper()
        if not symbol_u:
            raise ProviderError("Missing symbol")
        if _market_for_symbol(symbol_u, market) != "US":
            raise ProviderError("Massive supports US market only")

        multiplier, timespan, _ = _interval_to_massive(interval)
        date_from, date_to = _range_bounds(interval=interval, outputsize=outputsize)
        payload = self._get(
            f"/v2/aggs/ticker/{symbol_u}/range/{multiplier}/{timespan}/{date_from}/{date_to}",
            params={
                "adjusted": "true",
                "sort": "asc",
                "limit": max(10, min(int(outputsize) * 3, 50000)),
            },
        )
        results = payload.get("results") if isinstance(payload.get("results"), list) else []
        if not results:
            raise ProviderError(f"Massive bars empty for {symbol_u}")

        bars: List[BarModel] = []
        ts_ingest = _utc_now()
        for row in results:
            if not isinstance(row, dict):
                continue
            try:
                o = float(row.get("o"))
                h = float(row.get("h"))
                l = float(row.get("l"))
                c = float(row.get("c"))
            except Exception:
                continue
            ts_event = _to_utc_datetime(row.get("t"))
            volume = None
            try:
                if row.get("v") not in (None, ""):
                    volume = float(row.get("v"))
            except Exception:
                volume = None
            bars.append(
                BarModel(
                    instrument_id=f"MASSIVE:{symbol_u}",
                    ts_event=ts_event or row.get("t"),
                    ts_ingest=ts_ingest,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=volume,
                    source_provider="massive",
                    quality_flags=[],
                )
            )
        if not bars:
            raise ProviderError(f"Massive bars invalid for {symbol_u}")
        if outputsize > 0:
            bars = bars[-int(outputsize):]
        return BarsResult(provider="massive", bars=bars)

    def get_grouped_daily(self, date: Optional[str] = None, market: str = "US") -> Dict[str, Any]:
        if str(market or "US").strip().upper() != "US":
            raise ProviderError("Massive grouped daily supports US market only")
        if date:
            try:
                day = datetime.strptime(str(date).strip(), "%Y-%m-%d").date()
            except Exception:
                day = _utc_now().date()
        else:
            day = _utc_now().date()

        payload = self._get(
            f"/v2/aggs/grouped/locale/us/market/stocks/{day.strftime('%Y-%m-%d')}",
            params={"adjusted": "true"},
        )
        rows = payload.get("results") if isinstance(payload.get("results"), list) else []
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("T") or "").strip().upper()
            if not symbol:
                continue
            close_val = row.get("c")
            open_val = row.get("o")
            change_pct = None
            try:
                o = float(open_val)
                c = float(close_val)
                if o > 0:
                    change_pct = ((c - o) / o) * 100.0
            except Exception:
                change_pct = None
            normalized.append(
                {
                    "symbol": symbol,
                    "open": open_val,
                    "high": row.get("h"),
                    "low": row.get("l"),
                    "close": close_val,
                    "volume": row.get("v"),
                    "vwap": row.get("vw"),
                    "trade_count": row.get("n"),
                    "change_pct": change_pct,
                }
            )
        return {
            "provider": "massive",
            "date": day.strftime("%Y-%m-%d"),
            "results": normalized,
        }


def get_quote(symbol: str, market: Optional[str] = "US") -> QuoteResult:
    client = MassiveClient()
    return client.get_quote(symbol=symbol, market=market)


def get_bars(symbol: str, interval: str = "1day", outputsize: int = 500, market: Optional[str] = "US") -> BarsResult:
    client = MassiveClient()
    return client.get_bars(symbol=symbol, interval=interval, outputsize=outputsize, market=market)


def get_grouped_daily(date: Optional[str] = None, market: str = "US") -> Dict[str, Any]:
    client = MassiveClient()
    return client.get_grouped_daily(date=date, market=market)

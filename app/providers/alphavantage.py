import os
from datetime import datetime, time, timezone
from typing import Any, Optional

from app.contracts.market_data import CanonicalBar, CanonicalQuote
from app.providers.twelvedata import ProviderError


class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: Optional[str] = None, timeout_seconds: int = 10) -> None:
        self.api_key = api_key or os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
        self.timeout_seconds = timeout_seconds
        if not self.api_key:
            raise ProviderError("ALPHAVANTAGE_API_KEY is required")

    def _get(self, params: dict[str, Any]) -> dict[str, Any]:
        query = {**params, "apikey": self.api_key}
        try:
            response = _http_get(self.BASE_URL, params=query, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise ProviderError(f"Alpha Vantage request failed: {exc}") from exc

        if not isinstance(payload, dict):
            raise ProviderError("Alpha Vantage returned non-object payload")

        if payload.get("Error Message"):
            raise ProviderError(f"Alpha Vantage error: {payload.get('Error Message')}")

        note = payload.get("Note")
        if isinstance(note, str) and note.strip():
            raise ProviderError(f"Alpha Vantage rate limit: {note}")

        info = payload.get("Information")
        if isinstance(info, str) and info.strip():
            raise ProviderError(f"Alpha Vantage information: {info}")

        return payload

    def fetch_quote(self, symbol: str) -> CanonicalQuote:
        payload = self._get({"function": "GLOBAL_QUOTE", "symbol": symbol})
        row = payload.get("Global Quote", {}) if isinstance(payload, dict) else {}
        if not isinstance(row, dict) or not row:
            raise ProviderError("Alpha Vantage quote missing Global Quote payload")

        price_raw = row.get("05. price")
        if price_raw in (None, ""):
            raise ProviderError("Alpha Vantage quote missing price for symbol")

        ts_ingest = datetime.now(timezone.utc)
        ts_event = _parse_quote_day(row.get("07. latest trading day"), ts_ingest)
        bid = _optional_float(row.get("02. open"))
        ask = _optional_float(row.get("03. high"))

        return CanonicalQuote(
            instrument_id=f"ALPHAVANTAGE:{symbol}",
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            last=float(price_raw),
            bid=bid,
            ask=ask,
            source_provider="alphavantage",
            quality_flags=[],
        )

    def fetch_bars(self, symbol: str, interval: str, outputsize: int) -> list[CanonicalBar]:
        if interval != "1day":
            raise ProviderError(f"Alpha Vantage fallback supports interval=1day only, got {interval}")

        payload = self._get(
            {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "outputsize": "full" if outputsize > 100 else "compact",
            }
        )
        series = payload.get("Time Series (Daily)", {}) if isinstance(payload, dict) else {}
        if not isinstance(series, dict) or not series:
            raise ProviderError("Alpha Vantage bars missing Time Series (Daily) payload")

        ts_ingest = datetime.now(timezone.utc)
        rows: list[tuple[str, dict[str, Any]]] = sorted(series.items(), key=lambda item: item[0])
        if outputsize > 0:
            rows = rows[-outputsize:]

        bars: list[CanonicalBar] = []
        for day, item in rows:
            if not isinstance(item, dict):
                continue
            ts_event = _parse_daily_timestamp(day)
            bars.append(
                CanonicalBar(
                    instrument_id=f"ALPHAVANTAGE:{symbol}",
                    ts_event=ts_event,
                    ts_ingest=ts_ingest,
                    open=float(item.get("1. open", 0)),
                    high=float(item.get("2. high", 0)),
                    low=float(item.get("3. low", 0)),
                    close=float(item.get("4. close", 0)),
                    volume=float(item.get("6. volume", 0) or 0),
                    source_provider="alphavantage",
                    quality_flags=[],
                )
            )
        return bars


def _http_get(url: str, params: dict[str, Any], timeout: int):
    import requests

    return requests.get(url, params=params, timeout=timeout)


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    return float(value)


def _parse_daily_timestamp(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_quote_day(raw: Any, fallback: datetime) -> datetime:
    if raw in (None, ""):
        return fallback
    day = datetime.fromisoformat(str(raw)).date()
    dt = datetime.combine(day, time(0, 0, tzinfo=timezone.utc))
    return dt.astimezone(timezone.utc)

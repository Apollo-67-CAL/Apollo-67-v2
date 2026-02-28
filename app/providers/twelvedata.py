import os
from datetime import datetime, timezone
from typing import Any, Optional

from app.contracts.market_data import CanonicalBar, CanonicalQuote


class ProviderError(RuntimeError):
    pass


class TwelveDataClient:
    BASE_URL = "https://api.twelvedata.com"

    def __init__(self, api_key: Optional[str] = None, timeout_seconds: int = 10) -> None:
        self.api_key = api_key or os.getenv("TWELVEDATA_API_KEY", "").strip()
        self.timeout_seconds = timeout_seconds
        if not self.api_key:
            raise ProviderError("TWELVEDATA_API_KEY is required")

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        query = {**params, "apikey": self.api_key}
        try:
            response = _http_get(
                f"{self.BASE_URL}{endpoint}",
                params=query,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise ProviderError(f"Twelve Data request failed: {exc}") from exc

        if isinstance(payload, dict) and payload.get("status") == "error":
            code = payload.get("code", "unknown")
            message = payload.get("message", "unknown error")
            raise ProviderError(f"Twelve Data error {code}: {message}")

        return payload

    def search_symbols(self, query: str) -> list[dict]:
        payload = self._get("/symbol_search", {"symbol": query, "outputsize": 30})
        return payload.get("data", []) if isinstance(payload, dict) else []

    def fetch_bars(self, symbol: str, interval: str, outputsize: int) -> list[CanonicalBar]:
        payload = self._get(
            "/time_series",
            {
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "timezone": "UTC",
                "order": "ASC",
            },
        )

        values = payload.get("values", []) if isinstance(payload, dict) else []
        ts_ingest = datetime.now(timezone.utc)
        bars: list[CanonicalBar] = []
        for item in values:
            ts_event = _parse_twelvedata_timestamp(item.get("datetime", ""))
            bars.append(
                CanonicalBar(
                    instrument_id=f"TWELVEDATA:{symbol}",
                    ts_event=ts_event,
                    ts_ingest=ts_ingest,
                    open=float(item.get("open", 0)),
                    high=float(item.get("high", 0)),
                    low=float(item.get("low", 0)),
                    close=float(item.get("close", 0)),
                    volume=float(item.get("volume", 0) or 0),
                    source_provider="twelvedata",
                    quality_flags=[],
                )
            )
        return bars

    def fetch_quote(self, symbol: str) -> CanonicalQuote:
        payload = self._get("/quote", {"symbol": symbol})
        ts_ingest = datetime.now(timezone.utc)

        ts_event_raw = payload.get("timestamp") or payload.get("datetime")
        ts_event = _parse_quote_timestamp(ts_event_raw, ts_ingest)

        return CanonicalQuote(
            instrument_id=f"TWELVEDATA:{symbol}",
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            last=float(payload.get("close") or payload.get("price") or 0),
            bid=_optional_float(payload.get("bid")),
            ask=_optional_float(payload.get("ask")),
            source_provider="twelvedata",
            quality_flags=[],
        )


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    return float(value)


def _parse_twelvedata_timestamp(raw: str) -> datetime:
    if not raw:
        raise ProviderError("Missing datetime in time_series value")
    candidate = raw.replace(" ", "T")
    dt = datetime.fromisoformat(candidate)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _http_get(url: str, params: dict[str, Any], timeout: int):
    import requests

    return requests.get(url, params=params, timeout=timeout)


def _parse_quote_timestamp(raw: Any, fallback: datetime) -> datetime:
    if raw in (None, ""):
        return fallback

    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)

    text = str(raw).strip()
    if text.isdigit():
        return datetime.fromtimestamp(float(text), tz=timezone.utc)

    candidate = text.replace(" ", "T")
    dt = datetime.fromisoformat(candidate)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

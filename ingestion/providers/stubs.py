from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional, Set

from ingestion.models import ProviderResult
from ingestion.providers.base import ProviderUnavailableError


class StubProvider:
    def __init__(self, name: str, fail_datasets: Optional[Iterable[str]] = None) -> None:
        self.name = name
        self._fail_datasets: Set[str] = set(fail_datasets or [])

    def fetch_dataset(self, dataset: str) -> ProviderResult:
        if dataset in self._fail_datasets:
            raise ProviderUnavailableError(f"{self.name} unavailable for dataset={dataset}")

        now = datetime.now(timezone.utc)
        records: list[dict]
        if dataset == "instrument":
            records = [
                {
                    "instrument_id": "A67.AAPL",
                    "symbol": "AAPL",
                    "venue": "NASDAQ",
                    "asset_type": "equity",
                    "currency": "USD",
                    "is_tradable": True,
                    "effective_from": now.isoformat(),
                    "effective_to": None,
                    "source_provider": self.name,
                }
            ]
        elif dataset == "price_bar":
            records = [
                {
                    "instrument_id": "A67.AAPL",
                    "timeframe": "1m",
                    "ts_event": (now - timedelta(minutes=1)).isoformat(),
                    "ts_ingest": now.isoformat(),
                    "open": 182.15,
                    "high": 182.42,
                    "low": 181.98,
                    "close": 182.36,
                    "volume": 15230,
                    "source_provider": self.name,
                    "quality_flags": [],
                }
            ]
        elif dataset == "corporate_action":
            records = [
                {
                    "instrument_id": "A67.AAPL",
                    "action_type": "split",
                    "effective_date": date.today().isoformat(),
                    "factor_or_amount": 1.0,
                    "source_provider": self.name,
                }
            ]
        elif dataset == "session_calendar":
            records = [
                {
                    "venue": "NASDAQ",
                    "session_date": date.today().isoformat(),
                    "is_open": True,
                    "session_start": "09:30",
                    "session_end": "16:00",
                    "timezone": "America/New_York",
                    "source_provider": self.name,
                }
            ]
        else:
            records = []

        return ProviderResult(
            dataset=dataset,
            provider=self.name,
            records=records,
            latency_ms=12.5,
            used_fallback=False,
        )

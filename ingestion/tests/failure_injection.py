from datetime import datetime, timedelta, timezone

from core.config import get_config
from core.storage.db import init_db
from ingestion.models import PriceBar
from ingestion.providers.hierarchy import ProviderHierarchy
from ingestion.providers.stubs import StubProvider
from ingestion.service import DataIngestionService
from ingestion.validation import BlockingValidationError, DataValidator


def run() -> None:
    init_db()
    cfg = get_config()

    # Failover injection: primary fails on price bars, fallback succeeds.
    hierarchy = ProviderHierarchy(
        primary=StubProvider("stub_primary", fail_datasets=["price_bar"]),
        fallback=StubProvider("stub_fallback"),
    )
    service = DataIngestionService(config=cfg, hierarchy=hierarchy)
    result = service.ingest_dataset("price_bar", expected_count=1)
    assert result["used_fallback"] is True

    # Blocking freshness injection.
    stale_bar = PriceBar(
        instrument_id="A67.TEST",
        timeframe="1m",
        ts_event=datetime.now(timezone.utc) - timedelta(hours=3),
        ts_ingest=datetime.now(timezone.utc) - timedelta(hours=2),
        open=100,
        high=101,
        low=99,
        close=100,
        volume=10,
        source_provider="stub_primary",
        quality_flags=[],
    )

    validator = DataValidator(cfg)
    try:
        validator.validate_price_bars([stale_bar], expected_count=1)
        raise AssertionError("Expected BlockingValidationError for stale bar")
    except BlockingValidationError:
        pass

    print("PASS: failure injection tests")


if __name__ == "__main__":
    run()

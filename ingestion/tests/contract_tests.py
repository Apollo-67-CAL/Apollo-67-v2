from datetime import datetime, timezone

from core.config import get_config
from core.storage.db import init_db
from ingestion.models import CorporateAction, Instrument, PriceBar, SessionCalendar
from ingestion.repository import IngestionRepository
from ingestion.validation import DataValidator, ValidationHooks, default_drift_hook, default_spike_hook


def run() -> None:
    init_db()
    cfg = get_config()
    validator = DataValidator(
        cfg,
        hooks=ValidationHooks(drift_check=default_drift_hook, spike_check=default_spike_hook),
    )
    repo = IngestionRepository()

    now = datetime.now(timezone.utc)
    instrument = Instrument(
        instrument_id="A67.TEST",
        symbol="TEST",
        venue="NASDAQ",
        asset_type="equity",
        currency="USD",
        is_tradable=True,
        effective_from=now,
        effective_to=None,
        source_provider="contract_test",
    )
    bar = PriceBar(
        instrument_id="A67.TEST",
        timeframe="1m",
        ts_event=now,
        ts_ingest=now,
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=1000,
        source_provider="contract_test",
        quality_flags=[],
    )
    action = CorporateAction(
        instrument_id="A67.TEST",
        action_type="split",
        effective_date=now.date(),
        factor_or_amount=1.0,
        source_provider="contract_test",
    )
    session = SessionCalendar(
        venue="NASDAQ",
        session_date=now.date(),
        is_open=True,
        session_start=cfg.calendar_session_start,
        session_end=cfg.calendar_session_end,
        timezone="America/New_York",
        source_provider="contract_test",
    )

    validator.validate_instruments([instrument], expected_count=1)
    validator.validate_price_bars([bar], expected_count=1)
    validator.validate_corporate_actions([action], expected_count=1)
    validator.validate_session_calendar([session], expected_count=1)

    assert repo.capture_raw_payload("contract", "contract_test", [{"ok": True}]) > 0
    assert repo.persist_instruments([instrument]) == 1
    assert repo.persist_price_bars([bar]) == 1
    assert repo.persist_corporate_actions([action]) == 1
    assert repo.persist_session_calendar([session]) == 1
    assert repo.mark_curated_dataset("contract", "v1", payload={"ok": True}) > 0

    print("PASS: ingestion contract tests")


if __name__ == "__main__":
    run()

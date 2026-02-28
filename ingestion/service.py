from datetime import datetime, timezone
from typing import Dict, List, Optional

from core.config import AppConfig, get_config
from ingestion.models import CorporateAction, Instrument, PriceBar, SessionCalendar
from ingestion.observability import METRICS, log_event
from ingestion.providers.hierarchy import ProviderHierarchy
from ingestion.providers.stubs import StubProvider
from ingestion.repository import IngestionRepository
from ingestion.validation import (
    BlockingValidationError,
    DataValidator,
    ValidationHooks,
    default_drift_hook,
    default_spike_hook,
)


class DataIngestionService:
    def __init__(
        self,
        config: Optional[AppConfig] = None,
        hierarchy: Optional[ProviderHierarchy] = None,
        repository: Optional[IngestionRepository] = None,
        validator: Optional[DataValidator] = None,
    ) -> None:
        self.config = config or get_config()
        self.repository = repository or IngestionRepository()
        self.hierarchy = hierarchy or ProviderHierarchy(
            primary=StubProvider(self.config.data_provider_primary),
            fallback=StubProvider(self.config.data_provider_fallback),
        )
        self.validator = validator or DataValidator(
            self.config,
            hooks=ValidationHooks(
                drift_check=default_drift_hook,
                spike_check=default_spike_hook,
            ),
        )

    def ingest_dataset(self, dataset: str, expected_count: Optional[int] = None) -> Dict[str, object]:
        started = datetime.now(timezone.utc)
        result = self.hierarchy.fetch_with_failover(dataset)

        self.repository.capture_raw_payload(
            dataset=dataset,
            provider=result.provider,
            payload=result.records,
        )

        warnings: List[str] = []
        persisted = 0
        if dataset == "instrument":
            canonical_records = [Instrument.model_validate(item) for item in result.records]
            self.validator.validate_instruments(canonical_records, expected_count=expected_count)
            persisted = self.repository.persist_instruments(canonical_records)
        elif dataset == "price_bar":
            canonical_records = [PriceBar.model_validate(item) for item in result.records]
            validation = self.validator.validate_price_bars(canonical_records, expected_count=expected_count)
            warnings = validation.warnings
            persisted = self.repository.persist_price_bars(canonical_records)
            self._record_missing_bar_hook(canonical_records, expected_count)
        elif dataset == "corporate_action":
            canonical_records = [CorporateAction.model_validate(item) for item in result.records]
            self.validator.validate_corporate_actions(canonical_records, expected_count=expected_count)
            persisted = self.repository.persist_corporate_actions(canonical_records)
        elif dataset == "session_calendar":
            canonical_records = [SessionCalendar.model_validate(item) for item in result.records]
            self.validator.validate_session_calendar(canonical_records, expected_count=expected_count)
            persisted = self.repository.persist_session_calendar(canonical_records)
        else:
            raise BlockingValidationError(f"Unsupported dataset: {dataset}")

        curated_version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.repository.mark_curated_dataset(
            dataset_name=dataset,
            dataset_version=curated_version,
            status="placeholder",
            payload={"source_provider": result.provider, "records": persisted},
        )

        latency_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000.0
        METRICS.incr("ingestion_dataset_runs_total")
        log_event(
            "ingestion_complete",
            dataset=dataset,
            provider=result.provider,
            used_fallback=result.used_fallback,
            records=persisted,
            pipeline_latency_ms=latency_ms,
            warnings=warnings,
        )

        return {
            "dataset": dataset,
            "provider": result.provider,
            "used_fallback": result.used_fallback,
            "records": persisted,
            "warnings": warnings,
            "pipeline_latency_ms": latency_ms,
            "metrics": METRICS.snapshot(),
        }

    def _record_missing_bar_hook(self, bars: List[PriceBar], expected_count: Optional[int]) -> None:
        if expected_count is None:
            return
        missing = max(expected_count - len(bars), 0)
        if missing > 0:
            METRICS.incr("missing_bars_total", missing)
            log_event("missing_bars_detected", missing=missing, expected=expected_count, actual=len(bars))

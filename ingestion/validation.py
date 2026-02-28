from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, List, Optional, Sequence, Set

from core.config import AppConfig
from ingestion.models import CorporateAction, Instrument, PriceBar, SessionCalendar
from ingestion.observability import METRICS, log_event


class BlockingValidationError(ValueError):
    pass


@dataclass
class ValidationHooks:
    drift_check: Optional[Callable[[Sequence[PriceBar]], List[str]]] = None
    spike_check: Optional[Callable[[Sequence[PriceBar]], List[str]]] = None


@dataclass
class ValidationResult:
    warnings: List[str] = field(default_factory=list)


class DataValidator:
    def __init__(self, config: AppConfig, hooks: Optional[ValidationHooks] = None) -> None:
        self.config = config
        self.hooks = hooks or ValidationHooks()

    def validate_instruments(
        self,
        records: Iterable[Instrument],
        expected_count: Optional[int] = None,
    ) -> ValidationResult:
        items = list(records)
        if not items:
            raise BlockingValidationError("No instrument records received.")
        self._validate_completeness(len(items), expected_count)
        seen: Set[str] = set()
        for item in items:
            if item.instrument_id in seen:
                raise BlockingValidationError(f"Duplicate instrument_id: {item.instrument_id}")
            seen.add(item.instrument_id)
        return ValidationResult()

    def validate_price_bars(
        self,
        records: Iterable[PriceBar],
        expected_count: Optional[int] = None,
    ) -> ValidationResult:
        items = list(records)
        if not items:
            raise BlockingValidationError("No price bar records received.")
        self._validate_completeness(len(items), expected_count)

        now = datetime.now(timezone.utc)
        unique_keys: Set[str] = set()
        warnings: List[str] = []

        for item in items:
            age = now - item.ts_ingest
            if age > timedelta(seconds=self.config.data_freshness_sla_seconds):
                METRICS.incr("freshness_breach_total")
                raise BlockingValidationError(
                    f"Freshness SLA breach for {item.instrument_id}: {age.total_seconds():.2f}s"
                )

            if item.open < 0 or item.high < 0 or item.low < 0 or item.close < 0 or item.volume < 0:
                raise BlockingValidationError(f"Negative price/volume for {item.instrument_id}")
            if item.high < max(item.open, item.close, item.low):
                raise BlockingValidationError(f"Invalid high bound for {item.instrument_id}")
            if item.low > min(item.open, item.close, item.high):
                raise BlockingValidationError(f"Invalid low bound for {item.instrument_id}")

            key = f"{item.instrument_id}:{item.timeframe}:{item.ts_event.isoformat()}"
            if key in unique_keys:
                raise BlockingValidationError(f"Duplicate bar key: {key}")
            unique_keys.add(key)

        if self.hooks.drift_check:
            warnings.extend(self.hooks.drift_check(items))
        if self.hooks.spike_check:
            warnings.extend(self.hooks.spike_check(items))

        if warnings:
            METRICS.incr("warning_validation_total", len(warnings))
            log_event("validation_warning", dataset="price_bar", warnings=warnings)

        return ValidationResult(warnings=warnings)

    def validate_corporate_actions(
        self,
        records: Iterable[CorporateAction],
        expected_count: Optional[int] = None,
    ) -> ValidationResult:
        items = list(records)
        if not items:
            raise BlockingValidationError("No corporate action records received.")
        self._validate_completeness(len(items), expected_count)

        unique_keys: Set[str] = set()
        for item in items:
            key = f"{item.instrument_id}:{item.action_type}:{item.effective_date.isoformat()}"
            if key in unique_keys:
                raise BlockingValidationError(f"Duplicate corporate action key: {key}")
            unique_keys.add(key)

        return ValidationResult()

    def validate_session_calendar(
        self,
        records: Iterable[SessionCalendar],
        expected_count: Optional[int] = None,
    ) -> ValidationResult:
        items = list(records)
        if not items:
            raise BlockingValidationError("No session calendar records received.")
        self._validate_completeness(len(items), expected_count)

        unique_keys: Set[str] = set()
        for item in items:
            if item.session_start >= item.session_end:
                raise BlockingValidationError(
                    f"Invalid session window for {item.venue} {item.session_date}"
                )
            key = f"{item.venue}:{item.session_date.isoformat()}"
            if key in unique_keys:
                raise BlockingValidationError(f"Duplicate session calendar key: {key}")
            unique_keys.add(key)
        return ValidationResult()

    def _validate_completeness(self, actual_count: int, expected_count: Optional[int]) -> None:
        if expected_count is None or expected_count <= 0:
            return
        ratio = actual_count / float(expected_count)
        if ratio < self.config.data_completeness_min_ratio:
            METRICS.incr("completeness_breach_total")
            raise BlockingValidationError(
                f"Completeness breach: actual={actual_count}, expected={expected_count}, ratio={ratio:.4f}"
            )


# Warning hook examples for drift/spike monitoring.
def default_drift_hook(records: Sequence[PriceBar]) -> List[str]:
    warnings: List[str] = []
    if len(records) >= 2:
        closes = [record.close for record in records]
        mean_close = sum(closes) / len(closes)
        if mean_close > 0:
            latest = closes[-1]
            drift = abs(latest - mean_close) / mean_close
            if drift >= 0.15:
                warnings.append(f"price_drift_warning drift={drift:.4f}")
    return warnings


def default_spike_hook(records: Sequence[PriceBar]) -> List[str]:
    warnings: List[str] = []
    for record in records:
        if record.open > 0:
            spike = abs(record.close - record.open) / record.open
            if spike >= 0.12:
                warnings.append(
                    f"intrabar_spike_warning instrument={record.instrument_id} spike={spike:.4f}"
                )
    return warnings

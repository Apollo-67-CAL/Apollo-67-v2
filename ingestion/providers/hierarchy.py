from ingestion.models import ProviderResult
from ingestion.observability import METRICS, log_event
from ingestion.providers.base import DataProvider, ProviderUnavailableError


class ProviderHierarchy:
    def __init__(self, primary: DataProvider, fallback: DataProvider) -> None:
        self.primary = primary
        self.fallback = fallback

    def fetch_with_failover(self, dataset: str) -> ProviderResult:
        try:
            result = self.primary.fetch_dataset(dataset)
            METRICS.incr("ingestion_success_total")
            log_event(
                "provider_success",
                dataset=dataset,
                provider=result.provider,
                used_fallback=False,
                latency_ms=result.latency_ms,
            )
            return result
        except ProviderUnavailableError as exc:
            METRICS.incr("ingestion_fail_total")
            METRICS.incr("failover_events_total")
            log_event(
                "provider_failover",
                dataset=dataset,
                primary_provider=self.primary.name,
                fallback_provider=self.fallback.name,
                reason=str(exc),
            )
            result = self.fallback.fetch_dataset(dataset)
            result.used_fallback = True
            METRICS.incr("ingestion_success_total")
            log_event(
                "provider_success",
                dataset=dataset,
                provider=result.provider,
                used_fallback=True,
                latency_ms=result.latency_ms,
            )
            return result

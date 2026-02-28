from typing import Protocol

from ingestion.models import ProviderResult


class ProviderUnavailableError(RuntimeError):
    pass


class DataProvider(Protocol):
    name: str

    def fetch_dataset(self, dataset: str) -> ProviderResult:
        ...

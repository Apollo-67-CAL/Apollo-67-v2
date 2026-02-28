from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class Instrument(BaseModel):
    instrument_id: str
    symbol: str
    venue: str
    asset_type: str
    currency: str
    is_tradable: bool = True
    effective_from: datetime
    effective_to: Optional[datetime] = None
    source_provider: str


class PriceBar(BaseModel):
    instrument_id: str
    timeframe: str = "1m"
    ts_event: datetime
    ts_ingest: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_provider: str
    quality_flags: List[str] = Field(default_factory=list)

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, value: str) -> str:
        if not value:
            raise ValueError("timeframe cannot be empty")
        return value


class CorporateAction(BaseModel):
    instrument_id: str
    action_type: str
    effective_date: date
    factor_or_amount: float
    source_provider: str


class SessionCalendar(BaseModel):
    venue: str
    session_date: date
    is_open: bool
    session_start: str
    session_end: str
    timezone: str
    source_provider: str


class ProviderResult(BaseModel):
    dataset: str
    provider: str
    records: list[dict]
    latency_ms: float
    used_fallback: bool = False

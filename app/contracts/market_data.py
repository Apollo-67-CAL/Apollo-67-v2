from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class InstrumentMasterRecord(BaseModel):
    instrument_id: str
    symbol: str
    venue: Optional[str] = None
    asset_type: Optional[str] = None
    currency: Optional[str] = None
    source_provider: str = "twelvedata"


class CanonicalBar(BaseModel):
    instrument_id: str
    ts_event: datetime
    ts_ingest: datetime
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(ge=0)
    volume: float = Field(ge=0)
    source_provider: str
    quality_flags: list[str] = Field(default_factory=list)

    @field_validator("ts_event", "ts_ingest")
    @classmethod
    def ensure_tzaware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value


class CanonicalQuote(BaseModel):
    instrument_id: str
    ts_event: datetime
    ts_ingest: datetime
    last: float = Field(ge=0)
    bid: Optional[float] = Field(default=None, ge=0)
    ask: Optional[float] = Field(default=None, ge=0)
    source_provider: str
    quality_flags: list[str] = Field(default_factory=list)

    @field_validator("ts_event", "ts_ingest")
    @classmethod
    def ensure_tzaware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

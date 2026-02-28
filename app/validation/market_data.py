from datetime import datetime, timedelta, timezone

from app.contracts.market_data import CanonicalBar, CanonicalQuote


class ValidationError(ValueError):
    pass


def _ensure_utc(ts: datetime, field_name: str) -> None:
    if ts.tzinfo is None:
        raise ValidationError(f"{field_name} must be timezone-aware UTC")
    if ts.utcoffset() != timedelta(0):
        raise ValidationError(f"{field_name} must be UTC")


def validate_bars(bars: list[CanonicalBar]) -> None:
    if not bars:
        raise ValidationError("No bars returned")

    seen: set[str] = set()
    for bar in bars:
        _ensure_utc(bar.ts_event, "ts_event")
        _ensure_utc(bar.ts_ingest, "ts_ingest")

        if bar.open < 0 or bar.high < 0 or bar.low < 0 or bar.close < 0 or bar.volume < 0:
            raise ValidationError("Negative values are not allowed")
        if bar.high < max(bar.open, bar.close, bar.low):
            raise ValidationError("Invalid OHLC: high bound")
        if bar.low > min(bar.open, bar.close, bar.high):
            raise ValidationError("Invalid OHLC: low bound")

        key = f"{bar.instrument_id}:{bar.ts_event.isoformat()}"
        if key in seen:
            raise ValidationError("Duplicate bars in response")
        seen.add(key)


def validate_quote(quote: CanonicalQuote, freshness_seconds: int = 30) -> None:
    _ensure_utc(quote.ts_event, "ts_event")
    _ensure_utc(quote.ts_ingest, "ts_ingest")

    if quote.last < 0 or (quote.bid is not None and quote.bid < 0) or (quote.ask is not None and quote.ask < 0):
        raise ValidationError("Quote has negative values")

    now = datetime.now(timezone.utc)
    age = now - quote.ts_ingest
    if age.total_seconds() > freshness_seconds:
        raise ValidationError("Quote freshness SLA breach")

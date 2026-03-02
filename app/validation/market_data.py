from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional


class ValidationError(Exception):
    pass


def _as_mapping(bar: Any) -> dict[str, Any]:
    """
    Normalize a bar into a dict with keys:
    ts_event, open, high, low, close, volume (optional)

    Accepts:
    - Pydantic / objects with attributes
    - dict-like
    - tuple/list in (ts_event, open, high, low, close, volume?) order
    """
    # Tuple/list from DB rows or provider adapters
    if isinstance(bar, (tuple, list)):
        if len(bar) < 5:
            raise ValidationError(f"Invalid bar tuple length: {len(bar)}")
        return {
            "ts_event": bar[0],
            "open": bar[1],
            "high": bar[2],
            "low": bar[3],
            "close": bar[4],
            "volume": bar[5] if len(bar) > 5 else None,
        }

    # Dict
    if isinstance(bar, Mapping):
        return dict(bar)

    # Object with attributes
    ts_event = getattr(bar, "ts_event", None)
    if ts_event is None and hasattr(bar, "model_dump"):
        # Pydantic v2
        dumped = bar.model_dump(mode="json")
        if isinstance(dumped, dict):
            return dumped

    # Generic attribute extraction
    return {
        "ts_event": getattr(bar, "ts_event", None),
        "open": getattr(bar, "open", None),
        "high": getattr(bar, "high", None),
        "low": getattr(bar, "low", None),
        "close": getattr(bar, "close", None),
        "volume": getattr(bar, "volume", None),
    }


def _ensure_ts(ts: Any) -> Any:
    """
    Ensure ts_event is present. If it's a datetime, force UTC.
    We do not force a specific string format here because different providers vary.
    """
    if ts is None or ts == "":
        raise ValidationError("Missing ts_event")

    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    return ts


def _ensure_float(x: Any, field: str) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception as exc:
        raise ValidationError(f"Invalid {field}: {x!r}") from exc


def validate_bars(bars: Iterable[Any]) -> None:
    """
    Validates bars regardless of their shape.
    Accepts objects, dicts, tuples.
    """
    for bar in bars:
        b = _as_mapping(bar)

        b["ts_event"] = _ensure_ts(b.get("ts_event"))

        # Required OHLC
        for f in ("open", "high", "low", "close"):
            val = b.get(f)
            _ensure_float(val, f)

        # Optional volume
        if "volume" in b:
            _ensure_float(b.get("volume"), "volume")


def validate_quote(quote: Any) -> None:
    """
    Minimal quote validation for provider responses.
    Accepts dict or object with attributes.
    """
    if quote is None:
        raise ValidationError("Missing quote")

    if isinstance(quote, Mapping):
        price = quote.get("price") or quote.get("close") or quote.get("last")
        if price is None:
            raise ValidationError("Quote missing price")
        _ensure_float(price, "price")
        return

    price = getattr(quote, "price", None) or getattr(quote, "close", None) or getattr(quote, "last", None)
    if price is None:
        raise ValidationError("Quote missing price")
    _ensure_float(price, "price")

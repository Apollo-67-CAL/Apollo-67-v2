from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.providers.selector import (
    get_bars_cached_first,
    get_quote_cached_first,
)
from app.services.basic_signal import compute_basic_signal
from app.services.trade_signal import compute_trade_signal


def _num_or_none(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if num != num:  # NaN
        return None
    return num


def _bars_to_dicts(bars: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bar in bars or []:
        if hasattr(bar, "model_dump"):
            dumped = bar.model_dump(mode="json")
            if isinstance(dumped, dict):
                out.append(dumped)
        elif isinstance(bar, dict):
            out.append(bar)
    out.sort(key=lambda x: str(x.get("ts_event") or ""))
    return out


def _near_entry_tag(price: Any, entry_low: Any, entry_high: Any) -> bool:
    try:
        p = float(price)
        lo = float(entry_low)
        hi = float(entry_high)
    except Exception:
        return False
    pad = p * 0.005
    return (lo - pad) <= p <= (hi + pad)


def rank_buy_opportunity(row: Dict[str, Any]) -> float:
    score = 0.0
    confidence = row.get("confidence")
    try:
        score += float(confidence) * 100.0
    except Exception:
        pass
    action = str(row.get("action") or "").upper()
    if action == "BUY":
        score += 20.0
    if action == "SELL":
        score -= 30.0
    tags = row.get("tags") if isinstance(row.get("tags"), list) else []
    if "Near Entry" in tags:
        score += 10.0
    rr = row.get("rr")
    try:
        if float(rr) < 1.5:
            score -= 10.0
    except Exception:
        pass
    return score


def build_scanner_row(
    symbol: str,
    interval: str = "1day",
    bars: int = 60,
    allow_live: bool = False,
    bars_ttl_seconds: int = 21600,
    quote_ttl_seconds: int = 900,
) -> Dict[str, Any]:
    symbol_u = (symbol or "").strip().upper()
    if not symbol_u:
        raise ValueError("Missing symbol")

    quote_res = get_quote_cached_first(
        symbol=symbol_u,
        max_age_seconds=int(quote_ttl_seconds),
        allow_live=allow_live,
        freshness_seconds=60,
    )
    bars_res = get_bars_cached_first(
        symbol=symbol_u,
        interval=interval,
        outputsize=int(bars),
        max_age_seconds=int(bars_ttl_seconds),
        allow_live=allow_live,
    )
    bars_dicts = _bars_to_dicts(bars_res.bars if hasattr(bars_res, "bars") else [])
    if not bars_dicts:
        raise ValueError(f"No bars for {symbol_u}")

    basic = compute_basic_signal(bars_dicts)
    trade = compute_trade_signal(
        bars_dicts,
        symbol=symbol_u,
        provider_used=bars_res.provider,
        timeframe=interval,
    )

    action = str(trade.get("action") or "HOLD").upper()
    confidence = trade.get("confidence")
    entry_zone = trade.get("entry_zone") if isinstance(trade.get("entry_zone"), dict) else {}
    entry_low = _num_or_none(entry_zone.get("low"))
    entry_high = _num_or_none(entry_zone.get("high"))

    quote_last = _num_or_none(getattr(quote_res.quote, "last", None))
    trade_last = _num_or_none(trade.get("last_close"))
    bar_last = _num_or_none(bars_dicts[-1].get("close")) if bars_dicts else None

    last_price = None
    price_source = None
    if quote_last is not None and quote_last > 0:
        last_price = quote_last
        price_source = "quote"
    elif trade_last is not None and trade_last > 0:
        last_price = trade_last
        price_source = "trade"
    elif bar_last is not None and bar_last > 0:
        last_price = bar_last
        price_source = "bar"

    tags: List[str] = []
    if _near_entry_tag(last_price, entry_low, entry_high):
        tags.append("Near Entry")
    trend = str(basic.get("trend") or "").lower()
    momentum = str(basic.get("momentum") or "").lower()
    if "bull" in trend:
        tags.append("Uptrend")
    if "positive" in momentum:
        tags.append("Momentum+")

    row: Dict[str, Any] = {
        "symbol": symbol_u,
        "price": last_price,
        "price_source": price_source,
        "provider": bars_res.provider,
        "provider_used": quote_res.provider,
        "trade_provider_used": bars_res.provider,
        "timeframe": interval,
        "recommendation": action,
        "action": action,
        "confidence": confidence,
        "entry_zone": {"low": entry_low, "high": entry_high},
        "entry_low": entry_low,
        "entry_high": entry_high,
        "target_price": _num_or_none(trade.get("target_sell_price")),
        "target": _num_or_none(trade.get("target_sell_price")),
        "stop": _num_or_none(trade.get("stop_loss_price")),
        "trail": _num_or_none(trade.get("trailing_stop_price")),
        "rr": _num_or_none(trade.get("risk_reward_ratio")),
        "tags": tags,
        "reasons": trade.get("reasons") if isinstance(trade.get("reasons"), list) else [],
        "short_reason": (trade.get("reasons")[0] if isinstance(trade.get("reasons"), list) and trade.get("reasons") else ""),
        "explanation": trade.get("explanation"),
        "score": basic.get("score"),
        "trend": basic.get("trend"),
        "momentum": basic.get("momentum"),
        "snapshot": f"{action} setup from {bars_res.provider} bars",
    }
    row["buy_opportunity"] = rank_buy_opportunity(row)
    return row

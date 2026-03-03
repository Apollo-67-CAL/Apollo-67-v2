from __future__ import annotations

from typing import Any, Dict, List

from app.providers.selector import get_bars_with_fallback, get_quote_with_fallback
from app.services.basic_signal import compute_basic_signal
from app.services.trade_signal import compute_trade_signal


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


def build_scanner_row(symbol: str, interval: str = "1day", bars: int = 60) -> Dict[str, Any]:
    symbol_u = (symbol or "").strip().upper()
    if not symbol_u:
        raise ValueError("Missing symbol")

    quote_res = get_quote_with_fallback(symbol=symbol_u, freshness_seconds=60)
    bars_res = get_bars_with_fallback(symbol=symbol_u, interval=interval, outputsize=int(bars))
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
    entry_low = entry_zone.get("low")
    entry_high = entry_zone.get("high")
    last_price = trade.get("last_close")
    if last_price in (None, ""):
        last_price = getattr(quote_res.quote, "last", None)

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
        "provider": bars_res.provider,
        "timeframe": interval,
        "action": action,
        "confidence": confidence,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "target": trade.get("target_sell_price"),
        "stop": trade.get("stop_loss_price"),
        "trail": trade.get("trailing_stop_price"),
        "rr": trade.get("risk_reward_ratio"),
        "tags": tags,
        "reasons": trade.get("reasons") if isinstance(trade.get("reasons"), list) else [],
        "explanation": trade.get("explanation"),
        "score": basic.get("score"),
        "trend": basic.get("trend"),
        "momentum": basic.get("momentum"),
    }
    row["buy_opportunity"] = rank_buy_opportunity(row)
    return row

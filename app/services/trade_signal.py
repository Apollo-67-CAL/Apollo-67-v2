# app/services/trade_signal.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _safe_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _get_close(bar: Any) -> Optional[float]:
    if isinstance(bar, dict):
        return _safe_float(bar.get("close"))
    return None


def _sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    window = values[-period:]
    return sum(window) / float(period)


def _rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None

    gains = 0.0
    losses = 0.0
    # last (period) diffs
    for i in range(len(values) - period, len(values)):
        diff = values[i] - values[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses += abs(diff)

    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100.0 - (100.0 / (1.0 + rs))


def _atr(bars: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    if len(bars) < period + 1:
        return None

    trs: List[float] = []
    for i in range(1, len(bars)):
        high = _safe_float(bars[i].get("high"))
        low = _safe_float(bars[i].get("low"))
        prev_close = _safe_float(bars[i - 1].get("close"))
        if high is None or low is None or prev_close is None:
            continue
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    if len(trs) < period:
        return None

    window = trs[-period:]
    return sum(window) / float(period)


def _round2(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(f"{x:.2f}")


def compute_trade_signal(
    bars: List[Dict[str, Any]],
    symbol: str,
    provider_used: str,
    timeframe: str = "1day",
) -> Dict[str, Any]:
    """
    Basic trade-style signal:
    - Trend bias: SMA20 vs SMA50
    - Momentum: price vs SMA20
    - Mean reversion filter: RSI14
    - Risk: ATR14
    Output includes target sell + stops + entry zone + reasons.

    bars: list of dicts with at least ts_event, open, high, low, close, volume(optional)
    """

    # Clean closes
    closes: List[float] = []
    cleaned: List[Dict[str, Any]] = []
    for b in bars:
        if not isinstance(b, dict):
            continue
        c = _safe_float(b.get("close"))
        if c is None:
            continue
        cleaned.append(b)
        closes.append(c)

    if len(closes) < 20:
        fallback_target_why = (
            "Target is derived from the chosen entry anchor and stop distance, multiplied by the risk reward ratio (RR). "
            "ATR14 was not available, so a fallback method was used and no numeric target was produced."
        )
        return {
            "symbol": symbol.upper(),
            "provider_used": provider_used,
            "timeframe": timeframe,
            "entry_zone": None,
            "action": "HOLD",
            "confidence": 0.2,
            "last_close": _round2(closes[-1]) if closes else None,
            "target_sell_price": None,
            "stop_loss_price": None,
            "trailing_stop_price": None,
            "risk_reward_ratio": None,
            "indicators": {"sma20": None, "sma50": None, "rsi14": None, "atr14": None},
            "reasons": ["Not enough bars to compute indicators reliably"],
            "explanation": {
                "action_why": "HOLD because there are not enough bars to establish trend and momentum with confidence.",
                "target_why": fallback_target_why,
                "stop_why": "Stop is not set because ATR-based risk levels require more bars.",
                "calc": {
                    "entry_anchor": _round2(closes[-1]) if closes else None,
                    "stop": None,
                    "risk_per_share": None,
                    "risk_reward_ratio": None,
                    "target": None,
                    "atr14": None,
                    "method": "Fallback without ATR",
                },
                "notes": [
                    "Need at least 20 clean bars for trade setup.",
                    "No ATR-based stop/target could be computed.",
                ],
            },
        }

    last_close = closes[-1]

    sma20 = _sma(closes, 20)
    sma50 = _sma(closes, 50)
    rsi14 = _rsi(closes, 14)
    atr14 = _atr(cleaned, 14)

    # Entry zone: ATR half band around last close (simple, predictable)
    entry_low = None
    entry_high = None
    if atr14 is not None:
        entry_low = last_close - (0.5 * atr14)
        entry_high = last_close + (0.5 * atr14)

    # Risk model
    rr = 2.0
    target = None
    stop = None
    trailing = None
    if atr14 is not None:
        stop = last_close - (1.0 * atr14)
        target = last_close + (rr * atr14)
        trailing = last_close - (1.5 * atr14)

    reasons: List[str] = []
    uptrend_bias = (sma20 is not None and sma50 is not None and sma20 > sma50)
    downtrend_bias = (sma20 is not None and sma50 is not None and sma20 < sma50)
    momentum_weak = (sma20 is not None and last_close < sma20)
    momentum_strong = (sma20 is not None and last_close > sma20)

    if uptrend_bias:
        reasons.append("SMA20 above SMA50 (uptrend bias)")
    elif downtrend_bias:
        reasons.append("SMA20 below SMA50 (downtrend bias)")
    else:
        reasons.append("SMA trend unclear")

    if momentum_strong:
        reasons.append("Price above SMA20 (momentum strong)")
    elif momentum_weak:
        reasons.append("Price below SMA20 (momentum weak)")
    else:
        reasons.append("Price near SMA20")

    if rsi14 is not None:
        if rsi14 < 30:
            reasons.append("RSI14 oversold")
        elif rsi14 > 70:
            reasons.append("RSI14 overbought")
        else:
            reasons.append("RSI14 neutral")

    # Action logic (simple, stable, explainable)
    action = "HOLD"
    confidence = 0.45

    # BUY: uptrend + weak momentum + RSI not overbought and price inside entry band
    inside_entry = False
    if entry_low is not None and entry_high is not None:
        inside_entry = (entry_low <= last_close <= entry_high)

    if uptrend_bias and momentum_weak and (rsi14 is None or rsi14 <= 55) and inside_entry:
        action = "BUY"
        confidence = 0.7
    # SELL: downtrend or RSI overbought + momentum fading
    elif (downtrend_bias and momentum_weak) or (rsi14 is not None and rsi14 >= 70 and not momentum_strong):
        action = "SELL"
        confidence = 0.7
    else:
        action = "HOLD"
        confidence = 0.5 if uptrend_bias else 0.45

    entry_anchor = last_close
    risk_per_share = abs(entry_anchor - stop) if stop is not None else None

    if action == "BUY":
        action_why = (
            "BUY because trend bias is supportive (SMA20 above SMA50), momentum pullback is present "
            "(price below/near SMA20), and the setup triggered inside the entry band."
        )
    elif action == "SELL":
        action_why = (
            "SELL because downtrend or risk signals are active, with weak momentum and/or overbought RSI "
            "indicating elevated downside risk."
        )
    else:
        action_why = (
            "HOLD because signals are mixed or there is no clear edge from trend and momentum alignment."
        )

    if atr14 is not None:
        target_why = (
            "Target is derived from the chosen entry anchor and stop distance, multiplied by the risk reward ratio (RR). "
            "ATR14 was available, so the ATR half band + RR method was used."
        )
        stop_why = "Stop is set using ATR distance from the entry anchor (ATR-based risk band)."
        method = "ATR half band + RR"
    else:
        target_why = (
            "Target is derived from the chosen entry anchor and stop distance, multiplied by the risk reward ratio (RR). "
            "ATR14 was not available, so a fallback method was used."
        )
        stop_why = "Stop uses fallback logic because ATR was unavailable."
        method = "Fallback without ATR"

    payload: Dict[str, Any] = {
        "symbol": symbol.upper(),
        "provider_used": provider_used,
        "timeframe": timeframe,
        "entry_zone": (
            {
                "low": _round2(entry_low),
                "high": _round2(entry_high),
                "type": "ATR half band",
            }
            if entry_low is not None and entry_high is not None
            else None
        ),
        "action": action,
        "confidence": float(f"{confidence:.2f}"),
        "last_close": _round2(last_close),
        "target_sell_price": _round2(target),
        "stop_loss_price": _round2(stop),
        "trailing_stop_price": _round2(trailing),
        "risk_reward_ratio": rr if atr14 is not None else None,
        "indicators": {
            "sma20": _round2(sma20),
            "sma50": _round2(sma50),
            "rsi14": float(f"{rsi14:.2f}") if rsi14 is not None else None,
            "atr14": _round2(atr14),
        },
        "reasons": reasons,
        "explanation": {
            "action_why": action_why,
            "target_why": target_why,
            "stop_why": stop_why,
            "calc": {
                "entry_anchor": _round2(entry_anchor),
                "stop": _round2(stop),
                "risk_per_share": _round2(risk_per_share),
                "risk_reward_ratio": rr if atr14 is not None else None,
                "target": _round2(target),
                "atr14": _round2(atr14),
                "method": method,
            },
            "notes": [
                "Entry anchor is the same price reference used by current target/stop math.",
                "RR and ATR outputs are descriptive and do not alter signal/trade calculations.",
            ],
        },
    }

    return payload

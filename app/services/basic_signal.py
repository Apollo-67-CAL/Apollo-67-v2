from statistics import mean
from datetime import datetime, timezone

def compute_basic_signal(bars):
    ordered_bars = sorted(bars, key=_ts_sort_key)
    bars_count = len(ordered_bars)
    first_ts = ordered_bars[0].get("ts_event") if ordered_bars else None
    last_ts = ordered_bars[-1].get("ts_event") if ordered_bars else None
    first_close = ordered_bars[0].get("close") if ordered_bars else None
    last_close = ordered_bars[-1].get("close") if ordered_bars else None

    if bars_count < 20:
        return {
            "score": 0,
            "trend": "neutral",
            "momentum": "neutral",
            "confidence": 0.0,
            "debug": {
                "bars_count": bars_count or None,
                "first_ts": first_ts,
                "last_ts": last_ts,
                "first_close": first_close,
                "last_close": last_close,
                "ma10": None,
                "ma20": None,
                "rsi14": None,
                "clamped_score": 0.0,
                "raw_score": 0.0,
            },
        }

    closes = []
    for bar in ordered_bars:
        close = bar.get("close")
        if close is None:
            return {
                "score": 0,
                "trend": "neutral",
                "momentum": "neutral",
                "confidence": 0.0,
                "debug": {
                    "bars_count": bars_count,
                    "first_ts": first_ts,
                    "last_ts": last_ts,
                    "first_close": first_close,
                    "last_close": last_close,
                    "ma10": None,
                    "ma20": None,
                    "rsi14": None,
                    "raw_score": 0.0,
                },
            }
        closes.append(float(close))

    ma10 = mean(closes[-10:])
    ma20 = mean(closes[-20:])
    trend_pct_diff = ((ma10 - ma20) / ma20) if ma20 else 0.0
    ma_component = trend_pct_diff * 10000.0

    rsi14 = _compute_rsi14(closes)
    rsi_component = (rsi14 - 50.0) * 2.0

    raw_score = ma_component + rsi_component
    clamped_score = _clamp(raw_score, -100.0, 100.0)
    score = int(round(clamped_score))

    if trend_pct_diff > 0:
        trend = "bullish"
    elif trend_pct_diff < 0:
        trend = "bearish"
    else:
        trend = "neutral"

    if rsi14 > 50:
        momentum = "positive"
    elif rsi14 < 50:
        momentum = "negative"
    else:
        momentum = "neutral"

    agreement = (trend == "bullish" and momentum == "positive") or (
        trend == "bearish" and momentum == "negative"
    )
    agreement_factor = 1.0 if agreement else 0.6
    confidence = min(1.0, (abs(clamped_score) / 100.0) * agreement_factor)

    return {
        "score": score,
        "trend": trend,
        "momentum": momentum,
        "confidence": round(confidence, 2),
        "debug": {
            "bars_count": bars_count,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "first_close": first_close,
            "last_close": last_close,
            "ma10": round(ma10, 6),
            "ma20": round(ma20, 6),
            "rsi14": round(rsi14, 6),
            "clamped_score": round(clamped_score, 6),
            "raw_score": round(raw_score, 6),
        },
    }


def _ts_sort_key(bar):
    raw = bar.get("ts_event")
    if raw is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    text = str(raw).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _compute_rsi14(closes):
    period = 14
    if len(closes) <= period:
        return 50.0

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [abs(min(delta, 0.0)) for delta in deltas]

    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])

    for idx in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[idx]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[idx]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _clamp(value, lower, upper):
    return max(lower, min(upper, value))

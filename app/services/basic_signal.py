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
    sma20 = mean(closes[-20:])
    sma50 = mean(closes[-50:]) if len(closes) >= 50 else mean(closes)
    ema20 = _compute_ema(closes, 20)
    rsi14 = _compute_rsi14(closes)
    macd_line, macd_signal, macd_hist = _compute_macd(closes)

    trend_pct_diff = ((sma20 - sma50) / sma50) if sma50 else 0.0
    trend_component = trend_pct_diff * 8000.0
    ema_component = (((ema20 - sma20) / sma20) * 5000.0) if (ema20 is not None and sma20) else 0.0
    rsi_component = (rsi14 - 50.0) * 1.8
    macd_component = (macd_hist * 120.0) if macd_hist is not None else 0.0

    raw_score = trend_component + ema_component + rsi_component + macd_component
    clamped_score = _clamp(raw_score, -100.0, 100.0)
    score = int(round(clamped_score))

    trend_bias = trend_pct_diff
    if ema20 is not None and sma20:
        trend_bias += ((ema20 - sma20) / sma20)
    if trend_bias > 0:
        trend = "bullish"
    elif trend_bias < 0:
        trend = "bearish"
    else:
        trend = "neutral"

    momentum_bias = 0.0
    momentum_bias += (rsi14 - 50.0) / 50.0
    if macd_hist is not None:
        momentum_bias += max(-1.0, min(1.0, macd_hist))
    if momentum_bias > 0.05:
        momentum = "positive"
    elif momentum_bias < -0.05:
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
            "ma20": round(sma20, 6),
            "sma20": round(sma20, 6),
            "sma50": round(sma50, 6),
            "ema20": round(ema20, 6) if ema20 is not None else None,
            "rsi14": round(rsi14, 6),
            "macd": round(macd_line, 6) if macd_line is not None else None,
            "macd_signal": round(macd_signal, 6) if macd_signal is not None else None,
            "macd_hist": round(macd_hist, 6) if macd_hist is not None else None,
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


def _compute_ema(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = mean(values[:period])
    for value in values[period:]:
        ema = (float(value) * k) + (ema * (1.0 - k))
    return ema


def _compute_macd(closes):
    ema12 = _compute_ema(closes, 12)
    ema26 = _compute_ema(closes, 26)
    if ema12 is None or ema26 is None:
        return None, None, None
    macd_line = ema12 - ema26
    # approximate signal from macd history over last 9 points
    series = []
    for i in range(26, len(closes) + 1):
        sub = closes[:i]
        e12 = _compute_ema(sub, 12)
        e26 = _compute_ema(sub, 26)
        if e12 is None or e26 is None:
            continue
        series.append(e12 - e26)
    if len(series) < 9:
        return macd_line, None, None
    signal = mean(series[-9:])
    hist = macd_line - signal
    return macd_line, signal, hist


def _clamp(value, lower, upper):
    return max(lower, min(upper, value))

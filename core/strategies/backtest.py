from __future__ import annotations

from typing import Any, Dict, List, Optional


def _as_float(value: Any) -> Optional[float]:
    try:
        n = float(value)
        return n if n == n else None
    except Exception:
        return None


def _sma(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = []
    if window <= 0:
        return [None for _ in values]
    for i in range(len(values)):
        if i + 1 < window:
            out.append(None)
            continue
        chunk = values[i + 1 - window:i + 1]
        out.append(sum(chunk) / float(window))
    return out


def _rsi(values: List[float], period: int = 14) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if len(values) <= period:
        return out

    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    for i in range(period, len(values)):
        avg_gain = sum(gains[i + 1 - period:i + 1]) / float(period)
        avg_loss = sum(losses[i + 1 - period:i + 1]) / float(period)
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _max_drawdown(equity_curve: List[float]) -> float:
    peak = 0.0
    max_dd = 0.0
    for e in equity_curve:
        if e > peak:
            peak = e
        if peak > 0:
            dd = ((peak - e) / peak) * 100.0
            if dd > max_dd:
                max_dd = dd
    return max_dd


def run_backtest(strategy_payload: Dict[str, Any], bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not bars:
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "trades_count": 0,
            "win_rate": 0.0,
            "equity_curve": [],
        }

    closes: List[float] = []
    ts_labels: List[str] = []
    for b in bars:
        close_v = _as_float((b or {}).get("close"))
        if close_v is None:
            continue
        closes.append(close_v)
        ts_labels.append(str((b or {}).get("ts_event") or (b or {}).get("ts_ingest") or ""))

    if not closes:
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "trades_count": 0,
            "win_rate": 0.0,
            "equity_curve": [],
        }

    mode = str((strategy_payload or {}).get("mode") or "trend_breakout").strip().lower()
    lookback = int((strategy_payload or {}).get("lookback") or 20)

    sma_fast = _sma(closes, max(5, min(50, lookback // 2 if lookback > 10 else 10)))
    sma_slow = _sma(closes, max(20, min(200, lookback)))
    rsi14 = _rsi(closes, 14)

    equity = 100.0
    position = 0.0
    entry = None
    trades = 0
    wins = 0
    equity_curve: List[Dict[str, Any]] = []
    equity_values: List[float] = []

    for i, close in enumerate(closes):
        buy_signal = False
        sell_signal = False

        if mode == "mean_reversion":
            rv = rsi14[i]
            if rv is not None:
                buy_signal = rv <= 35
                sell_signal = rv >= 65
        elif mode == "value_overlay":
            f = sma_fast[i]
            s = sma_slow[i]
            if f is not None and s is not None:
                buy_signal = f > s
                sell_signal = close < s
        else:
            if i >= lookback:
                prev_high = max(closes[i - lookback:i])
                prev_low = min(closes[i - lookback:i])
                buy_signal = close >= prev_high
                sell_signal = close <= prev_low

        if position == 0.0 and buy_signal:
            position = equity / close
            entry = close
            trades += 1
        elif position > 0.0 and sell_signal:
            new_equity = position * close
            if entry is not None and close > entry:
                wins += 1
            equity = new_equity
            position = 0.0
            entry = None

        mark_equity = equity if position == 0.0 else position * close
        equity_values.append(mark_equity)
        equity_curve.append({"ts": ts_labels[i], "equity": round(mark_equity, 6)})

    if position > 0.0:
        final_close = closes[-1]
        final_equity = position * final_close
        if entry is not None and final_close > entry:
            wins += 1
        equity = final_equity

    start_equity = 100.0
    total_return_pct = ((equity - start_equity) / start_equity) * 100.0
    win_rate = (wins / float(trades)) * 100.0 if trades > 0 else 0.0

    return {
        "total_return_pct": round(total_return_pct, 4),
        "max_drawdown_pct": round(_max_drawdown(equity_values), 4),
        "trades_count": trades,
        "win_rate": round(win_rate, 4),
        "equity_curve": equity_curve,
    }

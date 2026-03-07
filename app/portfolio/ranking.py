from __future__ import annotations

from typing import Any, Dict, List, Optional


def _float_or_none(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if num != num:
        return None
    return num


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _score_0_100(value: Any) -> float:
    num = _float_or_none(value)
    if num is None:
        return 0.0
    if 0.0 <= num <= 1.0:
        num *= 100.0
    return _clamp(num, 0.0, 100.0)


def _confidence_0_1(value: Any) -> float:
    num = _float_or_none(value)
    if num is None:
        return 0.0
    if num > 1.0:
        num /= 100.0
    return _clamp(num, 0.0, 1.0)


def _trend_quality(position: Dict[str, Any], scanner_row: Dict[str, Any]) -> float:
    trend = str(scanner_row.get("trend") or scanner_row.get("signal_trend") or "").strip().lower()
    if "bull" in trend:
        return 1.0
    if "bear" in trend:
        return 0.2
    avg_price = _float_or_none(position.get("avg_price")) or 0.0
    last_price = _float_or_none(position.get("last_price")) or 0.0
    if avg_price > 0 and last_price > 0:
        return 0.8 if last_price >= avg_price else 0.4
    return 0.5


def _momentum_quality(position: Dict[str, Any], scanner_row: Dict[str, Any]) -> float:
    momentum = str(scanner_row.get("momentum") or scanner_row.get("signal_momentum") or "").strip().lower()
    if "positive" in momentum or "bull" in momentum:
        return 1.0
    if "negative" in momentum or "bear" in momentum:
        return 0.2
    unreal = _float_or_none(position.get("unrealised_pnl")) or 0.0
    return 0.7 if unreal >= 0 else 0.35


def _signal_basis_multiplier(value: Any) -> float:
    basis = str(value or "").strip().lower()
    if basis == "technical_plus_evidence":
        return 1.0
    if basis == "evidence_only":
        return 0.8
    return 0.9


def rank_open_position(
    position: Dict[str, Any],
    latest_signal: Optional[Dict[str, Any]],
    latest_scanner_data: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    signal = latest_signal if isinstance(latest_signal, dict) else {}
    scanner_row = latest_scanner_data if isinstance(latest_scanner_data, dict) else {}
    symbol = str(position.get("symbol") or "").upper()

    score = _score_0_100(signal.get("score") or scanner_row.get("score"))
    confidence = _confidence_0_1(signal.get("confidence") or scanner_row.get("confidence"))
    signal_basis = str(scanner_row.get("signal_basis") or "technical_only")
    evidence_score = _float_or_none(scanner_row.get("evidence_score_raw")) or 0.0

    avg_price = _float_or_none(position.get("avg_price")) or 0.0
    last_price = _float_or_none(position.get("last_price")) or avg_price
    take_profit = _float_or_none(position.get("take_profit"))
    stop_loss = _float_or_none(position.get("stop_loss"))
    trail = _float_or_none(position.get("trailing_stop"))

    unrealised_pnl_pct = (((last_price - avg_price) / avg_price) * 100.0) if avg_price > 0 and last_price > 0 else 0.0
    distance_to_target_pct = (((take_profit - last_price) / last_price) * 100.0) if take_profit and last_price > 0 else None
    distance_to_stop_pct = (((last_price - stop_loss) / last_price) * 100.0) if stop_loss and last_price > 0 else None
    distance_to_trail_pct = (((last_price - trail) / last_price) * 100.0) if trail and last_price > 0 else None

    trend_q = _trend_quality(position, scanner_row)
    momentum_q = _momentum_quality(position, scanner_row)
    basis_mult = _signal_basis_multiplier(signal_basis)

    pnl_bonus = _clamp(unrealised_pnl_pct * 0.4, -12.0, 12.0)
    target_penalty = -4.0 if distance_to_target_pct is not None and distance_to_target_pct < 2.0 else 0.0
    stop_penalty = -6.0 if distance_to_stop_pct is not None and distance_to_stop_pct < 2.5 else 0.0
    trail_penalty = -4.0 if distance_to_trail_pct is not None and distance_to_trail_pct < 2.0 else 0.0

    base = (
        (score * 0.42)
        + (confidence * 100.0 * 0.24)
        + (trend_q * 100.0 * 0.14)
        + (momentum_q * 100.0 * 0.14)
        + (evidence_score * 1.0)
    ) * basis_mult
    holding_quality_score = _clamp(base + pnl_bonus + target_penalty + stop_penalty + trail_penalty, 0.0, 100.0)

    reasons: List[str] = []
    if trend_q >= 0.8:
        reasons.append("Trend remains supportive.")
    elif trend_q <= 0.3:
        reasons.append("Trend quality is weakening.")
    if momentum_q >= 0.8:
        reasons.append("Momentum remains constructive.")
    elif momentum_q <= 0.3:
        reasons.append("Momentum is deteriorating.")
    if unrealised_pnl_pct > 5:
        reasons.append("Position is in healthy profit.")
    elif unrealised_pnl_pct < -5:
        reasons.append("Position is under pressure.")
    if distance_to_target_pct is not None and distance_to_target_pct < 2.0:
        reasons.append("Price is close to target; consider trimming.")
    if distance_to_stop_pct is not None and distance_to_stop_pct < 2.5:
        reasons.append("Price is close to stop; downside buffer is thin.")
    if evidence_score > 0:
        reasons.append("External evidence contributes to conviction.")

    action_bias = "HOLD"
    if holding_quality_score >= 72 and confidence >= 0.58 and (distance_to_target_pct is None or distance_to_target_pct > 2.5):
        action_bias = "ADD"
    elif holding_quality_score < 34:
        action_bias = "EXIT_CANDIDATE"
    elif holding_quality_score < 50:
        action_bias = "REDUCE"

    if not reasons:
        reasons.append("Insufficient context; maintain neutral hold.")

    return {
        "symbol": symbol,
        "current_score": score,
        "confidence": confidence,
        "signal_basis": signal_basis,
        "evidence_score": evidence_score,
        "trend_quality": trend_q,
        "momentum_quality": momentum_q,
        "unrealized_pnl_pct": unrealised_pnl_pct,
        "distance_to_target_pct": distance_to_target_pct,
        "distance_to_stop_pct": distance_to_stop_pct,
        "holding_quality_score": holding_quality_score,
        "action_bias": action_bias,
        "reasons": reasons[:5],
    }


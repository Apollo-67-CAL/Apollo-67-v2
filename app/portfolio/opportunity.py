from __future__ import annotations

from typing import Any, Dict, List, Optional


def _float_or_none(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


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


def _basis_bonus(signal_basis: str) -> float:
    basis = str(signal_basis or "").strip().lower()
    if basis == "technical_plus_evidence":
        return 8.0
    if basis == "evidence_only":
        return 2.5
    return 0.0


def _action_bonus(action: str) -> float:
    act = str(action or "").strip().upper()
    if act == "BUY":
        return 10.0
    if act == "BUY_CANDIDATE":
        return 5.0
    if act == "WATCHLIST_CANDIDATE":
        return 2.0
    return -8.0


def rank_new_opportunity(scanner_result: Dict[str, Any]) -> Dict[str, Any]:
    row = scanner_result if isinstance(scanner_result, dict) else {}
    symbol = str(row.get("symbol") or "").upper()
    action = str(row.get("action") or "WATCH").upper()
    signal_basis = str(row.get("signal_basis") or "technical_only")
    score = _score_0_100(row.get("score"))
    confidence = _confidence_0_1(row.get("confidence"))
    evidence_score = _float_or_none(row.get("evidence_score_raw")) or 0.0

    price = _float_or_none(row.get("price"))
    target = _float_or_none(row.get("target"))
    stop = _float_or_none(row.get("stop"))

    upside_to_target_pct = (((target - price) / price) * 100.0) if price and target else None
    downside_to_stop_pct = (((price - stop) / price) * 100.0) if price and stop else None
    risk_reward = None
    if upside_to_target_pct is not None and downside_to_stop_pct is not None and downside_to_stop_pct > 0:
        risk_reward = upside_to_target_pct / downside_to_stop_pct

    rr_bonus = 0.0
    if risk_reward is not None:
        rr_bonus = _clamp((risk_reward - 1.0) * 6.0, -8.0, 14.0)
    upside_bonus = _clamp((upside_to_target_pct or 0.0) * 0.25, -3.0, 10.0)
    confidence_component = confidence * 100.0 * 0.30
    score_component = score * 0.45
    evidence_component = min(12.0, evidence_score * 1.1)

    opportunity_score = _clamp(
        score_component
        + confidence_component
        + evidence_component
        + _basis_bonus(signal_basis)
        + _action_bonus(action)
        + rr_bonus
        + upside_bonus,
        0.0,
        100.0,
    )

    quality_tier = "C"
    if opportunity_score >= 72:
        quality_tier = "A"
    elif opportunity_score >= 55:
        quality_tier = "B"

    action_bias = "WATCH"
    if action == "BUY" and opportunity_score >= 55 and confidence >= 0.5:
        action_bias = "BUY"
    elif action == "BUY_CANDIDATE" and opportunity_score >= 62 and confidence >= 0.55:
        action_bias = "BUY"

    reasons: List[str] = []
    if action == "BUY":
        reasons.append("Scanner confirmed BUY setup.")
    elif action == "BUY_CANDIDATE":
        reasons.append("Opportunity detected pending full confirmation.")
    if signal_basis == "technical_plus_evidence":
        reasons.append("Technical and evidence layers are aligned.")
    elif signal_basis == "technical_only":
        reasons.append("Primarily technical signal.")
    if risk_reward is not None:
        reasons.append(f"Risk/reward approximately {risk_reward:.2f}.")
    if upside_to_target_pct is not None:
        reasons.append(f"Upside to target about {upside_to_target_pct:.1f}%.")

    return {
        "symbol": symbol,
        "opportunity_score": opportunity_score,
        "confidence": confidence,
        "signal_basis": signal_basis,
        "evidence_score": evidence_score,
        "upside_to_target_pct": upside_to_target_pct,
        "downside_to_stop_pct": downside_to_stop_pct,
        "risk_reward": risk_reward,
        "quality_tier": quality_tier,
        "action_bias": action_bias,
        "action": action,
        "strategy_id": row.get("strategy_id") or row.get("strategy_key") or row.get("tactic_id"),
        "tactic_label": row.get("tactic_label"),
        "reasons": reasons[:5],
    }


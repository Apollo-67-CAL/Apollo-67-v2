from __future__ import annotations

from typing import Any, Dict, List


def _float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out:
        return float(default)
    return float(out)


def compare_opportunity_to_holdings(
    new_opp: Dict[str, Any],
    open_positions: List[Dict[str, Any]],
    portfolio_state: Dict[str, Any],
) -> Dict[str, Any]:
    opp = new_opp if isinstance(new_opp, dict) else {}
    holdings = [row for row in (open_positions or []) if isinstance(row, dict)]
    state = portfolio_state if isinstance(portfolio_state, dict) else {}

    buy_symbol = str(opp.get("symbol") or "").upper()
    opp_score = _float(opp.get("opportunity_score"), 0.0)
    opp_conf = _float(opp.get("confidence"), 0.0)
    rotate_margin = max(1.0, _float(state.get("rotate_margin"), 10.0))

    if not buy_symbol:
        return {
            "action": "HOLD",
            "sell_symbol": None,
            "buy_symbol": None,
            "reason": "No symbol available for rotation decision.",
            "confidence": 0.0,
        }

    held_symbols = {str(row.get("symbol") or "").upper() for row in holdings}
    if buy_symbol in held_symbols:
        return {
            "action": "ADD_TO_EXISTING",
            "sell_symbol": None,
            "buy_symbol": buy_symbol,
            "reason": "Opportunity is already held; evaluate as add-to-winner.",
            "confidence": max(0.45, min(0.9, opp_conf)),
        }

    if not holdings:
        return {
            "action": "ADD_NEW",
            "sell_symbol": None,
            "buy_symbol": buy_symbol,
            "reason": "No current holdings; best opportunity can be opened directly.",
            "confidence": max(0.40, min(0.9, opp_conf)),
        }

    weakest = min(holdings, key=lambda x: _float(x.get("holding_quality_score"), 0.0))
    weakest_symbol = str(weakest.get("symbol") or "").upper() or None
    weakest_score = _float(weakest.get("holding_quality_score"), 0.0)
    score_gap = opp_score - weakest_score

    if score_gap > rotate_margin:
        confidence = max(0.45, min(0.95, (opp_conf * 0.6) + min(0.35, score_gap / 100.0)))
        return {
            "action": "ROTATE",
            "sell_symbol": weakest_symbol,
            "buy_symbol": buy_symbol,
            "reason": f"{buy_symbol} materially outranks weakest holding {weakest_symbol} by {score_gap:.1f} points.",
            "confidence": confidence,
            "score_gap": score_gap,
            "rotate_margin": rotate_margin,
        }

    return {
        "action": "HOLD",
        "sell_symbol": None,
        "buy_symbol": buy_symbol,
        "reason": "New opportunity does not exceed weakest holding by rotation margin.",
        "confidence": max(0.25, min(0.75, opp_conf * 0.8)),
        "score_gap": score_gap,
        "rotate_margin": rotate_margin,
    }


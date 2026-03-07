from __future__ import annotations

from typing import Any, Dict, Optional


def _float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out:
        return float(default)
    return float(out)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def allocate_capital(
    portfolio_state: Dict[str, Any],
    candidate: Dict[str, Any],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    state = portfolio_state if isinstance(portfolio_state, dict) else {}
    cfg = config if isinstance(config, dict) else {}
    symbol = str(candidate.get("symbol") or "").upper()

    cash_available = max(0.0, _float(state.get("cash_available"), 0.0))
    equity = max(0.0, _float(state.get("equity"), cash_available))
    open_positions_count = _int(state.get("open_positions_count"), 0)
    max_positions = max(1, _int(cfg.get("max_positions"), 8))
    max_position_pct = max(0.05, min(1.0, _float(cfg.get("max_position_pct"), 0.20)))
    min_trade_amount = max(50.0, _float(cfg.get("min_trade_amount"), 500.0))
    default_trade_amount = max(min_trade_amount, _float(cfg.get("default_trade_amount"), 1000.0))
    add_to_winner_amount = max(min_trade_amount, _float(cfg.get("add_to_winner_amount"), 750.0))

    held_map = state.get("positions_by_symbol") if isinstance(state.get("positions_by_symbol"), dict) else {}
    held = held_map.get(symbol) if symbol else None
    held_value = _float((held or {}).get("value"), 0.0)
    max_position_value = equity * max_position_pct if equity > 0 else default_trade_amount
    available_room = max(0.0, max_position_value - held_value)

    planned = add_to_winner_amount if held else default_trade_amount
    if available_room > 0:
        planned = min(planned, available_room)
    planned = min(planned, cash_available)

    can_fund = planned >= min_trade_amount
    needs_rotation = False
    rotation_candidate: Optional[str] = None
    reason = "Sufficient available cash."

    if not held and open_positions_count >= max_positions:
        needs_rotation = True
        can_fund = False
        reason = "Max positions reached; rotate out of weakest holding first."
    elif not can_fund:
        needs_rotation = True
        reason = "Insufficient free cash for minimum trade size."

    if needs_rotation:
        weakest = state.get("weakest_holding")
        if isinstance(weakest, dict):
            rotation_candidate = str(weakest.get("symbol") or "").upper() or None

    return {
        "can_fund": bool(can_fund),
        "allocation_amount": planned if can_fund else 0.0,
        "needs_rotation": bool(needs_rotation),
        "rotation_candidate": rotation_candidate,
        "reason": reason,
        "max_position_value": max_position_value,
        "position_value_room": available_room,
    }


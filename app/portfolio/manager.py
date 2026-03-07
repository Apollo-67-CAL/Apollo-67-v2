from __future__ import annotations

from typing import Any, Dict, List

from app.portfolio.allocation import allocate_capital
from app.portfolio.opportunity import rank_new_opportunity
from app.portfolio.ranking import rank_open_position
from app.portfolio.rotation import compare_opportunity_to_holdings


def _float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out:
        return float(default)
    return float(out)


def _default_config(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = dict(config or {})
    out = {
        "max_positions": int(cfg.get("max_positions") or 8),
        "max_position_pct": _float(cfg.get("max_position_pct"), 0.20),
        "min_trade_amount": _float(cfg.get("min_trade_amount"), 500.0),
        "default_trade_amount": _float(cfg.get("default_trade_amount"), 1000.0),
        "add_to_winner_amount": _float(cfg.get("add_to_winner_amount"), 750.0),
        "rotate_margin": _float(cfg.get("rotate_margin"), 10.0),
    }
    return out


def _scanner_map(scanner_results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in scanner_results or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        existing = out.get(symbol)
        if not existing:
            out[symbol] = row
            continue
        score_new = _float(row.get("score"), 0.0)
        score_old = _float(existing.get("score"), 0.0)
        if score_new > score_old:
            out[symbol] = row
    return out


def build_portfolio_recommendations(
    open_positions: List[Dict[str, Any]],
    scanner_results: List[Dict[str, Any]],
    portfolio_state: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = _default_config(config)
    positions = [row for row in (open_positions or []) if isinstance(row, dict)]
    scanner_rows = [row for row in (scanner_results or []) if isinstance(row, dict)]
    scanner_by_symbol = _scanner_map(scanner_rows)

    open_rankings: List[Dict[str, Any]] = []
    for pos in positions:
        symbol = str(pos.get("symbol") or "").upper()
        scanner_row = scanner_by_symbol.get(symbol, {})
        ranked = rank_open_position(pos, latest_signal=scanner_row, latest_scanner_data=scanner_row)
        ranked["position"] = {
            "symbol": symbol,
            "qty": pos.get("qty"),
            "value": pos.get("value"),
            "strategy_id": pos.get("strategy_id") or pos.get("strategy_key") or pos.get("tactic_id"),
            "tactic_label": pos.get("tactic_label"),
        }
        open_rankings.append(ranked)
    open_rankings.sort(key=lambda x: _float(x.get("holding_quality_score"), 0.0), reverse=True)

    opportunities: List[Dict[str, Any]] = []
    for row in scanner_rows:
        action = str(row.get("action") or "").upper()
        if action in {"REJECTED", "SELL"}:
            continue
        ranked = rank_new_opportunity(row)
        opportunities.append(ranked)
    opportunities.sort(key=lambda x: _float(x.get("opportunity_score"), 0.0), reverse=True)

    held_map = {str(row.get("symbol") or "").upper(): row for row in open_rankings}
    weakest_holding = open_rankings[-1] if open_rankings else None
    portfolio_state_full = dict(portfolio_state or {})
    portfolio_state_full["positions_by_symbol"] = {str(p.get("symbol") or "").upper(): p for p in positions}
    portfolio_state_full["open_positions_count"] = len(positions)
    portfolio_state_full["weakest_holding"] = weakest_holding
    portfolio_state_full["rotate_margin"] = cfg.get("rotate_margin", 10.0)

    recommendations: List[Dict[str, Any]] = []
    rotation_candidates: List[Dict[str, Any]] = []

    for holding in open_rankings[:10]:
        symbol = str(holding.get("symbol") or "").upper()
        bias = str(holding.get("action_bias") or "HOLD").upper()
        if bias == "ADD":
            alloc = allocate_capital(portfolio_state_full, {"symbol": symbol}, cfg)
            recommendations.append(
                {
                    "action": "ADD" if alloc.get("can_fund") else "HOLD",
                    "symbol": symbol,
                    "other_symbol": None,
                    "amount_usd": alloc.get("allocation_amount") if alloc.get("can_fund") else None,
                    "reason": "High-quality holding with supportive trend and momentum.",
                    "confidence": round(max(0.45, min(0.9, _float(holding.get("confidence"), 0.0))), 3),
                }
            )
        elif bias == "REDUCE":
            recommendations.append(
                {
                    "action": "REDUCE",
                    "symbol": symbol,
                    "other_symbol": None,
                    "amount_usd": None,
                    "reason": "Holding quality has weakened versus current portfolio baseline.",
                    "confidence": round(max(0.35, min(0.85, _float(holding.get("confidence"), 0.0))), 3),
                }
            )
        elif bias == "EXIT_CANDIDATE":
            recommendations.append(
                {
                    "action": "REDUCE",
                    "symbol": symbol,
                    "other_symbol": None,
                    "amount_usd": None,
                    "reason": "Position is an exit candidate due to weak holding quality.",
                    "confidence": round(max(0.4, min(0.9, _float(holding.get("confidence"), 0.0) + 0.1)), 3),
                }
            )

    for opp in opportunities[:12]:
        symbol = str(opp.get("symbol") or "").upper()
        if not symbol:
            continue
        rotation = compare_opportunity_to_holdings(opp, open_rankings, portfolio_state_full)
        rotation_candidates.append(rotation)
        action = str(rotation.get("action") or "HOLD").upper()

        if action == "ADD_TO_EXISTING":
            alloc = allocate_capital(portfolio_state_full, {"symbol": symbol}, cfg)
            recommendations.append(
                {
                    "action": "ADD" if alloc.get("can_fund") else "HOLD",
                    "symbol": symbol,
                    "other_symbol": None,
                    "amount_usd": alloc.get("allocation_amount") if alloc.get("can_fund") else None,
                    "reason": rotation.get("reason"),
                    "confidence": round(_float(rotation.get("confidence"), 0.5), 3),
                }
            )
            continue

        if action == "ROTATE":
            alloc = allocate_capital(portfolio_state_full, {"symbol": symbol}, cfg)
            recommendations.append(
                {
                    "action": "ROTATE",
                    "symbol": symbol,
                    "other_symbol": rotation.get("sell_symbol"),
                    "amount_usd": alloc.get("allocation_amount") if alloc.get("allocation_amount", 0) > 0 else cfg.get("default_trade_amount"),
                    "reason": rotation.get("reason"),
                    "confidence": round(_float(rotation.get("confidence"), 0.5), 3),
                }
            )
            continue

        if action == "ADD_NEW":
            alloc = allocate_capital(portfolio_state_full, {"symbol": symbol}, cfg)
            rec_action = "BUY_NEW" if alloc.get("can_fund") else "HOLD"
            reason = str(alloc.get("reason") or rotation.get("reason") or "Portfolio allocation rule applied.")
            recommendations.append(
                {
                    "action": rec_action,
                    "symbol": symbol,
                    "other_symbol": alloc.get("rotation_candidate"),
                    "amount_usd": alloc.get("allocation_amount") if rec_action == "BUY_NEW" else None,
                    "reason": reason,
                    "confidence": round(_float(rotation.get("confidence"), 0.45), 3),
                }
            )

    if not recommendations and open_rankings:
        for row in open_rankings[:5]:
            recommendations.append(
                {
                    "action": "HOLD",
                    "symbol": row.get("symbol"),
                    "other_symbol": None,
                    "amount_usd": None,
                    "reason": "No stronger replacement opportunity detected right now.",
                    "confidence": round(max(0.35, min(0.8, _float(row.get("confidence"), 0.0))), 3),
                }
            )
    elif not recommendations and opportunities:
        for row in opportunities[:5]:
            recommendations.append(
                {
                    "action": "BUY_NEW" if str(row.get("action_bias") or "").upper() == "BUY" else "HOLD",
                    "symbol": row.get("symbol"),
                    "other_symbol": None,
                    "amount_usd": cfg.get("default_trade_amount"),
                    "reason": "No open positions; evaluating top opportunities.",
                    "confidence": round(max(0.3, min(0.8, _float(row.get("confidence"), 0.0))), 3),
                }
            )

    seen = set()
    deduped: List[Dict[str, Any]] = []
    for rec in recommendations:
        key = (str(rec.get("action")), str(rec.get("symbol")), str(rec.get("other_symbol")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rec)

    return {
        "open_position_rankings": open_rankings,
        "new_opportunity_rankings": opportunities,
        "rotation_candidates": rotation_candidates,
        "recommendations": deduped[:30],
        "config_used": cfg,
    }


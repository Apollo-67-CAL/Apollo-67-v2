from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class StrategySpec:
    id: str
    name: str
    group: str
    description: str
    philosophy: str
    rules_summary: List[str]
    default_params: Dict[str, Any]
    signals_used: List[str]
    risk_notes: List[str]


STRATEGY_LIBRARY: List[StrategySpec] = [
    StrategySpec(
        id="buffett_value",
        name="Warren Buffett",
        group="Value / Long-horizon",
        description="Quality business accumulation with low turnover and durable edge focus.",
        philosophy="Prefer resilient companies with strong long-term trend support.",
        rules_summary=[
            "Require broad uptrend filter",
            "Avoid high-volatility breakdown periods",
            "Low trade frequency; hold longer when trend intact",
        ],
        default_params={"mode": "value_overlay", "sma_fast": 50, "sma_slow": 200, "risk": "low"},
        signals_used=["sma_trend", "drawdown_filter", "momentum_regime"],
        risk_notes=["Slow reaction to regime flips", "Position sizing should remain conservative"],
    ),
    StrategySpec(
        id="simons_quant",
        name="Jim Simons",
        group="Quant / Systematic",
        description="Systematic mean-reversion plus momentum blend with strict rule execution.",
        philosophy="Exploit repeatable short-horizon statistical edges.",
        rules_summary=[
            "RSI mean-reversion entries",
            "Volatility-aware exits",
            "No discretionary overrides",
        ],
        default_params={"mode": "mean_reversion", "rsi_buy": 35, "rsi_sell": 65, "risk": "medium"},
        signals_used=["rsi", "atr", "sma_context"],
        risk_notes=["Can underperform in runaway trends", "Needs disciplined stop handling"],
    ),
    StrategySpec(
        id="soros_macro",
        name="George Soros",
        group="Macro / Global Macro",
        description="Macro trend conviction with asymmetric risk-taking when momentum confirms.",
        philosophy="Press advantage in strong macro trend windows.",
        rules_summary=[
            "Trend breakout entries",
            "Scale only with confirmation",
            "Cut quickly on failed breaks",
        ],
        default_params={"mode": "trend_breakout", "lookback": 20, "risk": "high"},
        signals_used=["donchian_breakout", "sma_trend", "atr"],
        risk_notes=["Large swings possible", "Requires hard stop discipline"],
    ),
    StrategySpec(
        id="druckenmiller_macro",
        name="Stanley Druckenmiller",
        group="Macro / Global Macro",
        description="Concentrated momentum with top-down narrative alignment.",
        philosophy="Seek powerful trend persistence with adaptive risk.",
        rules_summary=[
            "Prefer high-momentum leaders",
            "Avoid range-bound markets",
            "Use trailing exits to retain trends",
        ],
        default_params={"mode": "trend_breakout", "lookback": 30, "risk": "high"},
        signals_used=["momentum", "breakout", "trailing_stop"],
        risk_notes=["High concentration risk", "Whipsaw during regime transitions"],
    ),
    StrategySpec(
        id="ptj_macro",
        name="Paul Tudor Jones",
        group="Macro / Global Macro",
        description="Momentum and risk-first tactical trading style.",
        philosophy="Preserve capital first, then compound with strong trends.",
        rules_summary=[
            "Breakout confirmation required",
            "Volatility-adjusted stops",
            "Reduce risk during weak signal quality",
        ],
        default_params={"mode": "trend_breakout", "lookback": 15, "risk": "medium"},
        signals_used=["breakout", "atr", "risk_filter"],
        risk_notes=["Fast stopouts in choppy tape", "Needs stable data quality"],
    ),
    StrategySpec(
        id="trend_following_seykota_dennis",
        name="Trend Following (Seykota/Dennis)",
        group="Trend / Managed Futures",
        description="Classic trend following representation of Ed Seykota and Richard Dennis (Turtles).",
        philosophy="Ride persistent trends and accept many small losses.",
        rules_summary=[
            "Donchian breakout entries",
            "Hold until trend invalidation",
            "Volatility-normalised risk",
        ],
        default_params={"mode": "trend_breakout", "lookback": 20, "risk": "medium"},
        signals_used=["donchian", "atr", "trend_strength"],
        risk_notes=["Multiple false breaks expected", "Requires patience across drawdowns"],
    ),
]


def strategy_by_id(strategy_id: str) -> StrategySpec:
    sid = (strategy_id or "").strip().lower()
    for spec in STRATEGY_LIBRARY:
        if spec.id == sid:
            return spec
    return STRATEGY_LIBRARY[0]


def strategy_list() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for spec in STRATEGY_LIBRARY:
        out.append(
            {
                "id": spec.id,
                "name": spec.name,
                "group": spec.group,
                "description": spec.description,
                "philosophy": spec.philosophy,
                "rules_summary": spec.rules_summary,
                "default_params": spec.default_params,
                "signals_used": spec.signals_used,
                "risk_notes": spec.risk_notes,
            }
        )
    return out

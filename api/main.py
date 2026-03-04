from __future__ import annotations

# api/main.py

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from api.admin_routes import router as admin_router
from app.providers.selector import get_bars_with_fallback, get_quote_with_fallback
from app.providers.twelvedata import ProviderError, TwelveDataClient
from app.services.basic_signal import compute_basic_signal
from app.services.scanner import build_scanner_row, rank_buy_opportunity
from app.services.trade_signal import compute_trade_signal
from app.validation.market_data import ValidationError, validate_bars
from core.config import get_config, initialise_config
from core.repositories.curated_datasets import CuratedDatasetsRepository
from core.repositories.monitor_positions import MonitorPositionsRepository
from core.repositories.paper_trading import PaperTradingRepository
from core.repositories.strategies_dashboard import StrategiesDashboardRepository
from core.repositories.scanner_source_controls import (
    ScannerSourceControlsRepository,
    normalize_source_key,
)
from core.repositories.scanner_connectors import ScannerConnectorsRepository
from core.repositories.scanner_sources import ScannerSourceBreakdownsRepository
from core.strategies.backtest import run_backtest
from core.strategies.library import STRATEGY_LIBRARY, strategy_by_id, strategy_list
from core.scanners.connectors.registry import get_default_connector_registry, registry_by_group
from core.scanners.pipeline import fetch_items
from core.scanners.analyse import analyse_items_openai
from core.papertrading.engine import (
    EVAL_INTERVAL_SECONDS,
    MAX_POSITIONS,
    NOTIONAL_PER_TRADE,
    ROTATE_INTERVAL_SECONDS,
    ROTATE_N,
    PaperTradingEngine,
)
from core.storage.db import DB_DRIVER_MARKER, check_db_connectivity, get_connection, init_db

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
# Load .env from repo root for local dev. Render injects env vars too, harmless.
load_dotenv(BASE_DIR / ".env")
load_dotenv()

app = FastAPI(title="Apollo 67")
# Local start command:
# uvicorn api.main:app --reload --port 8000
app.mount("/static", StaticFiles(directory="api/static"), name="static")
templates = Jinja2Templates(directory="api/templates")

_BATCH_MAX_SYMBOLS = 25
_BATCH_MAX_WORKERS = 4
_BATCH_CACHE_TTL_SECONDS = 60
_BATCH_CACHE_LOCK = Lock()
_BATCH_CACHE: dict[str, tuple[float, Any]] = {}
_SCANNER_UNIVERSE_PATH = BASE_DIR / "app" / "data" / "universe.json"
_SCANNER_UNIVERSE_CACHE: Optional[List[Dict[str, Any]]] = None
_curated_repo = CuratedDatasetsRepository()
_monitor_repo = MonitorPositionsRepository()
_paper_repo = PaperTradingRepository()
_paper_engine = PaperTradingEngine(_paper_repo)
_scanner_sources_repo = ScannerSourceBreakdownsRepository()
_scanner_source_controls_repo = ScannerSourceControlsRepository()
_scanner_connectors_repo = ScannerConnectorsRepository()
_strategies_repo = StrategiesDashboardRepository()
_SCANNER_SOURCES_CACHE_TTL_SECONDS = 300
_SCANNER_SOURCES_CACHE: Dict[str, tuple[float, Dict[str, Any]]] = {}
_SCANNER_SOURCES_CACHE_LOCK = Lock()
_SCANNER_CONNECTOR_RUNTIME: Dict[str, Dict[str, Any]] = {}

_ADMIN_DEFAULT_STATE = {
    "sentiment": {
        "overall": {"weight": 50, "influence": "medium"},
        "institution": {"weight": 50, "influence": "medium"},
        "news": {"weight": 50, "influence": "medium"},
        "social": {"weight": 50, "influence": "medium"},
    },
    "active_tactic_version": "none",
    "paper": {
        "notional_per_trade": NOTIONAL_PER_TRADE,
        "max_positions": MAX_POSITIONS,
        "rotate_n": ROTATE_N,
        "eval_interval_seconds": EVAL_INTERVAL_SECONDS,
        "rotate_interval_seconds": ROTATE_INTERVAL_SECONDS,
    },
    "updated_at": None,
}

_TACTIC_PRESETS: dict[str, dict[str, Any]] = {
    "Conservative Value": {
        "rsi_weight": 0.55,
        "sma_weight": 0.65,
        "atr_multiplier": 1.0,
        "stop_sensitivity": 0.8,
        "trade_threshold": 0.62,
        "risk_multiplier": 0.75,
        "timeframe_bias": "1day",
    },
    "Momentum Breakout": {
        "rsi_weight": 0.45,
        "sma_weight": 0.85,
        "atr_multiplier": 1.25,
        "stop_sensitivity": 0.6,
        "trade_threshold": 0.5,
        "risk_multiplier": 1.15,
        "timeframe_bias": "4h",
    },
    "Swing Trader": {
        "rsi_weight": 0.65,
        "sma_weight": 0.6,
        "atr_multiplier": 1.15,
        "stop_sensitivity": 0.7,
        "trade_threshold": 0.55,
        "risk_multiplier": 0.95,
        "timeframe_bias": "1day",
    },
    "Institutional Accumulation": {
        "rsi_weight": 0.5,
        "sma_weight": 0.8,
        "atr_multiplier": 1.1,
        "stop_sensitivity": 0.75,
        "trade_threshold": 0.58,
        "risk_multiplier": 0.85,
        "timeframe_bias": "1day",
    },
    "Quant Mean Reversion": {
        "rsi_weight": 0.85,
        "sma_weight": 0.4,
        "atr_multiplier": 1.05,
        "stop_sensitivity": 0.72,
        "trade_threshold": 0.57,
        "risk_multiplier": 0.9,
        "timeframe_bias": "1h",
    },
    "Custom": {
        "rsi_weight": 0.7,
        "sma_weight": 0.6,
        "atr_multiplier": 1.1,
        "stop_sensitivity": 0.7,
        "trade_threshold": 0.55,
        "risk_multiplier": 0.9,
        "timeframe_bias": "1day",
    },
}

_SCANNER_AGENT_UNIVERSES: Dict[str, List[str]] = {
    "overall": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX", "CRM", "ORCL", "INTC", "ADBE", "QCOM"],
    "institution": ["BRK-B", "JPM", "GS", "MS", "BLK", "SPGI", "V", "MA", "C", "BAC"],
    "news": ["TSLA", "NVDA", "META", "AAPL", "MSFT", "AMZN", "GOOGL", "NFLX", "AMD", "ORCL"],
    "social": ["TSLA", "NVDA", "PLTR", "AMD", "SOFI", "COIN", "META", "GME", "AAPL", "MSFT"],
}

_SCANNER_SYNTHETIC_SOURCES: Dict[str, List[str]] = {
    "social": ["x", "reddit", "hotcopper", "youtube", "tiktok"],
    "news": ["reuters", "bloomberg", "sec_filings", "company_pr"],
    "institution": ["analyst_ratings", "13f_filings", "insider_trades"],
    "overall": ["composite"],
}


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalise_admin_state(payload: Optional[Dict[str, Any]]) -> dict[str, Any]:
    state = json.loads(json.dumps(_ADMIN_DEFAULT_STATE))
    incoming = payload if isinstance(payload, dict) else {}
    incoming_sentiment = incoming.get("sentiment") if isinstance(incoming.get("sentiment"), dict) else {}

    for scope in ("overall", "institution", "news", "social"):
        src = incoming_sentiment.get(scope) if isinstance(incoming_sentiment.get(scope), dict) else {}
        weight_raw = src.get("weight", state["sentiment"][scope]["weight"])
        influence_raw = str(src.get("influence", state["sentiment"][scope]["influence"])).strip().lower()
        try:
            weight = int(float(weight_raw))
        except Exception:
            weight = state["sentiment"][scope]["weight"]
        weight = max(0, min(100, weight))
        if influence_raw not in {"low", "medium", "high"}:
            influence_raw = "medium"
        state["sentiment"][scope] = {"weight": weight, "influence": influence_raw}

    active_tactic_version = str(incoming.get("active_tactic_version", "none") or "none")
    state["active_tactic_version"] = active_tactic_version
    incoming_paper = incoming.get("paper") if isinstance(incoming.get("paper"), dict) else {}
    try:
        notional = float(incoming_paper.get("notional_per_trade", state["paper"]["notional_per_trade"]))
    except Exception:
        notional = float(state["paper"]["notional_per_trade"])
    try:
        max_positions = int(incoming_paper.get("max_positions", state["paper"]["max_positions"]))
    except Exception:
        max_positions = int(state["paper"]["max_positions"])
    try:
        rotate_n = int(incoming_paper.get("rotate_n", state["paper"]["rotate_n"]))
    except Exception:
        rotate_n = int(state["paper"]["rotate_n"])
    state["paper"] = {
        "notional_per_trade": max(10.0, float(notional)),
        "max_positions": max(1, int(max_positions)),
        "rotate_n": max(0, int(rotate_n)),
        "eval_interval_seconds": int(state["paper"]["eval_interval_seconds"]),
        "rotate_interval_seconds": int(state["paper"]["rotate_interval_seconds"]),
    }
    state["updated_at"] = str(incoming.get("updated_at") or _utc_iso_now())
    return state


def _apply_tactic_heuristics(base: dict[str, Any], instruction: str) -> tuple[dict[str, Any], list[str]]:
    text = (instruction or "").lower()
    next_overrides = dict(base)
    notes: list[str] = []

    def tweak(key: str, delta: float) -> None:
        raw = next_overrides.get(key)
        try:
            next_overrides[key] = float(raw) + delta
        except Exception:
            next_overrides[key] = delta

    if any(k in text for k in ("buffett", "value", "margin of safety")):
        tweak("risk_multiplier", -0.15)
        tweak("trade_threshold", 0.05)
        next_overrides["timeframe_bias"] = "1day"
        notes.append("value profile: lower risk, higher threshold, longer timeframe")

    if any(k in text for k in ("momentum", "breakout")):
        tweak("sma_weight", 0.15)
        tweak("trade_threshold", -0.05)
        tweak("risk_multiplier", 0.15)
        notes.append("momentum profile: higher trend weight, lower threshold, higher risk")

    if "mean reversion" in text:
        tweak("rsi_weight", 0.2)
        tweak("sma_weight", -0.1)
        tweak("trade_threshold", 0.02)
        notes.append("mean reversion profile: higher RSI, lower SMA, moderate threshold")

    if "tight stops" in text:
        tweak("atr_multiplier", -0.2)
        tweak("stop_sensitivity", 0.12)
        notes.append("tight stops: lower ATR, higher stop sensitivity")

    if "wide stops" in text:
        tweak("atr_multiplier", 0.2)
        tweak("stop_sensitivity", -0.12)
        notes.append("wide stops: higher ATR, lower stop sensitivity")

    for k in ("rsi_weight", "sma_weight", "atr_multiplier", "stop_sensitivity", "trade_threshold", "risk_multiplier"):
        try:
            next_overrides[k] = round(max(0.01, min(2.5, float(next_overrides[k]))), 4)
        except Exception:
            next_overrides[k] = base.get(k)

    if next_overrides.get("timeframe_bias") not in {"1h", "4h", "1day"}:
        next_overrides["timeframe_bias"] = str(base.get("timeframe_bias", "1day"))

    return next_overrides, notes


def _has_any_market_key() -> bool:
    keys = (
        os.getenv("FINNHUB_API_KEY", "").strip(),
        os.getenv("TWELVEDATA_API_KEY", "").strip(),
        os.getenv("ALPHAVANTAGE_API_KEY", "").strip(),
    )
    return any(keys)


def _load_scanner_universe() -> List[Dict[str, Any]]:
    global _SCANNER_UNIVERSE_CACHE
    if _SCANNER_UNIVERSE_CACHE is not None:
        return _SCANNER_UNIVERSE_CACHE

    try:
        raw = _SCANNER_UNIVERSE_PATH.read_text()
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            rows: List[Dict[str, Any]] = []
            for item in parsed:
                if isinstance(item, dict):
                    rows.append(item)
            _SCANNER_UNIVERSE_CACHE = rows
            return rows
    except Exception:
        pass

    _SCANNER_UNIVERSE_CACHE = []
    return _SCANNER_UNIVERSE_CACHE


def _score_sort_value(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("score"))
    except Exception:
        return float("-inf")


def _as_float_or_none(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    return num if num == num else None


def _build_sources_payload_from_row(row: Dict[str, Any], scanner_type: str) -> List[Dict[str, Any]]:
    candidate_sources = row.get("sources")
    sources_payload: List[Dict[str, Any]] = []

    if isinstance(candidate_sources, list):
        for source in candidate_sources:
            if not isinstance(source, dict):
                continue
            name = str(source.get("name") or source.get("source") or source.get("id") or "").strip()
            if not name:
                continue
            source_key = normalize_source_key(str(source.get("id") or name))
            mentions = int(source.get("mentions") or 0)
            confidence = _as_float_or_none(source.get("confidence"))
            score = _as_float_or_none(source.get("score"))
            sources_payload.append(
                {
                    "id": source.get("id") or source_key,
                    "name": name,
                    "origin": str(source.get("origin") or "auto"),
                    "mentions": mentions,
                    "positive": int(source.get("positive") or 0),
                    "negative": int(source.get("negative") or 0),
                    "neutral": int(source.get("neutral") or 0),
                    "score": score if score is not None else 0.0,
                    "confidence": confidence,
                    "meta": source.get("meta") if isinstance(source.get("meta"), dict) else {},
                }
            )

    if sources_payload:
        return sources_payload

    names = _SCANNER_SYNTHETIC_SOURCES.get(scanner_type, ["unclassified"])
    score = _as_float_or_none(row.get("score"))
    confidence = _as_float_or_none(row.get("confidence"))
    trend = str(row.get("trend") or "").lower()
    momentum = str(row.get("momentum") or "").lower()
    is_pos = "bull" in trend or "positive" in momentum
    is_neg = "bear" in trend or "negative" in momentum
    reason_count = len(row.get("reasons") if isinstance(row.get("reasons"), list) else [])
    base_mentions = max(1, reason_count or int(abs(score or 0) // 25) + 1)

    synthetic: List[Dict[str, Any]] = []
    for idx, name in enumerate(names):
        key = normalize_source_key(name)
        mentions = max(1, base_mentions - (idx % 2))
        positive = mentions if is_pos else 0
        negative = mentions if is_neg else 0
        neutral = mentions if not is_pos and not is_neg else 0
        synthetic.append(
            {
                "id": key,
                "name": str(name).upper() if scanner_type == "social" else str(name),
                "origin": "synthetic",
                "mentions": mentions,
                "positive": positive,
                "negative": negative,
                "neutral": neutral,
                "score": score if score is not None else 0.0,
                "confidence": confidence if confidence is not None else 0.35,
                "meta": {"generator": "scanner_synthetic_v1"},
            }
        )
    return synthetic


def _recompute_source_totals(sources: List[Dict[str, Any]]) -> Dict[str, Any]:
    mentions = 0
    positive = 0
    negative = 0
    neutral = 0
    score_values: List[float] = []
    confidence_values: List[float] = []
    for source in sources:
        mentions += int(source.get("mentions") or 0)
        positive += int(source.get("positive") or 0)
        negative += int(source.get("negative") or 0)
        neutral += int(source.get("neutral") or 0)
        score = _as_float_or_none(source.get("score"))
        confidence = _as_float_or_none(source.get("confidence"))
        if score is not None:
            score_values.append(score)
        if confidence is not None:
            confidence_values.append(confidence)
    avg_score = (sum(score_values) / len(score_values)) if score_values else 0.0
    avg_conf = (sum(confidence_values) / len(confidence_values)) if confidence_values else None
    return {
        "mentions": mentions,
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "avg_score": round(avg_score, 6),
        "avg_confidence": round(avg_conf, 6) if avg_conf is not None else None,
    }


def _save_scanner_breakdown(symbol: str, scanner_type: str, row: Dict[str, Any]) -> None:
    sources = _build_sources_payload_from_row(row, scanner_type)
    scanner_snapshot = {
        "symbol": row.get("symbol"),
        "price": row.get("price"),
        "price_source": row.get("price_source"),
        "timeframe": row.get("timeframe"),
        "action": row.get("action"),
        "recommendation": row.get("recommendation"),
        "score": row.get("score"),
        "confidence": row.get("confidence"),
        "entry_zone": row.get("entry_zone") if isinstance(row.get("entry_zone"), dict) else {
            "low": row.get("entry_low"),
            "high": row.get("entry_high"),
        },
        "entry_low": row.get("entry_low"),
        "entry_high": row.get("entry_high"),
        "target": row.get("target"),
        "target_price": row.get("target_price"),
        "stop": row.get("stop"),
        "trail": row.get("trail"),
        "rr": row.get("rr"),
        "tags": row.get("tags") if isinstance(row.get("tags"), list) else [],
        "reasons": row.get("reasons") if isinstance(row.get("reasons"), list) else [],
        "snapshot": row.get("snapshot"),
        "provider_used": row.get("provider_used") or row.get("provider"),
        "trade_provider_used": row.get("trade_provider_used") or row.get("provider"),
    }
    payload = {
        "symbol": symbol,
        "scanner_type": scanner_type,
        "ts": _utc_iso_now(),
        "scanner_row": scanner_snapshot,
        "sources": sources,
        "totals": _recompute_source_totals(sources),
    }
    _scanner_sources_repo.insert_breakdown(symbol=symbol, scanner_type=scanner_type, payload=payload)


def _strategy_instruction_to_payload(instruction: str, preset_id: Optional[str] = None) -> Dict[str, Any]:
    text = str(instruction or "").strip()
    lower = text.lower()
    spec = strategy_by_id(preset_id or "buffett_value")
    title = spec.name
    timeframe = "1day"
    risk = "medium"
    rules: List[str] = []

    if "swing" in lower or "1h" in lower:
        timeframe = "1h"
    if "intraday" in lower or "15min" in lower or "30min" in lower:
        timeframe = "30min"
    if any(k in lower for k in ("low risk", "conservative", "capital preserve")):
        risk = "low"
    if any(k in lower for k in ("aggressive", "high risk", "conviction")):
        risk = "high"

    if "breakout" in lower or "trend" in lower:
        rules.append("Prefer breakout confirmation above recent highs")
    if "mean reversion" in lower or "rsi" in lower:
        rules.append("Use RSI pullback entries and fade extremes")
    if "stop" in lower:
        rules.append("Apply strict stop discipline with volatility-aware exits")
    if "hold" in lower or "long-term" in lower:
        rules.append("Allow longer hold windows while trend remains valid")
    if not rules:
        rules = list(spec.rules_summary[:3])

    if ":" in text:
        maybe_title = text.split(":", 1)[0].strip()
        if maybe_title and len(maybe_title) <= 60:
            title = maybe_title
    elif len(text) > 0:
        title = text[:60]

    return {
        "title": title,
        "instruction": text,
        "timeframe_preference": timeframe,
        "risk_preference": risk,
        "rules": rules,
        "strategy_spec_id": spec.id,
        "derived_from": "heuristics_v1",
    }


def _monitor_summary_by_strategy(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sid = str(row.get("strategy_id") or "unassigned")
        bucket = grouped.setdefault(
            sid,
            {
                "positions": 0,
                "total_pnl": 0.0,
                "wins": 0,
                "pnl_pct_sum": 0.0,
                "pnl_pct_count": 0,
            },
        )
        bucket["positions"] += 1
        pnl = _as_float_or_none(row.get("pnl")) or 0.0
        bucket["total_pnl"] += pnl
        pnl_pct = _as_float_or_none(row.get("pnl_pct"))
        if pnl_pct is not None:
            bucket["pnl_pct_sum"] += pnl_pct
            bucket["pnl_pct_count"] += 1
            if pnl_pct > 0:
                bucket["wins"] += 1

    out: Dict[str, Dict[str, Any]] = {}
    for sid, bucket in grouped.items():
        positions = int(bucket["positions"])
        pnl_pct_count = int(bucket["pnl_pct_count"])
        out[sid] = {
            "positions": positions,
            "total_pnl": round(float(bucket["total_pnl"]), 6),
            "win_rate": round((float(bucket["wins"]) / positions) * 100.0, 4) if positions > 0 else 0.0,
            "avg_pnl_pct": round((float(bucket["pnl_pct_sum"]) / pnl_pct_count), 4) if pnl_pct_count > 0 else 0.0,
        }
    return out


@app.on_event("startup")
def startup() -> None:
    cfg = initialise_config()
    try:
        init_db()
    except Exception as e:
        logger.exception("init_db failed; continuing so server can start: %s", e)
    logger.info(
        "config_loaded env=%s lock=%s override=%s",
        cfg.app_env,
        cfg.config_lock_enabled,
        cfg.config_override_enabled,
    )
    try:
        _scanner_connectors_repo.initialise_defaults_if_missing(get_default_connector_registry())
    except Exception as exc:
        logger.warning("scanner connectors init failed: %s", exc)
    print(f"DB_DRIVER={DB_DRIVER_MARKER}")


@app.get("/healthz")
def health_check():
    db_ok, db_message = check_db_connectivity()
    health_status = "ok" if db_ok else "degraded"
    body = {
        "status": health_status,
        "app": "running",
        "db": {"ok": db_ok, "message": db_message},
    }
    if not db_ok:
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=body)
    return body


@app.get("/")
def root():
    return {"app": "Apollo 67", "message": "Backend running"}


@app.get("/ui")
def ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/admin")
def admin_ui(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})


@app.get("/admin/scanner-sources")
def admin_scanner_sources_ui(request: Request):
    return templates.TemplateResponse("admin_scanner_sources.html", {"request": request})


@app.get("/admin/sources")
def admin_scanner_sources_ui_alias(request: Request):
    return templates.TemplateResponse("admin_scanner_sources.html", {"request": request})


@app.get("/admin/connectors")
def admin_connectors_ui(request: Request):
    return templates.TemplateResponse("admin_connectors.html", {"request": request})


@app.get("/admin/strategies-dashboard")
def admin_strategies_dashboard(request: Request):
    return templates.TemplateResponse("admin_strategies.html", {"request": request})


@app.get("/admin/state")
def admin_get_state():
    row = _curated_repo.get("admin_state", "v1")
    payload = row.get("payload") if row else None
    state = _normalise_admin_state(payload if isinstance(payload, dict) else None)
    return {"ok": True, "data": state}


@app.post("/admin/state")
def admin_post_state(payload: dict[str, Any]):
    raw_state = payload.get("state") if isinstance(payload, dict) and isinstance(payload.get("state"), dict) else payload
    if not isinstance(raw_state, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "state object is required"})

    state = _normalise_admin_state(raw_state)
    _curated_repo.upsert("admin_state", "v1", state, status="active")
    return {"ok": True, "data": state}


@app.post("/admin/tactic/generate")
def admin_generate_tactic(payload: dict[str, Any]):
    preset = str(payload.get("preset") or "Custom")
    instruction = str(payload.get("instruction") or "")

    base = dict(_TACTIC_PRESETS.get(preset, _TACTIC_PRESETS["Custom"]))
    proposed, notes = _apply_tactic_heuristics(base, instruction)

    preview: list[dict[str, Any]] = []
    for param in ("rsi_weight", "sma_weight", "atr_multiplier", "stop_sensitivity", "trade_threshold", "risk_multiplier", "timeframe_bias"):
        current = base.get(param)
        proposed_value = proposed.get(param)
        effect = "unchanged"
        if param == "timeframe_bias":
            if proposed_value != current:
                effect = f"timeframe changed to {proposed_value}"
        else:
            try:
                c = float(current)
                p = float(proposed_value)
                if p > c:
                    effect = "increased"
                elif p < c:
                    effect = "decreased"
            except Exception:
                effect = "updated"
        preview.append(
            {
                "parameter": param,
                "current": current,
                "proposed": proposed_value,
                "effect": effect,
            }
        )

    profile = {
        "name": "Custom" if preset == "Custom" else preset,
        "preset": preset,
        "instruction": instruction,
        "overrides": proposed,
        "notes": notes or ["generated from heuristics"],
        "created_at": _utc_iso_now(),
    }
    return {"ok": True, "profile": profile, "preview": preview}


@app.post("/admin/tactic/activate")
def admin_activate_tactic(payload: dict[str, Any]):
    dataset_version = str(payload.get("dataset_version") or "").strip()
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else None

    if profile is not None:
        if not dataset_version:
            dataset_version = str(int(time.time()))
        _curated_repo.upsert("trading_tactic", dataset_version, profile, status="active")
    elif dataset_version:
        existing = _curated_repo.get("trading_tactic", dataset_version)
        if not existing:
            return JSONResponse(status_code=404, content={"ok": False, "error": "tactic profile not found"})
        profile = existing.get("payload") if isinstance(existing.get("payload"), dict) else {}
    else:
        return JSONResponse(status_code=400, content={"ok": False, "error": "dataset_version or profile is required"})

    current_state_row = _curated_repo.get("admin_state", "v1")
    current_state = _normalise_admin_state(current_state_row.get("payload") if current_state_row else None)
    current_state["active_tactic_version"] = dataset_version
    current_state["updated_at"] = _utc_iso_now()
    _curated_repo.upsert("admin_state", "v1", current_state, status="active")

    return {"ok": True, "data": {"dataset_version": dataset_version, "profile": profile}}


@app.get("/admin/tactic/active")
def admin_get_active_tactic():
    state_row = _curated_repo.get("admin_state", "v1")
    state = _normalise_admin_state(state_row.get("payload") if state_row else None)
    active_version = state.get("active_tactic_version", "none")
    if not active_version or active_version == "none":
        return {"ok": True, "data": None}

    tactic_row = _curated_repo.get("trading_tactic", str(active_version))
    if not tactic_row:
        return {"ok": True, "data": None}
    return {
        "ok": True,
        "data": {
            "dataset_version": tactic_row.get("dataset_version"),
            "profile": tactic_row.get("payload") if isinstance(tactic_row.get("payload"), dict) else {},
            "created_at": tactic_row.get("created_at"),
        },
    }


@app.get("/admin/active-profile")
def admin_active_profile():
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT dataset_name, dataset_version, status, payload, created_at
            FROM curated_datasets
            WHERE dataset_name = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            ("active_trading_tactic",),
        ).fetchall()
    if not rows:
        return {"ok": True, "data": None}
    row = rows[0]
    payload = row.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    return {
        "ok": True,
        "data": {
            "dataset_name": row.get("dataset_name"),
            "dataset_version": row.get("dataset_version"),
            "status": row.get("status"),
            "payload": payload if isinstance(payload, dict) else {},
            "created_at": row.get("created_at"),
        },
    }


@app.post("/admin/active-profile")
def admin_set_active_profile(payload: dict[str, Any]):
    profile = payload.get("profile")
    if not isinstance(profile, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "profile object is required"})

    version = str(int(time.time()))
    payload_json = json.dumps(profile)
    with get_connection() as conn:
        if conn.backend == "postgres":
            conn.execute(
                """
                INSERT INTO curated_datasets (dataset_name, dataset_version, status, payload)
                VALUES (?, ?, ?, ?::jsonb)
                """,
                ("active_trading_tactic", version, "active", payload_json),
            )
        else:
            conn.execute(
                """
                INSERT INTO curated_datasets (dataset_name, dataset_version, status, payload)
                VALUES (?, ?, ?, ?)
                """,
                ("active_trading_tactic", version, "active", payload_json),
            )
    return {"ok": True, "data": {"dataset_name": "active_trading_tactic", "dataset_version": version, "payload": profile}}


@app.get("/admin/sentiment-config")
def admin_get_sentiment_config():
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT dataset_name, dataset_version, status, payload, created_at
            FROM curated_datasets
            WHERE dataset_name = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            ("sentiment_manager_config",),
        ).fetchall()
    if not rows:
        return {"ok": True, "data": None}
    row = rows[0]
    payload = row.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    return {
        "ok": True,
        "data": {
            "dataset_name": row.get("dataset_name"),
            "dataset_version": row.get("dataset_version"),
            "status": row.get("status"),
            "payload": payload if isinstance(payload, dict) else {},
            "created_at": row.get("created_at"),
        },
    }


@app.put("/admin/sentiment-config")
def admin_set_sentiment_config(payload: dict[str, Any]):
    config = payload.get("config")
    if not isinstance(config, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "config object is required"})

    version = str(int(time.time()))
    payload_json = json.dumps(config)
    with get_connection() as conn:
        if conn.backend == "postgres":
            conn.execute(
                """
                INSERT INTO curated_datasets (dataset_name, dataset_version, status, payload)
                VALUES (?, ?, ?, ?::jsonb)
                """,
                ("sentiment_manager_config", version, "active", payload_json),
            )
        else:
            conn.execute(
                """
                INSERT INTO curated_datasets (dataset_name, dataset_version, status, payload)
                VALUES (?, ?, ?, ?)
                """,
                ("sentiment_manager_config", version, "active", payload_json),
            )
    return {"ok": True, "data": {"dataset_name": "sentiment_manager_config", "dataset_version": version, "payload": config}}


@app.get("/admin/api/scanner-source-controls")
def admin_list_scanner_source_controls(scanner_type: str):
    scanner_type_value = str(scanner_type or "").strip().lower()
    if not scanner_type_value:
        return JSONResponse(status_code=400, content={"ok": False, "error": "scanner_type is required"})
    controls = _scanner_source_controls_repo.list_controls(scanner_type_value)
    return {"ok": True, "data": controls}


@app.post("/admin/api/scanner-source-controls")
def admin_upsert_scanner_source_control(payload: dict[str, Any]):
    scanner_type = str(payload.get("scanner_type") or "").strip().lower()
    source_key = str(payload.get("source_key") or "").strip()
    if not scanner_type or not source_key:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "scanner_type and source_key are required"},
        )

    blocked = bool(payload.get("blocked", False))
    display_name_raw = payload.get("display_name")
    display_name = str(display_name_raw).strip() if isinstance(display_name_raw, str) and display_name_raw.strip() else None
    notes_raw = payload.get("notes")
    notes = str(notes_raw).strip() if isinstance(notes_raw, str) and notes_raw.strip() else None
    try:
        weight = float(payload.get("weight", 1.0))
    except Exception:
        weight = 1.0
    try:
        min_mentions = int(payload.get("min_mentions", 0))
    except Exception:
        min_mentions = 0
    try:
        min_confidence = float(payload.get("min_confidence", 0.0))
    except Exception:
        min_confidence = 0.0

    row = _scanner_source_controls_repo.upsert_control(
        scanner_type=scanner_type,
        source_key=source_key,
        display_name=display_name,
        blocked=blocked,
        weight=weight,
        min_mentions=max(0, min_mentions),
        min_confidence=max(0.0, min_confidence),
        notes=notes,
    )
    return {"ok": True, "data": row}


@app.delete("/admin/api/scanner-source-controls")
def admin_delete_scanner_source_control(scanner_type: str, source_key: str):
    scanner_type_value = str(scanner_type or "").strip().lower()
    source_key_value = str(source_key or "").strip()
    if not scanner_type_value or not source_key_value:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "scanner_type and source_key are required"},
        )
    _scanner_source_controls_repo.delete_control(scanner_type_value, source_key_value)
    return {"ok": True}


@app.get("/admin/api/connectors")
def admin_list_connectors():
    registry = get_default_connector_registry()
    enabled_map = _scanner_connectors_repo.get_all_enabled_map()
    output: List[Dict[str, Any]] = []
    for spec in registry:
        runtime = _SCANNER_CONNECTOR_RUNTIME.get(spec.group, {}).get(spec.id, {})
        key_present = None
        if spec.requires_key and spec.key_env:
            key_present = bool(os.getenv(spec.key_env, "").strip())
        output.append(
            {
                "id": spec.id,
                "group": spec.group,
                "label": spec.label,
                "status": spec.status,
                "enabled": bool(enabled_map.get(spec.id, False)),
                "requires_key": spec.requires_key,
                "key_env": spec.key_env,
                "key_present": key_present,
                "last_run": runtime.get("last_run"),
                "last_error": runtime.get("last_error"),
                "notes": spec.notes,
            }
        )
    return {"ok": True, "data": output}


@app.post("/admin/api/connectors/toggle")
def admin_toggle_connector(payload: dict[str, Any]):
    connector_id = str(payload.get("id") or "").strip()
    if not connector_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "id is required"})
    enabled = bool(payload.get("enabled"))
    known = {spec.id for spec in get_default_connector_registry()}
    if connector_id not in known:
        return JSONResponse(status_code=404, content={"ok": False, "error": f"unknown connector {connector_id}"})
    _scanner_connectors_repo.set_enabled(connector_id, enabled)
    return {"ok": True, "data": {"id": connector_id, "enabled": enabled}}


@app.get("/admin/strategies")
def admin_list_strategies(limit: int = 200):
    rows = _strategies_repo.list_strategies(limit=limit)
    return {"ok": True, "data": rows}


@app.post("/admin/strategies")
def admin_create_strategy(payload: dict[str, Any]):
    instruction = str(payload.get("instruction") or "").strip()
    if not instruction:
        return JSONResponse(status_code=400, content={"ok": False, "error": "instruction is required"})
    preset_id = str(payload.get("preset_id") or "buffett_value").strip()
    derived = _strategy_instruction_to_payload(instruction=instruction, preset_id=preset_id)
    strategy_name = str(payload.get("name") or derived.get("title") or "Custom Strategy").strip()
    spec = strategy_by_id(str(derived.get("strategy_spec_id") or preset_id))
    created = _strategies_repo.create_strategy(
        name=strategy_name,
        strategy_group=spec.group,
        payload=derived,
    )
    return {"ok": True, "data": created}


@app.get("/admin/strategies/{strategy_id}")
def admin_get_strategy(strategy_id: str):
    row = _strategies_repo.get_strategy(strategy_id)
    if not row:
        return JSONResponse(status_code=404, content={"ok": False, "error": "strategy not found"})
    return {"ok": True, "data": row}


@app.get("/admin/strategies/library")
def admin_strategies_library():
    return {"ok": True, "data": strategy_list()}


@app.get("/admin/monitors")
def admin_list_monitors(strategy_id: Optional[str] = None, limit: int = 500):
    rows = _strategies_repo.list_monitors(strategy_id=strategy_id, limit=limit)
    return {"ok": True, "data": rows, "summary_by_strategy": _monitor_summary_by_strategy(rows)}


@app.post("/admin/monitors")
def admin_create_monitor(payload: dict[str, Any]):
    symbol = str(payload.get("symbol") or "").strip().upper()
    if not symbol:
        return JSONResponse(status_code=400, content={"ok": False, "error": "symbol is required"})
    strategy_id = str(payload.get("strategy_id") or "").strip() or None
    notes = str(payload.get("notes") or "").strip() or None
    entry_price = _as_float_or_none(payload.get("entry_price"))
    quantity = _as_float_or_none(payload.get("quantity"))
    buy_amount = _as_float_or_none(payload.get("buy_amount"))

    if entry_price is None or entry_price <= 0:
        try:
            quote_result = get_quote_with_fallback(symbol=symbol, freshness_seconds=get_config().scanner_quote_ttl_seconds)
            entry_price = float(quote_result.quote.last)
        except Exception as exc:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Unable to resolve entry price: {exc}"})

    if quantity is None or quantity <= 0:
        if buy_amount is None or buy_amount <= 0:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Provide quantity or buy_amount > 0"},
            )
        quantity = float(buy_amount) / float(entry_price)

    created = _strategies_repo.create_monitor(
        strategy_id=strategy_id,
        symbol=symbol,
        entry_price=float(entry_price),
        quantity=float(quantity),
        notes=notes,
    )
    return {"ok": True, "data": created}


@app.post("/admin/monitors/{monitor_id}/refresh")
def admin_refresh_monitor(monitor_id: int):
    row = _strategies_repo.get_monitor(monitor_id)
    if not row:
        return JSONResponse(status_code=404, content={"ok": False, "error": "monitor not found"})
    symbol = str(row.get("symbol") or "").strip().upper()
    if not symbol:
        return JSONResponse(status_code=400, content={"ok": False, "error": "monitor symbol is invalid"})
    try:
        quote_result = get_quote_with_fallback(symbol=symbol, freshness_seconds=get_config().scanner_quote_ttl_seconds)
        refreshed = _strategies_repo.refresh_monitor(monitor_id=monitor_id, last_price=float(quote_result.quote.last))
        return {"ok": True, "data": refreshed}
    except Exception as exc:
        return JSONResponse(status_code=503, content={"ok": False, "error": str(exc)})


@app.get("/backtest/run")
def backtest_run(symbol: str, strategy_id: str, interval: str = "1day", lookback: int = 500):
    symbol_value = str(symbol or "").strip().upper()
    if not symbol_value:
        return JSONResponse(status_code=400, content={"ok": False, "error": "symbol is required"})

    strategy_row = _strategies_repo.get_strategy(strategy_id)
    if not strategy_row:
        spec = strategy_by_id(strategy_id)
        strategy_payload = dict(spec.default_params)
        strategy_meta = {"id": spec.id, "name": spec.name, "group": spec.group}
    else:
        strategy_payload = strategy_row.get("payload") if isinstance(strategy_row.get("payload"), dict) else {}
        strategy_meta = {"id": strategy_row.get("id"), "name": strategy_row.get("name"), "group": strategy_row.get("group")}

    try:
        bars_result = get_bars_with_fallback(symbol=symbol_value, interval=interval, outputsize=max(50, min(int(lookback), 2000)))
        bars_data: List[Dict[str, Any]] = []
        for bar in bars_result.bars or []:
            if hasattr(bar, "model_dump"):
                bars_data.append(bar.model_dump(mode="json"))
            elif isinstance(bar, dict):
                bars_data.append(bar)
        metrics = run_backtest(strategy_payload=strategy_payload, bars=bars_data)
        return {
            "ok": True,
            "symbol": symbol_value,
            "strategy": strategy_meta,
            "provider": bars_result.provider,
            "interval": interval,
            "lookback": lookback,
            "metrics": metrics,
        }
    except Exception as exc:
        return JSONResponse(status_code=503, content={"ok": False, "error": str(exc)})


@app.get("/config")
def config_view():
    cfg = get_config()
    payload = cfg.to_public_dict()
    payload["parameter_lock"] = {
        "enabled": cfg.config_lock_enabled,
        "override_enabled": cfg.config_override_enabled,
    }
    return payload


@app.get("/debug/init-db")
def force_init():
    init_db()
    return {"status": "init_db executed"}


def _mask(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if len(s) <= 8:
        return s[0:2] + "..." + s[-2:]
    return s[0:4] + "..." + s[-4:]


@app.get("/debug/keys")
def debug_keys():
    td = os.getenv("TWELVEDATA_API_KEY", "")
    fh = os.getenv("FINNHUB_API_KEY", "")
    av = os.getenv("ALPHAVANTAGE_API_KEY", "")
    return {
        "TWELVEDATA_API_KEY_present": bool(td.strip()),
        "FINNHUB_API_KEY_present": bool(fh.strip()),
        "ALPHAVANTAGE_API_KEY_present": bool(av.strip()),
        "TWELVEDATA_API_KEY_masked": _mask(td),
        "FINNHUB_API_KEY_masked": _mask(fh),
        "ALPHAVANTAGE_API_KEY_masked": _mask(av),
        "app_env": os.getenv("APP_ENV", ""),
    }


# -----------------------------------------------------------------------------
# Canonical market endpoints
# -----------------------------------------------------------------------------

@app.get("/market/quote")
def market_quote(symbol: str):
    try:
        cfg = get_config()
        result = get_quote_with_fallback(symbol=symbol, freshness_seconds=cfg.data_freshness_sla_seconds)
        return {
            "provider": result.provider,
            "symbol": symbol.upper(),
            "quote": result.quote.model_dump(mode="json"),
        }
    except (ProviderError, ValidationError, ValueError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "selector", "message": str(exc)},
        )


def _bars_to_json(bars: list[Any]) -> list[Any]:
    out: list[Any] = []
    for b in bars or []:
        if hasattr(b, "model_dump"):
            out.append(b.model_dump(mode="json"))
        elif isinstance(b, dict):
            out.append(b)
        else:
            out.append(b)
    return out


@app.get("/market/bars")
def market_bars(symbol: str, interval: str = "1day", outputsize: int = 500):
    try:
        result = get_bars_with_fallback(symbol=symbol, interval=interval, outputsize=outputsize)
        return {
            "provider": result.provider,
            "symbol": symbol.upper(),
            "interval": interval,
            "outputsize": outputsize,
            "bars": _bars_to_json(result.bars or []),
        }
    except (ProviderError, ValidationError, ValueError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "selector", "message": str(exc)},
        )


# -----------------------------------------------------------------------------
# Provider-specific endpoints
# -----------------------------------------------------------------------------

@app.get("/provider/twelvedata/search")
def provider_twelvedata_search(q: str):
    try:
        client = TwelveDataClient()
        return {"provider": "twelvedata", "query": q, "results": client.search_symbols(q)}
    except (ProviderError, ValidationError, ValueError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "twelvedata", "message": str(exc)},
        )


@app.get("/provider/twelvedata/bars")
def provider_twelvedata_bars(symbol: str, interval: str = "1day", outputsize: int = 500):
    try:
        result = get_bars_with_fallback(symbol=symbol, interval=interval, outputsize=outputsize)
        return {
            "provider": result.provider,
            "symbol": symbol.upper(),
            "interval": interval,
            "outputsize": outputsize,
            "bars": _bars_to_json(result.bars or []),
        }
    except (ProviderError, ValidationError, ValueError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "twelvedata", "message": str(exc)},
        )


@app.get("/provider/twelvedata/quote")
def provider_twelvedata_quote(symbol: str):
    try:
        cfg = get_config()
        result = get_quote_with_fallback(symbol=symbol, freshness_seconds=cfg.data_freshness_sla_seconds)
        return {
            "provider": result.provider,
            "symbol": symbol.upper(),
            "quote": result.quote.model_dump(mode="json"),
        }
    except (ProviderError, ValidationError, ValueError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "twelvedata", "message": str(exc)},
        )


# -----------------------------------------------------------------------------
# Signals
# -----------------------------------------------------------------------------

def _compute_basic_signal_payload(symbol: str) -> dict[str, Any]:
    # selector provides fallback
    result = get_bars_with_fallback(symbol=symbol, interval="1day", outputsize=60)
    bars = result.bars or []

    # Validate bars regardless of shape (your validator handles dict/tuple/object)
    if bars:
        validate_bars(bars)

    # Compute signal expects dict-ish
    bars_for_signal = []
    for b in bars:
        if hasattr(b, "model_dump"):
            bars_for_signal.append(b.model_dump(mode="json"))
        elif isinstance(b, dict):
            bars_for_signal.append(b)
        else:
            bars_for_signal.append(b)

    bars_for_signal = sorted(bars_for_signal, key=lambda x: str(getattr(x, "get", lambda k, d=None: None)("ts_event", "")) if isinstance(x, dict) else str(getattr(x, "ts_event", "")))

    signal = compute_basic_signal(bars_for_signal)

    debug = signal.get("debug", {}) if isinstance(signal, dict) else {}
    debug.setdefault("provider_used", result.provider)
    debug.setdefault("bars_count", len(bars_for_signal) if bars_for_signal else None)
    signal["debug"] = debug
    return signal


@app.get("/signal/basic")
def signal_basic(symbol: str):
    try:
        if not _has_any_market_key():
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Missing API key. Set FINNHUB_API_KEY or TWELVEDATA_API_KEY or ALPHAVANTAGE_API_KEY."},
            )
        return _compute_basic_signal_payload(symbol)
    except ProviderError as exc:
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"error": str(exc)})
    except (ValidationError, ValueError, TypeError) as exc:
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"error": str(exc)})


@app.get("/signal/trade")
def signal_trade(symbol: str, interval: str = "1day", outputsize: int = 60):
    try:
        if not _has_any_market_key():
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Missing API key. Set FINNHUB_API_KEY or TWELVEDATA_API_KEY or ALPHAVANTAGE_API_KEY."},
            )

        res = get_bars_with_fallback(symbol=symbol, interval=interval, outputsize=outputsize)
        bars = res.bars or []

        # normalize to list[dict] for compute_trade_signal
        bars_dicts: list[Any] = []
        for b in bars:
            if hasattr(b, "model_dump"):
                bars_dicts.append(b.model_dump(mode="json"))
            elif isinstance(b, dict):
                bars_dicts.append(b)
            else:
                bars_dicts.append(b)

        if bars_dicts:
            validate_bars(bars_dicts)

        trade = compute_trade_signal(
            bars_dicts,
            symbol=symbol.upper(),
            provider_used=res.provider,
            timeframe=interval,
        )

        return {
            "provider": res.provider,
            "symbol": symbol.upper(),
            "trade": trade,
        }

    except (ProviderError, ValidationError, ValueError, TypeError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "selector", "message": str(exc)},
        )


@app.get("/scanner/sector")
def scanner_sector():
    cfg = get_config()
    universe = _load_scanner_universe()
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for item in universe:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        sector = str(item.get("sector", "")).strip() or "Unclassified"

        try:
            quote_result = get_quote_with_fallback(
                symbol=symbol,
                freshness_seconds=cfg.data_freshness_sla_seconds,
            )
            signal_payload = _compute_basic_signal_payload(symbol)
            rows.append(
                {
                    "symbol": symbol,
                    "sector": sector,
                    "ok": True,
                    "last": quote_result.quote.last,
                    "score": signal_payload.get("score"),
                    "trend": signal_payload.get("trend"),
                    "momentum": signal_payload.get("momentum"),
                    "provider_quote": quote_result.provider,
                    "provider_signal": (
                        signal_payload.get("debug", {}).get("provider_used")
                        if isinstance(signal_payload.get("debug"), dict)
                        else None
                    )
                    or quote_result.provider,
                }
            )
        except Exception as exc:
            error_row = {
                "symbol": symbol,
                "sector": sector,
                "ok": False,
                "error": str(exc),
            }
            rows.append(error_row)
            errors.append(error_row)

    ok_rows = [r for r in rows if r.get("ok") is True]
    top_overall = sorted(ok_rows, key=_score_sort_value, reverse=True)[:10]

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in ok_rows:
        sector = str(row.get("sector", "")).strip() or "Unclassified"
        grouped.setdefault(sector, []).append(row)

    by_sector: Dict[str, List[Dict[str, Any]]] = {}
    for sector, sector_rows in grouped.items():
        by_sector[sector] = sorted(sector_rows, key=_score_sort_value, reverse=True)[:10]

    return {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "top_overall": top_overall,
        "by_sector": by_sector,
        "errors": errors,
    }


def _scanner_agent(
    agent: str,
    interval: str = "1day",
    bars: int = 60,
    limit: int = 10,
    refresh: bool = False,
):
    agent_key = str(agent or "overall").strip().lower()
    symbols = _SCANNER_AGENT_UNIVERSES.get(agent_key)
    if symbols is None:
        return JSONResponse(status_code=404, content={"error": f"Unknown scanner agent: {agent_key}"})

    cfg = get_config()
    bars_value = max(20, min(int(bars), 500))
    limit_value = max(1, min(int(limit), 50))
    refresh_limit = max(1, int(cfg.scanner_refresh_batch_limit))
    allow_live = bool(refresh)
    if cfg.scanner_cache_mode == "cache_then_live" and not refresh:
        allow_live = True

    cache_key = f"scanner:{agent_key}:{interval}:{bars_value}:{limit_value}:live={1 if allow_live else 0}"
    cached = _batch_cache_get(cache_key)
    if cached is not None:
        return cached

    symbols_to_scan = symbols[:refresh_limit] if allow_live else symbols
    rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(
                build_scanner_row,
                symbol,
                interval,
                bars_value,
                allow_live,
                int(cfg.scanner_bars_ttl_seconds),
                int(cfg.scanner_quote_ttl_seconds),
            ): symbol
            for symbol in symbols_to_scan
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                row = future.result()
                row["ok"] = True
                row["buy_opportunity"] = rank_buy_opportunity(row)
                try:
                    _save_scanner_breakdown(symbol=symbol, scanner_type=agent_key, row=row)
                except Exception as breakdown_exc:
                    logger.warning(
                        "scanner breakdown save failed symbol=%s type=%s err=%s",
                        symbol,
                        agent_key,
                        breakdown_exc,
                    )
                rows.append(row)
            except Exception as exc:
                rows.append(
                    {
                        "symbol": symbol,
                        "ok": False,
                        "error": str(exc),
                        "needs_refresh": True,
                    }
                )

    ok_rows = [row for row in rows if row.get("ok")]
    ok_rows.sort(key=lambda x: float(x.get("buy_opportunity", float("-inf"))), reverse=True)
    payload = {
        "agent": agent_key,
        "interval": interval,
        "bars": bars_value,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cache_mode": cfg.scanner_cache_mode,
        "refresh_used": allow_live,
        "refresh_limit": refresh_limit,
        "rows": ok_rows[:limit_value] + [r for r in rows if not r.get("ok")],
    }
    _batch_cache_set(cache_key, payload)
    return payload


@app.get("/scanner/overall")
def scanner_overall(interval: str = "1day", bars: int = 60, limit: int = 10, refresh: bool = False):
    return _scanner_agent("overall", interval=interval, bars=bars, limit=limit, refresh=refresh)


@app.get("/scanner/institution")
def scanner_institution(interval: str = "1day", bars: int = 60, limit: int = 10, refresh: bool = False):
    return _scanner_agent("institution", interval=interval, bars=bars, limit=limit, refresh=refresh)


@app.get("/scanner/news")
def scanner_news(interval: str = "1day", bars: int = 60, limit: int = 10, refresh: bool = False):
    return _scanner_agent("news", interval=interval, bars=bars, limit=limit, refresh=refresh)


@app.get("/scanner/social")
def scanner_social(interval: str = "1day", bars: int = 60, limit: int = 10, refresh: bool = False):
    return _scanner_agent("social", interval=interval, bars=bars, limit=limit, refresh=refresh)


def _row_from_breakdown_snapshot(symbol: str, scanner_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    scanner_row = payload.get("scanner_row") if isinstance(payload.get("scanner_row"), dict) else {}
    if scanner_row:
        merged = dict(scanner_row)
        merged["symbol"] = str(merged.get("symbol") or symbol).strip().upper()
        merged["ok"] = True
        merged["from_snapshot"] = True
        if "target" not in merged and merged.get("target_price") is not None:
            merged["target"] = merged.get("target_price")
        if "entry_zone" not in merged or not isinstance(merged.get("entry_zone"), dict):
            merged["entry_zone"] = {
                "low": merged.get("entry_low"),
                "high": merged.get("entry_high"),
            }
        return merged

    symbol_u = str(symbol or "").strip().upper()
    if symbol_u:
        try:
            cfg = get_config()
            live_row = build_scanner_row(
                symbol_u,
                interval="1day",
                bars=60,
                allow_live=True,
                bars_ttl_seconds=int(cfg.scanner_bars_ttl_seconds),
                quote_ttl_seconds=int(cfg.scanner_quote_ttl_seconds),
            )
            live_row["ok"] = True
            live_row["from_snapshot"] = False
            return live_row
        except Exception:
            pass

    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    avg_score = _as_float_or_none(totals.get("avg_score"))
    avg_conf = _as_float_or_none(totals.get("avg_confidence"))
    score = avg_score if avg_score is not None else 0.0
    confidence = avg_conf if avg_conf is not None else 0.35
    action = "HOLD"
    if score > 15:
        action = "BUY"
    elif score < -20:
        action = "SELL"
    return {
        "symbol": symbol,
        "action": action,
        "confidence": confidence,
        "score": score,
        "price": None,
        "timeframe": "1day",
        "entry_low": None,
        "entry_high": None,
        "target": None,
        "stop": None,
        "trail": None,
        "rr": None,
        "reasons": [f"Snapshot derived from {scanner_type} source breakdown"],
        "tags": ["Synthetic"] if scanner_type in {"news", "social"} else [],
        "ok": True,
        "from_snapshot": True,
    }


@app.get("/scanner/latest")
def scanner_latest(type: str = "overall", limit: int = 10):
    scanner_type = str(type or "overall").strip().lower()
    if scanner_type not in _SCANNER_AGENT_UNIVERSES:
        return JSONResponse(status_code=404, content={"error": f"Unknown scanner type: {scanner_type}"})

    recent = _scanner_sources_repo.list_recent_breakdowns(scanner_type=scanner_type, limit=1000)
    latest_by_symbol: Dict[str, Dict[str, Any]] = {}
    for row in recent:
        symbol = str(row.get("symbol") or "").strip().upper()
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if not symbol or symbol in latest_by_symbol:
            continue
        latest_by_symbol[symbol] = _row_from_breakdown_snapshot(symbol=symbol, scanner_type=scanner_type, payload=payload)

    rows = list(latest_by_symbol.values())
    if not rows and scanner_type in {"social", "news"}:
        for symbol in _SCANNER_AGENT_UNIVERSES.get(scanner_type, [])[:max(1, min(int(limit), 10))]:
            payload = {
                "totals": {
                    "avg_score": 5.0 if scanner_type == "news" else 3.0,
                    "avg_confidence": 0.32 if scanner_type == "news" else 0.28,
                }
            }
            rows.append(_row_from_breakdown_snapshot(symbol=symbol, scanner_type=scanner_type, payload=payload))

    rows.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    limit_value = max(1, min(int(limit), 50))
    if rows:
        return {
            "agent": scanner_type,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rows": rows[:limit_value],
            "message": None,
        }
    return {
        "agent": scanner_type,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": [],
        "message": "No snapshots yet. Click Refresh.",
    }


def _source_to_response_row(source: Dict[str, Any], control: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    name_raw = source.get("name") or source.get("source") or source.get("id") or "Unknown"
    source_key = normalize_source_key(str(source.get("id") or name_raw))
    mentions = int(source.get("mentions") or 0)
    confidence = _as_float_or_none(source.get("confidence"))

    blocked = bool(control.get("blocked")) if control else False
    min_mentions = int(control.get("min_mentions") or 0) if control else 0
    min_confidence = float(control.get("min_confidence") or 0.0) if control else 0.0
    if blocked:
        return None
    if min_mentions > 0 and mentions < min_mentions:
        return None
    if confidence is not None and min_confidence > 0 and confidence < min_confidence:
        return None

    display_name = control.get("display_name") if control and control.get("display_name") else source.get("name") or name_raw
    row = {
        "id": source.get("id") or source_key,
        "source_key": source_key,
        "name": display_name,
        "origin": source.get("origin") or "auto",
        "mentions": mentions,
        "positive": int(source.get("positive") or 0),
        "negative": int(source.get("negative") or 0),
        "neutral": int(source.get("neutral") or 0),
        "score": _as_float_or_none(source.get("score")) or 0.0,
        "confidence": confidence,
        "meta": source.get("meta") if isinstance(source.get("meta"), dict) else {},
        "weight": float(control.get("weight") or 1.0) if control else 1.0,
    }
    return row


def _scanner_sources_cache_key(symbol: str, scanner_type: str, timeframe: str) -> str:
    return f"sources:{scanner_type}:{timeframe}:{symbol}"


def _get_cached_scanner_sources(symbol: str, scanner_type: str, timeframe: str) -> Optional[Dict[str, Any]]:
    key = _scanner_sources_cache_key(symbol, scanner_type, timeframe)
    with _SCANNER_SOURCES_CACHE_LOCK:
        cached = _SCANNER_SOURCES_CACHE.get(key)
        if not cached:
            return None
        ts, payload = cached
        if (time.time() - ts) > _SCANNER_SOURCES_CACHE_TTL_SECONDS:
            _SCANNER_SOURCES_CACHE.pop(key, None)
            return None
        return payload


def _set_cached_scanner_sources(symbol: str, scanner_type: str, timeframe: str, payload: Dict[str, Any]) -> None:
    key = _scanner_sources_cache_key(symbol, scanner_type, timeframe)
    with _SCANNER_SOURCES_CACHE_LOCK:
        _SCANNER_SOURCES_CACHE[key] = (time.time(), payload)


def _connector_specs_for_group(scanner_type: str) -> List[Dict[str, Any]]:
    grouped = registry_by_group()
    specs = grouped.get(scanner_type, [])
    out: List[Dict[str, Any]] = []
    enabled_map = _scanner_connectors_repo.get_all_enabled_map()
    for spec in specs:
        out.append(
            {
                "id": spec.id,
                "group": spec.group,
                "label": spec.label,
                "status": spec.status,
                "requires_key": spec.requires_key,
                "key_env": spec.key_env,
                "key_present": bool(os.getenv(spec.key_env or "", "").strip()) if spec.requires_key and spec.key_env else None,
                "notes": spec.notes,
                "enabled": bool(enabled_map.get(spec.id, False)),
            }
        )
    return out


def _build_runtime_scanner_sources(symbol: str, scanner_type: str, timeframe: str = "1day") -> Dict[str, Any]:
    specs = _connector_specs_for_group(scanner_type)
    enabled_map = {spec["id"]: bool(spec.get("enabled")) for spec in specs}

    items: List[Dict[str, Any]] = []
    runtime: Dict[str, Dict[str, Any]] = {}
    if scanner_type == "overall":
        for group_name in ("social", "news", "institution"):
            group_items, group_runtime = fetch_items(
                symbol=symbol,
                group=group_name,
                connectors_enabled=enabled_map,
                timeframe=timeframe,
            )
            items.extend(group_items)
            runtime.update(group_runtime)
    else:
        items, runtime = fetch_items(
            symbol=symbol,
            group=scanner_type,
            connectors_enabled=enabled_map,
            timeframe=timeframe,
        )
    analysis = analyse_items_openai(items)

    per_source = analysis.get("per_source") if isinstance(analysis.get("per_source"), dict) else {}
    rows: List[Dict[str, Any]] = []
    for spec in specs:
        source_id = str(spec.get("id"))
        source_counts = per_source.get(source_id) if isinstance(per_source.get(source_id), dict) else {}
        posts = int(source_counts.get("posts") or 0)
        positive = int(source_counts.get("positive") or 0)
        negative = int(source_counts.get("negative") or 0)
        neutral = int(source_counts.get("neutral") or max(0, posts - positive - negative))
        net = int(source_counts.get("net") or (positive - negative))
        runtime_row = runtime.get(source_id) if isinstance(runtime.get(source_id), dict) else {}

        rows.append(
            {
                "id": source_id,
                "source_key": normalize_source_key(source_id),
                "name": str(spec.get("label") or source_id),
                "origin": "auto",
                "mentions": posts,
                "positive": positive,
                "negative": negative,
                "neutral": neutral,
                "score": float(net),
                "confidence": float(analysis.get("confidence") or 0.0),
                "weight": 1.0,
                "status": spec.get("status") or "stub",
                "enabled": bool(spec.get("enabled")),
                "requires_key": bool(spec.get("requires_key")),
                "key_env": spec.get("key_env"),
                "key_present": spec.get("key_present"),
                "last_run": runtime_row.get("last_run"),
                "last_error": runtime_row.get("last_error"),
                "notes": spec.get("notes"),
            }
        )

    _SCANNER_CONNECTOR_RUNTIME[scanner_type] = runtime
    return {
        "symbol": symbol,
        "channel": scanner_type,
        "scanner_type": scanner_type,
        "timeframe": timeframe,
        "ts": _utc_iso_now(),
        "sources": rows,
        "totals": {
            "posts": int(analysis.get("posts_total") or 0),
            "positive": int(analysis.get("positive_count") or 0),
            "negative": int(analysis.get("negative_count") or 0),
            "neutral": int(analysis.get("neutral_count") or 0),
            "net": int(analysis.get("net") or 0),
            "score": int(analysis.get("score") or 0),
            "confidence": float(analysis.get("confidence") or 0.0),
            "recommendation": str(analysis.get("recommendation") or "WATCH"),
            "target_price": analysis.get("target_price"),
        },
        "top_reasons": analysis.get("top_reasons") if isinstance(analysis.get("top_reasons"), list) else [],
    }


@app.get("/scanner/sources")
def scanner_sources(
    symbol: str,
    channel: Optional[str] = None,
    type: Optional[str] = None,
    group: Optional[str] = None,
    timeframe: str = "1day",
):
    symbol_value = str(symbol or "").strip().upper()
    scanner_type = str(channel or group or type or "").strip().lower()
    timeframe_value = str(timeframe or "1day").strip() or "1day"
    if not symbol_value or not scanner_type:
        return JSONResponse(status_code=400, content={"error": "symbol and channel/type/group are required"})

    runtime_payload: Optional[Dict[str, Any]] = None
    if scanner_type in {"social", "news", "institution", "overall"}:
        runtime_payload = _get_cached_scanner_sources(
            symbol=symbol_value,
            scanner_type=scanner_type,
            timeframe=timeframe_value,
        )
        if runtime_payload is None:
            runtime_payload = _build_runtime_scanner_sources(
                symbol=symbol_value,
                scanner_type=scanner_type,
                timeframe=timeframe_value,
            )
            _set_cached_scanner_sources(
                symbol=symbol_value,
                scanner_type=scanner_type,
                timeframe=timeframe_value,
                payload=runtime_payload,
            )

    raw_sources: List[Dict[str, Any]] = []
    ts_value = _utc_iso_now()
    payload_totals: Dict[str, Any] = {}

    if runtime_payload:
        ts_value = str(runtime_payload.get("ts") or ts_value)
        payload_totals = runtime_payload.get("totals") if isinstance(runtime_payload.get("totals"), dict) else {}
        rows = runtime_payload.get("sources") if isinstance(runtime_payload.get("sources"), list) else []
        for row in rows:
            if isinstance(row, dict):
                raw_sources.append(row)
    else:
        latest = _scanner_sources_repo.get_latest_breakdown(symbol=symbol_value, scanner_type=scanner_type)
        if latest:
            payload = latest.get("payload") if isinstance(latest.get("payload"), dict) else {}
            ts_value = str(payload.get("ts") or latest.get("created_at") or ts_value)
            rows = payload.get("sources") if isinstance(payload.get("sources"), list) else []
            for row in rows:
                if isinstance(row, dict):
                    raw_sources.append(row)

    if not raw_sources:
        connector_specs = _connector_specs_for_group(scanner_type)
        if connector_specs:
            for spec in connector_specs:
                raw_sources.append(
                    {
                        "id": spec.get("id"),
                        "source_key": normalize_source_key(str(spec.get("id") or "")),
                        "name": spec.get("label"),
                        "origin": "registry",
                        "mentions": 0,
                        "positive": 0,
                        "negative": 0,
                        "neutral": 0,
                        "score": 0.0,
                        "confidence": 0.0,
                        "status": spec.get("status"),
                        "enabled": spec.get("enabled"),
                        "requires_key": spec.get("requires_key"),
                        "key_env": spec.get("key_env"),
                        "key_present": spec.get("key_present"),
                        "last_run": None,
                        "last_error": None,
                        "notes": spec.get("notes"),
                    }
                )
        else:
            for name in _SCANNER_SYNTHETIC_SOURCES.get(scanner_type, []):
                key = normalize_source_key(name)
                raw_sources.append(
                    {
                        "id": key,
                        "source_key": key,
                        "name": str(name),
                        "origin": "synthetic",
                        "mentions": 1,
                        "positive": 0,
                        "negative": 0,
                        "neutral": 1,
                        "score": 0.0,
                        "confidence": 0.2,
                        "meta": {"generator": "scanner_synthetic_v1"},
                    }
                )

    controls = _scanner_source_controls_repo.list_controls(scanner_type)
    control_by_key = {str(c.get("source_key")): c for c in controls}

    filtered_sources: List[Dict[str, Any]] = []
    for source in raw_sources:
        if not isinstance(source, dict):
            continue
        source_name = source.get("id") or source.get("name") or source.get("source")
        key = normalize_source_key(str(source_name or "unknown"))
        control = control_by_key.get(key)
        row = _source_to_response_row(source, control)
        if row is None:
            continue
        row["status"] = source.get("status") or source.get("origin") or "auto"
        row["enabled"] = bool(source.get("enabled"))
        row["requires_key"] = bool(source.get("requires_key"))
        row["key_env"] = source.get("key_env")
        row["key_present"] = source.get("key_present")
        row["last_run"] = source.get("last_run")
        row["last_error"] = source.get("last_error")
        row["notes"] = source.get("notes")
        row["posts"] = row.get("mentions")
        row["net"] = int(row.get("positive") or 0) - int(row.get("negative") or 0)
        filtered_sources.append(row)

    totals = _recompute_source_totals(filtered_sources)
    if payload_totals:
        totals["posts"] = int(payload_totals.get("posts") or totals.get("mentions") or 0)
        totals["net"] = int(payload_totals.get("net") or (int(totals.get("positive") or 0) - int(totals.get("negative") or 0)))
        totals["score"] = int(payload_totals.get("score") or 0)
        totals["confidence"] = float(payload_totals.get("confidence") or 0.0)
        totals["recommendation"] = str(payload_totals.get("recommendation") or "WATCH")
        totals["target_price"] = payload_totals.get("target_price")
    else:
        totals["posts"] = int(totals.get("mentions") or 0)
        totals["net"] = int(totals.get("positive") or 0) - int(totals.get("negative") or 0)

    return {
        "symbol": symbol_value,
        "channel": scanner_type,
        "group": scanner_type,
        "scanner_type": scanner_type,
        "timeframe": timeframe_value,
        "ts": ts_value,
        "totals": totals,
        "sources": filtered_sources,
        "connectors": filtered_sources,
        "controls_applied": True,
        "meta": {"discovery": "automatic", "notes": ["Display filters may apply."]},
        "notes": ["Discovery is automatic. Display filters may apply."],
    }


@app.get("/scanner/sources/meta")
def scanner_sources_meta(limit: int = 300):
    recent = _scanner_sources_repo.list_recent_breakdowns(limit=max(10, min(int(limit), 2000)))
    grouped: Dict[str, set[str]] = {}
    for row in recent:
        scanner_type = str(row.get("scanner_type") or "overall")
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
        for source in sources:
            if not isinstance(source, dict):
                continue
            key = normalize_source_key(str(source.get("id") or source.get("name") or source.get("source") or "unknown"))
            grouped.setdefault(scanner_type, set()).add(key)
    for scanner_type, names in _SCANNER_SYNTHETIC_SOURCES.items():
        bucket = grouped.setdefault(scanner_type, set())
        for name in names:
            bucket.add(normalize_source_key(name))
    for spec in get_default_connector_registry():
        grouped.setdefault(spec.group, set()).add(normalize_source_key(spec.id))
        grouped.setdefault("overall", set()).add(normalize_source_key(spec.id))
    return {scanner_type: sorted(list(keys)) for scanner_type, keys in grouped.items()}


@app.get("/admin/api/scanner-source-discovered")
def admin_scanner_source_discovered(limit: int = 500):
    recent = _scanner_sources_repo.list_recent_breakdowns(limit=max(10, min(int(limit), 2000)))
    stats: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for row in recent:
        scanner_type = str(row.get("scanner_type") or "overall")
        created_at = str(row.get("created_at") or "")
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
        for source in sources:
            if not isinstance(source, dict):
                continue
            key = normalize_source_key(str(source.get("id") or source.get("name") or source.get("source") or "unknown"))
            bucket = stats.setdefault(scanner_type, {})
            item = bucket.get(key)
            if item is None:
                bucket[key] = {
                    "source_key": key,
                    "scanner_type": scanner_type,
                    "seen_count": 1,
                    "first_seen": created_at,
                    "last_seen": created_at,
                }
            else:
                item["seen_count"] = int(item.get("seen_count") or 0) + 1
                if created_at and (not item.get("first_seen") or created_at < item["first_seen"]):
                    item["first_seen"] = created_at
                if created_at and (not item.get("last_seen") or created_at > item["last_seen"]):
                    item["last_seen"] = created_at

    out: Dict[str, List[Dict[str, Any]]] = {}
    for scanner_type, entries in stats.items():
        out[scanner_type] = sorted(entries.values(), key=lambda x: str(x.get("source_key")))
    return {"ok": True, "data": out}


# -----------------------------------------------------------------------------
# Batch + in-memory cache
# -----------------------------------------------------------------------------

def _parse_symbols_csv(symbols: str) -> list[str]:
    parsed: list[str] = []
    seen = set()
    for raw in symbols.split(","):
        sym = raw.strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        parsed.append(sym)
    return parsed


def _batch_cache_get(key: str):
    now = time.monotonic()
    with _BATCH_CACHE_LOCK:
        entry = _BATCH_CACHE.get(key)
        if entry is None:
            return None
        expires_at, payload = entry
        if expires_at <= now:
            _BATCH_CACHE.pop(key, None)
            return None
        return payload


def _batch_cache_set(key: str, payload: Any) -> None:
    expires_at = time.monotonic() + _BATCH_CACHE_TTL_SECONDS
    with _BATCH_CACHE_LOCK:
        _BATCH_CACHE[key] = (expires_at, payload)


def _batch_quote_for_symbol(symbol: str):
    cached = _batch_cache_get(f"batch_quote:{symbol}")
    if cached is not None:
        return cached

    cfg = get_config()
    result = get_quote_with_fallback(symbol=symbol, freshness_seconds=cfg.data_freshness_sla_seconds)
    payload = {
        "ok": True,
        "data": {
            "provider": result.provider,
            "symbol": symbol.upper(),
            "quote": result.quote.model_dump(mode="json"),
        },
    }
    _batch_cache_set(f"batch_quote:{symbol}", payload)
    return payload


def _batch_signal_for_symbol(symbol: str):
    cached = _batch_cache_get(f"batch_signal:{symbol}")
    if cached is not None:
        return cached

    payload = {"ok": True, "data": _compute_basic_signal_payload(symbol)}
    _batch_cache_set(f"batch_signal:{symbol}", payload)
    return payload


# -----------------------------------------------------------------------------
# DB-backed cache endpoints (existing)
# -----------------------------------------------------------------------------

def _decode_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _safe_score(payload: dict) -> float:
    raw = None
    if isinstance(payload, dict):
        raw = payload.get("score", None)

    try:
        if raw is None:
            return 0.0
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_columns(conn, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        cols = set()
        for r in rows:
            # sqlite returns: cid, name, type, notnull, dflt_value, pk
            name = r.get("name") if hasattr(r, "get") else r[1]
            if name:
                cols.add(str(name))
        return cols
    except Exception:
        return set()


def _insert_worker_quote_event(symbol: str, provider: str, quote_payload: dict[str, Any]) -> None:
    payload_obj = {"provider": provider, "symbol": symbol, "quote": quote_payload}
    payload_json = json.dumps(payload_obj)

    with get_connection() as conn:
        if conn.backend == "postgres":
            # payload column is JSONB in Postgres
            conn.execute(
                "INSERT INTO events (event_type, source, payload) VALUES (?, ?, ?::jsonb)",
                ("worker.quote", provider or "selector", payload_json),
            )
        else:
            # payload column is TEXT in SQLite
            conn.execute(
                "INSERT INTO events (event_type, source, payload) VALUES (?, ?, ?)",
                ("worker.quote", provider or "selector", payload_json),
            )


def _insert_signal_row(symbol: str, timeframe: str, signal_payload: dict[str, Any]) -> None:
    # signals.score is NOT NULL, so we must always insert a number
    score = signal_payload.get("score")
    try:
        score_value = float(score) if score is not None else 0.0
    except Exception:
        score_value = 0.0

    payload_json = json.dumps(signal_payload)

    with get_connection() as conn:
        if conn.backend == "postgres":
            conn.execute(
                "INSERT INTO signals (symbol, timeframe, score, payload) VALUES (?, ?, ?, ?::jsonb)",
                (symbol, timeframe, score_value, payload_json),
            )
        else:
            conn.execute(
                "INSERT INTO signals (symbol, timeframe, score, payload) VALUES (?, ?, ?, ?)",
                (symbol, timeframe, score_value, payload_json),
            )


def _cached_quotes_results(symbols: list[str]) -> dict[str, Any]:
    results: dict[str, Any] = {symbol: {"ok": False, "error": "No cached quote"} for symbol in symbols}
    if not symbols:
        return results

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, source, payload, created_at
            FROM events
            WHERE event_type = ?
            ORDER BY id DESC
            LIMIT 2000
            """,
            ("worker.quote",),
        ).fetchall()

    wanted = set(symbols)
    found: set[str] = set()

    # 1) Serve from DB cache if present
    for row in rows:
        payload = _decode_payload(row.get("payload"))
        symbol = str(payload.get("symbol", "")).strip().upper()
        quote_payload = payload.get("quote")
        provider = payload.get("provider") or row.get("source") or "cache"

        if symbol not in wanted or symbol in found:
            continue
        if not isinstance(quote_payload, dict):
            continue

        results[symbol] = {"ok": True, "data": {"provider": provider, "symbol": symbol, "quote": quote_payload}}
        found.add(symbol)
        if len(found) == len(wanted):
            return results

    # 2) For misses, fetch live and write to DB
    missing = [s for s in symbols if s not in found]
    if missing and _has_any_market_key():
        cfg = get_config()
        for s in missing:
            try:
                live = get_quote_with_fallback(symbol=s, freshness_seconds=cfg.data_freshness_sla_seconds)
                quote_dict = live.quote.model_dump(mode="json")
                _insert_worker_quote_event(s, live.provider, quote_dict)
                results[s] = {"ok": True, "data": {"provider": live.provider, "symbol": s, "quote": quote_dict}}
            except Exception as exc:
                results[s] = {"ok": False, "error": str(exc)}

    return results


def _cached_signals_results(symbols: list[str]) -> dict[str, Any]:
    results: dict[str, Any] = {symbol: {"ok": False, "error": "No cached signal"} for symbol in symbols}
    if not symbols:
        return results

    placeholders = ", ".join(["?"] * len(symbols))
    params: tuple[Any, ...] = tuple(symbols)

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, symbol, payload
            FROM signals
            WHERE symbol IN ({placeholders})
            ORDER BY id DESC
            """,
            params,
        ).fetchall()

    found: set[str] = set()

    # 1) Serve from DB cache if present
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or symbol in found:
            continue

        payload = _decode_payload(row.get("payload"))
        if not payload:
            continue

        results[symbol] = {"ok": True, "data": payload}
        found.add(symbol)
        if len(found) == len(symbols):
            return results

    # 2) For misses, compute live and write to DB
    missing = [s for s in symbols if s not in found]
    if missing and _has_any_market_key():
        for s in missing:
            try:
                live_signal = _compute_basic_signal_payload(s)
                _insert_signal_row(s, "1day", live_signal)
                results[s] = {"ok": True, "data": live_signal}
            except Exception as exc:
                results[s] = {"ok": False, "error": str(exc)}

    return results


@app.get("/batch/quotes")
def batch_quotes(symbols: str):
    requested = _parse_symbols_csv(symbols)
    if len(requested) > _BATCH_MAX_SYMBOLS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Maximum {_BATCH_MAX_SYMBOLS} symbols allowed per request."},
        )

    if not _has_any_market_key():
        results = {s: {"ok": False, "error": "Missing API key. Set FINNHUB_API_KEY or TWELVEDATA_API_KEY or ALPHAVANTAGE_API_KEY."} for s in requested}
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"symbols": requested, "results": results})

    started = time.monotonic()
    results: dict[str, Any] = {}
    succeeded: list[str] = []
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=_BATCH_MAX_WORKERS) as pool:
        futures = {pool.submit(_batch_quote_for_symbol, symbol): symbol for symbol in requested}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                payload = future.result()
                results[symbol] = payload
                succeeded.append(symbol)
            except Exception as exc:
                results[symbol] = {"ok": False, "error": str(exc)}
                failed.append(symbol)

    duration_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "batch_quotes symbols=%s ok=%s failed=%s duration_ms=%s",
        requested,
        succeeded,
        failed,
        duration_ms,
    )
    return {"symbols": requested, "results": results}


@app.get("/cache/quotes")
def cache_quotes(symbols: str):
    requested = _parse_symbols_csv(symbols)
    if len(requested) > _BATCH_MAX_SYMBOLS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Maximum {_BATCH_MAX_SYMBOLS} symbols allowed per request."},
        )
    return {"symbols": requested, "results": _cached_quotes_results(requested)}


@app.get("/batch/signals/basic")
def batch_signals_basic(symbols: str):
    requested = _parse_symbols_csv(symbols)
    if len(requested) > _BATCH_MAX_SYMBOLS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Maximum {_BATCH_MAX_SYMBOLS} symbols allowed per request."},
        )

    if not _has_any_market_key():
        results = {s: {"ok": False, "error": "Missing API key. Set FINNHUB_API_KEY or TWELVEDATA_API_KEY or ALPHAVANTAGE_API_KEY."} for s in requested}
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"symbols": requested, "results": results})

    started = time.monotonic()
    results: dict[str, Any] = {}
    succeeded: list[str] = []
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=_BATCH_MAX_WORKERS) as pool:
        futures = {pool.submit(_batch_signal_for_symbol, symbol): symbol for symbol in requested}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                payload = future.result()
                results[symbol] = payload
                succeeded.append(symbol)
            except Exception as exc:
                results[symbol] = {"ok": False, "error": str(exc)}
                failed.append(symbol)

    duration_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "batch_signals_basic symbols=%s ok=%s failed=%s duration_ms=%s",
        requested,
        succeeded,
        failed,
        duration_ms,
    )
    return {"symbols": requested, "results": results}


@app.get("/cache/signals/basic")
def cache_signals_basic(symbols: str):
    requested = _parse_symbols_csv(symbols)
    if len(requested) > _BATCH_MAX_SYMBOLS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Maximum {_BATCH_MAX_SYMBOLS} symbols allowed per request."},
        )
    return {"symbols": requested, "results": _cached_signals_results(requested)}


@app.get("/cache/status")
def cache_status():
    with get_connection() as conn:
        signal_count_row = conn.execute("SELECT COUNT(*) AS count FROM signals").fetchall()
        quote_count_row = conn.execute(
            "SELECT COUNT(*) AS count FROM events WHERE event_type = ?",
            ("worker.quote",),
        ).fetchall()
        bars_count_row = conn.execute("SELECT COUNT(*) AS count FROM canonical_price_bars").fetchall()
        latest_signal_row = conn.execute("SELECT created_at FROM signals ORDER BY id DESC LIMIT 1").fetchall()
        latest_quote_row = conn.execute(
            "SELECT created_at FROM events WHERE event_type = ? ORDER BY id DESC LIMIT 1",
            ("worker.quote",),
        ).fetchall()
        latest_bar_row = conn.execute(
            "SELECT ts_ingest FROM canonical_price_bars ORDER BY id DESC LIMIT 1"
        ).fetchall()

    return {
        "signals": {
            "count": int(signal_count_row[0]["count"]) if signal_count_row else 0,
            "latest_created_at": latest_signal_row[0]["created_at"] if latest_signal_row else None,
        },
        "quotes": {
            "count": int(quote_count_row[0]["count"]) if quote_count_row else 0,
            "latest_created_at": latest_quote_row[0]["created_at"] if latest_quote_row else None,
        },
        "bars": {
            "count": int(bars_count_row[0]["count"]) if bars_count_row else 0,
            "latest_ts_ingest": latest_bar_row[0]["ts_ingest"] if latest_bar_row else None,
        },
    }


def _entry_reference_for_position(row: Dict[str, Any]) -> Optional[float]:
    buy_price = row.get("buy_price")
    if buy_price is not None:
        try:
            return float(buy_price)
        except Exception:
            return None
    low = row.get("buy_zone_low")
    high = row.get("buy_zone_high")
    try:
        if low is not None and high is not None:
            return (float(low) + float(high)) / 2.0
    except Exception:
        return None
    return None


def _paper_config() -> Dict[str, Any]:
    row = _curated_repo.get("admin_state", "v1")
    state = _normalise_admin_state(row.get("payload") if row else None)
    paper = state.get("paper") if isinstance(state.get("paper"), dict) else {}
    return {
        "notional_per_trade": float(paper.get("notional_per_trade") or NOTIONAL_PER_TRADE),
        "max_positions": int(paper.get("max_positions") or MAX_POSITIONS),
        "rotate_n": int(paper.get("rotate_n") or ROTATE_N),
        "eval_interval_seconds": int(paper.get("eval_interval_seconds") or EVAL_INTERVAL_SECONDS),
        "rotate_interval_seconds": int(paper.get("rotate_interval_seconds") or ROTATE_INTERVAL_SECONDS),
    }


@app.get("/paper/status")
def paper_status():
    positions = _paper_repo.list_positions(limit=1000)
    open_orders = _paper_repo.list_orders(status="OPEN", limit=2000)
    closed_orders = _paper_repo.list_orders(status="CLOSED", limit=2000)
    runs = _paper_repo.list_runs(limit=20)
    total_unreal = 0.0
    total_real = 0.0
    for row in positions:
        try:
            total_unreal += float(row.get("unrealised_pnl") or 0.0)
        except Exception:
            pass
        try:
            total_real += float(row.get("realised_pnl") or 0.0)
        except Exception:
            pass
    closed_wins = 0
    for row in closed_orders:
        try:
            if float(row.get("pnl") or 0.0) > 0:
                closed_wins += 1
        except Exception:
            pass
    return {
        "ok": True,
        "positions_count": len(positions),
        "open_orders_count": len(open_orders),
        "closed_orders_count": len(closed_orders),
        "closed_wins": closed_wins,
        "totals": {
            "unrealised_pnl": total_unreal,
            "realised_pnl": total_real,
            "net_pnl": total_unreal + total_real,
        },
        "leaderboard": runs,
        "config": _paper_config(),
    }


@app.get("/paper/positions")
def paper_positions():
    return {"ok": True, "rows": _paper_repo.list_positions(limit=2000)}


@app.get("/paper/orders")
def paper_orders(status: Optional[str] = None):
    status_value = str(status or "").strip().upper() or None
    if status_value not in {None, "OPEN", "CLOSED", "CANCELLED"}:
        return JSONResponse(status_code=400, content={"ok": False, "error": "status must be open|closed|cancelled"})
    return {"ok": True, "rows": _paper_repo.list_orders(status=status_value, limit=3000)}


@app.post("/paper/config")
def paper_config_update(payload: dict[str, Any]):
    cfg = _paper_config()
    try:
        notional = float(payload.get("notional_per_trade", cfg["notional_per_trade"]))
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "notional_per_trade must be numeric"})
    try:
        max_positions = int(payload.get("max_positions", cfg["max_positions"]))
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "max_positions must be int"})
    try:
        rotate_n = int(payload.get("rotate_n", cfg["rotate_n"]))
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "rotate_n must be int"})

    row = _curated_repo.get("admin_state", "v1")
    state = _normalise_admin_state(row.get("payload") if row else None)
    state["paper"] = {
        "notional_per_trade": max(10.0, notional),
        "max_positions": max(1, max_positions),
        "rotate_n": max(0, rotate_n),
        "eval_interval_seconds": int(cfg["eval_interval_seconds"]),
        "rotate_interval_seconds": int(cfg["rotate_interval_seconds"]),
    }
    state["updated_at"] = _utc_iso_now()
    _curated_repo.upsert("admin_state", "v1", state, status="active")
    return {"ok": True, "data": state["paper"]}


# NOTE: dev-only reset endpoint used for local paper-trading iteration.
@app.post("/paper/reset")
def paper_reset(confirm: Optional[bool] = False):
    if not confirm:
        return JSONResponse(status_code=400, content={"ok": False, "error": "confirm=true is required"})
    app_env = os.getenv("APP_ENV", "local").strip().lower()
    if app_env not in {"local", "dev", "development"}:
        return JSONResponse(status_code=403, content={"ok": False, "error": "reset is only allowed in local/dev"})
    _paper_repo.clear_all()
    return {"ok": True}


@app.post("/monitor/create")
def monitor_create(payload: dict[str, Any]):
    symbol = str(payload.get("symbol") or "").strip().upper()
    if not symbol:
        return JSONResponse(status_code=400, content={"error": "symbol is required"})
    try:
        buy_amount = float(payload.get("buy_amount"))
    except Exception:
        return JSONResponse(status_code=400, content={"error": "buy_amount must be a number"})
    if buy_amount <= 0:
        return JSONResponse(status_code=400, content={"error": "buy_amount must be > 0"})

    buy_price = payload.get("buy_price")
    buy_zone_low = payload.get("buy_zone_low")
    buy_zone_high = payload.get("buy_zone_high")
    notes = payload.get("notes")
    try:
        buy_price_v = float(buy_price) if buy_price not in (None, "") else None
    except Exception:
        buy_price_v = None
    try:
        buy_zone_low_v = float(buy_zone_low) if buy_zone_low not in (None, "") else None
    except Exception:
        buy_zone_low_v = None
    try:
        buy_zone_high_v = float(buy_zone_high) if buy_zone_high not in (None, "") else None
    except Exception:
        buy_zone_high_v = None

    if buy_price_v is None and buy_zone_low_v is None and buy_zone_high_v is None:
        try:
            quote = get_quote_with_fallback(symbol=symbol, freshness_seconds=60)
            buy_price_v = float(quote.quote.last)
        except Exception as exc:
            return JSONResponse(status_code=503, content={"error": f"Unable to determine buy_price: {exc}"})

    created = _monitor_repo.create_position(
        symbol=symbol,
        buy_amount=buy_amount,
        buy_price=buy_price_v,
        buy_zone_low=buy_zone_low_v,
        buy_zone_high=buy_zone_high_v,
        notes=str(notes).strip() if notes is not None else None,
    )
    return {"ok": True, "data": created}


@app.get("/monitor/list")
def monitor_list(status: Optional[str] = None, limit: int = 200):
    rows = _monitor_repo.list_positions(status=status, limit=limit)
    return {"ok": True, "rows": rows}


@app.post("/monitor/refresh")
def monitor_refresh():
    rows = _monitor_repo.list_positions(status="open", limit=500)
    refreshed: List[Dict[str, Any]] = []
    for row in rows:
        position_id = int(row.get("id"))
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        try:
            quote = get_quote_with_fallback(symbol=symbol, freshness_seconds=60)
            last_price = float(quote.quote.last)
        except Exception as exc:
            refreshed.append({"id": position_id, "symbol": symbol, "ok": False, "error": str(exc)})
            continue

        entry_ref = _entry_reference_for_position(row)
        pnl_pct = None
        if entry_ref and entry_ref > 0:
            pnl_pct = ((last_price - entry_ref) / entry_ref) * 100.0
        prev_max_up = row.get("max_up_pct")
        prev_max_down = row.get("max_down_pct")
        try:
            max_up = max(float(prev_max_up), float(pnl_pct)) if pnl_pct is not None and prev_max_up is not None else pnl_pct
        except Exception:
            max_up = pnl_pct
        try:
            max_down = min(float(prev_max_down), float(pnl_pct)) if pnl_pct is not None and prev_max_down is not None else pnl_pct
        except Exception:
            max_down = pnl_pct

        _monitor_repo.update_position_metrics(
            position_id=position_id,
            last_price=last_price,
            pnl_pct=pnl_pct,
            max_up_pct=max_up,
            max_down_pct=max_down,
            last_checked_at=datetime.now(timezone.utc).isoformat(),
        )
        refreshed.append(
            {
                "id": position_id,
                "symbol": symbol,
                "ok": True,
                "last_price": last_price,
                "pnl_pct": pnl_pct,
                "max_up_pct": max_up,
                "max_down_pct": max_down,
            }
        )

    return {"ok": True, "rows": refreshed}


@app.post("/monitor/close")
def monitor_close(payload: dict[str, Any]):
    try:
        position_id = int(payload.get("id"))
    except Exception:
        return JSONResponse(status_code=400, content={"error": "id is required"})

    status_value = str(payload.get("status") or "closed_manual").strip().lower()
    if status_value not in {"closed_target", "closed_stop", "closed_manual"}:
        return JSONResponse(status_code=400, content={"error": "invalid status"})
    _monitor_repo.close_position(position_id, status_value)
    return {"ok": True, "id": position_id, "status": status_value}


app.include_router(admin_router, prefix="/admin", tags=["admin"])

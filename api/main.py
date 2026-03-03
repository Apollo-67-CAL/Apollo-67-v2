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
from core.storage.db import DB_DRIVER_MARKER, check_db_connectivity, get_connection, init_db

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
# Load .env from repo root for local dev. Render injects env vars too, harmless.
load_dotenv(BASE_DIR / ".env")
load_dotenv()

app = FastAPI(title="Apollo 67")
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

_ADMIN_DEFAULT_STATE = {
    "sentiment": {
        "overall": {"weight": 50, "influence": "medium"},
        "institution": {"weight": 50, "influence": "medium"},
        "news": {"weight": 50, "influence": "medium"},
        "social": {"weight": 50, "influence": "medium"},
    },
    "active_tactic_version": "none",
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


def _scanner_agent(agent: str, interval: str = "1day", bars: int = 60, limit: int = 10):
    agent_key = str(agent or "overall").strip().lower()
    symbols = _SCANNER_AGENT_UNIVERSES.get(agent_key)
    if symbols is None:
        return JSONResponse(status_code=404, content={"error": f"Unknown scanner agent: {agent_key}"})

    bars_value = max(20, min(int(bars), 500))
    limit_value = max(1, min(int(limit), 50))
    cache_key = f"scanner:{agent_key}:{interval}:{bars_value}:{limit_value}"
    cached = _batch_cache_get(cache_key)
    if cached is not None:
        return cached

    rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(build_scanner_row, symbol, interval, bars_value): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                row = future.result()
                row["ok"] = True
                row["buy_opportunity"] = rank_buy_opportunity(row)
                rows.append(row)
            except Exception as exc:
                rows.append(
                    {
                        "symbol": symbol,
                        "ok": False,
                        "error": str(exc),
                    }
                )

    ok_rows = [row for row in rows if row.get("ok")]
    ok_rows.sort(key=lambda x: float(x.get("buy_opportunity", float("-inf"))), reverse=True)
    payload = {
        "agent": agent_key,
        "interval": interval,
        "bars": bars_value,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": ok_rows[:limit_value] + [r for r in rows if not r.get("ok")],
    }
    _batch_cache_set(cache_key, payload)
    return payload


@app.get("/scanner/overall")
def scanner_overall(interval: str = "1day", bars: int = 60, limit: int = 10):
    return _scanner_agent("overall", interval=interval, bars=bars, limit=limit)


@app.get("/scanner/institution")
def scanner_institution(interval: str = "1day", bars: int = 60, limit: int = 10):
    return _scanner_agent("institution", interval=interval, bars=bars, limit=limit)


@app.get("/scanner/news")
def scanner_news(interval: str = "1day", bars: int = 60, limit: int = 10):
    return _scanner_agent("news", interval=interval, bars=bars, limit=limit)


@app.get("/scanner/social")
def scanner_social(interval: str = "1day", bars: int = 60, limit: int = 10):
    return _scanner_agent("social", interval=interval, bars=bars, limit=limit)


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

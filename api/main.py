# api/main.py

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.providers.selector import get_bars_with_fallback, get_quote_with_fallback
from app.providers.twelvedata import ProviderError, TwelveDataClient
from app.services.basic_signal import compute_basic_signal
from app.services.trade_signal import compute_trade_signal
from app.validation.market_data import ValidationError, validate_bars
from core.config import get_config, initialise_config
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


def _has_any_market_key() -> bool:
    keys = (
        os.getenv("FINNHUB_API_KEY", "").strip(),
        os.getenv("TWELVEDATA_API_KEY", "").strip(),
        os.getenv("ALPHAVANTAGE_API_KEY", "").strip(),
    )
    return any(keys)


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
    found = set()
    for row in rows:
        payload = _decode_payload(row.get("payload"))
        symbol = str(payload.get("symbol", "")).strip().upper()
        quote_payload = payload.get("quote")
        provider = payload.get("provider") or row.get("source") or "cache"
        if symbol not in wanted or symbol in found:
            continue
        if not isinstance(quote_payload, dict):
            continue
        results[symbol] = {
            "ok": True,
            "data": {"provider": provider, "symbol": symbol, "quote": quote_payload},
        }
        found.add(symbol)
        if len(found) == len(wanted):
            break
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

    found = set()
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
            break
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
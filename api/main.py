import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Request, status
...

load_dotenv()

logger = logging.getLogger(__name__)
app = FastAPI(title="Apollo 67")

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.providers.selector import get_bars_with_fallback, get_quote_with_fallback
from app.providers.twelvedata import ProviderError, TwelveDataClient
from app.validation.market_data import ValidationError, validate_bars, validate_quote
from core.config import get_config, initialise_config
from core.storage.db import DB_DRIVER_MARKER, check_db_connectivity, init_db
from app.services.basic_signal import compute_basic_signal

logger = logging.getLogger(__name__)

app = FastAPI(title="Apollo 67")
app.mount("/static", StaticFiles(directory="api/static"), name="static")
templates = Jinja2Templates(directory="api/templates")

_BATCH_MAX_SYMBOLS = 25
_BATCH_MAX_WORKERS = 4
_BATCH_CACHE_TTL_SECONDS = 60
_BATCH_CACHE_LOCK = Lock()
_BATCH_CACHE: dict[str, tuple[float, Any]] = {}


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
        "db": {
            "ok": db_ok,
            "message": db_message,
        },
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
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "bars": [bar.model_dump(mode="json") for bar in result.bars],
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
            "symbol": symbol,
            "quote": result.quote.model_dump(mode="json"),
        }
    except (ProviderError, ValidationError, ValueError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "twelvedata", "message": str(exc)},
        )


def _compute_basic_signal_payload(symbol: str) -> dict[str, Any]:
    client = TwelveDataClient()
    get_bars = getattr(client, "get_bars", None)
    if get_bars is None:
        get_bars = client.fetch_bars

    bars_result = get_bars(symbol=symbol, interval="1day", outputsize=30)
    bars = bars_result.get("bars", []) if isinstance(bars_result, dict) else bars_result

    if bars:
        validate_bars(bars)

    bars_for_signal = [
        bar.model_dump(mode="json") if hasattr(bar, "model_dump") else bar for bar in bars
    ]
    bars_for_signal = sorted(
        bars_for_signal,
        key=lambda bar: str(bar.get("ts_event", "")),
    )
    signal = compute_basic_signal(bars_for_signal)
    debug = signal.get("debug", {}) if isinstance(signal, dict) else {}
    debug.setdefault("bars_count", len(bars_for_signal) if bars_for_signal else None)
    debug.setdefault("first_ts", bars_for_signal[0].get("ts_event") if bars_for_signal else None)
    debug.setdefault("last_ts", bars_for_signal[-1].get("ts_event") if bars_for_signal else None)
    debug.setdefault("first_close", bars_for_signal[0].get("close") if bars_for_signal else None)
    debug.setdefault("last_close", bars_for_signal[-1].get("close") if bars_for_signal else None)
    signal["debug"] = debug
    return signal


def _parse_symbols_csv(symbols: str) -> list[str]:
    parsed = []
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
            "symbol": symbol,
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


@app.get("/batch/quotes")
def batch_quotes(symbols: str):
    requested = _parse_symbols_csv(symbols)
    if len(requested) > _BATCH_MAX_SYMBOLS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Maximum {_BATCH_MAX_SYMBOLS} symbols allowed per request."},
        )

    started = time.monotonic()
    results: dict[str, Any] = {}
    succeeded: list[str] = []
    failed: list[str] = []

    api_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    if not api_key:
        message = "TWELVEDATA_API_KEY is required"
        for symbol in requested:
            results[symbol] = {"ok": False, "error": message}
            failed.append(symbol)
    else:
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
    return {
        "symbols": requested,
        "results": results,
    }


@app.get("/batch/signals/basic")
def batch_signals_basic(symbols: str):
    requested = _parse_symbols_csv(symbols)
    if len(requested) > _BATCH_MAX_SYMBOLS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Maximum {_BATCH_MAX_SYMBOLS} symbols allowed per request."},
        )

    started = time.monotonic()
    results: dict[str, Any] = {}
    succeeded: list[str] = []
    failed: list[str] = []

    api_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    if not api_key:
        message = "TWELVEDATA_API_KEY is required"
        for symbol in requested:
            results[symbol] = {"ok": False, "error": message}
            failed.append(symbol)
    else:
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
    return {
        "symbols": requested,
        "results": results,
    }


@app.get("/signal/basic")
def signal_basic(symbol: str):
    try:
        return _compute_basic_signal_payload(symbol)
    except ProviderError as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": str(exc)},
        )
    except (ValidationError, ValueError, TypeError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": str(exc)},
        )

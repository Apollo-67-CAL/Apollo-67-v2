import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.providers.twelvedata import ProviderError, TwelveDataClient
from app.validation.market_data import ValidationError, validate_bars, validate_quote
from core.config import get_config, initialise_config
from core.storage.db import DB_DRIVER_MARKER, check_db_connectivity, init_db
from app.services.basic_signal import compute_basic_signal

logger = logging.getLogger(__name__)

app = FastAPI(title="Apollo 67")
app.mount("/static", StaticFiles(directory="api/static"), name="static")
templates = Jinja2Templates(directory="api/templates")


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
        client = TwelveDataClient()
        bars = client.fetch_bars(symbol=symbol, interval=interval, outputsize=outputsize)
        validate_bars(bars)
        return {
            "provider": "twelvedata",
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "bars": [bar.model_dump(mode="json") for bar in bars],
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
        client = TwelveDataClient()
        quote = client.fetch_quote(symbol=symbol)
        validate_quote(quote, freshness_seconds=cfg.data_freshness_sla_seconds)
        return {
            "provider": "twelvedata",
            "symbol": symbol,
            "quote": quote.model_dump(mode="json"),
        }
    except (ProviderError, ValidationError, ValueError) as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "provider": "twelvedata", "message": str(exc)},
        )


@app.get("/signal/basic")
def signal_basic(symbol: str):
    try:
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

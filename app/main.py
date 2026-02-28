import logging

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from app.storage.db import DB_DRIVER_MARKER, check_db_connectivity, init_db

logger = logging.getLogger(__name__)

app = FastAPI(title="Apollo 67")

@app.on_event("startup")
def startup() -> None:
    try:
        init_db()
    except Exception as e:
        logger.exception("init_db failed; continuing so server can start: %s", e)
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

@app.get("/debug/init-db")
def force_init():
    from app.storage.db import DB_DRIVER_MARKER, init_db
    init_db()
    return {"status": "init_db executed"}

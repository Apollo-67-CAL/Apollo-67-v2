from fastapi import FastAPI

from app.storage.db import check_db_connectivity, init_db

app = FastAPI(title="Apollo 67")

@app.on_event("startup")
def startup() -> None:
    init_db()

@app.get("/healthz")
def health_check():
    db_ok, db_message = check_db_connectivity()
    status = "ok" if db_ok else "degraded"
    return {
        "status": status,
        "app": "running",
        "db": {
            "ok": db_ok,
            "message": db_message,
        },
    }

@app.get("/")
def root():
    return {"app": "Apollo 67", "message": "Backend running"}

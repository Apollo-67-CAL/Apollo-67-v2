import os
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from core.repositories.sentiment_settings import (
    DEFAULT_SENTIMENT_SETTINGS,
    SCOPES,
    SentimentSettingsRepository,
    is_local_or_dev_env,
)
from core.repositories.trading_tactics import TradingTacticsRepository

router = APIRouter()
templates = Jinja2Templates(directory="api/templates")


def _mask_admin_token(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return "local"
    if len(t) <= 8:
        return t[:2] + "***"
    return t[:4] + "***" + t[-2:]


def require_admin(request: Request) -> str:
    configured = (os.getenv("ADMIN_TOKEN") or "").strip()
    provided = (
        request.headers.get("X-Admin-Token")
        or request.query_params.get("admin_token")
        or ""
    ).strip()

    if configured:
        if provided != configured:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return provided

    if is_local_or_dev_env():
        return "local"

    raise HTTPException(status_code=403, detail="ADMIN_TOKEN is required")


settings_repo = SentimentSettingsRepository()
tactics_repo = TradingTacticsRepository()


@router.get("", include_in_schema=False)
def admin_index(request: Request, _: str = Depends(require_admin)):
    return templates.TemplateResponse("admin/index.html", {"request": request})


@router.get("/sentiment", include_in_schema=False)
def admin_sentiment_page(request: Request, _: str = Depends(require_admin)):
    return templates.TemplateResponse("admin/sentiment.html", {"request": request})


@router.get("/tactics", include_in_schema=False)
def admin_tactics_page(request: Request, _: str = Depends(require_admin)):
    return templates.TemplateResponse("admin/tactics.html", {"request": request})


@router.get("/api/sentiment")
def get_sentiment_settings(_: str = Depends(require_admin)):
    try:
        data = settings_repo.get_current()
        return {"ok": True, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


@router.put("/api/sentiment")
def put_sentiment_settings(payload: Dict[str, Any], actor: str = Depends(require_admin)):
    try:
        incoming = payload.get("settings") if isinstance(payload, dict) and "settings" in payload else payload
        if not isinstance(incoming, dict):
            return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid payload"})

        for scope in SCOPES:
            if scope not in incoming:
                incoming[scope] = DEFAULT_SENTIMENT_SETTINGS[scope]

        weights_sum = 0.0
        for src in ("institutional", "news", "social"):
            scope_payload = incoming.get(src) or {}
            weights_sum += float(scope_payload.get("weight", 0.0))

        if abs(weights_sum - 1.0) > 1e-9:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Weights for institutional+news+social must sum to 1.0"},
            )

        updated = settings_repo.replace_all(incoming, changed_by=_mask_admin_token(actor))
        return {"ok": True, "data": updated}
    except Exception as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})


@router.post("/api/sentiment/reset")
def reset_sentiment_settings(actor: str = Depends(require_admin)):
    try:
        updated = settings_repo.reset_defaults(changed_by=_mask_admin_token(actor))
        return {"ok": True, "data": updated}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


@router.get("/api/sentiment/audit")
def get_sentiment_audit(limit: int = Query(default=10, ge=1, le=100), _: str = Depends(require_admin)):
    try:
        data = settings_repo.list_audit(limit=limit)
        return {"ok": True, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


@router.get("/api/tactics")
def list_tactics(q: str = Query(default=""), _: str = Depends(require_admin)):
    try:
        data = tactics_repo.list_tactics(search=q or None, include_deleted=False)
        return {"ok": True, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


@router.post("/api/tactics")
def create_tactic(payload: Dict[str, Any], _: str = Depends(require_admin)):
    try:
        created = tactics_repo.create(payload)
        return {"ok": True, "data": created}
    except Exception as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})


@router.put("/api/tactics/{tactic_id}")
def update_tactic(tactic_id: str, payload: Dict[str, Any], _: str = Depends(require_admin)):
    try:
        updated = tactics_repo.update(tactic_id, payload)
        return {"ok": True, "data": updated}
    except Exception as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})


@router.delete("/api/tactics/{tactic_id}")
def delete_tactic(tactic_id: str, _: str = Depends(require_admin)):
    try:
        ok = tactics_repo.soft_delete(tactic_id)
        if not ok:
            return JSONResponse(status_code=404, content={"ok": False, "error": "tactic not found"})
        return {"ok": True, "data": {"id": tactic_id, "deleted": True}}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})

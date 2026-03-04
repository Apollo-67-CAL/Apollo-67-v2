import json
import os
from typing import Any, Dict, List

from core.storage.db import get_connection

SCOPES = ("overall", "institutional", "news", "social")

DEFAULT_SENTIMENT_SETTINGS: Dict[str, Dict[str, Any]] = {
    "overall": {
        "enabled": True,
        "weight": 1.0,
        "recency_minutes": 1440,
        "bullish_threshold": 0.2,
        "bearish_threshold": -0.2,
        "payload": {},
    },
    "institutional": {
        "enabled": True,
        "weight": 0.35,
        "recency_minutes": 1440,
        "bullish_threshold": 0.2,
        "bearish_threshold": -0.2,
        "payload": {},
    },
    "news": {
        "enabled": True,
        "weight": 0.35,
        "recency_minutes": 720,
        "bullish_threshold": 0.2,
        "bearish_threshold": -0.2,
        "payload": {},
    },
    "social": {
        "enabled": True,
        "weight": 0.30,
        "recency_minutes": 240,
        "bullish_threshold": 0.2,
        "bearish_threshold": -0.2,
        "payload": {},
    },
}


def _decode_json(raw: Any, fallback: Any) -> Any:
    if raw is None:
        return fallback
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return fallback
        try:
            return json.loads(txt)
        except Exception:
            return fallback
    return fallback


def _mask_token(token: str) -> str:
    txt = (token or "").strip()
    if not txt:
        return "local"
    if len(txt) <= 8:
        return txt[:2] + "***"
    return txt[:4] + "***" + txt[-2:]


def _payload_for_audit(scope: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "scope": scope,
        "enabled": bool(settings.get("enabled", True)),
        "weight": float(settings.get("weight", 0.0)),
        "recency_minutes": int(settings.get("recency_minutes", 0)),
        "bullish_threshold": float(settings.get("bullish_threshold", 0.0)),
        "bearish_threshold": float(settings.get("bearish_threshold", 0.0)),
        "payload": settings.get("payload", {}),
        "updated_at": settings.get("updated_at"),
    }


class SentimentSettingsRepository:
    def ensure_defaults(self) -> None:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT scope FROM sentiment_settings WHERE scope IN (?, ?, ?, ?)",
                tuple(SCOPES),
            ).fetchall()
            existing = {str(r.get("scope", "")) for r in rows}

            for scope in SCOPES:
                if scope in existing:
                    continue
                d = DEFAULT_SENTIMENT_SETTINGS[scope]
                payload_json = json.dumps(d.get("payload", {}))
                if conn.backend == "postgres":
                    conn.execute(
                        """
                        INSERT INTO sentiment_settings
                        (scope, enabled, weight, recency_minutes, bullish_threshold, bearish_threshold, payload)
                        VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)
                        """,
                        (
                            scope,
                            bool(d["enabled"]),
                            float(d["weight"]),
                            int(d["recency_minutes"]),
                            float(d["bullish_threshold"]),
                            float(d["bearish_threshold"]),
                            payload_json,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO sentiment_settings
                        (scope, enabled, weight, recency_minutes, bullish_threshold, bearish_threshold, payload)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            scope,
                            1 if bool(d["enabled"]) else 0,
                            float(d["weight"]),
                            int(d["recency_minutes"]),
                            float(d["bullish_threshold"]),
                            float(d["bearish_threshold"]),
                            payload_json,
                        ),
                    )

    def get_current(self) -> Dict[str, Dict[str, Any]]:
        self.ensure_defaults()
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT scope, enabled, weight, recency_minutes,
                       bullish_threshold, bearish_threshold, payload, updated_at
                FROM sentiment_settings
                WHERE scope IN (?, ?, ?, ?)
                ORDER BY scope
                """,
                tuple(SCOPES),
            ).fetchall()

        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            scope = str(row.get("scope", "")).strip().lower()
            if not scope:
                continue
            out[scope] = {
                "enabled": bool(row.get("enabled")),
                "weight": float(row.get("weight", 0.0)),
                "recency_minutes": int(row.get("recency_minutes", 0)),
                "bullish_threshold": float(row.get("bullish_threshold", 0.0)),
                "bearish_threshold": float(row.get("bearish_threshold", 0.0)),
                "payload": _decode_json(row.get("payload"), {}),
                "updated_at": row.get("updated_at"),
            }

        for scope in SCOPES:
            if scope not in out:
                d = DEFAULT_SENTIMENT_SETTINGS[scope]
                out[scope] = {
                    "enabled": bool(d["enabled"]),
                    "weight": float(d["weight"]),
                    "recency_minutes": int(d["recency_minutes"]),
                    "bullish_threshold": float(d["bullish_threshold"]),
                    "bearish_threshold": float(d["bearish_threshold"]),
                    "payload": dict(d.get("payload", {})),
                    "updated_at": None,
                }
        return out

    def replace_all(self, settings: Dict[str, Dict[str, Any]], changed_by: str) -> Dict[str, Dict[str, Any]]:
        current = self.get_current()

        with get_connection() as conn:
            for scope in SCOPES:
                incoming = settings.get(scope) or {}
                prev = current.get(scope, DEFAULT_SENTIMENT_SETTINGS[scope])

                payload = incoming.get("payload")
                if payload is None:
                    payload = prev.get("payload", {})
                payload_json = json.dumps(payload if isinstance(payload, dict) else {})

                next_row = {
                    "enabled": bool(incoming.get("enabled", prev.get("enabled", True))),
                    "weight": float(incoming.get("weight", prev.get("weight", 0.0))),
                    "recency_minutes": int(incoming.get("recency_minutes", prev.get("recency_minutes", 0))),
                    "bullish_threshold": float(incoming.get("bullish_threshold", prev.get("bullish_threshold", 0.0))),
                    "bearish_threshold": float(incoming.get("bearish_threshold", prev.get("bearish_threshold", 0.0))),
                    "payload": payload if isinstance(payload, dict) else {},
                }

                if conn.backend == "postgres":
                    conn.execute(
                        """
                        UPDATE sentiment_settings
                        SET enabled = ?,
                            weight = ?,
                            recency_minutes = ?,
                            bullish_threshold = ?,
                            bearish_threshold = ?,
                            payload = ?::jsonb,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE scope = ?
                        """,
                        (
                            next_row["enabled"],
                            next_row["weight"],
                            next_row["recency_minutes"],
                            next_row["bullish_threshold"],
                            next_row["bearish_threshold"],
                            payload_json,
                            scope,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE sentiment_settings
                        SET enabled = ?,
                            weight = ?,
                            recency_minutes = ?,
                            bullish_threshold = ?,
                            bearish_threshold = ?,
                            payload = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE scope = ?
                        """,
                        (
                            1 if next_row["enabled"] else 0,
                            next_row["weight"],
                            next_row["recency_minutes"],
                            next_row["bullish_threshold"],
                            next_row["bearish_threshold"],
                            payload_json,
                            scope,
                        ),
                    )

                before_payload = json.dumps(_payload_for_audit(scope, prev))
                after_payload = json.dumps(_payload_for_audit(scope, next_row))
                actor = _mask_token(changed_by)
                if conn.backend == "postgres":
                    conn.execute(
                        """
                        INSERT INTO sentiment_audit_log
                        (scope, changed_by, before_payload, after_payload)
                        VALUES (?, ?, ?::jsonb, ?::jsonb)
                        """,
                        (scope, actor, before_payload, after_payload),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO sentiment_audit_log
                        (scope, changed_by, before_payload, after_payload)
                        VALUES (?, ?, ?, ?)
                        """,
                        (scope, actor, before_payload, after_payload),
                    )

        return self.get_current()

    def reset_defaults(self, changed_by: str) -> Dict[str, Dict[str, Any]]:
        return self.replace_all(DEFAULT_SENTIMENT_SETTINGS, changed_by=changed_by)

    def list_audit(self, limit: int = 10) -> List[Dict[str, Any]]:
        max_limit = max(1, min(int(limit), 200))
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, scope, changed_by, before_payload, after_payload, created_at
                FROM sentiment_audit_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (max_limit,),
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "id": row.get("id"),
                    "scope": row.get("scope"),
                    "changed_by": row.get("changed_by"),
                    "before_payload": _decode_json(row.get("before_payload"), {}),
                    "after_payload": _decode_json(row.get("after_payload"), {}),
                    "created_at": row.get("created_at"),
                }
            )
        return out


def is_local_or_dev_env() -> bool:
    env = (
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("APOLLO_ENV")
        or "local"
    )
    return env.strip().lower() in {"local", "dev", "development"}

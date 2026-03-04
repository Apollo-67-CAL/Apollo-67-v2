import json
import uuid
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


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


class TradingTacticsRepository:
    def list_tactics(self, search: Optional[str] = None, include_deleted: bool = False) -> List[Dict[str, Any]]:
        where = []
        params: List[Any] = []

        if not include_deleted:
            where.append("deleted_at IS NULL")

        if search:
            needle = f"%{search.strip()}%"
            where.append("(name LIKE ? OR description LIKE ?)")
            params.extend([needle, needle])

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        with get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT id, name, description, tags, parameters,
                       enabled, deleted_at, created_at, updated_at
                FROM trading_tactics
                {where_sql}
                ORDER BY name ASC
                """,
                tuple(params),
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "description": row.get("description") or "",
                    "tags": _decode_json(row.get("tags"), []),
                    "parameters": _decode_json(row.get("parameters"), {}),
                    "enabled": bool(row.get("enabled")),
                    "deleted_at": row.get("deleted_at"),
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                }
            )
        return out

    def get(self, tactic_id: str) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, name, description, tags, parameters,
                       enabled, deleted_at, created_at, updated_at
                FROM trading_tactics
                WHERE id = ?
                LIMIT 1
                """,
                (tactic_id,),
            ).fetchall()

        if not rows:
            return None
        row = rows[0]
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "description": row.get("description") or "",
            "tags": _decode_json(row.get("tags"), []),
            "parameters": _decode_json(row.get("parameters"), {}),
            "enabled": bool(row.get("enabled")),
            "deleted_at": row.get("deleted_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        tactic_id = str(uuid.uuid4())
        name = str(payload.get("name", "")).strip()
        if not name:
            raise ValueError("name is required")

        description = str(payload.get("description", "")).strip()
        tags = payload.get("tags") or []
        if not isinstance(tags, list):
            raise ValueError("tags must be an array")
        tags = [str(t).strip() for t in tags if str(t).strip()]

        parameters = payload.get("parameters")
        if parameters is None:
            parameters = {}
        if not isinstance(parameters, dict):
            raise ValueError("parameters must be an object")

        enabled = bool(payload.get("enabled", True))

        tags_json = json.dumps(tags)
        params_json = json.dumps(parameters)

        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    INSERT INTO trading_tactics
                    (id, name, description, tags, parameters, enabled)
                    VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?)
                    """,
                    (tactic_id, name, description, tags_json, params_json, enabled),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO trading_tactics
                    (id, name, description, tags, parameters, enabled)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (tactic_id, name, description, tags_json, params_json, 1 if enabled else 0),
                )

        created = self.get(tactic_id)
        if not created:
            raise RuntimeError("failed to create tactic")
        return created

    def update(self, tactic_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        existing = self.get(tactic_id)
        if not existing:
            raise ValueError("tactic not found")

        name = str(payload.get("name", existing["name"])).strip()
        if not name:
            raise ValueError("name is required")

        description = str(payload.get("description", existing.get("description", ""))).strip()

        tags = payload.get("tags", existing.get("tags", []))
        if not isinstance(tags, list):
            raise ValueError("tags must be an array")
        tags = [str(t).strip() for t in tags if str(t).strip()]

        parameters = payload.get("parameters", existing.get("parameters", {}))
        if not isinstance(parameters, dict):
            raise ValueError("parameters must be an object")

        enabled = bool(payload.get("enabled", existing.get("enabled", True)))

        tags_json = json.dumps(tags)
        params_json = json.dumps(parameters)

        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    UPDATE trading_tactics
                    SET name = ?,
                        description = ?,
                        tags = ?::jsonb,
                        parameters = ?::jsonb,
                        enabled = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (name, description, tags_json, params_json, enabled, tactic_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE trading_tactics
                    SET name = ?,
                        description = ?,
                        tags = ?,
                        parameters = ?,
                        enabled = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (name, description, tags_json, params_json, 1 if enabled else 0, tactic_id),
                )

        updated = self.get(tactic_id)
        if not updated:
            raise RuntimeError("failed to update tactic")
        return updated

    def soft_delete(self, tactic_id: str) -> bool:
        existing = self.get(tactic_id)
        if not existing:
            return False

        with get_connection() as conn:
            conn.execute(
                """
                UPDATE trading_tactics
                SET deleted_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (tactic_id,),
            )
        return True

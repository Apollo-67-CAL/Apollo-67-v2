import json
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


class ModelsRepository:
    def create(
        self,
        model_name: str,
        model_version: str,
        status: str,
        metrics: Optional[Dict[str, Any]] = None,
        trained_at: Optional[str] = None,
    ) -> int:
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO models (model_name, model_version, status, metrics, trained_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (model_name, model_version, status, json.dumps(metrics or {}), trained_at),
            )
            return int(cursor.lastrowid)

    def list_recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, model_name, model_version, status, metrics, trained_at, created_at
                FROM models
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

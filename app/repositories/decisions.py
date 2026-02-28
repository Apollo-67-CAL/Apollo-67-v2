import json
from typing import Any, Dict, List, Optional

from app.storage.db import get_connection


class DecisionsRepository:
    def create(
        self,
        decision_type: str,
        reason: str,
        payload: Optional[Dict[str, Any]] = None,
        signal_id: Optional[int] = None,
    ) -> int:
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO decisions (signal_id, decision_type, reason, payload)
                VALUES (?, ?, ?, ?)
                """,
                (signal_id, decision_type, reason, json.dumps(payload or {})),
            )
            return int(cursor.lastrowid)

    def list_recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, signal_id, decision_type, reason, payload, created_at
                FROM decisions
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

import json
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


class EventsRepository:
    def create(self, event_type: str, payload: Dict[str, Any], source: Optional[str] = None) -> int:
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO events (event_type, source, payload)
                VALUES (?, ?, ?)
                """,
                (event_type, source, json.dumps(payload)),
            )
            return int(cursor.lastrowid)

    def list_recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, event_type, source, payload, created_at
                FROM events
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

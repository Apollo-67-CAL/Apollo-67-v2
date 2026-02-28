import json
from typing import Any, Dict, List, Optional

from app.storage.db import get_connection


class SignalsRepository:
    def create(
        self,
        symbol: str,
        score: float,
        payload: Optional[Dict[str, Any]] = None,
        timeframe: Optional[str] = None,
    ) -> int:
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO signals (symbol, timeframe, score, payload)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, timeframe, score, json.dumps(payload or {})),
            )
            return int(cursor.lastrowid)

    def list_recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, symbol, timeframe, score, payload, created_at
                FROM signals
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

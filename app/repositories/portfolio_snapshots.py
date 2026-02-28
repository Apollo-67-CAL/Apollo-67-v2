import json
from typing import Any, Dict, List, Optional

from app.storage.db import get_connection


class PortfolioSnapshotsRepository:
    def create(
        self,
        as_of: str,
        equity: float,
        cash: float,
        gross_exposure: float = 0.0,
        net_exposure: float = 0.0,
        heat: float = 0.0,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO portfolio_snapshots (
                    as_of, equity, cash, gross_exposure, net_exposure, heat, payload
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    as_of,
                    equity,
                    cash,
                    gross_exposure,
                    net_exposure,
                    heat,
                    json.dumps(payload or {}),
                ),
            )
            return int(cursor.lastrowid)

    def list_recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, as_of, equity, cash, gross_exposure, net_exposure, heat, payload, created_at
                FROM portfolio_snapshots
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MonitorPositionsRepository:
    def create_position(
        self,
        symbol: str,
        buy_amount: float,
        buy_price: Optional[float] = None,
        buy_zone_low: Optional[float] = None,
        buy_zone_high: Optional[float] = None,
        notes: Optional[str] = None,
        status: str = "open",
    ) -> Dict[str, Any]:
        created_at = _utc_now_iso()
        with get_connection() as conn:
            result = conn.execute(
                """
                INSERT INTO monitor_positions
                (symbol, created_at, buy_amount, buy_price, buy_zone_low, buy_zone_high, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    created_at,
                    float(buy_amount),
                    buy_price,
                    buy_zone_low,
                    buy_zone_high,
                    status,
                    notes,
                ),
            )
            row_id = result.lastrowid
            rows = conn.execute(
                "SELECT * FROM monitor_positions WHERE id = ? LIMIT 1",
                (row_id,),
            ).fetchall()
        return rows[0] if rows else {}

    def list_positions(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 1000))
        with get_connection() as conn:
            if status:
                rows = conn.execute(
                    """
                    SELECT * FROM monitor_positions
                    WHERE status = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (status, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM monitor_positions
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        return rows

    def update_position_metrics(
        self,
        position_id: int,
        last_price: Optional[float],
        pnl_pct: Optional[float],
        max_up_pct: Optional[float],
        max_down_pct: Optional[float],
        last_checked_at: Optional[str] = None,
    ) -> None:
        checked_at = last_checked_at or _utc_now_iso()
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE monitor_positions
                SET last_price = ?, pnl_pct = ?, max_up_pct = ?, max_down_pct = ?, last_checked_at = ?
                WHERE id = ?
                """,
                (last_price, pnl_pct, max_up_pct, max_down_pct, checked_at, int(position_id)),
            )

    def close_position(self, position_id: int, status: str) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE monitor_positions
                SET status = ?, last_checked_at = ?
                WHERE id = ?
                """,
                (status, _utc_now_iso(), int(position_id)),
            )

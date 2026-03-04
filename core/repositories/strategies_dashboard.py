from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class StrategiesDashboardRepository:
    def list_strategies(self, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 1000))
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, name, strategy_group, payload, created_at
                FROM strategies
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [self._strategy_row_to_dict(r) for r in rows]

    def create_strategy(self, name: str, strategy_group: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        sid = str(uuid.uuid4())
        payload_json = json.dumps(payload)
        created_at = _utc_now_iso()
        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    INSERT INTO strategies (id, name, strategy_group, payload, created_at)
                    VALUES (?, ?, ?, ?::jsonb, ?)
                    """,
                    (sid, name, strategy_group, payload_json, created_at),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO strategies (id, name, strategy_group, payload, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (sid, name, strategy_group, payload_json, created_at),
                )

        row = self.get_strategy(sid)
        return row if row else {
            "id": sid,
            "name": name,
            "group": strategy_group,
            "payload": payload,
            "created_at": created_at,
        }

    def get_strategy(self, strategy_id: str) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, name, strategy_group, payload, created_at
                FROM strategies
                WHERE id = ?
                LIMIT 1
                """,
                (strategy_id,),
            ).fetchall()
        if not rows:
            return None
        return self._strategy_row_to_dict(rows[0])

    def list_monitors(self, strategy_id: Optional[str] = None, limit: int = 500) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 2000))
        with get_connection() as conn:
            if strategy_id:
                rows = conn.execute(
                    """
                    SELECT id, strategy_id, symbol, entry_price, quantity, entry_date, notes,
                           last_price, pnl, pnl_pct, created_at, updated_at
                    FROM monitors
                    WHERE strategy_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (strategy_id, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, strategy_id, symbol, entry_price, quantity, entry_date, notes,
                           last_price, pnl, pnl_pct, created_at, updated_at
                    FROM monitors
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        return [self._monitor_row_to_dict(r) for r in rows]

    def create_monitor(
        self,
        symbol: str,
        entry_price: float,
        quantity: float,
        strategy_id: Optional[str] = None,
        entry_date: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        entry_date_value = entry_date or _utc_now_iso()
        now = _utc_now_iso()
        with get_connection() as conn:
            result = conn.execute(
                """
                INSERT INTO monitors
                (strategy_id, symbol, entry_price, quantity, entry_date, notes, last_price, pnl, pnl_pct, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy_id,
                    symbol,
                    float(entry_price),
                    float(quantity),
                    entry_date_value,
                    notes,
                    float(entry_price),
                    0.0,
                    0.0,
                    now,
                    now,
                ),
            )
            row_id = result.lastrowid
            rows = conn.execute(
                """
                SELECT id, strategy_id, symbol, entry_price, quantity, entry_date, notes,
                       last_price, pnl, pnl_pct, created_at, updated_at
                FROM monitors WHERE id = ? LIMIT 1
                """,
                (row_id,),
            ).fetchall()
        return self._monitor_row_to_dict(rows[0]) if rows else {}

    def refresh_monitor(self, monitor_id: int, last_price: float) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, strategy_id, symbol, entry_price, quantity, entry_date, notes,
                       last_price, pnl, pnl_pct, created_at, updated_at
                FROM monitors WHERE id = ? LIMIT 1
                """,
                (int(monitor_id),),
            ).fetchall()
            if not rows:
                return None
            row = rows[0]
            entry_price = float(row.get("entry_price") or 0.0)
            quantity = float(row.get("quantity") or 0.0)
            pnl = (float(last_price) - entry_price) * quantity
            pnl_pct = ((float(last_price) / entry_price) - 1.0) * 100.0 if entry_price > 0 else 0.0
            now = _utc_now_iso()
            conn.execute(
                """
                UPDATE monitors
                SET last_price = ?, pnl = ?, pnl_pct = ?, updated_at = ?
                WHERE id = ?
                """,
                (float(last_price), float(pnl), float(pnl_pct), now, int(monitor_id)),
            )
        return self.get_monitor(monitor_id)

    def get_monitor(self, monitor_id: int) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, strategy_id, symbol, entry_price, quantity, entry_date, notes,
                       last_price, pnl, pnl_pct, created_at, updated_at
                FROM monitors WHERE id = ? LIMIT 1
                """,
                (int(monitor_id),),
            ).fetchall()
        if not rows:
            return None
        return self._monitor_row_to_dict(rows[0])

    @staticmethod
    def _decode_payload(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                return {}
        return {}

    def _strategy_row_to_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "group": row.get("strategy_group"),
            "payload": self._decode_payload(row.get("payload")),
            "created_at": row.get("created_at"),
        }

    @staticmethod
    def _monitor_row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row.get("id"),
            "strategy_id": row.get("strategy_id"),
            "symbol": row.get("symbol"),
            "entry_price": row.get("entry_price"),
            "quantity": row.get("quantity"),
            "entry_date": row.get("entry_date"),
            "notes": row.get("notes"),
            "last_price": row.get("last_price"),
            "pnl": row.get("pnl"),
            "pnl_pct": row.get("pnl_pct"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

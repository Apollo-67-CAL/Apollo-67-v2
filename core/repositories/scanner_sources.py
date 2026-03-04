import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ScannerSourceBreakdownsRepository:
    def insert_breakdown(self, symbol: str, scanner_type: str, payload: Dict[str, Any]) -> None:
        payload_json = json.dumps(payload)
        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    INSERT INTO scanner_source_breakdowns (symbol, scanner_type, payload, created_at)
                    VALUES (?, ?, ?::jsonb, ?)
                    """,
                    (symbol, scanner_type, payload_json, _utc_now_iso()),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO scanner_source_breakdowns (symbol, scanner_type, payload, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (symbol, scanner_type, payload_json, _utc_now_iso()),
                )

    def get_latest_breakdown(self, symbol: str, scanner_type: str) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, symbol, scanner_type, payload, created_at
                FROM scanner_source_breakdowns
                WHERE symbol = ? AND scanner_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (symbol, scanner_type),
            ).fetchall()
        if not rows:
            return None
        row = rows[0]
        return {
            "id": row.get("id"),
            "symbol": row.get("symbol"),
            "scanner_type": row.get("scanner_type"),
            "payload": self._decode_payload(row.get("payload")),
            "created_at": row.get("created_at"),
        }

    def list_recent_breakdowns(
        self,
        scanner_type: Optional[str] = None,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 2000))
        with get_connection() as conn:
            if scanner_type:
                rows = conn.execute(
                    """
                    SELECT id, symbol, scanner_type, payload, created_at
                    FROM scanner_source_breakdowns
                    WHERE scanner_type = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (scanner_type, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, symbol, scanner_type, payload, created_at
                    FROM scanner_source_breakdowns
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()

        output: List[Dict[str, Any]] = []
        for row in rows:
            output.append(
                {
                    "id": row.get("id"),
                    "symbol": row.get("symbol"),
                    "scanner_type": row.get("scanner_type"),
                    "payload": self._decode_payload(row.get("payload")),
                    "created_at": row.get("created_at"),
                }
            )
        return output

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

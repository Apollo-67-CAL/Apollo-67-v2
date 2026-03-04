from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from core.storage.db import get_connection


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ScannerConnectorsRepository:
    def initialise_defaults_if_missing(self, registry: List[Any]) -> None:
        now = _utc_now_iso()
        connectors: List[Dict[str, Any]] = []
        for entry in registry:
            if isinstance(entry, dict):
                connector_id = str(entry.get("id") or "").strip()
                status = str(entry.get("status") or "").strip().lower()
            else:
                connector_id = str(getattr(entry, "id", "") or "").strip()
                status = str(getattr(entry, "status", "") or "").strip().lower()
            if not connector_id:
                continue
            connectors.append(
                {
                    "id": connector_id,
                    "enabled": 1 if status == "live" else 0,
                }
            )

        with get_connection() as conn:
            for connector in connectors:
                if conn.backend == "postgres":
                    conn.execute(
                        """
                        INSERT INTO scanner_connectors(id, enabled, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (connector["id"], connector["enabled"], now),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO scanner_connectors(id, enabled, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(id) DO NOTHING
                        """,
                        (connector["id"], connector["enabled"], now),
                    )

    def get_all_enabled_map(self) -> Dict[str, bool]:
        with get_connection() as conn:
            rows = conn.execute("SELECT id, enabled FROM scanner_connectors").fetchall()
        output: Dict[str, bool] = {}
        for row in rows:
            output[str(row.get("id"))] = bool(row.get("enabled"))
        return output

    def set_enabled(self, connector_id: str, enabled: bool) -> None:
        now = _utc_now_iso()
        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    INSERT INTO scanner_connectors(id, enabled, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (id)
                    DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at
                    """,
                    (connector_id, 1 if enabled else 0, now),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO scanner_connectors(id, enabled, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(id)
                    DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at
                    """,
                    (connector_id, 1 if enabled else 0, now),
                )

    def list_rows(self) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT id, enabled, updated_at FROM scanner_connectors ORDER BY id ASC"
            ).fetchall()
        return [
            {
                "id": row.get("id"),
                "enabled": bool(row.get("enabled")),
                "updated_at": row.get("updated_at"),
            }
            for row in rows
        ]

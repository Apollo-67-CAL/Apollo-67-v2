from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


class CuratedDatasetsRepository:
    def upsert(
        self,
        dataset_name: str,
        dataset_version: str,
        payload_dict: Dict[str, Any],
        status: str = "active",
    ) -> Dict[str, Any]:
        payload_json = json.dumps(payload_dict)

        with get_connection() as conn:
            if conn.backend == "sqlite":
                conn.execute(
                    """
                    INSERT OR REPLACE INTO curated_datasets
                    (dataset_name, dataset_version, status, payload)
                    VALUES (?, ?, ?, ?)
                    """,
                    (dataset_name, dataset_version, status, payload_json),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO curated_datasets (dataset_name, dataset_version, status, payload)
                    VALUES (?, ?, ?, ?::jsonb)
                    ON CONFLICT (dataset_name, dataset_version)
                    DO UPDATE SET
                      status = EXCLUDED.status,
                      payload = EXCLUDED.payload
                    """,
                    (dataset_name, dataset_version, status, payload_json),
                )

        return {
            "dataset_name": dataset_name,
            "dataset_version": dataset_version,
            "status": status,
            "payload": payload_dict,
        }

    def get(self, dataset_name: str, dataset_version: str) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT dataset_name, dataset_version, status, payload, created_at
                FROM curated_datasets
                WHERE dataset_name = ? AND dataset_version = ?
                LIMIT 1
                """,
                (dataset_name, dataset_version),
            ).fetchall()

        if not rows:
            return None
        return self._row_to_dict(rows[0])

    def get_latest_by_name(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT dataset_name, dataset_version, status, payload, created_at
                FROM curated_datasets
                WHERE dataset_name = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (dataset_name,),
            ).fetchall()

        if not rows:
            return None
        return self._row_to_dict(rows[0])

    def list_by_name(self, dataset_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 200))
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT dataset_name, dataset_version, status, payload, created_at
                FROM curated_datasets
                WHERE dataset_name = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (dataset_name, safe_limit),
            ).fetchall()

        return [self._row_to_dict(r) for r in rows]

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

    def _row_to_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "dataset_name": row.get("dataset_name"),
            "dataset_version": row.get("dataset_version"),
            "status": row.get("status"),
            "payload": self._decode_payload(row.get("payload")),
            "created_at": row.get("created_at"),
        }

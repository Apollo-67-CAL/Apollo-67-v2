from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


_ALIAS_MAP = {
    "twitter": "x",
    "twittercom": "x",
    "xcom": "x",
    "redditcom": "reddit",
    "hotcoppercomau": "hotcopper",
}


def normalize_source_key(value: Optional[str]) -> str:
    raw = (value or "").strip().lower()
    cleaned = "".join(ch for ch in raw if ch.isalnum())
    if not cleaned:
        return "unknown"
    return _ALIAS_MAP.get(cleaned, cleaned)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ScannerSourceControlsRepository:
    def list_controls(self, scanner_type: str) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, scanner_type, source_key, display_name, blocked, weight,
                       min_mentions, min_confidence, notes, updated_at
                FROM scanner_source_controls
                WHERE scanner_type = ?
                ORDER BY source_key ASC
                """,
                (scanner_type,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def upsert_control(
        self,
        scanner_type: str,
        source_key: str,
        display_name: Optional[str] = None,
        blocked: bool = False,
        weight: float = 1.0,
        min_mentions: int = 0,
        min_confidence: float = 0.0,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        norm_key = normalize_source_key(source_key)
        now = _utc_now_iso()
        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    INSERT INTO scanner_source_controls
                    (scanner_type, source_key, display_name, blocked, weight, min_mentions, min_confidence, notes, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (scanner_type, source_key)
                    DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        blocked = EXCLUDED.blocked,
                        weight = EXCLUDED.weight,
                        min_mentions = EXCLUDED.min_mentions,
                        min_confidence = EXCLUDED.min_confidence,
                        notes = EXCLUDED.notes,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        scanner_type,
                        norm_key,
                        display_name,
                        1 if blocked else 0,
                        float(weight),
                        int(min_mentions),
                        float(min_confidence),
                        notes,
                        now,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO scanner_source_controls
                    (scanner_type, source_key, display_name, blocked, weight, min_mentions, min_confidence, notes, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(scanner_type, source_key)
                    DO UPDATE SET
                        display_name = excluded.display_name,
                        blocked = excluded.blocked,
                        weight = excluded.weight,
                        min_mentions = excluded.min_mentions,
                        min_confidence = excluded.min_confidence,
                        notes = excluded.notes,
                        updated_at = excluded.updated_at
                    """,
                    (
                        scanner_type,
                        norm_key,
                        display_name,
                        1 if blocked else 0,
                        float(weight),
                        int(min_mentions),
                        float(min_confidence),
                        notes,
                        now,
                    ),
                )

        row = self.get_control(scanner_type, norm_key)
        return row if row else {
            "scanner_type": scanner_type,
            "source_key": norm_key,
        }

    def get_control(self, scanner_type: str, source_key: str) -> Optional[Dict[str, Any]]:
        norm_key = normalize_source_key(source_key)
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, scanner_type, source_key, display_name, blocked, weight,
                       min_mentions, min_confidence, notes, updated_at
                FROM scanner_source_controls
                WHERE scanner_type = ? AND source_key = ?
                LIMIT 1
                """,
                (scanner_type, norm_key),
            ).fetchall()
        if not rows:
            return None
        return self._row_to_dict(rows[0])

    def delete_control(self, scanner_type: str, source_key: str) -> None:
        norm_key = normalize_source_key(source_key)
        with get_connection() as conn:
            conn.execute(
                """
                DELETE FROM scanner_source_controls
                WHERE scanner_type = ? AND source_key = ?
                """,
                (scanner_type, norm_key),
            )

    @staticmethod
    def _row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row.get("id"),
            "scanner_type": row.get("scanner_type"),
            "source_key": row.get("source_key"),
            "display_name": row.get("display_name"),
            "blocked": bool(row.get("blocked")),
            "weight": float(row.get("weight") or 1.0),
            "min_mentions": int(row.get("min_mentions") or 0),
            "min_confidence": float(row.get("min_confidence") or 0.0),
            "notes": row.get("notes"),
            "updated_at": row.get("updated_at"),
        }

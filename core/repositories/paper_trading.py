from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.storage.db import get_connection


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PaperTradingRepository:
    def create_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        notional: float,
        price: float,
        status: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        opened_at = _utc_now_iso()
        meta_obj = meta if isinstance(meta, dict) else {}
        meta_json = json.dumps(meta_obj)
        with get_connection() as conn:
            if conn.backend == "postgres":
                res = conn.execute(
                    """
                    INSERT INTO paper_orders(symbol, side, qty, notional, price, status, opened_at, meta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                    """,
                    (symbol, side, float(qty), float(notional), float(price), status, opened_at, meta_json),
                )
            else:
                res = conn.execute(
                    """
                    INSERT INTO paper_orders(symbol, side, qty, notional, price, status, opened_at, meta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (symbol, side, float(qty), float(notional), float(price), status, opened_at, meta_json),
                )
            row_id = res.lastrowid
            rows = conn.execute("SELECT * FROM paper_orders WHERE id = ? LIMIT 1", (row_id,)).fetchall()
        return rows[0] if rows else {}

    def list_orders(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 1000))
        with get_connection() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM paper_orders WHERE status = ? ORDER BY id DESC LIMIT ?",
                    (status.upper(), safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM paper_orders ORDER BY id DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
        return rows

    def list_open_orders(self) -> List[Dict[str, Any]]:
        return self.list_orders(status="OPEN", limit=2000)

    def close_order(self, order_id: int, close_price: float, pnl: float) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE paper_orders
                SET status = 'CLOSED', closed_at = ?, close_price = ?, pnl = ?
                WHERE id = ?
                """,
                (_utc_now_iso(), float(close_price), float(pnl), int(order_id)),
            )

    def upsert_position(
        self,
        symbol: str,
        qty: float,
        avg_price: float,
        last_price: Optional[float],
        unrealised_pnl: float,
        realised_pnl: float,
        tactic_id: Optional[str] = None,
    ) -> None:
        symbol_u = str(symbol or "").strip().upper()
        now_iso = _utc_now_iso()
        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    INSERT INTO paper_positions(symbol, qty, avg_price, opened_at, updated_at, last_price, unrealised_pnl, realised_pnl, tactic_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol)
                    DO UPDATE SET
                        qty = EXCLUDED.qty,
                        avg_price = EXCLUDED.avg_price,
                        updated_at = EXCLUDED.updated_at,
                        last_price = EXCLUDED.last_price,
                        unrealised_pnl = EXCLUDED.unrealised_pnl,
                        realised_pnl = EXCLUDED.realised_pnl,
                        tactic_id = EXCLUDED.tactic_id
                    """,
                    (
                        symbol_u,
                        float(qty),
                        float(avg_price),
                        now_iso,
                        now_iso,
                        last_price,
                        float(unrealised_pnl),
                        float(realised_pnl),
                        tactic_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO paper_positions(symbol, qty, avg_price, opened_at, updated_at, last_price, unrealised_pnl, realised_pnl, tactic_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol)
                    DO UPDATE SET
                        qty = excluded.qty,
                        avg_price = excluded.avg_price,
                        updated_at = excluded.updated_at,
                        last_price = excluded.last_price,
                        unrealised_pnl = excluded.unrealised_pnl,
                        realised_pnl = excluded.realised_pnl,
                        tactic_id = excluded.tactic_id
                    """,
                    (
                        symbol_u,
                        float(qty),
                        float(avg_price),
                        now_iso,
                        now_iso,
                        last_price,
                        float(unrealised_pnl),
                        float(realised_pnl),
                        tactic_id,
                    ),
                )

    def remove_position(self, symbol: str) -> None:
        with get_connection() as conn:
            conn.execute("DELETE FROM paper_positions WHERE symbol = ?", (str(symbol or "").strip().upper(),))

    def list_positions(self, limit: int = 500) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 5000))
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM paper_positions ORDER BY updated_at DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return rows

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM paper_positions WHERE symbol = ? LIMIT 1",
                (str(symbol or "").strip().upper(),),
            ).fetchall()
        return rows[0] if rows else None

    def start_or_get_run(self, tactic_id: str = "default") -> Dict[str, Any]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM paper_runs
                WHERE tactic_id = ? AND ended_at IS NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (tactic_id,),
            ).fetchall()
            if rows:
                return rows[0]
            res = conn.execute(
                """
                INSERT INTO paper_runs(tactic_id, started_at, wins, losses, net_pnl, win_rate, notes)
                VALUES (?, ?, 0, 0, 0, 0, ?)
                """,
                (tactic_id, _utc_now_iso(), "{}"),
            )
            row_id = res.lastrowid
            rows = conn.execute("SELECT * FROM paper_runs WHERE id = ? LIMIT 1", (row_id,)).fetchall()
        return rows[0] if rows else {}

    def update_run_metrics(self, run_id: int, wins: int, losses: int, net_pnl: float, notes: Optional[Dict[str, Any]] = None) -> None:
        total = max(0, int(wins) + int(losses))
        win_rate = (float(wins) / float(total)) if total > 0 else 0.0
        notes_obj = notes if isinstance(notes, dict) else {}
        notes_json = json.dumps(notes_obj)
        with get_connection() as conn:
            if conn.backend == "postgres":
                conn.execute(
                    """
                    UPDATE paper_runs
                    SET wins = ?, losses = ?, net_pnl = ?, win_rate = ?, notes = ?::jsonb
                    WHERE id = ?
                    """,
                    (int(wins), int(losses), float(net_pnl), float(win_rate), notes_json, int(run_id)),
                )
            else:
                conn.execute(
                    """
                    UPDATE paper_runs
                    SET wins = ?, losses = ?, net_pnl = ?, win_rate = ?, notes = ?
                    WHERE id = ?
                    """,
                    (int(wins), int(losses), float(net_pnl), float(win_rate), notes_json, int(run_id)),
                )

    def list_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 200))
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM paper_runs ORDER BY wins DESC, net_pnl DESC, id DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return rows

    def clear_all(self) -> None:
        with get_connection() as conn:
            conn.execute("DELETE FROM paper_orders")
            conn.execute("DELETE FROM paper_positions")
            conn.execute("DELETE FROM paper_runs")

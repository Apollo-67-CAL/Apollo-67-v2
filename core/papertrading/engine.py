from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.repositories.paper_trading import PaperTradingRepository

NOTIONAL_PER_TRADE = 1000.0
MAX_POSITIONS = 10
ROTATE_N = 2
EVAL_INTERVAL_SECONDS = 60
ROTATE_INTERVAL_SECONDS = 300


class PaperTradingEngine:
    def __init__(self, repo: Optional[PaperTradingRepository] = None) -> None:
        self.repo = repo or PaperTradingRepository()

    def place_buy_from_scanner(
        self,
        symbol: str,
        notional: float,
        quote: Dict[str, Any],
        trade: Optional[Dict[str, Any]] = None,
        scanner_meta: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        symbol_u = str(symbol or "").strip().upper()
        if not symbol_u:
            return None

        price = self._positive_float(quote.get("last") if isinstance(quote, dict) else None)
        if price is None and isinstance(trade, dict):
            price = self._positive_float(trade.get("last_close"))
        if price is None:
            return None

        amount = self._positive_float(notional)
        if amount is None:
            amount = NOTIONAL_PER_TRADE
        qty = amount / price if price > 0 else 0.0
        if qty <= 0:
            return None

        if self.repo.get_position(symbol_u):
            return None

        meta = {
            "tactic_id": (scanner_meta or {}).get("tactic_id") if isinstance(scanner_meta, dict) else None,
            "scanner_score": (scanner_meta or {}).get("score") if isinstance(scanner_meta, dict) else None,
            "confidence": (scanner_meta or {}).get("confidence") if isinstance(scanner_meta, dict) else None,
            "provider_used": (quote or {}).get("provider") if isinstance(quote, dict) else None,
            "trade": trade if isinstance(trade, dict) else {},
            "scanner_meta": scanner_meta if isinstance(scanner_meta, dict) else {},
        }
        order = self.repo.create_order(
            symbol=symbol_u,
            side="BUY",
            qty=qty,
            notional=amount,
            price=price,
            status="OPEN",
            meta=meta,
        )
        self.repo.upsert_position(
            symbol=symbol_u,
            qty=qty,
            avg_price=price,
            last_price=price,
            unrealised_pnl=0.0,
            realised_pnl=0.0,
            tactic_id=str(meta.get("tactic_id") or "default"),
            strategy_id=str(meta.get("tactic_id") or "default"),
            tactic_label=str((scanner_meta or {}).get("tactic_label") or (scanner_meta or {}).get("strategy_label") or ""),
            market="AU" if symbol_u.endswith(".AX") else "US",
            stop_loss=self._positive_float((trade or {}).get("stop_loss_price")),
            take_profit=self._positive_float((trade or {}).get("target_sell_price")),
            trailing_stop=self._positive_float((trade or {}).get("trailing_stop_price")),
            highest_price=price,
            status="OPEN",
        )
        self._refresh_run_metrics()
        return order

    def evaluate_positions(
        self,
        prices_by_symbol: Dict[str, Any],
        trade_params_by_symbol: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        closed: List[Dict[str, Any]] = []
        positions = self.repo.list_positions(limit=500)

        for pos in positions:
            symbol = str(pos.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            price = self._positive_float(prices_by_symbol.get(symbol))
            if price is None:
                continue

            qty = self._positive_float(pos.get("qty")) or 0.0
            avg = self._positive_float(pos.get("avg_price")) or 0.0
            if qty <= 0 or avg <= 0:
                continue

            trade_params = trade_params_by_symbol.get(symbol) if isinstance(trade_params_by_symbol, dict) else None
            levels = self._resolve_levels(avg, trade_params)
            stop = self._positive_float(pos.get("stop_loss")) or levels.get("stop")
            target = self._positive_float(pos.get("take_profit")) or levels.get("target")
            trail = self._positive_float(pos.get("trailing_stop")) or levels.get("trail")
            highest_price_prev = self._positive_float(pos.get("highest_price")) or avg
            highest_price = max(highest_price_prev, price)
            if trail is not None and highest_price > highest_price_prev:
                trail_buffer = max(0.01, highest_price_prev - trail)
                trail = max(float(trail), float(highest_price - trail_buffer))
            strategy_id = str(pos.get("strategy_id") or pos.get("tactic_id") or "manual")
            tactic_label = str(pos.get("tactic_label") or strategy_id)
            market = str(pos.get("market") or ("AU" if symbol.endswith(".AX") else "US")).upper()
            status = str(pos.get("status") or "OPEN").upper()

            unreal = (price - avg) * qty
            self.repo.upsert_position(
                symbol=symbol,
                qty=qty,
                avg_price=avg,
                last_price=price,
                unrealised_pnl=unreal,
                realised_pnl=self._positive_or_zero(pos.get("realised_pnl")),
                tactic_id=str(pos.get("tactic_id") or strategy_id or "default"),
                strategy_id=strategy_id,
                tactic_label=tactic_label,
                market=market,
                stop_loss=stop,
                take_profit=target,
                trailing_stop=trail,
                highest_price=highest_price,
                status=status,
            )

            exit_reason = None
            if target is not None and price >= target:
                exit_reason = "target_hit"
            elif stop is not None and price <= stop:
                exit_reason = "stop_hit"
            elif trail is not None and price <= trail:
                exit_reason = "trailing_stop_hit"

            if not exit_reason:
                continue

            pnl = (price - avg) * qty
            exit_order = self.repo.create_order(
                symbol=symbol,
                side="SELL",
                qty=qty,
                notional=qty * price,
                price=price,
                status="OPEN",
                meta={
                    "source": "paper_engine_exit",
                    "reason": exit_reason,
                    "strategy_key": strategy_id,
                    "tactic_id": strategy_id,
                    "tactic_label": tactic_label,
                },
            )
            if exit_order and exit_order.get("id") is not None:
                self.repo.close_order(int(exit_order.get("id")), close_price=price, pnl=pnl)
            self.repo.remove_position(symbol)
            closed.append(
                {
                    "symbol": symbol,
                    "close_price": price,
                    "pnl": pnl,
                    "reason": exit_reason,
                }
            )

        if closed:
            self._refresh_run_metrics()

        return {"closed": closed, "closed_count": len(closed)}

    def rotation_cycle(
        self,
        scanner_buy_candidates: List[Dict[str, Any]],
        current_positions: List[Dict[str, Any]],
        max_positions: int,
        rotate_n: int,
    ) -> Dict[str, Any]:
        opened = []
        closed = []

        max_positions_val = max(1, int(max_positions))
        rotate_n_val = max(0, int(rotate_n))

        pos_by_symbol = {
            str(p.get("symbol") or "").strip().upper(): p
            for p in current_positions
            if str(p.get("symbol") or "").strip()
        }
        symbols_in_pos = set(pos_by_symbol.keys())

        sorted_candidates = sorted(
            [c for c in scanner_buy_candidates if str(c.get("action") or "").upper() == "BUY"],
            key=lambda x: (
                -self._positive_or_zero(x.get("confidence")),
                -self._positive_or_zero(x.get("rr")),
                -self._positive_or_zero(x.get("score")),
            ),
        )

        if len(symbols_in_pos) >= max_positions_val and rotate_n_val > 0:
            bottom = sorted(
                current_positions,
                key=lambda p: (
                    self._positive_or_zero(p.get("unrealised_pnl")),
                    self._positive_or_zero((p.get("meta") or {}).get("scanner_score") if isinstance(p.get("meta"), dict) else None),
                ),
            )[:rotate_n_val]

            open_orders = self.repo.list_open_orders()
            order_by_symbol = {}
            for order in open_orders:
                sym = str(order.get("symbol") or "").strip().upper()
                if sym and sym not in order_by_symbol:
                    order_by_symbol[sym] = order

            for pos in bottom:
                sym = str(pos.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                ord_row = order_by_symbol.get(sym)
                last_price = self._positive_float(pos.get("last_price")) or self._positive_float(pos.get("avg_price"))
                avg = self._positive_float(pos.get("avg_price"))
                qty = self._positive_float(pos.get("qty"))
                if not ord_row or last_price is None or avg is None or qty is None:
                    continue
                pnl = (last_price - avg) * qty
                self.repo.close_order(int(ord_row.get("id")), close_price=last_price, pnl=pnl)
                self.repo.remove_position(sym)
                symbols_in_pos.discard(sym)
                closed.append({"symbol": sym, "pnl": pnl, "reason": "rotation"})

        for row in sorted_candidates:
            if len(symbols_in_pos) >= max_positions_val:
                break
            sym = str(row.get("symbol") or "").strip().upper()
            if not sym or sym in symbols_in_pos:
                continue
            quote = {"last": row.get("price"), "provider": row.get("provider_used") or row.get("provider")}
            trade = {
                "last_close": row.get("price"),
                "target_sell_price": row.get("target"),
                "stop_loss_price": row.get("stop"),
                "trailing_stop_price": row.get("trail"),
                "entry_zone": row.get("entry_zone") if isinstance(row.get("entry_zone"), dict) else {
                    "low": row.get("entry_low"),
                    "high": row.get("entry_high"),
                },
                "indicators": row.get("indicators") if isinstance(row.get("indicators"), dict) else {},
            }
            order = self.place_buy_from_scanner(
                symbol=sym,
                notional=NOTIONAL_PER_TRADE,
                quote=quote,
                trade=trade,
                scanner_meta=row,
            )
            if order:
                symbols_in_pos.add(sym)
                opened.append({"symbol": sym, "order_id": order.get("id")})

        self._refresh_run_metrics()
        return {"opened": opened, "closed": closed}

    def _resolve_levels(self, entry: float, trade_params: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
        target = self._positive_float((trade_params or {}).get("target_sell_price"))
        stop = self._positive_float((trade_params or {}).get("stop_loss_price"))
        trail = self._positive_float((trade_params or {}).get("trailing_stop_price"))

        if target is not None and stop is not None and trail is not None:
            return {"target": target, "stop": stop, "trail": trail}

        atr = self._positive_float(((trade_params or {}).get("indicators") or {}).get("atr14"))
        if atr is None:
            atr = max(0.5, entry * 0.01)

        return {
            "target": target if target is not None else entry + (2.0 * atr),
            "stop": stop if stop is not None else max(0.01, entry - atr),
            "trail": trail if trail is not None else max(0.01, entry - (1.5 * atr)),
        }

    def _refresh_run_metrics(self) -> None:
        run = self.repo.start_or_get_run("default")
        run_id = int(run.get("id")) if run and run.get("id") is not None else None
        if run_id is None:
            return

        closed = self.repo.list_orders(status="CLOSED", limit=5000)
        wins = 0
        losses = 0
        net = 0.0
        for row in closed:
            pnl = self._float_or_none(row.get("pnl"))
            if pnl is None:
                continue
            net += pnl
            if pnl > 0:
                wins += 1
            else:
                losses += 1

        self.repo.update_run_metrics(run_id=run_id, wins=wins, losses=losses, net_pnl=net, notes={"closed_trades": wins + losses})

    @staticmethod
    def _float_or_none(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            num = float(value)
        except Exception:
            return None
        if num != num:
            return None
        return num

    @classmethod
    def _positive_float(cls, value: Any) -> Optional[float]:
        num = cls._float_or_none(value)
        if num is None or num <= 0:
            return None
        return num

    @classmethod
    def _positive_or_zero(cls, value: Any) -> float:
        num = cls._float_or_none(value)
        return num if num is not None else 0.0

#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.papertrading.engine import PaperTradingEngine
from core.repositories.paper_trading import PaperTradingRepository
from core.storage.db import init_db


def main() -> int:
    init_db()
    repo = PaperTradingRepository()
    repo.clear_all()
    engine = PaperTradingEngine(repo)

    buys = [
        ("AAPL", 190.0, {"target_sell_price": 196.0, "stop_loss_price": 186.0, "trailing_stop_price": 187.0, "entry_zone": {"low": 189.0, "high": 191.0}}),
        ("MSFT", 410.0, {"target_sell_price": 418.0, "stop_loss_price": 404.0, "trailing_stop_price": 405.0, "entry_zone": {"low": 409.0, "high": 411.0}}),
        ("NVDA", 170.0, {"target_sell_price": 176.0, "stop_loss_price": 166.0, "trailing_stop_price": 167.0, "entry_zone": {"low": 169.0, "high": 171.0}}),
    ]

    created = 0
    for sym, price, trade in buys:
        row = engine.place_buy_from_scanner(
            symbol=sym,
            notional=1000.0,
            quote={"last": price, "provider": "mock"},
            trade=trade,
            scanner_meta={"score": 75, "confidence": 0.7},
        )
        if row:
            created += 1

    if created < 3:
        raise AssertionError(f"expected 3 orders, got {created}")

    result = engine.evaluate_positions(
        prices_by_symbol={"AAPL": 197.0, "MSFT": 409.0, "NVDA": 169.0},
        trade_params_by_symbol={
            "AAPL": buys[0][2],
            "MSFT": buys[1][2],
            "NVDA": buys[2][2],
        },
    )

    closed_count = int(result.get("closed_count") or 0)
    if closed_count < 1:
        raise AssertionError("expected at least one closed position when target/stop is hit")

    print("paper_smoke ok")
    print({"created_orders": created, "closed_count": closed_count})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

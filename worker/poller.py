import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from app.providers.alphavantage import AlphaVantageClient
from app.providers.twelvedata import ProviderError, TwelveDataClient
from app.services.basic_signal import compute_basic_signal
from core.storage.db import get_connection

logger = logging.getLogger(__name__)


@dataclass
class ProviderState:
    failures: int = 0
    next_retry_at: float = 0.0
    circuit_open_until: float = 0.0

    def can_attempt(self, now: float) -> bool:
        return now >= self.next_retry_at and now >= self.circuit_open_until

    def record_success(self) -> None:
        self.failures = 0
        self.next_retry_at = 0.0
        self.circuit_open_until = 0.0

    def record_failure(
        self,
        now: float,
        *,
        base_backoff_seconds: int,
        max_backoff_seconds: int,
        circuit_failures: int,
        circuit_seconds: int,
    ) -> None:
        self.failures += 1
        backoff = min(max_backoff_seconds, base_backoff_seconds * (2 ** (self.failures - 1)))
        self.next_retry_at = now + backoff
        if self.failures >= circuit_failures:
            self.circuit_open_until = now + circuit_seconds


class MarketPoller:
    def __init__(
        self,
        *,
        poll_interval_seconds: int = 30,
        bars_interval: str = "1day",
        bars_outputsize: int = 60,
        bars_cache_ttl_seconds: int = 60,
        backoff_base_seconds: int = 2,
        backoff_max_seconds: int = 300,
        circuit_failures: int = 3,
        circuit_seconds: int = 60,
    ) -> None:
        self.poll_interval_seconds = poll_interval_seconds
        self.bars_interval = bars_interval
        self.bars_outputsize = bars_outputsize
        self.bars_cache_ttl_seconds = bars_cache_ttl_seconds

        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self.circuit_failures = circuit_failures
        self.circuit_seconds = circuit_seconds

        self._provider_states: dict[str, ProviderState] = {
            "twelvedata": ProviderState(),
            "alphavantage": ProviderState(),
        }
        self._provider_clients: dict[str, Any] = {}

        self._bars_cache: dict[str, dict[str, Any]] = {}

    def run_forever(self) -> None:
        logger.info("poller_start interval=%ss", self.poll_interval_seconds)
        while True:
            started = time.monotonic()
            try:
                self.run_once()
            except Exception as exc:  # pragma: no cover - defensive safety
                logger.exception("poller_cycle_failed error=%s", exc)

            elapsed = time.monotonic() - started
            sleep_for = max(1.0, self.poll_interval_seconds - elapsed)
            time.sleep(sleep_for)

    def run_once(self) -> None:
        started = time.monotonic()
        symbols = self._load_watchlist_symbols()
        if not symbols:
            logger.info("poller_cycle symbols=0 message=no watchlist symbols")
            return

        ok_count = 0
        fail_count = 0
        for symbol in symbols:
            try:
                self._poll_symbol(symbol)
                ok_count += 1
            except Exception as exc:
                fail_count += 1
                logger.warning("poller_symbol_failed symbol=%s error=%s", symbol, exc)

        duration_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            "poller_cycle_done symbols=%s ok=%s failed=%s duration_ms=%s",
            len(symbols),
            ok_count,
            fail_count,
            duration_ms,
        )

    def _poll_symbol(self, symbol: str) -> None:
        quote, quote_provider = self._fetch_quote(symbol)
        bars, bars_provider = self._fetch_bars(symbol)

        if quote is not None and quote_provider is not None:
            self._persist_quote_event(symbol, quote_provider, quote)

        if bars is not None and bars_provider is not None:
            self._persist_bars(bars)
            self._bars_cache[symbol] = {
                "provider": bars_provider,
                "cached_at": time.monotonic(),
                "bars": bars,
            }

        signal_payload = self._compute_signal_from_cached_bars(symbol)
        if signal_payload is not None:
            self._persist_signal(symbol, signal_payload)

    def _fetch_quote(self, symbol: str):
        now = time.monotonic()
        last_error: Optional[Exception] = None

        for provider_name in ("twelvedata", "alphavantage"):
            state = self._provider_states[provider_name]
            if not state.can_attempt(now):
                continue

            try:
                client = self._get_provider_client(provider_name)
                quote = client.fetch_quote(symbol=symbol)
                state.record_success()
                logger.info("poller_quote_ok symbol=%s provider=%s", symbol, provider_name)
                return quote, provider_name
            except Exception as exc:
                last_error = exc
                self._mark_provider_failure(provider_name, exc)

        if last_error is not None:
            raise last_error
        raise ProviderError("No quote provider available")

    def _fetch_bars(self, symbol: str):
        now = time.monotonic()
        last_error: Optional[Exception] = None

        for provider_name in ("twelvedata", "alphavantage"):
            state = self._provider_states[provider_name]
            if not state.can_attempt(now):
                continue

            try:
                client = self._get_provider_client(provider_name)
                bars = client.fetch_bars(
                    symbol=symbol,
                    interval=self.bars_interval,
                    outputsize=self.bars_outputsize,
                )
                state.record_success()
                logger.info("poller_bars_ok symbol=%s provider=%s count=%s", symbol, provider_name, len(bars))
                return bars, provider_name
            except Exception as exc:
                last_error = exc
                self._mark_provider_failure(provider_name, exc)

        cached = self._bars_cache.get(symbol)
        if cached is not None:
            age = time.monotonic() - float(cached["cached_at"])
            if age <= self.bars_cache_ttl_seconds:
                logger.info("poller_bars_cache_hit symbol=%s age_s=%.1f", symbol, age)
                return cached["bars"], str(cached["provider"])

        if last_error is not None:
            raise last_error
        raise ProviderError("No bars provider available")

    def _compute_signal_from_cached_bars(self, symbol: str) -> Optional[dict[str, Any]]:
        cached = self._bars_cache.get(symbol)
        if cached is None:
            return None

        bars = cached["bars"]
        bars_payload = [bar.model_dump(mode="json") if hasattr(bar, "model_dump") else bar for bar in bars]
        if not bars_payload:
            return None

        return compute_basic_signal(bars_payload)

    def _mark_provider_failure(self, provider_name: str, exc: Exception) -> None:
        now = time.monotonic()
        state = self._provider_states[provider_name]
        state.record_failure(
            now,
            base_backoff_seconds=self.backoff_base_seconds,
            max_backoff_seconds=self.backoff_max_seconds,
            circuit_failures=self.circuit_failures,
            circuit_seconds=self.circuit_seconds,
        )
        logger.warning(
            "poller_provider_fail provider=%s failures=%s next_retry_in_s=%.1f circuit_open_for_s=%.1f error=%s",
            provider_name,
            state.failures,
            max(0.0, state.next_retry_at - now),
            max(0.0, state.circuit_open_until - now),
            exc,
        )

    def _get_provider_client(self, provider_name: str):
        if provider_name in self._provider_clients:
            return self._provider_clients[provider_name]

        if provider_name == "twelvedata":
            client = TwelveDataClient()
        elif provider_name == "alphavantage":
            client = AlphaVantageClient()
        else:
            raise ValueError(f"Unknown provider: {provider_name}")

        self._provider_clients[provider_name] = client
        return client

    def _load_watchlist_symbols(self) -> list[str]:
        queries = [
            "SELECT symbol FROM apollo_watchlist WHERE COALESCE(is_active, 1) = 1 ORDER BY symbol",
            "SELECT symbol FROM apollo_watchlist ORDER BY symbol",
            "SELECT ticker FROM apollo_watchlist ORDER BY ticker",
        ]

        for query in queries:
            try:
                with get_connection() as conn:
                    rows = conn.execute(query).fetchall()
                symbols = []
                seen = set()
                for row in rows:
                    raw = row.get("symbol") if "symbol" in row else row.get("ticker")
                    symbol = (str(raw).strip().upper() if raw else "")
                    if not symbol or symbol in seen:
                        continue
                    seen.add(symbol)
                    symbols.append(symbol)
                return symbols
            except Exception as exc:
                last_error = str(exc).lower()
                if "apollo_watchlist" in last_error or "no such table" in last_error or "does not exist" in last_error:
                    continue
                logger.warning("watchlist_query_failed query=%s error=%s", query, exc)

        logger.warning("apollo_watchlist not found or empty; no symbols to poll")
        return []

    def _persist_quote_event(self, symbol: str, provider: str, quote: Any) -> None:
        payload = {
            "symbol": symbol,
            "provider": provider,
            "quote": quote.model_dump(mode="json") if hasattr(quote, "model_dump") else quote,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO events (event_type, source, payload)
                VALUES (?, ?, ?)
                """,
                ("worker.quote", provider, json.dumps(payload)),
            )

    def _persist_bars(self, bars: list[Any]) -> None:
        with get_connection() as conn:
            for bar in bars:
                instrument_id = getattr(bar, "instrument_id", None)
                ts_event = getattr(bar, "ts_event", None)
                ts_ingest = getattr(bar, "ts_ingest", None)
                quality_flags = getattr(bar, "quality_flags", [])

                if conn.backend == "postgres":
                    conn.execute(
                        """
                        INSERT INTO canonical_price_bars (
                            instrument_id, timeframe, ts_event, ts_ingest,
                            open, high, low, close, volume, source_provider, quality_flags
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (instrument_id, timeframe, ts_event) DO UPDATE SET
                            ts_ingest = EXCLUDED.ts_ingest,
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            source_provider = EXCLUDED.source_provider,
                            quality_flags = EXCLUDED.quality_flags
                        """,
                        (
                            instrument_id,
                            self.bars_interval,
                            ts_event.isoformat() if ts_event else None,
                            ts_ingest.isoformat() if ts_ingest else None,
                            float(getattr(bar, "open", 0)),
                            float(getattr(bar, "high", 0)),
                            float(getattr(bar, "low", 0)),
                            float(getattr(bar, "close", 0)),
                            float(getattr(bar, "volume", 0)),
                            getattr(bar, "source_provider", "unknown"),
                            json.dumps(list(quality_flags or [])),
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO canonical_price_bars (
                            instrument_id, timeframe, ts_event, ts_ingest,
                            open, high, low, close, volume, source_provider, quality_flags
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            instrument_id,
                            self.bars_interval,
                            ts_event.isoformat() if ts_event else None,
                            ts_ingest.isoformat() if ts_ingest else None,
                            float(getattr(bar, "open", 0)),
                            float(getattr(bar, "high", 0)),
                            float(getattr(bar, "low", 0)),
                            float(getattr(bar, "close", 0)),
                            float(getattr(bar, "volume", 0)),
                            getattr(bar, "source_provider", "unknown"),
                            json.dumps(list(quality_flags or [])),
                        ),
                    )

    def _persist_signal(self, symbol: str, signal_payload: dict[str, Any]) -> None:
        score = float(signal_payload.get("score", 0) or 0)
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO signals (symbol, timeframe, score, payload)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, self.bars_interval, score, json.dumps(signal_payload)),
            )


def _configure_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Apollo 67 market data poller")
    parser.add_argument("--once", action="store_true", help="Run a single polling cycle and exit")
    parser.add_argument("--interval", type=int, default=30, help="Polling interval in seconds")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    _configure_logging(args.debug)
    poller = MarketPoller(poll_interval_seconds=args.interval)

    if args.once:
        poller.run_once()
        return
    poller.run_forever()


if __name__ == "__main__":
    main()

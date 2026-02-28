import json
from typing import Iterable, Optional

from app.storage.db import get_connection
from ingestion.models import CorporateAction, Instrument, PriceBar, SessionCalendar


class IngestionRepository:
    def capture_raw_payload(self, dataset: str, provider: str, payload: list[dict]) -> int:
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO raw_payloads (dataset, provider, payload)
                VALUES (?, ?, ?)
                """,
                (dataset, provider, json.dumps(payload)),
            )
            return int(cursor.lastrowid)

    def persist_instruments(self, records: Iterable[Instrument]) -> int:
        count = 0
        with get_connection() as conn:
            for item in records:
                if conn.backend == "postgres":
                    conn.execute(
                        """
                        INSERT INTO canonical_instruments (
                            instrument_id, symbol, venue, asset_type, currency,
                            is_tradable, effective_from, effective_to, source_provider
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (instrument_id) DO UPDATE SET
                            symbol = EXCLUDED.symbol,
                            venue = EXCLUDED.venue,
                            asset_type = EXCLUDED.asset_type,
                            currency = EXCLUDED.currency,
                            is_tradable = EXCLUDED.is_tradable,
                            effective_from = EXCLUDED.effective_from,
                            effective_to = EXCLUDED.effective_to,
                            source_provider = EXCLUDED.source_provider
                        """,
                        (
                            item.instrument_id,
                            item.symbol,
                            item.venue,
                            item.asset_type,
                            item.currency,
                            item.is_tradable,
                            item.effective_from.isoformat(),
                            item.effective_to.isoformat() if item.effective_to else None,
                            item.source_provider,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO canonical_instruments (
                            instrument_id, symbol, venue, asset_type, currency,
                            is_tradable, effective_from, effective_to, source_provider
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item.instrument_id,
                            item.symbol,
                            item.venue,
                            item.asset_type,
                            item.currency,
                            int(item.is_tradable),
                            item.effective_from.isoformat(),
                            item.effective_to.isoformat() if item.effective_to else None,
                            item.source_provider,
                        ),
                    )
                count += 1
        return count

    def persist_price_bars(self, records: Iterable[PriceBar]) -> int:
        count = 0
        with get_connection() as conn:
            for item in records:
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
                            item.instrument_id,
                            item.timeframe,
                            item.ts_event.isoformat(),
                            item.ts_ingest.isoformat(),
                            item.open,
                            item.high,
                            item.low,
                            item.close,
                            item.volume,
                            item.source_provider,
                            json.dumps(item.quality_flags),
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
                            item.instrument_id,
                            item.timeframe,
                            item.ts_event.isoformat(),
                            item.ts_ingest.isoformat(),
                            item.open,
                            item.high,
                            item.low,
                            item.close,
                            item.volume,
                            item.source_provider,
                            json.dumps(item.quality_flags),
                        ),
                    )
                count += 1
        return count

    def persist_corporate_actions(self, records: Iterable[CorporateAction]) -> int:
        count = 0
        with get_connection() as conn:
            for item in records:
                if conn.backend == "postgres":
                    conn.execute(
                        """
                        INSERT INTO canonical_corporate_actions (
                            instrument_id, action_type, effective_date, factor_or_amount, source_provider
                        ) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (instrument_id, action_type, effective_date) DO UPDATE SET
                            factor_or_amount = EXCLUDED.factor_or_amount,
                            source_provider = EXCLUDED.source_provider
                        """,
                        (
                            item.instrument_id,
                            item.action_type,
                            item.effective_date.isoformat(),
                            item.factor_or_amount,
                            item.source_provider,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO canonical_corporate_actions (
                            instrument_id, action_type, effective_date, factor_or_amount, source_provider
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            item.instrument_id,
                            item.action_type,
                            item.effective_date.isoformat(),
                            item.factor_or_amount,
                            item.source_provider,
                        ),
                    )
                count += 1
        return count

    def persist_session_calendar(self, records: Iterable[SessionCalendar]) -> int:
        count = 0
        with get_connection() as conn:
            for item in records:
                if conn.backend == "postgres":
                    conn.execute(
                        """
                        INSERT INTO canonical_session_calendars (
                            venue, session_date, is_open, session_start, session_end, timezone, source_provider
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (venue, session_date) DO UPDATE SET
                            is_open = EXCLUDED.is_open,
                            session_start = EXCLUDED.session_start,
                            session_end = EXCLUDED.session_end,
                            timezone = EXCLUDED.timezone,
                            source_provider = EXCLUDED.source_provider
                        """,
                        (
                            item.venue,
                            item.session_date.isoformat(),
                            item.is_open,
                            item.session_start,
                            item.session_end,
                            item.timezone,
                            item.source_provider,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO canonical_session_calendars (
                            venue, session_date, is_open, session_start, session_end, timezone, source_provider
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item.venue,
                            item.session_date.isoformat(),
                            int(item.is_open),
                            item.session_start,
                            item.session_end,
                            item.timezone,
                            item.source_provider,
                        ),
                    )
                count += 1
        return count

    def mark_curated_dataset(
        self,
        dataset_name: str,
        dataset_version: str,
        status: str = "placeholder",
        payload: Optional[dict] = None,
    ) -> int:
        with get_connection() as conn:
            if conn.backend == "postgres":
                cursor = conn.execute(
                    """
                    INSERT INTO curated_datasets (dataset_name, dataset_version, status, payload)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (dataset_name, dataset_version) DO UPDATE SET
                        status = EXCLUDED.status,
                        payload = EXCLUDED.payload
                    """,
                    (dataset_name, dataset_version, status, json.dumps(payload or {})),
                )
            else:
                cursor = conn.execute(
                    """
                    INSERT OR REPLACE INTO curated_datasets (dataset_name, dataset_version, status, payload)
                    VALUES (?, ?, ?, ?)
                    """,
                    (dataset_name, dataset_version, status, json.dumps(payload or {})),
                )
            return int(cursor.lastrowid)

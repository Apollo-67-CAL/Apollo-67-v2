import os
from datetime import datetime, timezone

import app.providers.twelvedata as twd
from app.contracts.market_data import CanonicalBar, CanonicalQuote
from app.validation.market_data import validate_bars, validate_quote
from ingestion.tests.contract_tests import run as run_contract_tests


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def _run_canonical_model_checks() -> None:
    now = datetime.now(timezone.utc)
    bars = [
        CanonicalBar(
            instrument_id="TWELVEDATA:AAPL",
            ts_event=now,
            ts_ingest=now,
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=1200,
            source_provider="twelvedata",
            quality_flags=[],
        )
    ]
    quote = CanonicalQuote(
        instrument_id="TWELVEDATA:AAPL",
        ts_event=now,
        ts_ingest=now,
        last=100.4,
        bid=100.3,
        ask=100.5,
        source_provider="twelvedata",
        quality_flags=[],
    )
    validate_bars(bars)
    validate_quote(quote, freshness_seconds=60)


def _run_mocked_provider_test() -> None:
    original_http_get = twd._http_get

    def fake_http_get(url: str, params: dict, timeout: int):
        if url.endswith("/symbol_search"):
            return _FakeResponse(
                {
                    "data": [
                        {
                            "symbol": params["symbol"],
                            "instrument_name": "Apple Inc",
                            "exchange": "NASDAQ",
                            "type": "Common Stock",
                            "currency": "USD",
                        }
                    ]
                }
            )
        if url.endswith("/time_series"):
            return _FakeResponse(
                {
                    "values": [
                        {
                            "datetime": "2026-02-28 00:00:00",
                            "open": "100.0",
                            "high": "101.0",
                            "low": "99.0",
                            "close": "100.5",
                            "volume": "1000",
                        }
                    ]
                }
            )
        if url.endswith("/quote"):
            return _FakeResponse(
                {
                    "timestamp": str(int(datetime.now(timezone.utc).timestamp())),
                    "price": "100.4",
                    "bid": "100.3",
                    "ask": "100.5",
                }
            )
        return _FakeResponse({})

    twd._http_get = fake_http_get
    os.environ["TWELVEDATA_API_KEY"] = "dummy-key"
    try:
        client = twd.TwelveDataClient()
        symbols = client.search_symbols("AAPL")
        bars = client.fetch_bars("AAPL", "1day", 10)
        quote = client.fetch_quote("AAPL")

        assert len(symbols) == 1
        assert len(bars) == 1
        assert bars[0].instrument_id == "TWELVEDATA:AAPL"
        assert quote.instrument_id == "TWELVEDATA:AAPL"

        validate_bars(bars)
        validate_quote(quote, freshness_seconds=60)
    finally:
        twd._http_get = original_http_get


def main() -> int:
    try:
        run_contract_tests()
        _run_canonical_model_checks()
        _run_mocked_provider_test()
    except Exception as exc:
        print(f"FAIL: ingestion contract tests - {exc}")
        return 1
    print("PASS: ingestion contract tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

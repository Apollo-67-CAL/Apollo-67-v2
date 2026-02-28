# Apollo 67 Data Provider: Twelve Data

This document defines the initial real provider integration for Apollo 67 Phase 1.

## Overview

Provider module: `app/providers/twelvedata.py`

Implemented operations:

- `search_symbols(query)`
- `fetch_bars(symbol, interval, outputsize)`
- `fetch_quote(symbol)`

All outputs are converted to canonical contracts and validated using blocking checks before they are returned by API routes.

## Environment Variables

Required:

- `TWELVEDATA_API_KEY`: Twelve Data API key.

Related runtime configuration:

- `DATA_FRESHNESS_SLA_SECONDS` (default: `300`)

## API Endpoints

- `GET /provider/twelvedata/search?q=AAPL`
- `GET /provider/twelvedata/bars?symbol=AAPL&interval=1day&outputsize=500`
- `GET /provider/twelvedata/quote?symbol=AAPL`

Fail-closed behaviour:

- On provider failure or validation failure, endpoint returns `503`.
- Invalid data is not persisted.

## Canonical Contracts

Models in `app/contracts/market_data.py`:

- `InstrumentMasterRecord`
- `CanonicalBar`
- `CanonicalQuote`

Validation in `app/validation/market_data.py`:

- UTC timestamps
- non-negative values
- OHLC consistency checks
- uniqueness checks within response
- quote freshness checks

## Rate Limits Note

Twelve Data enforces rate limits by plan. For production, configure call cadence and caching to remain within your account limits. If rate limits are hit, the integration returns `503` as part of fail-closed behaviour.

## Example curl Commands

```bash
curl -s "http://127.0.0.1:8000/provider/twelvedata/search?q=AAPL" | python3 -m json.tool
curl -s "http://127.0.0.1:8000/provider/twelvedata/bars?symbol=AAPL&interval=1day&outputsize=30" | python3 -m json.tool
curl -s "http://127.0.0.1:8000/provider/twelvedata/quote?symbol=AAPL" | python3 -m json.tool
```

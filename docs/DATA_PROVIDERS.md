# Apollo 67 Phase 1: Data Providers

This document defines the data-provider architecture and controls for Apollo 67 Phase 1 (long-only).
The objective is to ensure market data is trustworthy, timely, and reproducible across research, simulation, and live operation.

## 1) Phase 1 Data Scope

In scope:

- Long-only tradable universe reference data.
- Historical OHLCV bars for signal research and backtesting.
- Live market data for execution-time decision support.
- Corporate action support sufficient for price/volume continuity (splits and key adjustments).
- Trading calendar/session state required for market-open controls.

Out of scope:

- Options/derivatives chains.
- Full-depth order book analytics.
- Alternative data sources (news, sentiment, satellite, etc.).

## 2) Data Design Principles

- Authoritative source first: each dataset has a designated primary provider.
- Controlled fallback: secondary provider is used only via explicit failover rules.
- Canonical schema: all provider payloads are normalised before strategy use.
- Deterministic snapshots: research and replay must use versioned, immutable extracts.
- Fail closed: if required data quality checks fail, block new risk.

## 3) Provider Hierarchy and Responsibilities

For each required dataset, define these fields before implementation:

- Dataset name
- Primary provider
- Secondary provider (fallback)
- Refresh cadence
- Latency tolerance
- Retention period
- Owner

Minimum required datasets:

1. Instrument master (symbols, identifiers, listing venue, status)
2. Historical bars (open, high, low, close, volume, timestamp)
3. Live quote/last-trade stream
4. Corporate actions (at least splits; dividends if required by PnL model)
5. Trading calendar/session schedule

Failover rules:

- Primary healthy: use primary only.
- Primary degraded and fallback healthy: route to fallback and mark source provenance.
- Both degraded: halt new entries and escalate incident.

## 4) Canonical Data Contracts

All strategy and risk components consume canonical records only.

### 4.1 Instrument Master Contract

Required fields:

- `instrument_id` (internal stable key)
- `symbol`
- `venue`
- `asset_type`
- `currency`
- `is_tradable`
- `effective_from`
- `effective_to`

### 4.2 Historical/Live Price Contract

Required fields:

- `instrument_id`
- `ts_event` (provider event timestamp, UTC)
- `ts_ingest` (ingestion timestamp, UTC)
- `open`
- `high`
- `low`
- `close`
- `volume`
- `source_provider`
- `quality_flags`

Rules:

- Timestamps stored in UTC.
- No duplicate (`instrument_id`, `ts_event`, interval) records.
- `high >= max(open, close, low)` and `low <= min(open, close, high)`.
- Negative price/volume values are invalid.

### 4.3 Corporate Actions Contract

Required fields:

- `instrument_id`
- `action_type`
- `effective_date`
- `factor_or_amount`
- `source_provider`

Rules:

- Backtest/simulation must explicitly document whether adjusted or unadjusted bars are used.
- Adjustment policy must be consistent across training and evaluation periods.

## 5) Data Quality Governance

Data quality checks run at ingestion and pre-strategy consumption.

### 5.1 Core Checks (Blocking)

- Freshness: data age within configured SLA.
- Completeness: expected records arrived for active universe/session.
- Schema validity: required fields and types present.
- Domain validity: no impossible values (negative prices, inverted OHLC logic).
- Uniqueness: key collisions prevented.

Failure behaviour:

- Any blocking check failure -> reject dataset slice, do not trade on it.

### 5.2 Warning Checks (Non-Blocking, Alerting)

- Abnormal spread/volume spikes outside historical bounds.
- Unexpected symbol churn or tradability status changes.
- Provider-to-provider drift above configured tolerance.

## 6) Session and Calendar Policy

- Use one canonical market calendar per venue.
- Only allow new entries during configured trading windows.
- Pre-open, auction, and holiday handling must be explicit in config.
- If session status is unknown, block new entries (fail closed).

## 7) Storage, Lineage, and Retention

- Raw zone: provider-native payloads for audit.
- Normalised zone: canonical schema used by strategy/risk.
- Curated zone: feature-ready datasets with version tags.

Lineage requirements:

- Every record includes provider source and ingestion timestamp.
- Dataset versions map to code revision and parameter set used in runs.

Retention minimums for Phase 1:

- Raw payloads: retain for incident forensics window.
- Canonical/curated research datasets: retain for reproducibility of published results.

## 8) Monitoring and Alerts

Minimum observability:

- Ingestion success/failure rate by provider
- End-to-end data latency
- Freshness SLA breaches
- Missing-bar count by symbol/session
- Failover events and duration

Alert severities:

- `SEV-1`: both primary and fallback unavailable for required live dataset.
- `SEV-2`: primary unavailable, operating on fallback.
- `SEV-3`: non-blocking anomalies requiring review.

## 9) Testing and Validation Requirements

Before passing Data Providers stage gate, complete:

1. Contract tests for each canonical schema.
2. Backfill replay test for a representative historical window.
3. Live simulation test with forced provider failover.
4. Data-quality failure injection (stale, missing, invalid values).
5. Session/calendar boundary tests (open/close/holiday).

Acceptance criteria:

- Blocking checks reliably prevent invalid data use.
- Fallback routing works and is fully logged.
- Canonical outputs are deterministic for identical inputs.

## 10) Operational Runbook (Minimum)

Runbook must include:

- How to identify provider degradation quickly.
- How to switch to fallback safely.
- When to halt new entries.
- Who approves manual override and how long it is valid.
- How to recover to primary and verify data parity.

## 11) Ownership and Change Control

- Data platform owner approves provider and schema changes.
- Risk owner approves changes affecting live trading eligibility.
- Any contract-breaking change requires version bump and migration plan.

## 12) Stage Gate Definition (Data Providers)

The Data Providers stage is complete only when:

- Provider hierarchy is documented for all required datasets.
- Canonical schemas and validation rules are implemented and tested.
- Monitoring/alerts are active with on-call ownership.
- Failover and failure-injection tests pass.
- Upstream/downstream dependencies are signed off by strategy and risk owners.

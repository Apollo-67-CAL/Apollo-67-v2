# Apollo 67 Technical Design (Phase 1)

This document maps implementation structure to the Apollo 67 architecture and governance model.

## 1) Module Structure

Top-level structure:

- `api/`
  - FastAPI entrypoint and HTTP endpoints.
- `core/`
  - shared persistence and domain repositories.
- `ingestion/`
  - planned data-provider integrations and canonicalisation.
- `scoring/`
  - planned EIS/signal computation modules.
- `risk/`
  - planned hard-gate engines and tiered controls.
- `execution/`
  - planned order orchestration and lifecycle handling.
- `governance/`
  - planned cost/risk policy orchestration and reporting.
- `app/`
  - compatibility shim namespace to preserve legacy imports and Render entrypoint assumptions.

### Compatibility policy

- `app.main` re-exports `api.main.app`.
- `app.storage` and `app.repositories` re-export from `core.*`.

This allows refactoring without breaking external start commands or imports.

## 2) Database Schema Design

DB implementation is in `core/storage/db.py` with backend selection via `DATABASE_URL`.

### 2.1 Backends

- SQLite for local development (`sqlite:///...`).
- Postgres for non-local environments (`postgres://...` or `postgresql://...`).

### 2.2 Schema objects

- `schema_migrations`
  - applied schema versions.
- `events`
  - system/event timeline for observability and audit.
- `signals`
  - scored opportunities and signal metadata.
- `decisions`
  - decision outputs with rationale and optional signal FK.
- `portfolio_snapshots`
  - point-in-time equity/cash/exposure/heat records.
- `models`
  - model versions, status, metrics, and training metadata.

### 2.3 Design properties

- primary keys on all tables,
- index coverage on core query paths (created_at, symbol, as_of, signal_id),
- decision-to-signal FK integrity,
- Postgres JSONB support for payload/metrics fields,
- idempotent schema creation for startup safety.

## 3) Repository Interfaces

Repository API surface is intentionally minimal and stable:

- `create(...) -> int`
- `list_recent(limit=...) -> List[Dict[str, Any]]`

Repositories:

- `EventsRepository`
- `SignalsRepository`
- `DecisionsRepository`
- `PortfolioSnapshotsRepository`
- `ModelsRepository`

Design choice:

- keep persistence API narrow now,
- preserve compatibility across SQLite and Postgres,
- extend behaviour in subsequent phases without endpoint breakage.

## 4) Interfaces Between Ingestion, Scoring, and Risk

Planned interface contracts:

### 4.1 Ingestion -> Scoring

Input contract:

- canonical instrument and bar records,
- freshness and quality flags,
- provider/source metadata.

Output expectation:

- scoring-ready dataset with deterministic timestamps and IDs.

### 4.2 Scoring -> Risk

Input contract:

- scored candidate signal (`symbol`, score, payload),
- context metadata (timeframe, market state).

Risk gate actions:

- enforce long-only invariant,
- enforce heat/sleeve/concentration caps,
- enforce drawdown tier restrictions,
- enforce dilution and expectancy thresholds.

### 4.3 Risk -> Execution

Output contract:

- approved decision with explicit reason code,
- rejected decision with blocking reason.

Every decision (approve/reject) is persisted for auditability.

## 5) Config + Parameter Control Model

### 5.1 Control sources

- environment variables for runtime infra concerns (DB, environment mode),
- versioned parameter baseline for trading/risk/cost thresholds,
- document-governed phase gates and sign-off workflow.

### 5.2 Parameter governance

- conservative defaults,
- owner-based approval workflow,
- blocking vs warning semantics,
- explicit rollback criteria for parameter changes.

### 5.3 Operational safety defaults

- fail closed on critical dependency uncertainty,
- `/healthz` reports degraded state with HTTP 503 when DB is unavailable,
- startup initialisation failures logged without preventing process bind.

## 6) Deployment and Render Compatibility

Current design keeps deployment stable while structure evolves:

- `runtime.txt` and `requirements.txt` remain intact,
- existing route surface retained (`/`, `/healthz`, debug endpoint),
- compatibility shim ensures `app.main:app` remains valid,
- `api.main:app` is available as canonical entrypoint moving forward.

## 7) Incremental Build Path (Implementation Alignment)

Near-term implementation sequence:

1. Complete ingestion connectors + canonical contracts.
2. Implement scoring pipeline with EIS thresholds.
3. Implement risk gate engine and drawdown/heat controls.
4. Add execution workflow with decision logging integration.
5. Add governance automation for cost/validation reporting.

This preserves white-paper ordering while maintaining a deployable, testable platform at each step.

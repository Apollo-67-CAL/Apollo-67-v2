# Apollo 67 Architecture (Phase 1 Long-Only)

This document synthesises the Apollo 67 white paper (`/docs`) into a single architecture view for Phase 1.

## 1) System Overview

Apollo 67 Phase 1 is a deterministic long-only trading platform built around strict governance gates:

1. Parameter Baseline
2. Data Providers
3. Risk Governance
4. Cost Governance
5. Deployment Validation

Primary objective:

- run a production-safe long-only system,
- enforce hard pre-trade/runtime controls,
- preserve auditability and reproducibility,
- validate progressively from paper to live (`$10k -> $15k -> $20k`).

## 2) Core System Components

### 2.1 API Layer (`api/`)

- FastAPI application, health and control endpoints.
- Startup initialises persistence schema.
- `/healthz` exposes application + DB readiness.

### 2.2 Core Domain Layer (`core/`)

- `core/storage/db.py`
  - unified DB adapter (SQLite local, Postgres non-local),
  - schema initialisation,
  - connectivity checks,
  - transaction/context handling.
- `core/repositories/*`
  - persistence interfaces for:
    - `events`
    - `signals`
    - `decisions`
    - `portfolio_snapshots`
    - `models`

### 2.3 Planned Functional Layers

- `ingestion/`: provider connectors, canonical mapping, data quality gates.
- `scoring/`: EIS and signal construction.
- `risk/`: hard constraints (long-only, heat, drawdown, dilution).
- `execution/`: order lifecycle and decision-to-action plumbing.
- `governance/`: cost, policy enforcement, incident escalation, validation workflows.

## 3) Data Flow (Phase 1)

1. Ingestion collects provider data and maps it to canonical records.
2. Scoring computes candidate signals with integrity checks.
3. Risk gate validates all hard constraints before any executable decision.
4. Execution (future step) submits only risk-approved actions.
5. Governance records audit events, costs, and stage-validation metrics.
6. Persistence layer stores signals, decisions, events, model metadata, and portfolio snapshots.

### Persistence-first audit trail

The current foundation supports deterministic forensics by storing:

- event timeline (`events`),
- scoring outputs (`signals`),
- decision rationale (`decisions`),
- risk/capital state (`portfolio_snapshots`),
- model lineage (`models`).

## 4) Risk Layer Design

Risk architecture is fail-closed and non-bypassable:

- absolute rules:
  - no averaging down,
  - no control bypass,
  - no net-short state,
- portfolio controls:
  - heat caps,
  - sleeve caps,
  - concentration caps,
- drawdown tier actions:
  - normal -> caution -> defensive -> protect -> halt,
- emergency controls:
  - kill-switch,
  - incident escalation,
  - controlled restart criteria.

All breaches and overrides require explicit, auditable records.

## 5) Cost Governance Layer

Cost is a first-class control surface, not a reporting afterthought.

Key mechanisms:

- CPAS (`Cost per Actionable Signal`) as primary efficiency KPI,
- adaptive sampling frequency strategy,
- budget alert tiers (`INFO/WARN/CRITICAL/HARD STOP`),
- cache-first policy with TTL and invalidation safeguards,
- provider tier progression aligned to validation stage.

Cost controls are constrained by safety: no savings action may weaken risk checks.

## 6) Deployment Model

### Runtime

- local development:
  - default SQLite (`sqlite:///./apollo67.db`) if `DATABASE_URL` absent,
- non-local environments:
  - explicit Postgres URL required (`postgresql://...` or `postgres://...`),
  - Render should use internal Postgres URL for service-to-service connectivity.

### Compatibility model

- primary API app now lives in `api/main.py`.
- compatibility shim in `app/main.py` preserves existing deployment entrypoints (`app.main:app`) so Render start commands do not break.

## 7) Validation Stages

Validation is mandatory and stage-gated:

1. Paper validation
2. Live `$10k`
3. Live `$15k`
4. Live `$20k`

Hard gates across all stages:

- drawdown must remain within policy threshold,
- zero rule violations,
- controls/auditability intact,
- sample minimums and weekly reporting complete.

Escalation and downgrade rules apply before hard-fail is reached.

## 8) Agent Orchestration Design

Agent operations are policy-bound and auditable.

### 8.1 Responsibilities by layer

- `ingestion` agents: provider health, schema normalisation, quality checks.
- `scoring` agents: deterministic signal computation and metadata.
- `risk` agents: hard-gate evaluation and breach handling.
- `execution` agents: controlled order actions only after risk approval.
- `governance` agents: cost, overrides, reporting, escalation workflows.

### 8.2 Coordination model

- publish decisions/events through persistence repositories,
- consume only canonical records,
- enforce phase gates before capital progression,
- block on missing critical dependencies (data freshness, risk engine integrity).

### 8.3 Non-negotiable agent constraints

- no bypass of risk controls,
- no silent override,
- no unaudited action paths,
- no UI regressions introduced by backend changes.

## 9) Architectural Rationale

This architecture prioritises:

- safety before throughput,
- deterministic behaviour over opaque automation,
- incremental rollout with measurable gates,
- operational resilience through compatibility-preserving refactors.

It is intentionally modular so later phases can add execution sophistication without weakening governance foundations.

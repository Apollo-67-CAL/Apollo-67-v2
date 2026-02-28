# Apollo 67 Phase 1: Cost Governance

This document defines mandatory cost governance for Apollo 67 Phase 1 (long-only).
Goal: maintain predictable operating cost while preserving decision quality and risk control integrity.

## 1) Scope and Principles

- Cost control must not weaken risk controls or long-only invariants.
- Every cost decision must be measurable and auditable.
- Prefer deterministic, cached, and right-sized data access patterns.
- Scale provider spend only after stage-gated validation success.

## 2) Core Metric: Cost per Actionable Signal

### 2.1 Definition

`Cost per Actionable Signal (CPAS)` is the primary efficiency metric.

Formula:

`CPAS = Total Data + Compute + Execution-Infra Cost / Count(Actionable Signals)`

Where:

- Actionable signal = signal that passes all entry filters and could legally submit an order.
- Count is measured per stage window (weekly and cumulative).

### 2.2 Baseline Targets

- Paper stage target: `CPAS <= $6.00`
- Live `$10k` stage target: `CPAS <= $8.00`
- Live `$15k` stage target: `CPAS <= $7.00`
- Live `$20k` stage target: `CPAS <= $6.00`

Interpretation:

- Temporary spikes may occur; governance is based on rolling windows and alert tiers.

### 2.3 Required Reporting Dimensions

CPAS must be reported by:

- stage (`paper`, `$10k`, `$15k`, `$20k`),
- sleeve,
- provider,
- environment (`sim`, `live`).

## 3) Sampling Frequency Strategy

Sampling frequency must be dynamic and regime-aware to control spend.

### 3.1 Baseline Frequencies

- Universe refresh: daily pre-session.
- Core signal bars: 5-minute and daily bars.
- Risk/position monitoring: 1-minute checks during active session.
- Slow-moving reference datasets: daily or on-change polling.

### 3.2 Adaptive Sampling Rules

- Normal regime: baseline frequencies.
- Elevated volatility: tighten only risk-critical feeds first; avoid blanket frequency increases.
- Low-activity regime: downshift non-critical polling to reduce cost.

### 3.3 Guardrails

- No frequency increase without measurable decision-value justification.
- If CPAS breaches warning thresholds, reduce non-critical sampling before changing provider tier.
- Risk-critical checks are never reduced below minimum safe cadence.

## 4) Budget and Alert Framework

Budgets are managed at monthly and weekly levels.

### 4.1 Budget Buckets

- Data provider spend
- Compute/training spend
- Execution infrastructure spend
- Observability/storage overhead

### 4.2 Alert Thresholds

- `INFO`: 70% of budget consumed (monitoring notice).
- `WARN`: 85% of budget consumed (cost review required).
- `CRITICAL`: 95% of budget consumed (freeze discretionary spend and enforce reductions).
- `HARD STOP`: 100% of budget consumed (no non-essential cost-incurring operations).

### 4.3 Mandatory Actions by Alert

- `WARN`:
  - review CPAS by sleeve/provider,
  - cut non-critical sampling,
  - defer non-essential retraining runs.
- `CRITICAL`:
  - block new discretionary experiments,
  - enforce fallback cache-first mode for non-critical requests,
  - require owner sign-off for incremental spend.
- `HARD STOP`:
  - allow only risk/safety-critical operations,
  - escalate to risk and operations owners immediately.

## 5) Caching Policy

Caching is mandatory to reduce repeated provider calls and improve determinism.

### 5.1 Cache Layers

- L1 in-memory cache for hot intraday reads.
- L2 persistent local cache for session-level reuse.
- L3 historical archive cache for backtest/replay.

### 5.2 TTL and Invalidation

- Live quote cache TTL: `5-15 seconds` (strategy-dependent).
- Intraday bar cache TTL: `60 seconds`.
- Daily/reference cache TTL: `24 hours` or event-based invalidation.
- Invalidate immediately on corporate action/symbol-status updates.

### 5.3 Cache Safety Rules

- Cache entries must store source, timestamp, and version hash.
- Stale cache beyond TTL cannot be used for new entries.
- If cache invalidation fails, system fails closed for affected instruments.

### 5.4 Cache Hit Targets

- Intraday read hit rate target: `>= 80%`
- Historical/research read hit rate target: `>= 90%`

## 6) Provider Tiering by Phase

Provider tiering controls spend progression by validation phase.

### 6.1 Tier Definitions

- `Tier 0 (Paper/Core)`:
  - cost-efficient provider mix,
  - delayed/non-premium feeds acceptable where risk-safe.
- `Tier 1 (Live $10k)`:
  - upgrade to stronger live reliability for execution-critical data.
- `Tier 2 (Live $15k)`:
  - improve redundancy and latency where justified by observed value.
- `Tier 3 (Live $20k)`:
  - full Phase 1 production-grade provider profile for validated scale envelope.

### 6.2 Promotion Criteria

Tier promotion requires:

- prior stage pass,
- CPAS and budget metrics within threshold,
- no unresolved provider-related incidents,
- owner sign-off (risk + operations).

### 6.3 Demotion Criteria

Demote provider tier if:

- budget reaches `CRITICAL` without mitigation,
- provider reliability underperforms SLA repeatedly,
- incremental tier cost does not improve actionable signal quality.

## 7) ML and Compute Cost Controls

- Respect training caps defined in parameter baseline.
- Retraining is gated by measurable performance drift, not schedule alone.
- Expensive backfills/retrains must include expected CPAS impact estimate.
- Shadow-evaluate before promoting any cost-increasing model path.

## 8) Weekly Cost Reporting Requirements

Weekly report must include:

- total spend vs budget by bucket,
- CPAS (weekly and rolling 4-week),
- top 5 cost drivers,
- cache hit/miss rates and stale-read incidents,
- provider tier status and any transitions,
- sampling frequency changes and justification,
- open cost incidents and remediation ETA.

Report SLA:

- publish within 24 hours after week close.

## 9) Escalation and Governance

Escalate to risk + operations owners if:

- CPAS breaches target for 2 consecutive weeks,
- any budget bucket reaches `CRITICAL`,
- cache safety failure impacts tradability decisions,
- provider costs drift beyond approved plan without signed change request.

Required governance artefacts:

- approved budget plan,
- change log for tier/frequency/cache policy updates,
- incident records for all critical cost-control failures.

## 10) Stage Gate (Cost Governance)

Cost governance is complete for Phase 1 stage progression only when:

- CPAS is measured and reported by required dimensions.
- Sampling strategy is implemented with adaptive guardrails.
- Budget alerts and mandatory actions are operational.
- Caching policy is enforced with audit evidence.
- Provider tiering rules are active and stage-aligned.

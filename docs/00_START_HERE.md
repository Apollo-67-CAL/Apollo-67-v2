# Apollo 67 Phase 1: Start Here

This is the entry point for building and validating Apollo 67 Phase 1.
Phase 1 is a long-only system with strict governance and controlled capital rollout.

## 1) Objective

Deliver a production-ready long-only trading system that is:

- deterministic (repeatable decisions from versioned inputs),
- risk-enforced (hard pre-trade and runtime limits),
- cost-aware (realistic net performance accounting),
- operationally safe (validated deployment, rollback, and kill-switch paths).

## 2) Phase 1 Scope

In scope:

- Long-only execution flow (buy to open/increase, sell to reduce/close).
- Parameter baseline and change control.
- Data provider contracts and quality gates.
- Risk governance and breach handling.
- Cost governance and net-of-cost reporting.
- Deployment validation before any live capital.

Out of scope:

- Shorting or synthetic short exposure.
- Derivatives overlays and multi-strategy allocation.
- Autonomous parameter self-tuning.

## 3) Non-Negotiable Constraints

- Long-only invariant must hold at all times; no net-short portfolio state.
- Fail closed on uncertainty (stale/missing data, limit breaches, validation failures).
- Every live order must pass pre-trade risk checks.
- All critical actions must be auditable with timestamps and version metadata.
- No UI regressions: Phase 1 changes must not degrade existing UI behaviour, layout, data display, or interaction quality.
- No stage skipping: mandatory sequence and stage gates must be respected.

## 4) Mandatory Build Order

Build in this order only:

1. Parameter Baseline  
   `docs/PARAMETERS_BASELINE.md`
2. Data Providers  
   `docs/DATA_PROVIDERS.md`
3. Risk Governance  
   `docs/RISK_GOVERNANCE.md`
4. Cost Governance  
   `docs/COST_GOVERNANCE.md`
5. Deployment Validation  
   `docs/DEPLOYMENT_VALIDATION.md`

If any downstream choice conflicts with an upstream decision, stop and resolve before proceeding.

## 5) Validation Path: Paper Then Live

Capital progression is gated. Do not advance unless the prior stage passes.

### Stage A: Paper Validation (Mandatory First)

- Run in paper/simulated mode with full production-equivalent controls.
- Pass criteria:
  - No long-only violations.
  - No unresolved critical risk breaches.
  - Stable data quality and execution pipeline.
  - Net-of-cost behaviour within expected bounds.

### Stage B: Live Validation 1 ($10,000)

- Enable live trading with a $10k cap.
- Focus: execution correctness, risk control enforcement, operational runbooks.
- Advance only if metrics and incident thresholds remain within limits for the defined observation window.

### Stage C: Live Validation 2 ($15,000)

- Increase live cap to $15k after Stage B sign-off.
- Focus: consistency under slightly larger exposure and normal market variance.
- Advance only if all risk, cost, and ops checks remain green.

### Stage D: Live Validation 3 ($20,000)

- Increase live cap to $20k after Stage C sign-off.
- Focus: confirming repeatability and governance discipline before broader scaling decisions.
- Completion of this stage finalises Phase 1 validation readiness.

## 6) Stage Gates (Quick Reference)

- Gate 1: Parameters locked and approved.
- Gate 2: Data providers validated (quality + failover).
- Gate 3: Risk controls proven non-bypassable.
- Gate 4: Costs modelled and reported net-of-cost.
- Gate 5: Deployment validation and failure drills passed.
- Gate 6: Paper validation passed.
- Gate 7: Live validation passed at $10k -> $15k -> $20k.

## 7) Read Next

Read and execute these docs in order:

1. `docs/PARAMETERS_BASELINE.md`
2. `docs/DATA_PROVIDERS.md`
3. `docs/RISK_GOVERNANCE.md`
4. `docs/COST_GOVERNANCE.md`
5. `docs/DEPLOYMENT_VALIDATION.md`

Then return to this file to confirm stage-gate completion and validation progression.

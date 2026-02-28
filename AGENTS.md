# Apollo 67 Agent Governance

This file defines mandatory operating rules for humans and AI agents working on Apollo 67 Phase 1.
Phase 1 is long-only and safety-first. These constraints are not optional.

## 1) System Constraints (Hard Requirements)

### 1.1 Strategy Scope

- The system must remain long-only in Phase 1.
- No short-selling, synthetic short exposure, or net-short portfolio state is allowed.
- Orders are limited to buy-to-open/increase and sell-to-reduce/close actions.

### 1.2 Capital and Exposure

- Use capped risk budgets at instrument and portfolio level.
- Enforce maximum position size, maximum single-name concentration, and maximum gross exposure.
- New entries must be blocked when portfolio-level limits are reached.

### 1.3 Determinism and Reproducibility

- Trading decisions must be reproducible from versioned inputs (data snapshot + parameters + code revision).
- Runtime behaviour must be driven by configuration, not ad-hoc code changes.
- Every deployment must record version metadata for traceability.

### 1.4 Fail-Safe Behaviour

- On uncertainty (stale/missing data, breached limits, failed validations), fail closed and block new risk.
- Kill-switch controls must be available and tested.
- Critical failures must generate alerts and audit records.

### 1.5 Change Management

- Parameter and risk-limit changes require documented rationale and review approval.
- Emergency overrides must be logged with owner, timestamp, reason, and expiry.
- Do not bypass stage gates to accelerate delivery.

## 2) Risk Governance (Mandatory Controls)

### 2.1 Pre-Trade Controls

- Validate market/session state and instrument tradability.
- Validate long-only invariant before order release.
- Check per-order notional, position cap, concentration cap, and portfolio exposure limits.
- Reject any order that violates hard limits.

### 2.2 In-Trade/Runtime Controls

- Monitor realised/unrealised drawdown against defined thresholds.
- Apply volatility throttles when market conditions exceed configured bounds.
- Pause new entries when risk state is degraded.
- Trigger kill-switch when critical breach conditions are met.

### 2.3 Post-Trade Controls

- Reconcile fills, positions, and cash after execution windows.
- Emit breach and exception logs with sufficient forensic detail.
- Track gross vs net performance with explicit cost attribution.

### 2.4 Risk Ownership and Escalation

- Define named owners for risk policy, execution, and incident response.
- Breach severity tiers must map to explicit actions and response times.
- No unresolved critical risk issue may remain open during production trading.

## 3) Build Order (Phase 1 Long-Only)

Build strictly in this sequence:

1. Parameter Baseline (`docs/PARAMETERS_BASELINE.md`)
2. Data Providers (`docs/DATA_PROVIDERS.md`)
3. Risk Governance (`docs/RISK_GOVERNANCE.md`)
4. Cost Governance (`docs/COST_GOVERNANCE.md`)
5. Deployment Validation (`docs/DEPLOYMENT_VALIDATION.md`)
6. Entry Guide (`docs/00_START_HERE.md`) kept aligned with all above

If a downstream change conflicts with an upstream decision, stop and resolve before proceeding.

## 4) Stage Gates (Definition of Progress)

### Gate 1: Parameters Locked

- Baseline parameters versioned, bounded, and review-approved.

### Gate 2: Data Trusted

- Provider contracts, schema mapping, and quality checks validated for historical and live flows.

### Gate 3: Risk Enforced

- All pre-trade/runtime risk controls implemented and verified to block invalid actions.

### Gate 4: Costs Realistic

- Fee/spread/slippage assumptions integrated; reporting includes net-of-cost outcomes.

### Gate 5: Deployment Ready

- Staging dress rehearsal and failure drills passed; rollback path verified.

No production capital deployment before Gate 5 sign-off.

## 5) Engineering Rules for Agents

- Prefer minimal, auditable changes.
- Do not modify app runtime behaviour without updating relevant docs and validations.
- Keep tests and documentation in sync with risk/control changes.
- Never introduce shortcuts that weaken long-only guarantees or risk checks.

## 6) File Modification Policy

- Agents may edit documentation and configuration in scope of approved work.
- Do not modify existing app files unless explicitly requested by the user.
- If app-file changes are necessary to satisfy a request, obtain explicit confirmation first.

## 7) Completion Criteria (Phase 1)

Phase 1 is complete only when:

- All build-order stages are finished in sequence.
- Long-only invariant is enforced in logic and validated by tests.
- Risk and cost governance are active in simulation and live workflows.
- Deployment validation and incident runbooks are approved by designated owners.

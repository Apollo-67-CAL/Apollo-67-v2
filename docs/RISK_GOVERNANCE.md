# Apollo 67 Phase 1: Risk Governance

This document defines mandatory risk governance for Apollo 67 Phase 1 (long-only).
All rules are enforceable controls, not guidelines.

## 1) Purpose and Scope

- Preserve capital while validating Phase 1 system reliability.
- Enforce non-bypassable long-only risk controls.
- Define exactly what actions are required under stress.

Scope:

- Paper and live validation environments.
- All sleeves and instruments in Phase 1 universe.
- Pre-trade, in-trade, and post-trade controls.

## 2) Absolute Rules (Zero Exceptions Unless Explicit Emergency Override)

### 2.1 No Averaging Down

- Never increase position size in a name that is below its average entry price.
- Allowed actions when underwater:
  - hold within risk limits,
  - reduce,
  - fully exit.
- Any order that would average down must be blocked automatically.

### 2.2 No Bypass of Risk Controls

- No trade may bypass pre-trade checks.
- No manual route may skip hard limits, kill-switch checks, or audit logging.
- If risk engine health is unknown/degraded, system must fail closed (no new risk).

### 2.3 Long-Only Invariant

- No short-selling, no synthetic net-short exposure, no negative net position.
- Orders that would create net-short state must be rejected.

## 3) Drawdown Tiers and Required Actions

Drawdown measured from rolling high-water mark of portfolio equity.

### Tier 0: Normal (`DD < 4%`)

- Standard operation.
- Full baseline position sizing permitted.

### Tier 1: Caution (`4% <= DD < 7%`)

Required actions:

- Reduce new position sizing by 25%.
- Raise monitoring cadence for heat and slippage.
- Risk owner review required within same trading day.

### Tier 2: Defensive (`7% <= DD < 10%`)

Required actions:

- Reduce new position sizing by 50%.
- Tighten entry quality filter (EIS floor uplift).
- Disallow discretionary adds to existing positions.
- Daily risk review required until tier improves.

### Tier 3: Protect (`10% <= DD < 12%`)

Required actions:

- Freeze all new entries by default.
- Permit only risk-reducing orders (trim/exit) unless formally approved override.
- Incident ticket opened with named owner and recovery plan.

### Tier 4: Halt (`DD >= 12%`)

Required actions:

- Trigger emergency shutdown for new risk immediately.
- Close or reduce positions only per emergency playbook.
- Conduct incident review before any restart decision.

Recovery rule:

- Tier downgrade allowed only after recovery threshold is met and risk owner signs off.

## 4) Heat and Sleeve Constraints

Heat and sleeve limits are hard caps for new risk.

### 4.1 Portfolio Heat Caps

- Soft cap: 18% of risk budget (warning, heightened monitoring).
- Hard cap: 22% of risk budget (block new entries).

### 4.2 Concentration Heat Caps

- Single-name heat cap: 4.5% of risk budget (hard block).
- Sector heat cap: 28% of risk budget (hard block).
- Correlated-cluster heat cap: 35% of risk budget (hard block).

### 4.3 Sleeve Caps

- Core trend sleeve: max 40% of live capital.
- Momentum sleeve: max 30%.
- Reversion sleeve: max 20%.
- Event sleeve: max 10%.

Rules:

- Total sleeve allocation must not exceed 100%.
- Breach of any sleeve cap blocks additional entries in that sleeve.

## 5) Dilution Risk Rules

Dilution risk = degradation in edge quality due to crowding, slippage expansion, and execution decay.

### 5.1 Trigger Conditions

- Edge decay warning: expectancy drops >= 20% versus trailing baseline.
- Edge decay block: expectancy drops >= 35%.
- Slippage warning: realised/modelled slippage >= 1.5x.
- Slippage block: realised/modelled slippage >= 2.0x.
- Fill-quality floor: below 85% acceptable fill quality is blocking.

### 5.2 Required Actions

- Warning state:
  - reduce order aggressiveness,
  - tighten entry threshold,
  - increase monitoring cadence.
- Blocking state:
  - stop new entries for affected sleeve/instrument cohort,
  - run dilution incident review,
  - resume only after metrics normalise and risk owner approves.

## 6) Override Governance

Overrides are exceptional and time-bound.

### 6.1 Override Eligibility

- Only permitted for operational continuity when failure to act is riskier than constrained action.
- Never permitted to violate long-only invariant.
- Never permitted to hide/log-skip risk actions.

### 6.2 Required Approval

- Minimum two-party approval:
  - Risk owner (mandatory),
  - Strategy or operations owner (second approver).
- For Tier 3/Tier 4 conditions, executive risk sign-off is also required.

### 6.3 Override Record Requirements

Each override must record:

- reason,
- scope (instrument/sleeve/portfolio),
- exact parameter or rule adjusted,
- start timestamp,
- expiry timestamp (mandatory),
- approvers,
- rollback condition.

Auto-expiry:

- Override expires automatically at stated time; no silent extension allowed.

## 7) Emergency Shutdown Conditions

Immediate shutdown of new risk is mandatory if any condition is true:

- Drawdown reaches Tier 4 (`>= 12%`).
- Risk engine unavailable, stale, or returning inconsistent decisions.
- Both primary and fallback data paths for required live inputs are unavailable.
- Order acknowledgements/fill reconciliation are unreliable or broken.
- Unauthorised manual action detected in execution path.
- Critical security or integrity incident impacting trade safety.

During shutdown:

- Only risk-reducing actions allowed unless incident commander documents exception.
- Incident bridge is opened.
- Restart requires explicit go/no-go checklist and risk owner sign-off.

## 8) Control Ownership and Review Cadence

- Risk owner: policy thresholds, drawdown actions, shutdown authority.
- Strategy owner: signal-quality constraints and dilution monitoring integration.
- Operations owner: runbook execution, incident coordination, audit completeness.

Review cadence:

- Daily during live validation stages.
- Weekly in stable operation.
- Immediate review after any Tier 3/Tier 4 event.

## 9) Compliance and Audit

- All risk decisions must be timestamped and immutable in logs.
- Every blocked order requires a machine-readable reason code.
- Every override and shutdown event requires post-incident documentation.
- Missing audit evidence is treated as a control failure.

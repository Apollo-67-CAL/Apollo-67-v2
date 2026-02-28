# Apollo 67 Phase 1: Deployment Validation

This document defines mandatory deployment validation for Apollo 67 Phase 1 (long-only).
Validation is stage-gated: paper first, then live capital progression.

## 1) Validation Objective

- Confirm the system is operationally safe before and during live deployment.
- Prove risk governance is enforced under normal and stressed conditions.
- Scale capital only after measurable pass criteria are met.

## 2) Stage Sequence (Mandatory)

Progress strictly in this order:

1. Paper Validation (mandatory first)
2. Live Stage 1: `$10,000`
3. Live Stage 2: `$15,000`
4. Live Stage 3: `$20,000`

No stage skipping is permitted.

## 3) Hard Pass/Fail Gates (Applies to Every Stage)

These are non-negotiable:

- Maximum peak-to-trough drawdown must remain `<= 25%` for the stage window.
- Zero rule violations:
  - no long-only breaches,
  - no risk-control bypass,
  - no unauthorised overrides,
  - no unaudited manual intervention.
- All required controls and logs must be operational:
  - pre-trade checks active,
  - kill-switch functional,
  - decision and execution audit trail complete.

Immediate fail conditions:

- Drawdown exceeds `25%`.
- Any confirmed rule violation.
- Any critical control outage without approved fail-safe handling.

## 4) Minimum Sample Requirements

Each stage must satisfy minimum sample quality before a pass decision.

### 4.1 Paper Validation Minimums

- Minimum calendar duration: `4 weeks`.
- Minimum executed trades: `40`.
- Minimum market regimes represented: `2` distinct volatility states.
- Minimum complete weekly reports: `4`.

### 4.2 Live Stage 1 ($10k) Minimums

- Minimum calendar duration: `4 weeks`.
- Minimum executed trades: `25`.
- Minimum complete weekly reports: `4`.

### 4.3 Live Stage 2 ($15k) Minimums

- Minimum calendar duration: `4 weeks`.
- Minimum executed trades: `25`.
- Minimum complete weekly reports: `4`.

### 4.4 Live Stage 3 ($20k) Minimums

- Minimum calendar duration: `4 weeks`.
- Minimum executed trades: `25`.
- Minimum complete weekly reports: `4`.

Sample integrity rules:

- Data gaps that invalidate metric reliability pause the stage clock.
- Trades executed during unresolved incident windows are excluded from pass calculations until reviewed.

## 5) Stage-Specific Validation Criteria

### Stage A: Paper Validation

Required outcomes:

- Strategy behaviour matches design expectations under production-equivalent controls.
- No rule violations.
- Net-of-cost performance remains within accepted baseline tolerance.

Pass decision:

- Eligible for Live Stage 1 only after all hard gates and minimum samples are met.

### Stage B: Live Stage 1 ($10k)

Primary goal:

- Verify production execution path and control integrity with limited capital.

Pass decision:

- Advance to `$15k` only if hard gates pass, sample minimums are met, and no unresolved critical incidents remain.

### Stage C: Live Stage 2 ($15k)

Primary goal:

- Validate consistency at higher but still constrained exposure.

Pass decision:

- Advance to `$20k` only if hard gates pass and weekly reporting confirms stable control behaviour.

### Stage D: Live Stage 3 ($20k)

Primary goal:

- Confirm repeatability, governance discipline, and readiness for post-Phase-1 scaling review.

Pass decision:

- Stage complete only when all hard gates pass for full sample window with no unresolved rule/control issues.

## 6) Weekly Reporting Requirements (Mandatory)

A weekly validation report is required for every active stage.

Each report must include:

- Stage identifier and active capital cap (`paper`, `$10k`, `$15k`, `$20k`).
- Start/end dates for the reporting week.
- Trade count and cumulative stage trade count.
- Stage drawdown (current and max to date).
- Rule violation summary (must be zero for pass trajectory).
- Override log summary (reason, approvers, expiry, status).
- Data/control health summary (provider uptime, risk engine health, incident count).
- Net vs gross performance and cost attribution.
- Decision: `on-track`, `watch`, or `escalate`.
- Named owner sign-off.

Report timing:

- Published within 24 hours of week end.

## 7) Escalation Conditions

Escalate immediately if any of the following occurs:

- Drawdown reaches `>= 18%` (warning escalation threshold).
- Two consecutive weeks of degraded control-health status.
- Any breach of hard risk limits, even if auto-blocked.
- Any evidence of model drift causing repeated dilution warnings.
- Any incident that compromises auditability or order-state certainty.

Escalation actions:

- Freeze capital progression.
- Open incident review with risk owner and operations owner.
- Require remediation plan with owner, ETA, and verification steps.

## 8) Downgrade and Rollback Conditions

Downgrade stage (or return to paper) if:

- Drawdown reaches `>= 22%` even without hard-gate breach.
- Repeated warning escalations remain unresolved for 2+ weeks.
- Control reliability degrades materially (risk engine/data instability).
- Weekly reporting is incomplete for 2 consecutive weeks.

Mandatory downgrade actions:

- Reduce capital to previous validated stage immediately.
- Restrict activity to risk-reducing or reduced-size operation until revalidated.
- Restart stage clock after remediation sign-off.

## 9) Governance and Sign-Off

- Risk owner: final authority on pass/fail and progression approvals.
- Strategy owner: confirms behavioural validity versus system design.
- Operations owner: confirms runbook execution and control uptime evidence.

Sign-off required at:

- completion of paper stage,
- promotion to `$10k`, `$15k`, `$20k`,
- any downgrade, rollback, or restart.

## 10) Final Phase 1 Validation Completion

Deployment validation is complete only when:

- Paper and all live stages (`$10k`, `$15k`, `$20k`) are passed in sequence.
- Every stage satisfies hard pass/fail gates.
- Minimum sample requirements are met for each stage.
- Weekly reporting is complete and approved.
- No unresolved critical incidents or rule violations remain open.

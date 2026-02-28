# Apollo 67 Phase 1: Parameters Baseline

This document defines the baseline parameter set for Apollo 67 Phase 1 (long-only).
These are default operating values, not optimisation targets.

## 1) Baseline Policy

- Baseline values are conservative by design.
- Any change requires rationale, owner approval, and changelog entry.
- Hard-limit breaches are blocking (no trade).
- Soft-limit breaches are warning-only unless explicitly marked blocking.

## 2) EIS Thresholds

EIS = Entry Integrity Score (0 to 100). Higher is better.

- `eis_min_entry_score`: `67` (blocking)
- `eis_watchlist_score`: `72` (warning threshold)
- `eis_high_conviction_score`: `80` (priority tier)
- `eis_data_quality_floor`: `90` (% required non-missing feature coverage, blocking)
- `eis_signal_stability_min`: `0.60` (0 to 1 stability score, blocking)

Execution rule:

- Enter long only if `EIS >= 67` and all risk/cost gates pass.

## 3) Explosion Thresholds

Explosion = abnormal expansion in range/volatility/volume that can invalidate expected execution quality.

- `explosion_atr_multiple_warn`: `1.8x` 20-day ATR (warning)
- `explosion_atr_multiple_block`: `2.4x` 20-day ATR (blocking for new entries)
- `explosion_intraday_range_warn`: `2.0x` 30-day median intraday range (warning)
- `explosion_intraday_range_block`: `2.8x` 30-day median intraday range (blocking)
- `explosion_volume_spike_warn`: `2.5x` 30-day median volume (warning)
- `explosion_volume_spike_block`: `4.0x` 30-day median volume (blocking unless explicitly allowed event window)

## 4) Dilution Thresholds

Dilution = signal weakening due to crowding, slippage pressure, and edge compression.

- `dilution_edge_decay_warn`: `20%` drop vs trailing baseline expectancy (warning)
- `dilution_edge_decay_block`: `35%` drop vs trailing baseline expectancy (blocking for new entries)
- `dilution_slippage_ratio_warn`: `1.5x` modelled slippage (warning)
- `dilution_slippage_ratio_block`: `2.0x` modelled slippage (blocking)
- `dilution_fill_quality_min`: `85%` acceptable fill-quality score (blocking below threshold)

## 5) Sleeve Caps

Sleeves are portfolio sub-buckets. Caps are percentage of total live capital.

- `sleeve_core_trend_cap`: `40%`
- `sleeve_momentum_cap`: `30%`
- `sleeve_reversion_cap`: `20%`
- `sleeve_event_cap`: `10%`

Rules:

- Sum of sleeve allocations must not exceed `100%`.
- Any single instrument max allocation within a sleeve: `8%` of total capital.

## 6) Heat Caps

Heat = aggregate portfolio risk-in-play.

- `portfolio_heat_soft_cap`: `18%` of risk budget (warning)
- `portfolio_heat_hard_cap`: `22%` of risk budget (blocking new entries)
- `single_name_heat_cap`: `4.5%` of risk budget (blocking)
- `sector_heat_cap`: `28%` of risk budget (blocking)
- `correlated_cluster_heat_cap`: `35%` of risk budget (blocking)

## 7) Drawdown Tiers

Drawdown is measured from rolling equity high-water mark.

- `tier_0_normal`: `< 4%` drawdown, normal operation.
- `tier_1_caution`: `>= 4% and < 7%`, reduce new position size by `25%`.
- `tier_2_defensive`: `>= 7% and < 10%`, reduce new position size by `50%`, tighten EIS min to `72`.
- `tier_3_protect`: `>= 10% and < 12%`, freeze new entries except explicit risk-approved exceptions.
- `tier_4_halt`: `>= 12%`, kill-switch for new risk, initiate incident review.

Recovery rule:

- Step down one tier only after equity recovers at least `1.5%` from tier trigger and stability checks pass.

## 8) Rotation Advantage Ratio

Rotation Advantage Ratio (RAR) compares candidate sleeve opportunity quality versus current holdings.

- `rar_min_to_rotate`: `1.20` (blocking; no rotation below this)
- `rar_preferred_rotate`: `1.35` (priority threshold)
- `rar_forced_defer_below`: `1.10` (auto defer unless risk override)

Interpretation:

- `RAR = expected value(candidate) / expected value(current holding at same risk unit)`.

## 9) Expectancy Requirements

Expectancy is net-of-cost.

- `expectancy_min_per_trade_r`: `+0.18R` (blocking)
- `expectancy_min_rolling_20_r`: `+0.12R` (warning below)
- `expectancy_min_rolling_50_r`: `+0.15R` (blocking below for new entries)
- `win_rate_floor_rolling_50`: `42%` (warning)
- `profit_factor_floor_rolling_50`: `1.25` (warning)
- `profit_factor_block_level`: `1.05` (blocking for new entries)

## 10) ML Training Caps

Phase 1 ML is assistive only and must remain bounded.

- `ml_training_window_max_days`: `750`
- `ml_lookback_min_days`: `180`
- `ml_features_max`: `120`
- `ml_model_count_max_per_cycle`: `6`
- `ml_training_runs_max_per_week`: `4`
- `ml_retrain_frequency_min_days`: `7`
- `ml_cv_folds_max`: `5`
- `ml_max_training_walltime_minutes`: `90` per run
- `ml_overfit_gap_max`: `4.0%` (train vs validation metric delta)
- `ml_live_weight_cap`: `35%` of signal blend (rest must be rules-based)

Blocking ML rules:

- Do not promote a model if overfit gap exceeds `4.0%`.
- Do not promote if live shadow performance is below baseline rules-only expectancy over validation window.

## 11) Parameter Governance

- Owner approvals required:
  - Strategy owner: EIS, rotation, expectancy parameters.
  - Risk owner: heat caps, drawdown tiers, blocking thresholds.
  - Platform owner: ML training caps and runtime limits.
- Change ticket must include:
  - old value,
  - new value,
  - reason,
  - expected impact,
  - rollback condition.

## 12) Stage Gate (Parameters Baseline)

This stage is complete only when:

- All baseline values above are implemented in versioned config.
- Blocking vs warning behaviour is enforced in tests.
- Parameter ownership and change workflow are signed off.

#!/usr/bin/env python3
"""Matched-event statistical models for Phase 2 retention diagnostics.

This script reads the version-2 event summary.  It does not modify Phase 1.
Biochar-minus-control differences are the unit of analysis, so irrigation,
weather, and timing shared by a matched pair are controlled by design.

For each regime and outcome it reports:
  * paired mean and median differences;
  * a year-stratified bootstrap 95% CI for the paired mean;
  * paired Wilcoxon and exact two-sided sign tests;
  * Cohen's dz;
  * an OLS model of paired difference ~ precipitation + year fixed effects,
    with HC3 heteroskedasticity-robust standard errors.  Precipitation is
    omitted and explicitly flagged when fewer than five events are wet or
    fewer than four precipitation values exist, because its slope would not
    be estimable with defensible support;
  * Benjamini-Hochberg false-discovery-rate q-values.

Important limitation: each regime contains one biochar/control field pair.
Events are repeated observations of those field strips, not independent field
replicates.  Inference therefore describes consistency within these specific
paired strips and cannot by itself establish population-level biochar effects.
"""

from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd
from scipy import stats

from biochar_app.config.paths import IRRIGATION_DIAGNOSTICS_DIR

RETENTION_DIR = (
    IRRIGATION_DIAGNOSTICS_DIR
    / "post_irrigation_retention"
)
INPUT_CSV = RETENTION_DIR / "post_irrigation_retention_event_summary.csv"
OUTPUT_DIR = IRRIGATION_DIAGNOSTICS_DIR / "post_irrigation_retention_statistics"

BOOTSTRAP_REPLICATES: Final[int] = 20_000
RANDOM_SEED: Final[int] = 20260816
ALPHA: Final[float] = 0.05

PAIR_DEFINITIONS: Final[dict[str, tuple[str, str, str]]] = {
    "S1_S2": ("S1", "S2", "monthly"),
    "S3_S4": ("S3", "S4", "biweekly"),
}

# Outcome -> (human label, unit, matching precipitation accumulation).
OUTCOMES: Final[dict[str, tuple[str, str, str]]] = {
    "reference_water_in": (
        "Redistributed reference water", "in", "precip_0_24h_in"
    ),
    "water_24h_in": ("Profile water at 24 h", "in", "precip_0_24h_in"),
    "water_72h_in": ("Profile water at 72 h", "in", "precip_0_72h_in"),
    "water_7d_in": ("Profile water at 7 d", "in", "precip_0_7d_in"),
    "water_half_interval_in": (
        "Profile water at half interval", "in", "precip_0_half_interval_in"
    ),
    "water_pre_next_in": (
        "Profile water before next irrigation", "in",
        "precip_0_next_irrigation_in",
    ),
    "mean_interval_water_in": (
        "Mean profile water over interval", "in",
        "precip_0_next_irrigation_in",
    ),
    "fraction_remaining_72h": (
        "Reference fraction remaining at 72 h", "fraction",
        "precip_0_72h_in",
    ),
    "fraction_remaining_7d": (
        "Reference fraction remaining at 7 d", "fraction",
        "precip_0_7d_in",
    ),
    "fraction_remaining_pre_next": (
        "Reference fraction remaining pre-next", "fraction",
        "precip_0_next_irrigation_in",
    ),
    "depletion_in_per_day": (
        "Reference-to-pre-next depletion rate", "in/day",
        "precip_0_next_irrigation_in",
    ),
}

PRIMARY_OUTCOMES: Final[set[str]] = {
    "reference_water_in",
    "water_24h_in",
    "water_72h_in",
    "water_7d_in",
    "water_half_interval_in",
    "water_pre_next_in",
    "mean_interval_water_in",
}


def print_section(title: str) -> None:
    print("\n" + "=" * 96)
    print(title)
    print("=" * 96)


def bh_adjust(p_values: pd.Series) -> pd.Series:
    """Benjamini-Hochberg adjustment, preserving missing values and index."""
    out = pd.Series(np.nan, index=p_values.index, dtype=float)
    valid = pd.to_numeric(p_values, errors="coerce").dropna()
    if valid.empty:
        return out
    ordered = valid.sort_values()
    m = len(ordered)
    adjusted = ordered.to_numpy() * m / np.arange(1, m + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    out.loc[ordered.index] = np.minimum(adjusted, 1.0)
    return out


def stratified_bootstrap_mean_ci(
    values: np.ndarray,
    years: np.ndarray,
) -> tuple[float, float]:
    """Resample events within each year, retaining observed year weights."""
    rng = np.random.default_rng(RANDOM_SEED)
    groups = [values[years == year] for year in np.unique(years)]
    draws = np.empty(BOOTSTRAP_REPLICATES, dtype=float)
    for i in range(BOOTSTRAP_REPLICATES):
        sampled = [rng.choice(group, size=len(group), replace=True) for group in groups]
        draws[i] = np.concatenate(sampled).mean()
    low, high = np.quantile(draws, [ALPHA / 2.0, 1.0 - ALPHA / 2.0])
    return float(low), float(high)


def make_matched_events(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    keys = ["year", "strip_group", "irrigation_start"]
    for pair, (biochar_strip, control_strip, regime) in PAIR_DEFINITIONS.items():
        pair_df = events.loc[events["strip_group"].eq(pair)].copy()
        bio = pair_df.loc[pair_df["strip"].eq(biochar_strip)]
        control = pair_df.loc[pair_df["strip"].eq(control_strip)]
        matched = bio.merge(control, on=keys, how="inner", suffixes=("_biochar", "_control"))
        if matched.empty:
            continue
        matched["pair"] = pair
        matched["regime"] = regime
        matched["biochar_strip"] = biochar_strip
        matched["control_strip"] = control_strip
        for outcome, (_, _, precip_col) in OUTCOMES.items():
            matched[f"difference__{outcome}"] = (
                pd.to_numeric(matched[f"{outcome}_biochar"], errors="coerce")
                - pd.to_numeric(matched[f"{outcome}_control"], errors="coerce")
            )
            # Rain is shared within a pair; average guards against rounding noise.
            matched[f"precip__{outcome}"] = matched[
                [f"{precip_col}_biochar", f"{precip_col}_control"]
            ].apply(pd.to_numeric, errors="coerce").mean(axis=1)
        rows.append(matched)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def paired_summary(matched: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for pair, (_, _, regime) in PAIR_DEFINITIONS.items():
        pair_df = matched.loc[matched["pair"].eq(pair)]
        for outcome, (label, unit, _) in OUTCOMES.items():
            working = pd.DataFrame({
                "difference": pd.to_numeric(
                    pair_df[f"difference__{outcome}"], errors="coerce"
                ),
                "year": pd.to_numeric(pair_df["year"], errors="coerce"),
            }).dropna()
            if working.empty:
                continue
            differences = working["difference"].to_numpy(dtype=float)
            ci_low, ci_high = stratified_bootstrap_mean_ci(
                differences, working["year"].to_numpy()
            )
            nonzero = differences[~np.isclose(differences, 0.0)]
            wilcoxon_p = (
                float(stats.wilcoxon(nonzero, alternative="two-sided").pvalue)
                if len(nonzero) else np.nan
            )
            sign_p = (
                float(stats.binomtest(
                    int((nonzero > 0).sum()), len(nonzero), 0.5,
                    alternative="two-sided",
                ).pvalue)
                if len(nonzero) else np.nan
            )
            sd = differences.std(ddof=1) if len(differences) > 1 else np.nan
            rows.append({
                "pair": pair,
                "regime": regime,
                "outcome": outcome,
                "outcome_label": label,
                "unit": unit,
                "outcome_family": "primary_absolute_water" if outcome in PRIMARY_OUTCOMES else "secondary",
                "n_matched": len(differences),
                "mean_difference": differences.mean(),
                "bootstrap_ci_low": ci_low,
                "bootstrap_ci_high": ci_high,
                "median_difference": np.median(differences),
                "biochar_higher_pct": 100.0 * (differences > 0).mean(),
                "cohen_dz": differences.mean() / sd if np.isfinite(sd) and sd > 0 else np.nan,
                "wilcoxon_p": wilcoxon_p,
                "sign_test_p": sign_p,
            })
    out = pd.DataFrame(rows)
    out["wilcoxon_q_within_regime_family"] = np.nan
    for _, idx in out.groupby(["regime", "outcome_family"]).groups.items():
        out.loc[idx, "wilcoxon_q_within_regime_family"] = bh_adjust(
            out.loc[idx, "wilcoxon_p"]
        )
    return out


def fit_hc3_model(data: pd.DataFrame) -> dict[str, float] | None:
    """Fit difference ~ centered precipitation + year FE with HC3 covariance."""
    working = data[["difference", "precip", "year"]].dropna().copy()
    if len(working) < 8 or working["year"].nunique() < 2:
        return None
    precip_mean = float(working["precip"].mean())
    precip_nonzero = int(working["precip"].gt(0).sum())
    precip_unique = int(working["precip"].nunique())
    include_precip = precip_nonzero >= 5 and precip_unique >= 4
    working["precip_centered"] = working["precip"] - precip_mean
    years = sorted(int(year) for year in working["year"].unique())
    year_columns = years[1:]
    x_parts = [np.ones(len(working))]
    if include_precip:
        x_parts.append(working["precip_centered"].to_numpy(dtype=float))
    x_parts.extend(
        working["year"].eq(year).to_numpy(dtype=float) for year in year_columns
    )
    x = np.column_stack(x_parts)
    y = working["difference"].to_numpy(dtype=float)
    if np.linalg.matrix_rank(x) < x.shape[1] or len(y) <= x.shape[1]:
        return None
    xtx_inv = np.linalg.inv(x.T @ x)
    beta = xtx_inv @ x.T @ y
    residual = y - x @ beta
    leverage = np.einsum("ij,jk,ik->i", x, xtx_inv, x)
    scaled_sq = (residual / np.clip(1.0 - leverage, 1e-9, None)) ** 2
    meat = x.T @ (x * scaled_sq[:, None])
    covariance = xtx_inv @ meat @ xtx_inv
    df_resid = len(y) - x.shape[1]

    # Marginal adjusted mean: mean of year-specific predictions at mean rain,
    # weighted by the observed year frequencies.
    contrast = np.zeros(x.shape[1])
    contrast[0] = 1.0
    year_start = 2 if include_precip else 1
    for j, year in enumerate(year_columns, start=year_start):
        contrast[j] = working["year"].eq(year).mean()
    estimate = float(contrast @ beta)
    se = float(np.sqrt(max(contrast @ covariance @ contrast, 0.0)))
    critical = float(stats.t.ppf(1.0 - ALPHA / 2.0, df_resid))
    p_value = (
        float(2.0 * stats.t.sf(abs(estimate / se), df_resid)) if se > 0 else np.nan
    )
    precip_se = (
        float(np.sqrt(max(covariance[1, 1], 0.0))) if include_precip else np.nan
    )
    precip_slope = float(beta[1]) if include_precip else np.nan
    precip_p = (
        float(2.0 * stats.t.sf(abs(precip_slope / precip_se), df_resid))
        if include_precip and precip_se > 0 else np.nan
    )
    return {
        "n_model": len(y),
        "n_years": len(years),
        "parameters": x.shape[1],
        "residual_df": df_resid,
        "mean_precip_in": precip_mean,
        "precip_nonzero_events": precip_nonzero,
        "precip_unique_values": precip_unique,
        "precip_min_in": float(working["precip"].min()),
        "precip_max_in": float(working["precip"].max()),
        "precipitation_in_model": include_precip,
        "adjusted_mean_difference": estimate,
        "adjusted_hc3_se": se,
        "adjusted_ci_low": estimate - critical * se,
        "adjusted_ci_high": estimate + critical * se,
        "adjusted_p": p_value,
        "precip_slope_per_in": precip_slope,
        "precip_slope_hc3_se": precip_se,
        "precip_slope_p": precip_p,
        "r_squared": float(1.0 - (residual @ residual) / np.sum((y - y.mean()) ** 2))
        if np.sum((y - y.mean()) ** 2) > 0 else np.nan,
    }


def adjusted_models(matched: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for pair, (_, _, regime) in PAIR_DEFINITIONS.items():
        pair_df = matched.loc[matched["pair"].eq(pair)]
        for outcome, (label, unit, precip_col) in OUTCOMES.items():
            data = pd.DataFrame({
                "difference": pd.to_numeric(
                    pair_df[f"difference__{outcome}"], errors="coerce"
                ),
                "precip": pd.to_numeric(
                    pair_df[f"precip__{outcome}"], errors="coerce"
                ),
                "year": pd.to_numeric(pair_df["year"], errors="coerce"),
            })
            fit = fit_hc3_model(data)
            if fit is None:
                continue
            rows.append({
                "pair": pair,
                "regime": regime,
                "outcome": outcome,
                "outcome_label": label,
                "unit": unit,
                "outcome_family": "primary_absolute_water" if outcome in PRIMARY_OUTCOMES else "secondary",
                "precipitation_metric": precip_col,
                **fit,
            })
    out = pd.DataFrame(rows)
    out["adjusted_q_within_regime_family"] = np.nan
    for _, idx in out.groupby(["regime", "outcome_family"]).groups.items():
        out.loc[idx, "adjusted_q_within_regime_family"] = bh_adjust(
            out.loc[idx, "adjusted_p"]
        )
    return out


def write_csv(df: pd.DataFrame, filename: str) -> None:
    path = OUTPUT_DIR / filename
    df.to_csv(path, index=False, float_format="%.6f")
    print(f"Wrote {len(df):,} rows: {path}")


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"Run inspect_post_irrigation_retention.py first; missing:\n{INPUT_CSV}"
        )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    events = pd.read_csv(INPUT_CSV, parse_dates=[
        "irrigation_start", "irrigation_end", "next_irrigation_start"
    ])
    matched = make_matched_events(events)
    if matched.empty:
        raise ValueError("No matched biochar/control events were found.")
    paired = paired_summary(matched)
    models = adjusted_models(matched)

    print_section("PHASE 2 MATCHED-EVENT STATISTICAL ANALYSIS")
    print(f"Input event rows: {len(events):,}")
    print(
        matched.groupby(["pair", "regime"]).size().rename("matched_events").to_string()
    )

    print_section("PRIMARY PAIRED EFFECTS (BIOCHAR MINUS CONTROL)")
    primary = paired.loc[paired["outcome_family"].eq("primary_absolute_water")]
    print(primary[[
        "pair", "outcome_label", "n_matched", "mean_difference",
        "bootstrap_ci_low", "bootstrap_ci_high", "cohen_dz",
        "wilcoxon_p", "wilcoxon_q_within_regime_family",
    ]].round(4).to_string(index=False))

    print_section("YEAR- AND PRECIPITATION-ADJUSTED PRIMARY MODELS")
    primary_models = models.loc[
        models["outcome_family"].eq("primary_absolute_water")
    ]
    print(primary_models[[
        "pair", "outcome_label", "n_model", "adjusted_mean_difference",
        "adjusted_ci_low", "adjusted_ci_high", "adjusted_p",
        "adjusted_q_within_regime_family", "precip_slope_per_in",
    ]].round(4).to_string(index=False))

    print_section("WRITING STATISTICAL OUTPUTS")
    detail_columns = [
        "pair", "regime", "year", "irrigation_start",
        *[f"difference__{outcome}" for outcome in OUTCOMES],
        *[f"precip__{outcome}" for outcome in OUTCOMES],
    ]
    write_csv(matched[detail_columns], "retention_matched_event_differences.csv")
    write_csv(paired, "retention_paired_inference.csv")
    write_csv(models, "retention_year_precip_adjusted_models.csv")

    print_section("INTERPRETATION LIMIT")
    print(
        "These are repeated matched events from one field pair per regime. "
        "They quantify within-pair consistency, not population-level spatial replication."
    )


if __name__ == "__main__":
    main()

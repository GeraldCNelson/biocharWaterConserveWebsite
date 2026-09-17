from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from biochar_app.config.experiment_config import (
    STRIP_IRRIGATION_REGIME,
    STRIP_TREATMENT,
    STRIP_TREATMENT_PAIR,
)


REPO = Path(__file__).resolve().parents[4]
ANALYSIS = REPO / "biochar_app/data-processed/management/irrigation/analysis"
HC = ANALYSIS / "holding_capacity"
DIAG = ANALYSIS / "diagnostics"
OUT = REPO / "biochar_app/docs/research/irrigation_flow_water_balance"

RNG = np.random.default_rng(20260914)


def robust_model(outcome: str, data: pd.DataFrame, predictors: list[str] | None = None) -> dict:
    predictors = predictors or ["log_flow"]
    dummies = pd.get_dummies(data[["strip", "year"]].astype(str), drop_first=True, dtype=float)
    xdf = pd.concat([
        pd.Series(1.0, index=data.index, name="intercept"),
        data[predictors].astype(float), dummies,
    ], axis=1)
    X = xdf.to_numpy(float)
    y = data[outcome].to_numpy(float)
    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    resid = y - X @ beta
    xtxi = np.linalg.pinv(X.T @ X)
    leverage = np.sum((X @ xtxi) * X, axis=1)
    # Cluster sandwich by irrigation event because paired strips share the same
    # meter-derived flow rate and are not independent observations.
    meat = np.zeros((X.shape[1], X.shape[1]))
    groups = data["event_id"].astype(str).to_numpy()
    for group in np.unique(groups):
        mask = groups == group
        score = X[mask].T @ resid[mask]
        meat += np.outer(score, score)
    g = len(np.unique(groups)); n, k = X.shape
    if g > 1 and n > k:
        meat *= (g / (g - 1)) * ((n - 1) / (n - k))
    cov = xtxi @ meat @ xtxi
    se = np.sqrt(np.clip(np.diag(cov), 0, None))
    idx = list(xdf.columns).index("log_flow")
    z = beta[idx] / se[idx] if se[idx] > 0 else np.nan
    p = math.erfc(abs(z) / math.sqrt(2)) if np.isfinite(z) else np.nan
    sst = np.sum((y-y.mean())**2)
    r2 = 1 - np.sum(resid**2)/sst if sst > 0 else np.nan
    return {
        "outcome": outcome, "controls": "strip and year fixed effects",
        "predictors": predictors, "n": len(y), "n_event_clusters": g, "r2": float(r2),
        "log_flow_coef": float(beta[idx]), "log_flow_se": float(se[idx]),
        "log_flow_p_normal_approx": float(p),
        "log_flow_ci_low": float(beta[idx] - 1.96*se[idx]),
        "log_flow_ci_high": float(beta[idx] + 1.96*se[idx]),
    }


def corr(x: pd.Series, y: pd.Series) -> dict:
    z = pd.concat([x, y], axis=1).dropna()
    if len(z) < 4 or z.iloc[:, 0].nunique() < 2 or z.iloc[:, 1].nunique() < 2:
        return {"n": len(z), "pearson_r": np.nan, "pearson_p": np.nan,
                "spearman_rho": np.nan, "spearman_p": np.nan}
    a, b = z.iloc[:, 0].to_numpy(float), z.iloc[:, 1].to_numpy(float)
    pr = np.corrcoef(a, b)[0, 1]
    ar = pd.Series(a).rank().to_numpy(); br = pd.Series(b).rank().to_numpy()
    sr = np.corrcoef(ar, br)[0, 1]
    nperm = 5000
    pvals = np.empty((nperm, 2))
    for i in range(nperm):
        bp = RNG.permutation(b)
        pvals[i, 0] = np.corrcoef(a, bp)[0, 1]
        pvals[i, 1] = np.corrcoef(ar, pd.Series(bp).rank().to_numpy())[0, 1]
    return {"n": len(z), "pearson_r": pr, "pearson_p_permutation": (np.sum(np.abs(pvals[:,0])>=abs(pr))+1)/(nperm+1),
            "spearman_rho": sr, "spearman_p_permutation": (np.sum(np.abs(pvals[:,1])>=abs(sr))+1)/(nperm+1)}


def bootstrap_ci(x: pd.Series, nboot: int = 10000) -> tuple[float, float]:
    a = x.dropna().to_numpy(float)
    if len(a) < 2:
        return np.nan, np.nan
    means = np.array([RNG.choice(a, len(a), replace=True).mean() for _ in range(nboot)])
    return tuple(np.quantile(means, [0.025, 0.975]))


def sign_flip_p(x: pd.Series, nperm: int = 20000) -> float:
    a = x.dropna().to_numpy(float)
    if len(a) < 2:
        return np.nan
    observed = abs(a.mean())
    sims = np.array([(a * RNG.choice([-1, 1], len(a))).mean() for _ in range(nperm)])
    return float((np.sum(np.abs(sims) >= observed) + 1) / (nperm + 1))


def build_zone_capacity_table(capacity: pd.DataFrame) -> pd.DataFrame:
    """Return a compact reader-facing summary of modeled zone capacity."""
    columns = {
        "strip": "strip",
        "treatment": "treatment",
        "pair": "pair",
        "logger_position": "zone_position",
        "zone_length_ft": "zone_length_ft",
        "zone_area_sqft": "zone_area_sqft",
        "represented_profile_thickness_in": "profile_thickness_in",
        "profile_weighted_mean_vwc_pct": "upper_retained_vwc_pct",
        "profile_equivalent_water_in": "upper_retained_water_in",
        "estimated_upper_retained_water_gal": "upper_retained_water_gal",
        "estimated_upper_retained_water_acre_in": "upper_retained_water_acre_in",
        "strip_total_upper_retained_water_gal": "strip_total_upper_retained_water_gal",
        "minimum_eligible_event_count": "minimum_eligible_event_count",
        "estimate_status": "estimate_status",
        "water_volume_basis": "capacity_definition",
    }
    table = capacity[list(columns)].rename(columns=columns).copy()
    table.insert(3, "irrigation_regime", table["strip"].map(STRIP_IRRIGATION_REGIME))
    zone_order = pd.CategoricalDtype(["T", "M", "B"], ordered=True)
    table["zone_position"] = table["zone_position"].astype(zone_order)
    table = table.sort_values(["strip", "zone_position"]).reset_index(drop=True)
    table["zone_position"] = table["zone_position"].astype(str)
    numeric = table.select_dtypes(include="number").columns
    table[numeric] = table[numeric].round(2)
    return table


def build_typical_event_summary(water_balance: pd.DataFrame) -> pd.DataFrame:
    """Summarize the middle 50% of eligible observed events by strip."""
    eligible = water_balance.loc[water_balance["unretained_eligible"].eq(True)].copy()
    eligible["treatment"] = eligible["strip"].map(STRIP_TREATMENT)
    eligible["irrigation_regime"] = eligible["strip"].map(STRIP_IRRIGATION_REGIME)
    eligible["pair"] = eligible["strip"].map(STRIP_TREATMENT_PAIR)

    rows: list[dict] = []
    for strip, group in eligible.groupby("strip", sort=True):
        row = {
            "strip": strip,
            "treatment": STRIP_TREATMENT[strip],
            "irrigation_regime": STRIP_IRRIGATION_REGIME[strip],
            "pair": STRIP_TREATMENT_PAIR[strip],
            "eligible_event_count": len(group),
            "first_year": int(group["year"].min()),
            "last_year": int(group["year"].max()),
        }
        for source, label in {
            "gallons_strip": "applied_water",
            "estimated_storage_gal_strip_0_18in": "total_monitored_uptake",
            "unretained_gal_strip": "unaccounted_water",
        }.items():
            values = pd.to_numeric(group[source], errors="coerce").dropna()
            row[f"{label}_median_gal"] = values.median()
            row[f"{label}_q25_gal"] = values.quantile(0.25)
            row[f"{label}_q75_gal"] = values.quantile(0.75)
        for source, label in {
            "top_zone_storage_gal_0_18in": "top_zone_uptake",
            "middle_zone_storage_gal_0_18in": "middle_zone_uptake",
            "bottom_zone_storage_gal_0_18in": "bottom_zone_uptake",
        }.items():
            row[f"{label}_median_gal"] = pd.to_numeric(
                group[source], errors="coerce"
            ).median()
        row["monitored_uptake_median_pct"] = 100 * pd.to_numeric(
            group["estimated_storage_fraction_0_18in"], errors="coerce"
        ).median()
        row["unaccounted_water_median_pct"] = 100 * pd.to_numeric(
            group["unretained_fraction"], errors="coerce"
        ).median()
        row["summary_method"] = (
            "Independent medians and quartiles across events eligible for the "
            "canonical unaccounted-water calculation"
        )
        row["interpretation"] = (
            "Unaccounted water is not measured runoff; it can include field-end flow, "
            "deep drainage, lateral redistribution, continued storage, and measurement error"
        )
        rows.append(row)

    table = pd.DataFrame(rows)
    numeric = table.select_dtypes(include="number").columns
    table[numeric] = table[numeric].round(1)
    return table


def build_report(result: dict, correlations: dict, models: dict,
                 paired_stats: dict, notes_n: int) -> str:
    """Build the research summary from the current model results."""
    def percent_change(model: dict) -> tuple[float, float, float]:
        scale = math.log(2)
        return tuple(100 * (math.exp(model[key] * scale) - 1)
                     for key in ("log_flow_coef", "log_flow_ci_low", "log_flow_ci_high"))

    def point_change(model: dict) -> tuple[float, float, float]:
        scale = 100 * math.log(2)
        return tuple(model[key] * scale
                     for key in ("log_flow_coef", "log_flow_ci_low", "log_flow_ci_high"))

    counts = result["counts"]
    tb = models["log_top_to_bottom"]
    strict_tb = models["log_top_to_bottom_strict"]
    ta = models["log_top_arrival"]
    sf = models["storage_fraction_volume_adjusted"]
    uf = models["unretained_fraction_volume_adjusted"]
    av = models["absolute_storage_volume_adjusted"]
    monthly_tb = models["log_top_to_bottom_monthly"]
    biweekly_tb = models["log_top_to_bottom_biweekly"]
    monthly_sf = models["storage_fraction_volume_adjusted_monthly"]
    biweekly_sf = models["storage_fraction_volume_adjusted_biweekly"]
    tb_pct, tb_lo, tb_hi = percent_change(tb)
    strict_pct, strict_lo, strict_hi = percent_change(strict_tb)
    ta_pct, ta_lo, ta_hi = percent_change(ta)
    sf_pt, sf_lo, sf_hi = point_change(sf)
    uf_pt, uf_lo, uf_hi = point_change(uf)
    av_pct, av_lo, av_hi = percent_change(av)
    monthly_pct, monthly_lo, monthly_hi = percent_change(monthly_tb)
    biweekly_pct, biweekly_lo, biweekly_hi = percent_change(biweekly_tb)
    monthly_sf_pt, monthly_sf_lo, monthly_sf_hi = point_change(monthly_sf)
    biweekly_sf_pt, biweekly_sf_lo, biweekly_sf_hi = point_change(biweekly_sf)
    s12 = paired_stats["S1_S2"]["diff_top_to_bottom_min"]
    s34 = paired_stats["S3_S4"]["diff_top_to_bottom_min"]
    s12_res = paired_stats["S1_S2"]["diff_unretained_fraction"]
    s34_res = paired_stats["S3_S4"]["diff_unretained_fraction"]
    by_strip = counts["arrival_model_eligible_by_strip"]

    return f"""# Flow rate, wetting-front advance, and the unresolved water balance

## Bottom line

This analysis uses the corrected 2023–2026 logger timestamps and treats Top→Bottom travel as the primary horizontal-advance outcome. Middle timing is retained as a separate spatial-coverage diagnostic; strict Top→Middle→Bottom order is no longer required for the primary model.

That revision restores S3 to the analysis and changes the conclusion. A doubling of recorded pair-level flow is associated with an estimated **{abs(tb_pct):.0f}% shorter Top→Bottom travel time**, but the 95% interval ranges from {abs(tb_lo):.0f}% shorter to {tb_hi:.0f}% longer (event-clustered normal-approximation p={tb['log_flow_p_normal_approx']:.3f}). The full-field flow effect is therefore uncertain.

The stricter subset that requires Top→Middle→Bottom order estimates a **{abs(strict_pct):.0f}% shorter** Top→Bottom time when flow doubles (95% interval {abs(strict_hi):.0f}% to {abs(strict_lo):.0f}% shorter; p={strict_tb['log_flow_p_normal_approx']:.3f}). Because that restriction excludes every S3 observation and many nonuniform events, it is reported as sensitivity evidence rather than the principal result.

## Experimental design

The field combines two treatment dimensions. S1 is biochar with monthly irrigation, S2 is its non-biochar monthly control, S3 is biochar with approximately biweekly irrigation, and S4 is its non-biochar biweekly control. Consequently:

- S1−S2 estimates the biochar contrast under monthly irrigation.
- S3−S4 estimates the biochar contrast under biweekly irrigation.
- A difference between those two contrasts is consistent with a biochar × irrigation-frequency interaction, including effects mediated by antecedent soil moisture.

However, each of the four treatment combinations is represented by only one strip. Irrigation frequency is therefore confounded with strip pair and east/west field position, and biochar effects within a regime are confounded with individual strip differences. The results are repeated-event comparisons within these four experimental units, not replicated treatment estimates.

## Data and eligibility

- There are {counts['event_rows']} strip-events; {counts['complete_arrival_rows']} have all three 6-inch arrival estimates.
- {counts['top_to_bottom_ordered_rows']} have Bottom after Top. Of these, {counts['arrival_model_eligible_rows']} records from {counts['arrival_model_event_clusters']} irrigation events also pass canonical event QC and enter the primary model.
- Primary records by strip are S1={by_strip.get('S1', 0)}, S2={by_strip.get('S2', 0)}, S3={by_strip.get('S3', 0)}, and S4={by_strip.get('S4', 0)}.
- {counts['strict_ordered_arrival_rows']} records have strict Top→Middle→Bottom order. S3 has {counts['s3_top_to_bottom_ordered_rows']} Top→Bottom-ordered records, {counts['s3_arrival_model_eligible_rows']} of which pass QC, but no strict three-position records.
- The water-balance analysis uses {counts['unretained_residual_rows']} eligible records. The {counts['eof_proxy_rows']} end-of-field proxy records remain upper-bound estimates, not measured runoff.

## Horizontal advance and spatial coverage

Top arrival itself is earlier at higher flow: doubling flow is associated with an estimated **{abs(ta_pct):.0f}% earlier** Top response (95% interval {abs(ta_hi):.0f}% to {abs(ta_lo):.0f}% earlier; p<0.001). By contrast, the primary Top→Bottom model is uncertain, and its unadjusted Spearman correlation is {correlations['flow_vs_top_to_bottom']['spearman_rho']:.3f} (permutation p={correlations['flow_vs_top_to_bottom']['spearman_p_permutation']:.3f}).

When stratified by irrigation regime, doubling flow is associated with **{abs(monthly_pct):.0f}% shorter** Top→Bottom time in the monthly S1/S2 observations (95% interval {abs(monthly_hi):.0f}% to {abs(monthly_lo):.0f}% shorter; p<0.001). In the biweekly S3/S4 observations, the estimate is only **{abs(biweekly_pct):.0f}% shorter** and is highly uncertain (95% interval ranges from {abs(biweekly_lo):.0f}% shorter to {biweekly_hi:.0f}% longer; p={biweekly_tb['log_flow_p_normal_approx']:.3f}). These regime-specific estimates are descriptive because regime and strip pair cannot be separated statistically.

The corrected 2024 S3 plots commonly show Top first, Bottom shortly afterward, and Middle near the end of irrigation. The traces are smooth and the Middle response is a genuine VWC increase, so this pattern no longer looks like a simple clock error. It indicates nonuniform spatial delivery or a different water pathway at S3M. Middle timing should therefore be analyzed as a response characteristic rather than used as a gate that discards otherwise credible Top→Bottom observations.

## Monitored storage and residual water

Holding applied gallons, strip, and year constant:

- Doubling flow is associated with **{abs(sf_pt):.1f} percentage points less monitored storage** (95% interval {abs(sf_hi):.1f} to {abs(sf_lo):.1f} points less; p={sf['log_flow_p_normal_approx']:.3f}).
- The complementary residual fraction is **{uf_pt:.1f} percentage points larger** (95% interval {uf_lo:.1f} to {uf_hi:.1f} points; p<{0.001:.3f}).
- Absolute monitored storage is estimated to be **{abs(av_pct):.1f}% lower** when flow doubles at fixed applied volume (95% interval {abs(av_hi):.1f}% to {abs(av_lo):.1f}% lower; p<{0.001:.3f}).

The storage association is similar in the two timing regimes: doubling flow is associated with {abs(monthly_sf_pt):.1f} percentage points less monitored storage in the monthly pair (p={monthly_sf['log_flow_p_normal_approx']:.3f}) and {abs(biweekly_sf_pt):.1f} points less in the biweekly pair (p={biweekly_sf['log_flow_p_normal_approx']:.3f}). This similarity does not remove the lack of replicated strips.

These are observational associations. The residual is not measured runoff: it may contain tailwater, deep percolation, lateral redistribution, continued infiltration, unequal strip delivery, and measurement error.

## Biochar-control comparisons

For {s12['n']} eligible monthly-irrigation S1–S2 events, the biochar strip S1 has Top→Bottom travel averaging **{s12['mean']:.0f} minutes longer** than control strip S2 (bootstrap 95% interval {s12['bootstrap_ci_low']:.0f} to {s12['bootstrap_ci_high']:.0f}; sign-randomization p<0.001).

For {s34['n']} eligible biweekly-irrigation S3–S4 events, the biochar strip S3 has Top→Bottom travel averaging **{abs(s34['mean']):.0f} minutes shorter** than control strip S4 (bootstrap 95% interval {abs(s34['bootstrap_ci_high']):.0f} to {abs(s34['bootstrap_ci_low']):.0f} minutes shorter; sign-randomization p<0.001).

The mean residual is {100*s12_res['mean']:.1f} percentage points higher in S1 than S2 under monthly irrigation and {abs(100*s34_res['mean']):.1f} points lower in S3 than S4 under biweekly irrigation. These opposing within-regime contrasts may reflect an interaction between biochar and irrigation timing through antecedent moisture, but they may also reflect fixed differences between the individual strips, unequal gate flow, or east/west field conditions. They do not justify either a pooled biochar conclusion or a standalone irrigation-frequency conclusion.

## Field observations and next measurements

Only {notes_n} management records contain runoff, field-end, flooding, or cross-strip language. These provide valuable checks but are too sparse to estimate runoff across all events.

The most useful next measurements remain strip-specific flow and field-end tailwater. The next analytical step is to summarize Middle timing by strip, year, and event and compare it with irrigation duration and field notes. Deep drainage requires measurements below the monitored profile or a defensible calibrated drainage model.

## Reproducibility

The event table, paired-event table, field-note subset, and machine-readable results were regenerated by `biochar_app/scripts/management/irrigation_analysis/analyze_flow_infiltration.py`. Canonical inputs were read only.

Two reader-facing tables accompany the detailed outputs. `zone_capacity_summary.csv` reports the modeled upper retained-water state for each Top, Middle, and Bottom zone. `typical_event_summary.csv` reports strip-level medians and middle-50% ranges from eligible observed irrigation events. Its component medians are calculated independently and therefore are descriptive rather than a synthetic water budget for one event.
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    advance = pd.concat(
        [pd.read_csv(DIAG / f"irrigation_horizontal_advance_summary_{year}.csv")
         for year in (2023, 2024, 2025, 2026)],
        ignore_index=True,
    )
    wb = pd.read_csv(HC / "first_pass_water_balance_all_years.csv")
    wb["year"] = wb["year"].astype(int)
    zone_capacity = build_zone_capacity_table(
        pd.read_csv(HC / "combined_empirical_zone_upper_retained_water.csv")
    )
    typical_events = build_typical_event_summary(wb)
    zone_capacity.to_csv(OUT / "zone_capacity_summary.csv", index=False)
    typical_events.to_csv(OUT / "typical_event_summary.csv", index=False)

    # The 6-inch sensor is the primary near-surface wetting-front indicator.
    event = advance.loc[advance["depth_inches"].eq(6)].copy()
    event["year"] = event["year"].astype(int)
    event["avg_flow_gph_strip"] = event["gallons_strip"] / event["event_duration_hours"]
    event["treatment"] = event["strip"].map(STRIP_TREATMENT)
    event["irrigation_regime"] = event["strip"].map(STRIP_IRRIGATION_REGIME)
    event["pair"] = event["strip"].map(STRIP_TREATMENT_PAIR)

    for col in ["arrival_T_min", "arrival_M_min", "arrival_B_min", "T_to_M_min", "M_to_B_min", "T_to_B_min"]:
        event[col] = pd.to_numeric(event[col], errors="coerce")
    event["strict_ordered_arrivals"] = (
        event[["arrival_T_min", "arrival_M_min", "arrival_B_min"]].notna().all(axis=1)
        & event["arrival_T_min"].ge(0)
        & event["arrival_M_min"].ge(event["arrival_T_min"])
        & event["arrival_B_min"].ge(event["arrival_M_min"])
    )
    event["top_to_bottom_ordered"] = (
        event[["arrival_T_min", "arrival_B_min"]].notna().all(axis=1)
        & event["arrival_T_min"].ge(0)
        & event["arrival_B_min"].gt(event["arrival_T_min"])
    )
    event["top_to_middle_min"] = event["arrival_M_min"] - event["arrival_T_min"]
    event["middle_to_bottom_min"] = event["arrival_B_min"] - event["arrival_M_min"]
    event["top_to_bottom_min"] = event["arrival_B_min"] - event["arrival_T_min"]

    keep_wb = [
        "year", "strip", "event_id", "event_qc_eligible", "event_qc_reason",
        "complete_three_zone_coverage", "unretained_eligible", "unretained_gal_strip",
        "unretained_fraction", "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in", "bottom_6in_arrival_delay_hr",
        "post_bottom_6in_arrival_runtime_hr", "post_bottom_6in_arrival_applied_gal",
        "post_bottom_6in_arrival_applied_fraction", "end_of_field_unretained_eligible",
        "estimated_end_of_field_unretained_gal_strip",
        "estimated_end_of_field_unretained_fraction",
    ]
    event = event.merge(wb[keep_wb], on=["year", "strip", "event_id"], how="left", validate="one_to_one")
    event["log_flow"] = np.log(event["avg_flow_gph_strip"])
    event["log_gallons"] = np.log(event["gallons_strip"])

    event["arrival_model_eligible"] = (
        event["top_to_bottom_ordered"]
        & event["event_qc_eligible"].eq(True)
        & event["T_to_B_min"].gt(0)
    )
    ordered = event.loc[event["arrival_model_eligible"]].copy()
    # Exclude zero travel times from log response models.
    ordered = ordered.loc[ordered["top_to_bottom_min"].gt(0)].copy()
    ordered["log_top_to_bottom"] = np.log(ordered["top_to_bottom_min"])
    ordered["log_top_arrival"] = np.log(ordered["arrival_T_min"].where(ordered["arrival_T_min"].gt(0)))
    ordered["log_top_to_middle"] = np.log(ordered["top_to_middle_min"].where(ordered["top_to_middle_min"].gt(0)))
    ordered["log_middle_to_bottom"] = np.log(ordered["middle_to_bottom_min"].where(ordered["middle_to_bottom_min"].gt(0)))

    models = {}
    for outcome in ["log_top_arrival", "log_top_to_middle", "log_middle_to_bottom", "log_top_to_bottom"]:
        d = ordered.dropna(subset=[outcome, "log_flow", "strip", "year"])
        if len(d) >= 12:
            models[outcome] = robust_model(outcome, d)
    strict = ordered.loc[ordered["strict_ordered_arrivals"]].copy()
    if len(strict) >= 12:
        models["log_top_to_bottom_strict"] = robust_model("log_top_to_bottom", strict)
    for regime, regime_data in ordered.groupby("irrigation_regime"):
        if len(regime_data) >= 12:
            models[f"log_top_to_bottom_{regime}"] = robust_model(
                "log_top_to_bottom", regime_data
            )

    residual = event.loc[event["unretained_eligible"].eq(True)].copy()
    residual["unretained_pct"] = 100 * residual["unretained_fraction"]
    if len(residual) >= 12:
        models["unretained_fraction_raw"] = robust_model("unretained_fraction", residual)
        models["storage_fraction_raw"] = robust_model("estimated_storage_fraction_0_18in", residual)
        # Holding total event volume constant reduces the built-in mathematical
        # coupling caused by flow = gallons / duration and fraction = storage / gallons.
        models["unretained_fraction_volume_adjusted"] = robust_model(
            "unretained_fraction", residual, ["log_flow", "log_gallons"]
        )
        models["storage_fraction_volume_adjusted"] = robust_model(
            "estimated_storage_fraction_0_18in", residual, ["log_flow", "log_gallons"]
        )
        positive_storage = residual.loc[residual["estimated_storage_gal_strip_0_18in"].gt(0)].copy()
        positive_storage["log_storage_gallons"] = np.log(positive_storage["estimated_storage_gal_strip_0_18in"])
        models["absolute_storage_volume_adjusted"] = robust_model(
            "log_storage_gallons", positive_storage, ["log_flow", "log_gallons"]
        )
        for regime, regime_data in residual.groupby("irrigation_regime"):
            if len(regime_data) >= 12:
                models[f"storage_fraction_volume_adjusted_{regime}"] = robust_model(
                    "estimated_storage_fraction_0_18in",
                    regime_data,
                    ["log_flow", "log_gallons"],
                )

    top_middle = ordered.loc[ordered["top_to_middle_min"].gt(0)]
    middle_bottom = ordered.loc[ordered["middle_to_bottom_min"].gt(0)]
    correlations = {
        "flow_vs_top_arrival": corr(ordered["avg_flow_gph_strip"], ordered["arrival_T_min"]),
        "flow_vs_top_to_middle": corr(top_middle["avg_flow_gph_strip"], top_middle["top_to_middle_min"]),
        "flow_vs_middle_to_bottom": corr(middle_bottom["avg_flow_gph_strip"], middle_bottom["middle_to_bottom_min"]),
        "flow_vs_top_to_bottom": corr(ordered["avg_flow_gph_strip"], ordered["top_to_bottom_min"]),
        "flow_vs_unretained_fraction": corr(residual["avg_flow_gph_strip"], residual["unretained_fraction"]),
        "flow_vs_storage_fraction": corr(residual["avg_flow_gph_strip"], residual["estimated_storage_fraction_0_18in"]),
    }

    # Pair strips by shared event ID for treatment contrasts under the same irrigation event.
    pair_rows = []
    for pair, bio, ctrl in [("S1_S2", "S1", "S2"), ("S3_S4", "S3", "S4")]:
        p = event.loc[event["pair"].eq(pair)].set_index(["year", "event_id", "strip"])
        for (year, eid), g in p.groupby(level=[0, 1]):
            strips = set(g.index.get_level_values("strip"))
            if not {bio, ctrl}.issubset(strips):
                continue
            b, c = g.xs(bio, level="strip").iloc[0], g.xs(ctrl, level="strip").iloc[0]
            row = {"year": year, "event_id": eid, "pair": pair, "biochar_strip": bio, "control_strip": ctrl,
                   "irrigation_regime": b.irrigation_regime,
                   "flow_gph": np.nanmean([b.avg_flow_gph_strip, c.avg_flow_gph_strip])}
            for col in ["arrival_T_min", "top_to_middle_min", "middle_to_bottom_min", "top_to_bottom_min",
                        "unretained_fraction", "unretained_gal_strip",
                        "estimated_storage_fraction_0_18in"]:
                row[f"bio_{col}"] = b[col]
                row[f"ctrl_{col}"] = c[col]
                row[f"diff_{col}"] = b[col] - c[col]
            row["both_arrival_model_eligible"] = bool(b.arrival_model_eligible and c.arrival_model_eligible)
            row["both_unretained_eligible"] = bool(b.unretained_eligible == True and c.unretained_eligible == True)
            pair_rows.append(row)
    paired = pd.DataFrame(pair_rows)

    paired_stats = {}
    for pair, g in paired.groupby("pair"):
        paired_stats[pair] = {}
        for col, eligibility in [
            ("diff_top_to_bottom_min", "both_arrival_model_eligible"),
            ("diff_unretained_fraction", "both_unretained_eligible"),
            ("diff_unretained_gal_strip", "both_unretained_eligible"),
        ]:
            x = g.loc[g[eligibility].eq(True), col].dropna()
            if len(x):
                ci = bootstrap_ci(x)
                paired_stats[pair][col] = {
                    "n": len(x), "mean": float(x.mean()), "median": float(x.median()),
                    "bootstrap_ci_low": float(ci[0]), "bootstrap_ci_high": float(ci[1]),
                    "sign_flip_p": sign_flip_p(x),
                }

    # Does flow modify the within-event biochar-control residual difference?
    interaction_models = {}
    for pair, g in paired.loc[paired["both_unretained_eligible"].eq(True)].groupby("pair"):
        g = g.dropna(subset=["flow_gph", "diff_unretained_fraction"]).copy()
        if len(g) >= 5 and g["flow_gph"].nunique() >= 3:
            g["log_flow"] = np.log(g["flow_gph"])
            X = np.column_stack([np.ones(len(g)), g["log_flow"]])
            y = g["diff_unretained_fraction"].to_numpy(float)
            beta = np.linalg.lstsq(X, y, rcond=None)[0]
            pred = X @ beta
            interaction_models[pair] = {"n": len(g), "coef": float(beta[1]),
                                        "r2": float(1-np.sum((y-pred)**2)/np.sum((y-y.mean())**2))}

    event.to_csv(OUT / "flow_arrival_event_analysis.csv", index=False)
    paired.to_csv(OUT / "flow_arrival_paired_analysis.csv", index=False)
    irrigation = pd.read_csv(
        REPO / "biochar_app/data-processed/management/irrigation/irrigation_clean.csv"
    ).drop_duplicates("event_id")
    note_mask = irrigation["notes"].fillna("").str.contains(
        r"run.?off|ran off|end of field|flood|onto", case=False, regex=True
    )
    irrigation.loc[note_mask, [
        "year", "date", "event_id", "strip_group", "gallons_group",
        "avg_flow_gpm_group", "avg_flow_gph_strip", "event_duration_hours", "notes"
    ]].to_csv(OUT / "field_runoff_notes.csv", index=False)
    result = {
        "counts": {
            "event_rows": len(event),
            "complete_arrival_rows": int(event[["arrival_T_min", "arrival_M_min", "arrival_B_min"]].notna().all(axis=1).sum()),
            "top_to_bottom_ordered_rows": int(event["top_to_bottom_ordered"].sum()),
            "strict_ordered_arrival_rows": int((event["strict_ordered_arrivals"] & event["T_to_B_min"].gt(0)).sum()),
            "arrival_model_eligible_rows": len(ordered),
            "arrival_model_event_clusters": int(ordered["event_id"].nunique()),
            "unretained_residual_rows": len(residual),
            "eof_proxy_rows": int(event["end_of_field_unretained_eligible"].eq(True).sum()),
            "arrival_model_eligible_by_strip": ordered["strip"].value_counts().sort_index().to_dict(),
            "arrival_model_eligible_by_irrigation_regime": ordered["irrigation_regime"].value_counts().sort_index().to_dict(),
            "unretained_by_strip": residual["strip"].value_counts().sort_index().to_dict(),
            "s3_complete_arrival_rows": int((event["strip"].eq("S3") & event[["arrival_T_min", "arrival_M_min", "arrival_B_min"]].notna().all(axis=1)).sum()),
            "s3_top_to_bottom_ordered_rows": int((event["strip"].eq("S3") & event["top_to_bottom_ordered"]).sum()),
            "s3_arrival_model_eligible_rows": int((event["strip"].eq("S3") & event["arrival_model_eligible"]).sum()),
            "s3_strict_ordered_rows": int((event["strip"].eq("S3") & event["strict_ordered_arrivals"] & event["T_to_B_min"].gt(0)).sum()),
        },
        "correlations": correlations,
        "models": models,
        "paired_stats": paired_stats,
        "flow_modifies_pair_difference": interaction_models,
    }
    (OUT / "flow_infiltration_results.json").write_text(json.dumps(result, indent=2, default=float))

    tb = models["log_top_to_bottom"]
    tm = models["log_top_to_middle"]
    mb = models["log_middle_to_bottom"]
    ta = models["log_top_arrival"]
    sf = models["storage_fraction_volume_adjusted"]
    uf = models["unretained_fraction_volume_adjusted"]
    av = models["absolute_storage_volume_adjusted"]

    def pct_change(model: dict) -> tuple[float, float, float]:
        scale = math.log(2)
        return tuple(100 * (math.exp(model[key] * scale) - 1)
                     for key in ("log_flow_coef", "log_flow_ci_low", "log_flow_ci_high"))

    def point_change(model: dict) -> tuple[float, float, float]:
        scale = 100 * math.log(2)
        return tuple(model[key] * scale
                     for key in ("log_flow_coef", "log_flow_ci_low", "log_flow_ci_high"))

    tb_pct, tb_lo, tb_hi = pct_change(tb)
    tm_pct, tm_lo, tm_hi = pct_change(tm)
    mb_pct, mb_lo, mb_hi = pct_change(mb)
    ta_pct, ta_lo, ta_hi = pct_change(ta)
    sf_pt, sf_lo, sf_hi = point_change(sf)
    uf_pt, uf_lo, uf_hi = point_change(uf)
    av_pct, av_lo, av_hi = pct_change(av)
    s12 = paired_stats["S1_S2"]["diff_top_to_bottom_min"]
    s12_res = paired_stats["S1_S2"]["diff_unretained_fraction"]
    notes_n = int(note_mask.sum())

    legacy_report_template = """# Flow rate, wetting-front advance, and the unresolved water balance

## Bottom line

This analysis was rebuilt from scratch from the corrected 2023–2026 canonical irrigation outputs. It does not use the earlier flow-analysis tables or statistics as inputs.

The corrected data still support one clear result: higher recorded pair-level flow is associated with faster wetting-front advance down the field. A doubling of flow is associated with an estimated **{abs(tb_pct):.0f}% shorter top-to-bottom travel time** (95% interval: {abs(tb_hi):.0f}% to {abs(tb_lo):.0f}% shorter; event-clustered normal-approximation p={tb['log_flow_p_normal_approx']:.3f}). The association is strongest from the top to the middle of the field.

Higher flow is also associated with less water remaining in the sensor-monitored profile after holding total applied volume constant. That finding is useful, but the complementary water-balance residual is **not measured runoff**. It can contain tailwater, deep percolation below the monitored profile, lateral redistribution, continued infiltration, measurement error, and errors in the assumed equal split of pair-level flow.

## Data and eligibility

- The source is the corrected irrigation horizontal-advance summaries, the canonical first-pass water balance, and the cleaned irrigation-management record.
- The analysis contains {len(event)} strip-events. Of these, {result['counts']['complete_arrival_rows']} have all three 6-inch arrival estimates, {result['counts']['physically_ordered_arrival_rows']} have a physically ordered top→middle→bottom sequence, and {len(ordered)} also pass the canonical event-quality rule. The main arrival models use those {len(ordered)} records from {result['counts']['arrival_model_event_clusters']} irrigation events.
- Main arrival records by strip: S1={result['counts']['ordered_by_strip'].get('S1', 0)}, S2={result['counts']['ordered_by_strip'].get('S2', 0)}, S3={result['counts']['ordered_by_strip'].get('S3', 0)}, and S4={result['counts']['ordered_by_strip'].get('S4', 0)}.
- S3 has {result['counts']['s3_complete_arrival_rows']} complete arrival records but **zero** physically ordered records. Correcting the known logger-clock offsets therefore did not resolve the S3 reversal. S3 is excluded from arrival-time models, not relabeled or reordered after the fact.
- The water-balance analysis uses {len(residual)} records that pass the canonical unretained-water eligibility rule. A stricter end-of-field proxy exists for {result['counts']['eof_proxy_rows']} records, but it is not promoted to observed runoff.

## Wetting-front movement

Models use log flow, strip and year fixed effects, and event-clustered uncertainty estimates because paired strips share the same meter-derived flow.

- Top→middle: doubling flow is associated with **{abs(tm_pct):.0f}% shorter** travel time (95% interval {abs(tm_hi):.0f}% to {abs(tm_lo):.0f}% shorter; p<{0.001:.3f}).
- Middle→bottom: estimated **{abs(mb_pct):.0f}% shorter**, but highly uncertain (95% interval ranges from {abs(mb_lo):.0f}% shorter to {mb_hi:.0f}% longer; p={mb['log_flow_p_normal_approx']:.3f}).
- Top→bottom: estimated **{abs(tb_pct):.0f}% shorter** (95% interval {abs(tb_hi):.0f}% to {abs(tb_lo):.0f}% shorter; p={tb['log_flow_p_normal_approx']:.3f}).
- Arrival at the top logger: estimated **{abs(ta_pct):.0f}% earlier**, but the interval includes no association (p={ta['log_flow_p_normal_approx']:.3f}).

The corresponding unadjusted Spearman correlation for top→bottom travel time is {correlations['flow_vs_top_to_bottom']['spearman_rho']:.3f} (permutation p={correlations['flow_vs_top_to_bottom']['spearman_p_permutation']:.3f}). Together, these results support faster surface advance at higher flow; they do not by themselves measure infiltration or runoff.

## Monitored storage and residual water

The canonical storage estimate represents change in the monitored soil profile. In models holding total applied gallons, strip, and year constant:

- Doubling flow is associated with **{abs(sf_pt):.1f} percentage points less monitored storage** (95% interval {abs(sf_hi):.1f} to {abs(sf_lo):.1f} points less; p<{0.001:.3f}).
- The complementary unretained fraction is **{uf_pt:.1f} percentage points larger** (95% interval {uf_lo:.1f} to {uf_hi:.1f} points; p<{0.001:.3f}).
- As a check that the fraction result is not only denominator arithmetic, absolute monitored storage is estimated to be **{abs(av_pct):.1f}% lower** when flow doubles at fixed applied volume (95% interval {abs(av_hi):.1f}% to {abs(av_lo):.1f}% lower; p<{0.001:.3f}).

These are observational associations. Flow is derived from gallons divided by duration, and gallons are measured for a strip pair rather than for each strip. The models reduce, but cannot eliminate, mathematical coupling and confounding.

## Biochar-control contrasts

There are {s12['n']} S1–S2 paired events where both strips pass the arrival criteria. S1’s top-to-bottom travel time is, on average, **{s12['mean']:.0f} minutes longer** than S2’s (bootstrap 95% interval {s12['bootstrap_ci_low']:.0f} to {s12['bootstrap_ci_high']:.0f} minutes; sign-randomization p={s12['sign_flip_p']:.3f}). This is consistent with slower advance in S1, but unequal gate-level flow remains a plausible alternative explanation.

In {s12_res['n']} eligible S1–S2 water-balance pairs, S1 has an average **{100*s12_res['mean']:.1f}-percentage-point larger residual** than S2. That combination—slower advance but a larger residual—is direct evidence that the residual cannot safely be called runoff. It may reflect additional water movement below or outside the monitored profile, unequal water delivery, or other balance errors.

No defensible S3–S4 arrival contrast is reported because S3 has no eligible ordered arrival records.

## Field observations

Only {notes_n} distinct management records contain runoff, field-end, flooding, or cross-strip language. The May 23, 2023 note is the strongest qualitative case: it says S1 did not really run off the field, while S2 ran off after about 1.5 hours and flooded. These notes support using observed tailwater as a separate validation outcome, but they are too sparse to estimate runoff for all events.

## Interpretation and next measurements

What the present data support:

1. Higher pair-level flow is associated with faster horizontal wetting-front advance.
2. At a fixed recorded application volume, higher flow is associated with less monitored-profile storage.
3. S1 generally advances more slowly than S2 in the small set of mutually eligible paired events.

What the present data do not establish:

1. The volume that ran off the end of the field.
2. The volume that percolated below the deepest monitored layer.
3. Causality, because flow was not randomized and actual strip/furrow flow was not measured.
4. A valid S3–S4 arrival comparison.

The most informative next field measurements are strip-specific flow and tailwater at the field end. Deep drainage would require sensors or lysimetry below the existing monitored profile, or a defensible calibrated drainage model. Until then, report monitored storage and residual water separately and label runoff only when it was directly observed.

## Reproducibility

The event table, paired-event table, field-note subset, and machine-readable model results alongside this report were regenerated by `biochar_app/scripts/management/irrigation_analysis/analyze_flow_infiltration.py`. The canonical source files were read only and were not modified.
"""
    report = build_report(result, correlations, models, paired_stats, notes_n)
    report_name = "irrigation_flow_water_balance_report.md"
    (OUT / report_name).write_text(report)
    (OUT / "README.md").write_text(
        "# Irrigation flow and water-balance research outputs\n\n"
        f"The complete analysis report is [{report_name}]({report_name}).\n\n"
        "Supporting files:\n\n"
        "- `flow_arrival_event_analysis.csv`: event-level arrival and water-balance data\n"
        "- `flow_arrival_paired_analysis.csv`: matched strip-pair comparisons\n"
        "- `field_runoff_notes.csv`: management notes relevant to runoff and field-end flow\n"
        "- `flow_infiltration_results.json`: machine-readable model results and diagnostics\n"
        "- `zone_capacity_summary.csv`: modeled upper retained-water capacity by strip zone\n"
        "- `typical_event_summary.csv`: median and middle-50% water accounting by strip\n\n"
        "Research figure:\n\n"
        "- `zone_capacity_field_layout.png`: nominal 47-ft logger zones shaded by modeled capacity\n\n"
        "Regenerate all seven outputs with "
        "`python biochar_app/scripts/management/irrigation_analysis/analyze_flow_infiltration.py`. "
        "Regenerate the map with "
        "`python biochar_app/scripts/management/irrigation_analysis/plot_zone_capacity_field_layout.py`.\n"
    )

    print(json.dumps(result, indent=2, default=float))


if __name__ == "__main__":
    main()

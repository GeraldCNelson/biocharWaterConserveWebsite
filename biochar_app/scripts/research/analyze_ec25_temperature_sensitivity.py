#!/usr/bin/env python3
"""Assess CS650 bulk-EC normalization to 25 degrees Celsius.

This is a research-only sensitivity analysis.  It never rewrites the source
parquets or the published EC columns.  Campbell Scientific describes the
generic conversion as::

    EC25 = EC_T / (1 + 0.02 * (T_soil_C - 25))

The canonical logger parquets store soil temperature in degrees Fahrenheit,
despite retaining the logger-style ``T_*_raw_*`` names.  This script converts
those values back to Celsius before applying the equation.

Two derived series are written:

* ``EC25_sensitivity_*`` applies the formula whenever EC and temperature are
  finite and the denominator is positive.  Frozen observations are retained
  only so their influence can be inspected explicitly.
* ``EC25_nonfreezing_*`` additionally requires soil temperature > 0 degrees C
  and is the primary assessment series.

The 2%/degree-C coefficient is not a site-specific calibration.  CS650 EC is
bulk soil EC, not pore-water EC or laboratory extract EC, and the result still
depends strongly on water content.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib
import numpy as np
import pandas as pd


matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PARQUET_DIR = ROOT / "biochar_app/data-processed/parquet/summary/15min"
DEFAULT_OUTPUT_DIR = (
    ROOT / "biochar_app/data-processed/research/ec25_temperature_sensitivity"
)

YEARS = (2023, 2024, 2025, 2026)
STRIPS = ("S1", "S2", "S3", "S4")
DEPTHS = (1, 2, 3)
POSITIONS = ("T", "M", "B")
PAIRS = {"S1/S2": ("S1", "S2"), "S3/S4": ("S3", "S4")}
SCOPES = ("all_formula_valid", "nonfreezing")

TEMPERATURE_COEFFICIENT_PER_C = 0.02
REFERENCE_TEMPERATURE_C = 25.0
FREEZING_THRESHOLD_C = 0.0

CAMPBELL_EC_GUIDANCE_URL = (
    "https://help.campbellsci.com/cs650-cs655/cs650-cs655/operation/"
    "temp-correction-soil-electric-conduct.htm"
)
CAMPBELL_VWC_GUIDANCE_URL = (
    "https://help.campbellsci.com/cs650-cs655/cs650-cs655/operation/"
    "temp-dependence-correction.htm"
)


@dataclass(frozen=True)
class Sensor:
    strip: str
    depth: int
    position: str

    @property
    def ec_column(self) -> str:
        return f"EC_{self.depth}_raw_{self.strip}_{self.position}"

    @property
    def temperature_column(self) -> str:
        return f"T_{self.depth}_raw_{self.strip}_{self.position}"

    @property
    def suffix(self) -> str:
        return f"{self.depth}_{self.strip}_{self.position}"


def sensors() -> Iterable[Sensor]:
    for strip in STRIPS:
        for position in POSITIONS:
            for depth in DEPTHS:
                yield Sensor(strip=strip, depth=depth, position=position)


def fahrenheit_to_celsius(values: pd.Series) -> pd.Series:
    return (pd.to_numeric(values, errors="coerce") - 32.0) * (5.0 / 9.0)


def season_for_month(month: pd.Series) -> pd.Series:
    """Use the repository's Apr-Oct growing / Nov-Mar winter convention."""
    return pd.Series(
        np.where(month.between(4, 10), "growing_apr_oct", "winter_nov_mar"),
        index=month.index,
        dtype="string",
    )


def correction_denominator(temperature_c: pd.Series) -> pd.Series:
    return 1.0 + TEMPERATURE_COEFFICIENT_PER_C * (
        temperature_c - REFERENCE_TEMPERATURE_C
    )


def formula_valid_mask(
    ec: pd.Series, temperature_c: pd.Series, denominator: pd.Series
) -> pd.Series:
    return (
        np.isfinite(ec)
        & np.isfinite(temperature_c)
        & np.isfinite(denominator)
        & denominator.gt(0)
    )


def scope_mask(valid: pd.Series, temperature_c: pd.Series, scope: str) -> pd.Series:
    if scope == "all_formula_valid":
        return valid
    if scope == "nonfreezing":
        return valid & temperature_c.gt(FREEZING_THRESHOLD_C)
    raise ValueError(f"Unknown scope: {scope}")


def safe_ratio(numerator: float, denominator: float) -> float:
    if not np.isfinite(numerator) or not np.isfinite(denominator) or denominator <= 0:
        return float("nan")
    return float(numerator / denominator)


def exact_direction(value: float) -> str:
    if not np.isfinite(value):
        return "not_available"
    if value > 1.0:
        return "treated_higher"
    if value < 1.0:
        return "treated_lower"
    return "equal"


def descriptive_band(value: float) -> str:
    """Descriptive only; the 5% band is not an uncertainty interval."""
    if not np.isfinite(value):
        return "not_available"
    if value >= 1.05:
        return "treated_at_least_5pct_higher"
    if value <= 0.95:
        return "treated_at_least_5pct_lower"
    return "within_5pct_of_parity"


def quantile(values: pd.Series, probability: float) -> float:
    finite = pd.to_numeric(values, errors="coerce")
    finite = finite[np.isfinite(finite)]
    if finite.empty:
        return float("nan")
    return float(finite.quantile(probability))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format="%.10g")


def load_year(path: Path) -> pd.DataFrame:
    wanted = ["timestamp"]
    for sensor in sensors():
        wanted.extend([sensor.ec_column, sensor.temperature_column])

    frame = pd.read_parquet(path, columns=wanted)
    missing = sorted(set(wanted).difference(frame.columns))
    if missing:
        raise ValueError(f"{path} is missing required columns: {', '.join(missing)}")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    if frame["timestamp"].isna().any():
        count = int(frame["timestamp"].isna().sum())
        raise ValueError(f"{path} contains {count} invalid timestamps")
    frame["season"] = season_for_month(frame["timestamp"].dt.month)
    frame["year_month"] = frame["timestamp"].dt.to_period("M").astype(str)
    return frame


def derived_sensor_values(frame: pd.DataFrame, sensor: Sensor) -> dict[str, pd.Series]:
    ec = pd.to_numeric(frame[sensor.ec_column], errors="coerce")
    temperature_c = fahrenheit_to_celsius(frame[sensor.temperature_column])
    denominator = correction_denominator(temperature_c)
    valid = formula_valid_mask(ec, temperature_c, denominator)
    factor = pd.Series(np.nan, index=frame.index, dtype="float64")
    factor.loc[valid] = 1.0 / denominator.loc[valid]
    ec25 = pd.Series(np.nan, index=frame.index, dtype="float64")
    ec25.loc[valid] = ec.loc[valid] * factor.loc[valid]
    return {
        "ec": ec,
        "temperature_c": temperature_c,
        "denominator": denominator,
        "valid": valid,
        "factor": factor,
        "ec25": ec25,
    }


def sensor_coverage_rows(
    frame: pd.DataFrame,
    year: int,
    sensor: Sensor,
    values: dict[str, pd.Series],
) -> dict[str, object]:
    ec = values["ec"]
    temperature_c = values["temperature_c"]
    denominator = values["denominator"]
    finite_ec = np.isfinite(ec)
    finite_temperature = np.isfinite(temperature_c)
    paired_finite = finite_ec & finite_temperature
    valid = values["valid"]
    freezing = valid & temperature_c.le(FREEZING_THRESHOLD_C)
    nonfreezing = valid & temperature_c.gt(FREEZING_THRESHOLD_C)
    timestamps = frame.loc[valid, "timestamp"]
    return {
        "year": year,
        "strip": sensor.strip,
        "depth": sensor.depth,
        "position": sensor.position,
        "source_rows": len(frame),
        "finite_ec_rows": int(finite_ec.sum()),
        "finite_temperature_rows": int(finite_temperature.sum()),
        "finite_paired_rows": int(paired_finite.sum()),
        "missing_ec_or_temperature_rows": int((~paired_finite).sum()),
        "nonpositive_denominator_rows": int((paired_finite & denominator.le(0)).sum()),
        "nonfinite_denominator_rows": int((paired_finite & ~np.isfinite(denominator)).sum()),
        "formula_valid_rows": int(valid.sum()),
        "formula_valid_fraction_of_source_rows": float(valid.mean()),
        "freezing_formula_valid_rows": int(freezing.sum()),
        "nonfreezing_formula_valid_rows": int(nonfreezing.sum()),
        "first_formula_valid_timestamp": timestamps.min(),
        "last_formula_valid_timestamp": timestamps.max(),
        "minimum_temperature_c": float(temperature_c.loc[valid].min()) if valid.any() else np.nan,
        "maximum_temperature_c": float(temperature_c.loc[valid].max()) if valid.any() else np.nan,
    }


def sensor_summary_row(
    frame: pd.DataFrame,
    year: int,
    sensor: Sensor,
    values: dict[str, pd.Series],
    season: str,
    scope: str,
) -> dict[str, object]:
    in_season = frame["season"].eq(season)
    selected = in_season & scope_mask(values["valid"], values["temperature_c"], scope)
    ec = values["ec"].loc[selected]
    ec25 = values["ec25"].loc[selected]
    temperature_c = values["temperature_c"].loc[selected]
    factor = values["factor"].loc[selected]
    measured_mean = float(ec.mean()) if not ec.empty else np.nan
    ec25_mean = float(ec25.mean()) if not ec25.empty else np.nan
    return {
        "year": year,
        "season": season,
        "scope": scope,
        "strip": sensor.strip,
        "depth": sensor.depth,
        "position": sensor.position,
        "source_rows_in_season": int(in_season.sum()),
        "valid_rows": int(selected.sum()),
        "coverage_fraction_in_season": float(selected.sum() / in_season.sum())
        if in_season.any()
        else np.nan,
        "first_valid_timestamp": frame.loc[selected, "timestamp"].min(),
        "last_valid_timestamp": frame.loc[selected, "timestamp"].max(),
        "temperature_c_mean": float(temperature_c.mean()) if not temperature_c.empty else np.nan,
        "temperature_c_p05": quantile(temperature_c, 0.05),
        "temperature_c_median": quantile(temperature_c, 0.50),
        "temperature_c_p95": quantile(temperature_c, 0.95),
        "correction_factor_mean": float(factor.mean()) if not factor.empty else np.nan,
        "correction_factor_p05": quantile(factor, 0.05),
        "correction_factor_median": quantile(factor, 0.50),
        "correction_factor_p95": quantile(factor, 0.95),
        "ec_measured_mean_ds_m": measured_mean,
        "ec25_mean_ds_m": ec25_mean,
        "ec25_mean_change_pct": 100.0 * safe_ratio(ec25_mean, measured_mean) - 100.0,
    }


def pair_group_row(
    frame: pd.DataFrame,
    treated: dict[str, pd.Series],
    control: dict[str, pd.Series],
    group_mask: pd.Series,
    scope: str,
) -> dict[str, object]:
    selected = (
        group_mask
        & scope_mask(treated["valid"], treated["temperature_c"], scope)
        & scope_mask(control["valid"], control["temperature_c"], scope)
    )

    measured_treated = float(treated["ec"].loc[selected].mean()) if selected.any() else np.nan
    measured_control = float(control["ec"].loc[selected].mean()) if selected.any() else np.nan
    ec25_treated = float(treated["ec25"].loc[selected].mean()) if selected.any() else np.nan
    ec25_control = float(control["ec25"].loc[selected].mean()) if selected.any() else np.nan
    measured_ratio = safe_ratio(measured_treated, measured_control)
    corrected_ratio = safe_ratio(ec25_treated, ec25_control)
    measured_direction = exact_direction(measured_ratio)
    corrected_direction = exact_direction(corrected_ratio)
    measured_band = descriptive_band(measured_ratio)
    corrected_band = descriptive_band(corrected_ratio)

    return {
        "matched_rows": int(selected.sum()),
        "first_matched_timestamp": frame.loc[selected, "timestamp"].min(),
        "last_matched_timestamp": frame.loc[selected, "timestamp"].max(),
        "treated_temperature_c_mean": float(treated["temperature_c"].loc[selected].mean())
        if selected.any()
        else np.nan,
        "control_temperature_c_mean": float(control["temperature_c"].loc[selected].mean())
        if selected.any()
        else np.nan,
        "treated_minus_control_temperature_c_mean": float(
            (
                treated["temperature_c"].loc[selected]
                - control["temperature_c"].loc[selected]
            ).mean()
        )
        if selected.any()
        else np.nan,
        "treated_correction_factor_mean": float(treated["factor"].loc[selected].mean())
        if selected.any()
        else np.nan,
        "control_correction_factor_mean": float(control["factor"].loc[selected].mean())
        if selected.any()
        else np.nan,
        "treated_ec_measured_mean_ds_m": measured_treated,
        "control_ec_measured_mean_ds_m": measured_control,
        "treated_ec25_mean_ds_m": ec25_treated,
        "control_ec25_mean_ds_m": ec25_control,
        "measured_ratio_of_means": measured_ratio,
        "ec25_ratio_of_means": corrected_ratio,
        "ratio_of_means_change_pct": 100.0 * safe_ratio(corrected_ratio, measured_ratio) - 100.0,
        "measured_exact_direction": measured_direction,
        "ec25_exact_direction": corrected_direction,
        "exact_direction_changed": measured_direction != corrected_direction,
        "measured_descriptive_5pct_band": measured_band,
        "ec25_descriptive_5pct_band": corrected_band,
        "descriptive_5pct_band_changed": measured_band != corrected_band,
    }


def instantaneous_ratio_row(
    treated: dict[str, pd.Series],
    control: dict[str, pd.Series],
    base_mask: pd.Series,
    scope: str,
) -> dict[str, object]:
    selected = (
        base_mask
        & scope_mask(treated["valid"], treated["temperature_c"], scope)
        & scope_mask(control["valid"], control["temperature_c"], scope)
    )
    positive = selected & treated["ec"].gt(0) & control["ec"].gt(0)
    measured = treated["ec"].loc[positive] / control["ec"].loc[positive]
    corrected = treated["ec25"].loc[positive] / control["ec25"].loc[positive]
    return {
        "matched_formula_rows": int(selected.sum()),
        "positive_ec_rows": int(positive.sum()),
        "nonpositive_ec_rows_excluded": int((selected & ~positive).sum()),
        "measured_instantaneous_ratio_mean": float(measured.mean()) if not measured.empty else np.nan,
        "measured_instantaneous_ratio_median": quantile(measured, 0.50),
        "measured_instantaneous_ratio_p05": quantile(measured, 0.05),
        "measured_instantaneous_ratio_p95": quantile(measured, 0.95),
        "ec25_instantaneous_ratio_mean": float(corrected.mean()) if not corrected.empty else np.nan,
        "ec25_instantaneous_ratio_median": quantile(corrected, 0.50),
        "ec25_instantaneous_ratio_p05": quantile(corrected, 0.05),
        "ec25_instantaneous_ratio_p95": quantile(corrected, 0.95),
    }


def plot_monthly_ratios(monthly: pd.DataFrame, output_path: Path) -> None:
    data = monthly.loc[monthly["scope"].eq("nonfreezing")].copy()
    data["month"] = pd.to_datetime(data["year_month"] + "-01")
    fig, axes = plt.subplots(3, 2, figsize=(15, 11), sharex=True)
    colors = {"T": "#1b9e77", "M": "#d95f02", "B": "#7570b3"}
    position_labels = {"T": "Top", "M": "Middle", "B": "Bottom"}
    for row, depth in enumerate(DEPTHS):
        for column, pair in enumerate(PAIRS):
            ax = axes[row, column]
            subset = data.loc[data["pair"].eq(pair) & data["depth"].eq(depth)]
            for position in POSITIONS:
                line = subset.loc[subset["position"].eq(position)].sort_values("month")
                ax.plot(
                    line["month"],
                    line["measured_ratio_of_means"],
                    color=colors[position],
                    linewidth=1.2,
                    alpha=0.75,
                    label=f"{position_labels[position]} measured"
                    if row == 0 and column == 0
                    else None,
                )
                ax.plot(
                    line["month"],
                    line["ec25_ratio_of_means"],
                    color=colors[position],
                    linewidth=1.4,
                    linestyle="--",
                    label=f"{position_labels[position]} EC25"
                    if row == 0 and column == 0
                    else None,
                )
            ax.axhline(1.0, color="#444444", linewidth=0.8, linestyle=":")
            ax.set_title(f"{pair}, depth {depth}")
            ax.set_ylabel("Treated/control ratio of means")
            ax.grid(alpha=0.2)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.965),
        ncol=6,
        frameon=False,
    )
    fig.suptitle(
        "Monthly measured and EC25 bulk-EC ratios (nonfreezing matched observations)",
        y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_ratio_change_heatmap(seasonal: pd.DataFrame, output_path: Path) -> None:
    data = seasonal.loc[seasonal["scope"].eq("nonfreezing")].copy()
    panels = [(pair, season) for pair in PAIRS for season in ("growing_apr_oct", "winter_nov_mar")]
    fig, axes = plt.subplots(2, 2, figsize=(14, 9), constrained_layout=True)
    row_labels = [f"D{depth}-{position}" for depth in DEPTHS for position in POSITIONS]
    limit = max(1.0, float(data["ratio_of_means_change_pct"].abs().max()))
    image = None
    for ax, (pair, season) in zip(axes.flat, panels):
        subset = data.loc[data["pair"].eq(pair) & data["season"].eq(season)].copy()
        subset["sensor"] = "D" + subset["depth"].astype(str) + "-" + subset["position"]
        pivot = subset.pivot(index="sensor", columns="year", values="ratio_of_means_change_pct")
        pivot = pivot.reindex(index=row_labels, columns=list(YEARS))
        image = ax.imshow(pivot, cmap="RdBu_r", vmin=-limit, vmax=limit, aspect="auto")
        ax.set_xticks(range(len(YEARS)), YEARS)
        ax.set_yticks(range(len(row_labels)), row_labels)
        ax.set_title(f"{pair} — {season.replace('_', ' ')}")
        for y in range(len(row_labels)):
            for x in range(len(YEARS)):
                value = pivot.iloc[y, x]
                if np.isfinite(value):
                    ax.text(x, y, f"{value:+.1f}%", ha="center", va="center", fontsize=7)
    fig.suptitle("Effect of EC25 normalization on seasonal pair ratios", fontsize=14)
    if image is not None:
        fig.colorbar(
            image,
            ax=axes.ravel().tolist(),
            shrink=0.80,
            label="EC25 change in pair ratio (%)",
        )
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_correction_factor_monthly(series: pd.DataFrame, output_path: Path) -> None:
    data = series.loc[series["scope"].eq("nonfreezing")].copy()
    data["month"] = pd.to_datetime(data["year_month"] + "-01")
    grouped = data.groupby("month")["correction_factor_mean"].agg(
        median="median", low=lambda x: x.quantile(0.05), high=lambda x: x.quantile(0.95)
    )
    fig, ax = plt.subplots(figsize=(13, 4.5))
    ax.fill_between(grouped.index, grouped["low"], grouped["high"], color="#80cdc1", alpha=0.35)
    ax.plot(grouped.index, grouped["median"], color="#01665e", linewidth=1.8)
    ax.axhline(1.0, color="#444444", linewidth=0.8, linestyle=":")
    ax.set_ylabel("EC25 / measured EC factor")
    ax.set_title("Monthly correction-factor pattern across sensors (nonfreezing)")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_growing_season_6in_attached_metric(
    focused: pd.DataFrame, output_path: Path
) -> None:
    """Reproduce the attached EC estimator before and after normalization.

    The dashboard's EC comparison chart uses the mean of simultaneous,
    positive 15-minute treated/control ratios.  This is intentionally distinct
    from the ratio-of-means estimator used for the principal pair assessment.
    """
    data = focused.loc[focused["scope"].eq("all_formula_valid")].copy()
    data["position_label"] = data["position"].map(
        {"T": "Top", "M": "Middle", "B": "Bottom"}
    )
    data["category"] = (
        data["year"].astype(str) + " · " + data["position_label"].astype(str)
    )
    category_order = [
        f"{year} · {position}"
        for year in YEARS
        for position in ("Top", "Middle", "Bottom")
    ]
    data["temperature_change_pct"] = (
        100.0
        * data["ec25_instantaneous_ratio_mean"]
        / data["measured_instantaneous_ratio_mean"]
        - 100.0
    )

    colors = {"S1/S2": "#3f8fc1", "S3/S4": "#df7f3f"}
    x = np.arange(len(category_order), dtype=float)
    width = 0.36
    fig, axes = plt.subplots(
        3,
        1,
        figsize=(18, 12),
        sharex=True,
        gridspec_kw={"height_ratios": [3.0, 3.0, 1.55], "hspace": 0.16},
    )

    def ratio_panel(ax: plt.Axes, field: str, title: str) -> None:
        for pair_index, pair in enumerate(PAIRS):
            subset = data.loc[data["pair"].eq(pair)].set_index("category")
            values = subset.reindex(category_order)[field].to_numpy(dtype=float)
            offset = (-0.5 if pair_index == 0 else 0.5) * width
            bars = ax.bar(
                x + offset,
                values,
                width,
                color=colors[pair],
                label=pair,
                zorder=3,
            )
            for bar, value in zip(bars, values):
                if np.isfinite(value) and value < 1.0:
                    bar.set_edgecolor("#3b1f2b")
                    bar.set_linewidth(2.6)
        ax.axhline(1.0, color="#666666", linewidth=1.2, linestyle=":", zorder=4)
        ax.set_ylabel("EC treatment/control ratio")
        ax.set_title(title, loc="left", fontsize=13, fontweight="bold")
        ax.grid(axis="y", alpha=0.22, zorder=0)

    ratio_panel(
        axes[0],
        "measured_instantaneous_ratio_mean",
        "A. Current chart metric (measured bulk EC)",
    )
    ratio_panel(
        axes[1],
        "ec25_instantaneous_ratio_mean",
        "B. Same metric after generic normalization to 25 °C",
    )
    ratio_max = float(
        np.nanmax(
            data[
                [
                    "measured_instantaneous_ratio_mean",
                    "ec25_instantaneous_ratio_mean",
                ]
            ].to_numpy(dtype=float)
        )
    )
    for ax in axes[:2]:
        ax.set_ylim(0, ratio_max * 1.08)
    axes[0].legend(loc="upper left", ncol=2, frameon=False)
    axes[0].text(
        0.995,
        0.98,
        "Dark outline: ratio below 1",
        transform=axes[0].transAxes,
        ha="right",
        va="top",
        color="#3b1f2b",
        fontsize=10,
    )

    delta_ax = axes[2]
    for pair_index, pair in enumerate(PAIRS):
        subset = data.loc[data["pair"].eq(pair)].set_index("category")
        values = subset.reindex(category_order)["temperature_change_pct"].to_numpy(
            dtype=float
        )
        offset = (-0.5 if pair_index == 0 else 0.5) * width
        delta_ax.bar(
            x + offset,
            values,
            width,
            color=colors[pair],
            alpha=0.90,
            zorder=3,
        )
    change_limit = max(1.0, float(data["temperature_change_pct"].abs().max()) * 1.18)
    delta_ax.set_ylim(-change_limit, change_limit)
    delta_ax.axhline(0.0, color="#555555", linewidth=1.0, zorder=4)
    delta_ax.set_ylabel("Change in ratio (%)")
    delta_ax.set_title(
        "C. Effect of temperature normalization", loc="left", fontsize=13, fontweight="bold"
    )
    delta_ax.grid(axis="y", alpha=0.22, zorder=0)
    delta_ax.set_xticks(x, category_order, rotation=28, ha="right")
    delta_ax.set_xlabel("Anchor year and logger position")

    fig.suptitle(
        "Growing season (April 1–October 31): 6-inch EC treatment ratios",
        fontsize=18,
        fontweight="bold",
        y=0.985,
    )
    fig.text(
        0.5,
        0.955,
        (
            "Mean of simultaneous positive 15-minute ratios, matching the attached "
            "dashboard chart; generic coefficient = 2% per °C"
        ),
        ha="center",
        fontsize=11,
    )
    fig.text(
        0.01,
        0.012,
        (
            "Diagnostic sensitivity only. The generic coefficient is not a site-specific soil "
            "calibration. 2023 has missing observations; the 2026 season ends October 7."
        ),
        ha="left",
        fontsize=9,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.035, 1, 0.94))
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-dir", type=Path, default=DEFAULT_PARQUET_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--no-series", action="store_true", help="Do not write derived 15-minute parquets")
    parser.add_argument("--no-plots", action="store_true", help="Do not write PNG summaries")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    series_dir = output_dir / "series"
    if not args.no_series:
        series_dir.mkdir(parents=True, exist_ok=True)

    source_metadata: list[dict[str, object]] = []
    coverage_rows: list[dict[str, object]] = []
    pair_coverage_rows: list[dict[str, object]] = []
    sensor_seasonal_rows: list[dict[str, object]] = []
    sensor_monthly_rows: list[dict[str, object]] = []
    pair_seasonal_rows: list[dict[str, object]] = []
    pair_monthly_rows: list[dict[str, object]] = []
    instantaneous_rows: list[dict[str, object]] = []

    for year in YEARS:
        path = args.parquet_dir / f"{year}_15min.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        frame = load_year(path)
        source_metadata.append(
            {
                "year": year,
                "path": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
                "sha256": sha256(path),
                "rows": len(frame),
                "first_timestamp": frame["timestamp"].min().isoformat(),
                "last_timestamp": frame["timestamp"].max().isoformat(),
                "duplicate_local_timestamps": int(frame["timestamp"].duplicated(keep=False).sum()),
            }
        )

        values_by_sensor: dict[Sensor, dict[str, pd.Series]] = {}
        derived_columns: dict[str, pd.Series] = {"timestamp": frame["timestamp"]}
        for sensor in sensors():
            values = derived_sensor_values(frame, sensor)
            values_by_sensor[sensor] = values
            coverage_rows.append(sensor_coverage_rows(frame, year, sensor, values))

            valid = values["valid"]
            nonfreezing = valid & values["temperature_c"].gt(FREEZING_THRESHOLD_C)
            derived_columns[f"TsoilC_{sensor.suffix}"] = values["temperature_c"]
            derived_columns[f"EC25_factor_{sensor.suffix}"] = values["factor"]
            derived_columns[f"EC25_sensitivity_{sensor.suffix}"] = values["ec25"]
            derived_columns[f"EC25_nonfreezing_{sensor.suffix}"] = values["ec25"].where(
                nonfreezing
            )

            for season in ("growing_apr_oct", "winter_nov_mar"):
                for scope in SCOPES:
                    sensor_seasonal_rows.append(
                        sensor_summary_row(frame, year, sensor, values, season, scope)
                    )

            for year_month, month_indices in frame.groupby("year_month", sort=True).groups.items():
                month_mask = frame.index.isin(month_indices)
                for scope in SCOPES:
                    selected = month_mask & scope_mask(
                        values["valid"], values["temperature_c"], scope
                    )
                    sensor_monthly_rows.append(
                        {
                            "year": year,
                            "year_month": year_month,
                            "scope": scope,
                            "strip": sensor.strip,
                            "depth": sensor.depth,
                            "position": sensor.position,
                            "valid_rows": int(selected.sum()),
                            "temperature_c_mean": float(values["temperature_c"].loc[selected].mean())
                            if selected.any()
                            else np.nan,
                            "correction_factor_mean": float(values["factor"].loc[selected].mean())
                            if selected.any()
                            else np.nan,
                            "ec_measured_mean_ds_m": float(values["ec"].loc[selected].mean())
                            if selected.any()
                            else np.nan,
                            "ec25_mean_ds_m": float(values["ec25"].loc[selected].mean())
                            if selected.any()
                            else np.nan,
                        }
                    )

        if not args.no_series:
            derived = pd.DataFrame(derived_columns)
            derived.to_parquet(series_dir / f"{year}_ec25_research_15min.parquet", index=False)

        for pair, (treated_strip, control_strip) in PAIRS.items():
            for position in POSITIONS:
                for depth in DEPTHS:
                    treated_sensor = Sensor(treated_strip, depth, position)
                    control_sensor = Sensor(control_strip, depth, position)
                    treated = values_by_sensor[treated_sensor]
                    control = values_by_sensor[control_sensor]
                    matched_formula = treated["valid"] & control["valid"]
                    pair_coverage_rows.append(
                        {
                            "year": year,
                            "pair": pair,
                            "depth": depth,
                            "position": position,
                            "source_rows": len(frame),
                            "matched_formula_valid_rows": int(matched_formula.sum()),
                            "matched_formula_valid_fraction": float(matched_formula.mean()),
                            "matched_rows_with_either_sensor_freezing": int(
                                (
                                    matched_formula
                                    & (
                                        treated["temperature_c"].le(FREEZING_THRESHOLD_C)
                                        | control["temperature_c"].le(FREEZING_THRESHOLD_C)
                                    )
                                ).sum()
                            ),
                            "matched_nonfreezing_rows": int(
                                (
                                    matched_formula
                                    & treated["temperature_c"].gt(FREEZING_THRESHOLD_C)
                                    & control["temperature_c"].gt(FREEZING_THRESHOLD_C)
                                ).sum()
                            ),
                        }
                    )

                    for season in ("growing_apr_oct", "winter_nov_mar"):
                        season_mask = frame["season"].eq(season)
                        for scope in SCOPES:
                            pair_seasonal_rows.append(
                                {
                                    "year": year,
                                    "season": season,
                                    "scope": scope,
                                    "pair": pair,
                                    "treated_strip": treated_strip,
                                    "control_strip": control_strip,
                                    "depth": depth,
                                    "position": position,
                                    **pair_group_row(
                                        frame, treated, control, season_mask, scope
                                    ),
                                }
                            )
                            instantaneous_rows.append(
                                {
                                    "year": year,
                                    "season": season,
                                    "scope": scope,
                                    "pair": pair,
                                    "depth": depth,
                                    "position": position,
                                    **instantaneous_ratio_row(
                                        treated, control, season_mask, scope
                                    ),
                                }
                            )

                    for year_month, month_indices in frame.groupby(
                        "year_month", sort=True
                    ).groups.items():
                        month_mask = frame.index.isin(month_indices)
                        for scope in SCOPES:
                            pair_monthly_rows.append(
                                {
                                    "year": year,
                                    "year_month": year_month,
                                    "scope": scope,
                                    "pair": pair,
                                    "treated_strip": treated_strip,
                                    "control_strip": control_strip,
                                    "depth": depth,
                                    "position": position,
                                    **pair_group_row(
                                        frame, treated, control, month_mask, scope
                                    ),
                                }
                            )

    coverage = pd.DataFrame(coverage_rows)
    pair_coverage = pd.DataFrame(pair_coverage_rows)
    sensor_seasonal = pd.DataFrame(sensor_seasonal_rows)
    sensor_monthly = pd.DataFrame(sensor_monthly_rows)
    pair_seasonal = pd.DataFrame(pair_seasonal_rows)
    pair_monthly = pd.DataFrame(pair_monthly_rows)
    instantaneous = pd.DataFrame(instantaneous_rows)

    write_csv(coverage, output_dir / "sensor_year_coverage.csv")
    write_csv(pair_coverage, output_dir / "pair_year_coverage.csv")
    write_csv(sensor_seasonal, output_dir / "sensor_seasonal_means.csv")
    write_csv(sensor_monthly, output_dir / "sensor_monthly_means.csv")
    write_csv(pair_seasonal, output_dir / "pair_seasonal_ratios_of_means.csv")
    write_csv(pair_monthly, output_dir / "pair_monthly_ratios_of_means.csv")
    write_csv(instantaneous, output_dir / "instantaneous_ratio_summary_supplemental.csv")
    focused_growing_6in = instantaneous.loc[
        instantaneous["season"].eq("growing_apr_oct")
        & instantaneous["depth"].eq(1)
    ].copy()
    focused_growing_6in["temperature_normalization_change_pct"] = (
        100.0
        * focused_growing_6in["ec25_instantaneous_ratio_mean"]
        / focused_growing_6in["measured_instantaneous_ratio_mean"]
        - 100.0
    )
    write_csv(
        focused_growing_6in,
        output_dir / "growing_season_6in_attached_metric_temperature_comparison.csv",
    )

    metadata = {
        "analysis": "CS650 bulk-EC temperature-normalization sensitivity assessment",
        "formula": "EC25 = EC_T / (1 + 0.02 * (T_soil_C - 25))",
        "temperature_coefficient_per_c": TEMPERATURE_COEFFICIENT_PER_C,
        "reference_temperature_c": REFERENCE_TEMPERATURE_C,
        "source_temperature_unit": "degrees Fahrenheit in canonical processed parquets",
        "temperature_conversion": "T_soil_C = (T_soil_F - 32) * 5 / 9",
        "ec_unit": "dS/m bulk soil electrical conductivity",
        "season_definition": {
            "growing_apr_oct": "April 1 through October 31 within each calendar-year file",
            "winter_nov_mar": "January-March plus November-December within each calendar-year file",
        },
        "scopes": {
            "all_formula_valid": (
                "finite EC and temperature with finite positive formula denominator; "
                "includes frozen observations only for sensitivity inspection"
            ),
            "nonfreezing": "all_formula_valid plus T_soil_C > 0; primary assessment",
        },
        "pair_ratio_method": (
            "ratio of treated and control seasonal means calculated from identical "
            "simultaneous matched observations"
        ),
        "instantaneous_ratio_file": (
            "supplemental only; requires positive EC in both strips and is not used "
            "as the primary pair estimator"
        ),
        "treatment_pairs": PAIRS,
        "manufacturer_sources": {
            "bulk_ec_temperature_correction": CAMPBELL_EC_GUIDANCE_URL,
            "separate_vwc_temperature_guidance": CAMPBELL_VWC_GUIDANCE_URL,
        },
        "limitations": [
            "The generic coefficient is not a site-specific soil calibration.",
            "CS650 EC is bulk EC, not laboratory extract EC or pore-water EC.",
            "Temperature normalization does not remove moisture dependence.",
            "Direction and 5% band labels are descriptive, not significance tests.",
        ],
        "source_files": source_metadata,
    }
    (output_dir / "assessment_metadata.json").write_text(
        json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8"
    )

    if not args.no_plots:
        plot_monthly_ratios(pair_monthly, output_dir / "monthly_pair_ratio_timeseries.png")
        plot_ratio_change_heatmap(
            pair_seasonal, output_dir / "seasonal_pair_ratio_change_heatmap.png"
        )
        plot_correction_factor_monthly(
            sensor_monthly, output_dir / "monthly_correction_factor_pattern.png"
        )
        plot_growing_season_6in_attached_metric(
            focused_growing_6in,
            output_dir / "growing_season_6in_measured_vs_ec25.png",
        )

    primary = pair_seasonal.loc[pair_seasonal["scope"].eq("nonfreezing")]
    valid_primary = primary.dropna(
        subset=["measured_ratio_of_means", "ec25_ratio_of_means"]
    )
    print(f"Analyzed {coverage['formula_valid_rows'].sum():,} sensor-observations")
    print(
        "Nonfreezing seasonal pair cells: "
        f"{len(valid_primary):,}; exact direction changes: "
        f"{int(valid_primary['exact_direction_changed'].sum()):,}; "
        "descriptive 5% band changes: "
        f"{int(valid_primary['descriptive_5pct_band_changed'].sum()):,}"
    )
    print(f"Wrote research outputs to {output_dir}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Analyze six-inch EC–moisture behavior between irrigation events.

The analysis keeps the two field contrasts separate:

* S1 (biochar) versus S2 (control), monthly-irrigation regime
* S3 (biochar) versus S4 (control), approximately biweekly regime

CS650 bulk EC is normalized to 25 C before moisture modeling.  Because the
processed logger files do not retain dielectric permittivity, the script does
not calculate Hilhorst pore-water EC.  Instead it creates an explicitly named
``apparent_pore_ec_index`` from VWC and reports exponent sensitivity results.
The index is useful for asking whether the treatment contrast survives removal
of the dominant water-content signal; it is not a laboratory salinity value.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
PARQUET_DIR = ROOT / "biochar_app/data-processed/parquet/summary/15min"
IRRIGATION_CSV = (
    ROOT / "biochar_app/data-processed/management/irrigation/irrigation_clean.csv"
)
OUTPUT_DIR = ROOT / "biochar_app/data-processed/research/ec_irrigation_intervals"
REPORT_PATH = ROOT / "biochar_app/docs/research/ec_irrigation_interval_analysis.md"

YEARS = (2023, 2024, 2025, 2026)
POSITIONS = ("T", "M", "B")
POSITION_LABELS = {"T": "Top", "M": "Middle", "B": "Bottom"}
PAIRS = {
    "S1/S2": {"biochar": "S1", "control": "S2", "regime": "monthly"},
    "S3/S4": {"biochar": "S3", "control": "S4", "regime": "biweekly"},
}
PAIR_FROM_GROUP = {"S1_S2": "S1/S2", "S3_S4": "S3/S4"}
TREATMENT_COLORS = {"biochar": "#0072B2", "control": "#D55E00"}
SENSITIVITY_EXPONENTS = (1.0, 1.5, 2.0)
MIN_DAILY_SAMPLES = 48
MIN_INTERVAL_DAYS = 4
BOOTSTRAP_REPETITIONS = 2_000
BOOTSTRAP_SEED = 20261011


@dataclass(frozen=True)
class SensorSpec:
    strip: str
    position: str

    @property
    def ec(self) -> str:
        return f"EC_1_raw_{self.strip}_{self.position}"

    @property
    def vwc(self) -> str:
        return f"VWC_1_raw_{self.strip}_{self.position}"

    @property
    def temperature(self) -> str:
        return f"T_1_raw_{self.strip}_{self.position}"


def temperature_correct_ec25(
    ec_bulk: pd.Series | np.ndarray,
    temperature_f: pd.Series | np.ndarray,
) -> np.ndarray:
    """Apply the manufacturer's generic 2%/C bulk-EC sensitivity equation."""
    ec = np.asarray(ec_bulk, dtype=float)
    temp_f = np.asarray(temperature_f, dtype=float)
    temp_c = (temp_f - 32.0) * (5.0 / 9.0)
    denominator = 1.0 + 0.02 * (temp_c - 25.0)
    valid = (
        np.isfinite(ec)
        & np.isfinite(temp_c)
        & (ec > 0)
        & (temp_c > 0)
        & (denominator > 0)
    )
    result = np.full(ec.shape, np.nan, dtype=float)
    result[valid] = ec[valid] / denominator[valid]
    return result


def build_irrigation_intervals(events: pd.DataFrame) -> pd.DataFrame:
    """Build complete calendar-day drydowns bounded by consecutive events."""
    frame = events.copy()
    frame["pair"] = frame["strip_group"].map(PAIR_FROM_GROUP)
    frame["start_timestamp"] = pd.to_datetime(frame["start_timestamp"], errors="coerce")
    frame["end_timestamp"] = pd.to_datetime(frame["end_timestamp"], errors="coerce")
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame = frame.dropna(
        subset=["pair", "year", "event_id", "start_timestamp", "end_timestamp"]
    )
    frame = frame.loc[frame["year"].isin(YEARS)].drop_duplicates(
        ["pair", "event_id"]
    )

    records: list[dict[str, object]] = []
    for (year, pair), group in frame.groupby(["year", "pair"], sort=True):
        ordered = group.sort_values(["start_timestamp", "end_timestamp"])
        rows = list(ordered.itertuples(index=False))
        for sequence, (current, following) in enumerate(zip(rows[:-1], rows[1:]), 1):
            # Exclude both irrigation dates because their daily medians mix
            # pre-event and post-event observations.
            first_date = pd.Timestamp(current.end_timestamp).normalize() + pd.Timedelta(days=1)
            last_date = pd.Timestamp(following.start_timestamp).normalize() - pd.Timedelta(days=1)
            full_days = int((last_date - first_date).days + 1)
            if full_days < MIN_INTERVAL_DAYS:
                continue
            records.append(
                {
                    "interval_id": f"{str(pair).replace('/', '')}_{int(year)}_{sequence:02d}",
                    "year": int(year),
                    "pair": pair,
                    "regime": PAIRS[str(pair)]["regime"],
                    "preceding_event_id": current.event_id,
                    "following_event_id": following.event_id,
                    "preceding_irrigation_end": current.end_timestamp,
                    "following_irrigation_start": following.start_timestamp,
                    "first_full_date": first_date,
                    "last_full_date": last_date,
                    "full_days": full_days,
                }
            )
    return pd.DataFrame(records).sort_values(["pair", "year", "first_full_date"])


def _wanted_columns() -> list[str]:
    columns = ["timestamp"]
    for strip in ("S1", "S2", "S3", "S4"):
        for position in POSITIONS:
            spec = SensorSpec(strip, position)
            columns.extend([spec.ec, spec.vwc, spec.temperature])
    return columns


def load_daily_drydowns(intervals: pd.DataFrame) -> pd.DataFrame:
    """Read 15-minute values, correct EC, and summarize complete drydown days."""
    records: list[pd.DataFrame] = []
    wanted = _wanted_columns()

    for year in YEARS:
        path = PARQUET_DIR / f"{year}_15min.parquet"
        frame = pd.read_parquet(path, columns=wanted)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame = frame.dropna(subset=["timestamp"])
        frame["date"] = frame["timestamp"].dt.normalize()
        year_intervals = intervals.loc[intervals["year"].eq(year)]

        for pair, pair_info in PAIRS.items():
            pair_intervals = year_intervals.loc[year_intervals["pair"].eq(pair)]
            if pair_intervals.empty:
                continue
            for treatment in ("biochar", "control"):
                strip = str(pair_info[treatment])
                for position in POSITIONS:
                    spec = SensorSpec(strip, position)
                    source = frame[["timestamp", "date", spec.ec, spec.vwc, spec.temperature]].copy()
                    source = source.rename(
                        columns={spec.ec: "ec_bulk", spec.vwc: "vwc_pct", spec.temperature: "temp_f"}
                    )
                    for column in ("ec_bulk", "vwc_pct", "temp_f"):
                        source[column] = pd.to_numeric(source[column], errors="coerce")
                    source["temp_c"] = (source["temp_f"] - 32.0) * (5.0 / 9.0)
                    source["ec25"] = temperature_correct_ec25(
                        source["ec_bulk"], source["temp_f"]
                    )
                    source = source.loc[
                        source["ec_bulk"].gt(0)
                        & source["vwc_pct"].between(1, 80)
                        & source["temp_c"].between(0, 60, inclusive="right")
                        & source["ec25"].notna()
                    ].copy()

                    for interval in pair_intervals.itertuples(index=False):
                        selection = source.loc[
                            source["date"].between(
                                interval.first_full_date,
                                interval.last_full_date,
                            )
                        ].copy()
                        if selection.empty:
                            continue
                        daily = (
                            selection.groupby("date", as_index=False)
                            .agg(
                                ec_bulk=("ec_bulk", "median"),
                                ec25=("ec25", "median"),
                                vwc_pct=("vwc_pct", "median"),
                                temp_c=("temp_c", "median"),
                                observations=("ec_bulk", "size"),
                            )
                        )
                        daily = daily.loc[daily["observations"].ge(MIN_DAILY_SAMPLES)].copy()
                        if daily.empty:
                            continue
                        daily["theta"] = daily["vwc_pct"] / 100.0
                        daily["drydown_day"] = (
                            daily["date"] - pd.Timestamp(interval.first_full_date)
                        ).dt.days + 1
                        daily["interval_fraction"] = (
                            daily["drydown_day"] / float(interval.full_days)
                        ).clip(upper=1.0)
                        daily["interval_id"] = interval.interval_id
                        daily["year"] = year
                        daily["pair"] = pair
                        daily["regime"] = pair_info["regime"]
                        daily["strip"] = strip
                        daily["treatment"] = treatment
                        daily["position"] = POSITION_LABELS[position]
                        daily["position_code"] = position
                        records.append(daily)

    if not records:
        raise RuntimeError("No complete inter-irrigation daily records were available.")
    return pd.concat(records, ignore_index=True).sort_values(
        ["pair", "position", "interval_id", "treatment", "date"]
    )


def _fixed_effect_exponent(frame: pd.DataFrame) -> float:
    """Estimate log(EC25)~m*log(theta) after interval-strip demeaning."""
    contributions = _exponent_contributions(frame)
    if contributions.empty:
        return np.nan
    denominator = float(contributions["sum_xx"].sum())
    if denominator <= 0:
        return np.nan
    return float(contributions["sum_xy"].sum() / denominator)


def _exponent_contributions(frame: pd.DataFrame) -> pd.DataFrame:
    """Return interval-level sufficient statistics for fast block bootstrap."""
    rows: list[dict[str, float | str]] = []
    for _, group in frame.groupby(["interval_id", "strip"], sort=False):
        subset = group.loc[group["ec25"].gt(0) & group["theta"].gt(0)].copy()
        if len(subset) < MIN_INTERVAL_DAYS or subset["vwc_pct"].max() - subset["vwc_pct"].min() < 1.0:
            continue
        x = np.log(subset["theta"].to_numpy(dtype=float))
        y = np.log(subset["ec25"].to_numpy(dtype=float))
        centered_x = x - x.mean()
        centered_y = y - y.mean()
        rows.append(
            {
                "interval_id": str(subset["interval_id"].iloc[0]),
                "sum_xx": float(centered_x @ centered_x),
                "sum_xy": float(centered_x @ centered_y),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["interval_id", "sum_xx", "sum_xy"])
    return (
        pd.DataFrame(rows)
        .groupby("interval_id", as_index=False)[["sum_xx", "sum_xy"]]
        .sum()
    )


def estimate_moisture_exponents(daily: pd.DataFrame) -> pd.DataFrame:
    """Estimate pooled and treatment-specific within-drydown exponents."""
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    rows: list[dict[str, object]] = []
    for (pair, position), group in daily.groupby(["pair", "position"], sort=False):
        for treatment in ("pooled", "biochar", "control"):
            subset = group if treatment == "pooled" else group.loc[group["treatment"].eq(treatment)]
            contributions = _exponent_contributions(subset)
            total_xx = float(contributions["sum_xx"].sum())
            estimate = (
                float(contributions["sum_xy"].sum() / total_xx)
                if total_xx > 0
                else np.nan
            )
            interval_ids = contributions["interval_id"].to_numpy()
            draws = []
            if len(interval_ids) >= 2:
                sum_xx = contributions["sum_xx"].to_numpy(dtype=float)
                sum_xy = contributions["sum_xy"].to_numpy(dtype=float)
                for _ in range(BOOTSTRAP_REPETITIONS):
                    sampled_indices = rng.integers(0, len(interval_ids), size=len(interval_ids))
                    draw_xx = float(sum_xx[sampled_indices].sum())
                    if draw_xx > 0:
                        draws.append(float(sum_xy[sampled_indices].sum() / draw_xx))
            low, high = (
                np.quantile(draws, [0.025, 0.975])
                if draws
                else (np.nan, np.nan)
            )
            rows.append(
                {
                    "pair": pair,
                    "position": position,
                    "treatment": treatment,
                    "intervals": int(subset["interval_id"].nunique()),
                    "days": len(subset),
                    "moisture_exponent": estimate,
                    "ci95_low": low,
                    "ci95_high": high,
                }
            )
    return pd.DataFrame(rows)


def add_pore_ec_indices(daily: pd.DataFrame, exponents: pd.DataFrame) -> pd.DataFrame:
    """Add empirical and prespecified VWC-normalized EC25 indices."""
    pooled = exponents.loc[
        exponents["treatment"].eq("pooled"),
        ["pair", "position", "moisture_exponent"],
    ].rename(columns={"moisture_exponent": "empirical_m"})
    out = daily.merge(pooled, on=["pair", "position"], how="left", validate="many_to_one")
    # The exponent is constrained only for constructing the proxy.  The raw
    # estimate remains in the exponent table so unstable cells stay visible.
    out["empirical_m_used"] = out["empirical_m"].clip(lower=0.25, upper=3.0)
    out["apparent_pore_ec_index"] = out["ec25"] / np.power(
        out["theta"], out["empirical_m_used"]
    )
    out["ec25_at_25pct_vwc"] = out["ec25"] * np.power(
        0.25 / out["theta"], out["empirical_m_used"]
    )
    for exponent in SENSITIVITY_EXPONENTS:
        suffix = str(exponent).replace(".", "p")
        out[f"apparent_pore_ec_index_m{suffix}"] = out["ec25"] / np.power(
            out["theta"], exponent
        )
    return out


def make_daily_pair_contrasts(daily: pd.DataFrame) -> pd.DataFrame:
    """Match biochar and control observations within date and irrigation interval."""
    metric_columns = [
        "ec_bulk",
        "ec25",
        "vwc_pct",
        "apparent_pore_ec_index",
        "ec25_at_25pct_vwc",
        *[
            f"apparent_pore_ec_index_m{str(exponent).replace('.', 'p')}"
            for exponent in SENSITIVITY_EXPONENTS
        ],
    ]
    index = [
        "date",
        "year",
        "pair",
        "regime",
        "position",
        "position_code",
        "interval_id",
        "drydown_day",
        "interval_fraction",
    ]
    wide = daily.pivot_table(
        index=index,
        columns="treatment",
        values=metric_columns,
        aggfunc="first",
    )
    wide.columns = [f"{metric}_{treatment}" for metric, treatment in wide.columns]
    wide = wide.reset_index()
    for metric in metric_columns:
        numerator = pd.to_numeric(wide[f"{metric}_biochar"], errors="coerce")
        denominator = pd.to_numeric(wide[f"{metric}_control"], errors="coerce")
        wide[f"{metric}_ratio"] = numerator / denominator.where(denominator.gt(0))
        wide[f"log_{metric}_ratio"] = np.log(wide[f"{metric}_ratio"])
    wide["delta_vwc_pct"] = wide["vwc_pct_biochar"] - wide["vwc_pct_control"]
    return wide.sort_values(["pair", "position", "interval_id", "date"])


def _bootstrap_interval_mean(
    interval_values: pd.DataFrame,
    value_column: str,
    rng: np.random.Generator,
) -> tuple[float, float, float]:
    values = interval_values[value_column].dropna().to_numpy(dtype=float)
    if not len(values):
        return np.nan, np.nan, np.nan
    estimate = float(values.mean())
    if len(values) == 1:
        return estimate, np.nan, np.nan
    indices = rng.integers(0, len(values), size=(BOOTSTRAP_REPETITIONS, len(values)))
    draws = values[indices].mean(axis=1)
    low, high = np.quantile(draws, [0.025, 0.975])
    return estimate, float(low), float(high)


def summarize_pair_ratios(
    contrasts: pd.DataFrame,
    group_columns: Iterable[str] = ("pair", "position"),
) -> pd.DataFrame:
    """Give each irrigation interval equal weight in treatment contrasts."""
    metrics = [
        "ec_bulk",
        "ec25",
        "apparent_pore_ec_index",
        "ec25_at_25pct_vwc",
        *[
            f"apparent_pore_ec_index_m{str(exponent).replace('.', 'p')}"
            for exponent in SENSITIVITY_EXPONENTS
        ],
    ]
    group_columns = list(group_columns)
    interval_group = [*group_columns, "interval_id"]
    rng = np.random.default_rng(BOOTSTRAP_SEED + len(group_columns))
    rows: list[dict[str, object]] = []
    for metric in metrics:
        log_column = f"log_{metric}_ratio"
        interval_values = (
            contrasts.groupby(interval_group, as_index=False)[log_column]
            .median()
            .dropna(subset=[log_column])
        )
        for keys, group in interval_values.groupby(group_columns, sort=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            estimate, low, high = _bootstrap_interval_mean(group, log_column, rng)
            row = dict(zip(group_columns, keys))
            row.update(
                {
                    "metric": metric,
                    "intervals": len(group),
                    "biochar_control_ratio": np.exp(estimate),
                    "ci95_low": np.exp(low) if np.isfinite(low) else np.nan,
                    "ci95_high": np.exp(high) if np.isfinite(high) else np.nan,
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


def summarize_drydown_trends(daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Estimate interval-specific and treatment-summary trends with drydown day."""
    metrics = ("vwc_pct", "ec_bulk", "ec25", "apparent_pore_ec_index")
    interval_rows: list[dict[str, object]] = []
    for keys, group in daily.groupby(
        ["pair", "position", "treatment", "strip", "year", "interval_id"],
        sort=False,
    ):
        x = group["drydown_day"].to_numpy(dtype=float)
        if len(np.unique(x)) < MIN_INTERVAL_DAYS:
            continue
        base = dict(
            zip(
                ["pair", "position", "treatment", "strip", "year", "interval_id"],
                keys,
            )
        )
        for metric in metrics:
            y = pd.to_numeric(group[metric], errors="coerce").to_numpy(dtype=float)
            valid = np.isfinite(x) & np.isfinite(y) & (y > 0)
            if valid.sum() < MIN_INTERVAL_DAYS:
                continue
            slope = np.polyfit(x[valid], np.log(y[valid]), 1)[0]
            interval_rows.append(
                {
                    **base,
                    "metric": metric,
                    "days": int(valid.sum()),
                    "log_slope_per_day": slope,
                    "percent_change_per_day": 100.0 * np.expm1(slope),
                }
            )
    interval_frame = pd.DataFrame(interval_rows)
    summary = (
        interval_frame.groupby(["pair", "position", "treatment", "metric"], as_index=False)
        .agg(
            intervals=("interval_id", "nunique"),
            median_percent_change_per_day=("percent_change_per_day", "median"),
            mean_percent_change_per_day=("percent_change_per_day", "mean"),
        )
    )
    return interval_frame, summary


def plot_ec25_vs_vwc(daily: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(2, 3, figsize=(15, 9), sharex=False, sharey=False)
    for row, pair in enumerate(PAIRS):
        for col, position in enumerate(("Top", "Middle", "Bottom")):
            ax = axes[row, col]
            panel = daily.loc[daily["pair"].eq(pair) & daily["position"].eq(position)]
            for treatment in ("biochar", "control"):
                treatment_data = panel.loc[panel["treatment"].eq(treatment)]
                for _, interval in treatment_data.groupby("interval_id", sort=False):
                    interval = interval.sort_values("date")
                    ax.plot(
                        interval["vwc_pct"],
                        interval["ec25"],
                        color=TREATMENT_COLORS[treatment],
                        alpha=0.16,
                        linewidth=0.8,
                    )
                ax.scatter(
                    treatment_data["vwc_pct"],
                    treatment_data["ec25"],
                    s=7,
                    alpha=0.20,
                    color=TREATMENT_COLORS[treatment],
                    edgecolors="none",
                    label=treatment.capitalize() if row == 0 and col == 0 else None,
                )
            ax.set_title(f"{pair} · {position}")
            ax.grid(alpha=0.2)
            if row == 1:
                ax.set_xlabel("VWC (%)")
            if col == 0:
                ax.set_ylabel("Bulk EC25 (dS/m)")
    fig.suptitle(
        "Six-inch bulk EC25 versus VWC within complete inter-irrigation drydowns",
        fontsize=15,
    )
    fig.legend(loc="upper center", ncol=2, bbox_to_anchor=(0.5, 0.955), frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_pore_index_drydown(daily: pd.DataFrame, output_path: Path) -> None:
    normalized_parts = []
    for _, group in daily.groupby(["interval_id", "strip", "position"], sort=False):
        group = group.sort_values("date").copy()
        baseline = group.loc[group["drydown_day"].le(2), "apparent_pore_ec_index"].median()
        if not np.isfinite(baseline) or baseline <= 0:
            continue
        group["relative_pore_index"] = group["apparent_pore_ec_index"] / baseline
        normalized_parts.append(group)
    normalized = pd.concat(normalized_parts, ignore_index=True)

    fig, axes = plt.subplots(2, 3, figsize=(15, 9), sharey=True)
    for row, pair in enumerate(PAIRS):
        for col, position in enumerate(("Top", "Middle", "Bottom")):
            ax = axes[row, col]
            panel = normalized.loc[
                normalized["pair"].eq(pair) & normalized["position"].eq(position)
            ]
            for treatment in ("biochar", "control"):
                subset = panel.loc[panel["treatment"].eq(treatment)]
                daily_summary = (
                    subset.groupby("drydown_day")["relative_pore_index"]
                    .agg(
                        median="median",
                        q25=lambda x: x.quantile(0.25),
                        q75=lambda x: x.quantile(0.75),
                        n="size",
                    )
                    .reset_index()
                )
                daily_summary = daily_summary.loc[daily_summary["n"].ge(5)]
                ax.plot(
                    daily_summary["drydown_day"],
                    daily_summary["median"],
                    color=TREATMENT_COLORS[treatment],
                    linewidth=2,
                    label=treatment.capitalize() if row == 0 and col == 0 else None,
                )
                ax.fill_between(
                    daily_summary["drydown_day"],
                    daily_summary["q25"],
                    daily_summary["q75"],
                    color=TREATMENT_COLORS[treatment],
                    alpha=0.13,
                )
            ax.axhline(1.0, color="0.35", linewidth=1, linestyle="--")
            ax.set_title(f"{pair} · {position}")
            ax.grid(alpha=0.2)
            if row == 1:
                ax.set_xlabel("Full day after irrigation event")
            if col == 0:
                ax.set_ylabel("Apparent pore-EC index / days 1–2")
    fig.suptitle(
        "Moisture-normalized EC25 behavior during irrigation drydowns",
        fontsize=15,
    )
    fig.legend(loc="upper center", ncol=2, bbox_to_anchor=(0.5, 0.955), frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_pair_ratio_summary(summary: pd.DataFrame, output_path: Path) -> None:
    metric_order = [
        ("ec_bulk", "Measured\nbulk EC"),
        ("ec25", "Bulk EC25"),
        ("apparent_pore_ec_index_m1p0", "VWC proxy\nm=1.0"),
        ("apparent_pore_ec_index_m1p5", "VWC proxy\nm=1.5"),
        ("apparent_pore_ec_index_m2p0", "VWC proxy\nm=2.0"),
        ("apparent_pore_ec_index", "Empirical\nVWC proxy"),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(15, 8), sharey=False)
    for row, pair in enumerate(PAIRS):
        for col, position in enumerate(("Top", "Middle", "Bottom")):
            ax = axes[row, col]
            panel = summary.loc[summary["pair"].eq(pair) & summary["position"].eq(position)]
            lookup = panel.set_index("metric")
            values = []
            low = []
            high = []
            labels = []
            for metric, label in metric_order:
                labels.append(label)
                if metric not in lookup.index:
                    values.append(np.nan)
                    low.append(np.nan)
                    high.append(np.nan)
                    continue
                result = lookup.loc[metric]
                values.append(float(result["biochar_control_ratio"]))
                low.append(float(result["ci95_low"]))
                high.append(float(result["ci95_high"]))
            x = np.arange(len(labels))
            values_array = np.asarray(values)
            ax.errorbar(
                x,
                values_array,
                yerr=np.vstack([values_array - np.asarray(low), np.asarray(high) - values_array]),
                fmt="o",
                color="#0072B2",
                ecolor="#555555",
                capsize=3,
            )
            ax.axhline(1.0, color="black", linewidth=1, linestyle="--")
            ax.set_xticks(x, labels, fontsize=8)
            ax.set_title(f"{pair} · {position}")
            ax.grid(axis="y", alpha=0.2)
            if col == 0:
                ax.set_ylabel("Biochar / control ratio")
    fig.suptitle(
        "Six-inch paired EC contrasts: measured, temperature-corrected, and VWC-modeled",
        fontsize=15,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _markdown_table(frame: pd.DataFrame, columns: list[str], digits: int = 3) -> str:
    subset = frame[columns].copy()
    for column in subset.select_dtypes(include=["number"]).columns:
        if column in {"year", "intervals", "days", "observations"}:
            subset[column] = subset[column].map(
                lambda value: "" if pd.isna(value) else f"{int(value)}"
            )
        else:
            subset[column] = subset[column].map(
                lambda value: "" if pd.isna(value) else f"{value:.{digits}f}"
            )
    headers = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = [
        "| " + " | ".join(str(value) for value in row) + " |"
        for row in subset.itertuples(index=False, name=None)
    ]
    return "\n".join([headers, divider, *rows])


def write_report(
    intervals: pd.DataFrame,
    daily: pd.DataFrame,
    exponents: pd.DataFrame,
    ratio_summary: pd.DataFrame,
    trend_summary: pd.DataFrame,
) -> None:
    empirical_ratios = ratio_summary.loc[
        ratio_summary["metric"].eq("apparent_pore_ec_index")
    ].copy()
    empirical_ratios["ratio_95_interval"] = empirical_ratios.apply(
        lambda row: f"{row.biochar_control_ratio:.2f} ({row.ci95_low:.2f}–{row.ci95_high:.2f})",
        axis=1,
    )
    exponent_display = exponents.loc[exponents["treatment"].eq("pooled")].copy()
    exponent_display["estimate_95_interval"] = exponent_display.apply(
        lambda row: f"{row.moisture_exponent:.2f} ({row.ci95_low:.2f}–{row.ci95_high:.2f})",
        axis=1,
    )
    pore_trends = trend_summary.loc[
        trend_summary["metric"].eq("apparent_pore_ec_index")
    ].copy()
    contrast_comparison = (
        ratio_summary.loc[
            ratio_summary["metric"].isin(
                ["ec_bulk", "ec25", "apparent_pore_ec_index"]
            ),
            ["pair", "position", "metric", "biochar_control_ratio"],
        ]
        .pivot(index=["pair", "position"], columns="metric", values="biochar_control_ratio")
        .reset_index()
        .rename(
            columns={
                "ec_bulk": "measured_bulk_ratio",
                "ec25": "ec25_ratio",
                "apparent_pore_ec_index": "vwc_modeled_ratio",
            }
        )
    )
    temperature_change = 100.0 * (
        contrast_comparison["ec25_ratio"]
        / contrast_comparison["measured_bulk_ratio"]
        - 1.0
    )
    max_temperature_change = float(temperature_change.abs().max())
    ec25_trends = trend_summary.loc[trend_summary["metric"].eq("ec25")]
    ec25_trend_min = float(ec25_trends["median_percent_change_per_day"].min())
    ec25_trend_max = float(ec25_trends["median_percent_change_per_day"].max())

    report = f"""# Six-inch EC behavior between irrigation events

## Bottom line

This analysis separates the moisture, temperature, and paired-strip components
of the six-inch EC signal. It uses {daily['interval_id'].nunique()} complete
inter-irrigation intervals and {len(daily):,} strip-position daily records from
{daily['date'].min().date().isoformat()} through
{daily['date'].max().date().isoformat()}.

The results remain paired field contrasts, not replicated treatment effects.
S1/S2 estimates the observed biochar-control contrast under the monthly regime;
S3/S4 estimates it under the approximately biweekly regime. Each treatment
combination is represented by one physical strip, so biochar remains confounded
with persistent strip, soil, and field-position differences.

## What the results suggest

The median temperature-corrected bulk EC declines within every pair-position-
treatment cell during the drydown, ranging from {ec25_trend_min:.1f}% to
{ec25_trend_max:.1f}% per day. VWC declines at the same time. This is strong
evidence that the repeated rise-and-fall pattern is dominated by soil-water
content and conductive-path continuity.

Temperature normalization changes the six overall treatment ratios by at most
{max_temperature_change:.1f}%, so temperature is not the explanation for the
large pair and position differences. VWC modeling matters much more:

{_markdown_table(contrast_comparison, ['pair', 'position', 'measured_bulk_ratio', 'ec25_ratio', 'vwc_modeled_ratio'])}

The adjustment does not reveal a uniform biochar signal. In S1/S2, the Top
contrast reverses from higher measured bulk EC in S1 to a lower VWC-modeled
index, while the Bottom contrast remains strongly above 1. In S3/S4, the
Bottom contrast remains strongly below 1, Middle moves close to 1, and Top is
still moderately above 1 under the empirical model. The opposite Bottom
directions are especially difficult to reconcile with one common soluble-salt
effect of biochar.

The VWC-normalized index sometimes rises and sometimes falls through a drydown.
That mixed behavior is not evidence for general salt precipitation. It is
consistent with a combination of concentration, redistribution, sensor/soil
differences, and an imperfect VWC-only conductivity model.

## What was calculated

1. Fifteen-minute six-inch EC, VWC, and temperature measurements were matched
   within sensor, strip, and logger position.
2. Measured bulk EC was normalized to 25 C before daily aggregation using
   `EC25 = EC / (1 + 0.02 × (soil temperature C - 25))`.
3. Irrigation dates themselves were excluded because a daily median on those
   dates mixes pre-irrigation and post-irrigation conditions.
4. EC25 was plotted against VWC separately within each complete interval.
5. A common moisture exponent was estimated within each pair and logger
   position after removing interval-by-strip means. The apparent pore-EC index
   is `EC25 / theta^m`, where `theta = VWC / 100`.
6. Prespecified `m = 1.0`, `1.5`, and `2.0` results are retained as sensitivity
   checks. Treatment ratios give each irrigation interval equal weight.

The processed logger data do **not** contain dielectric permittivity. Therefore
the Hilhorst method cannot be applied, and the apparent pore-EC index must not be
called measured pore-water EC, saturation-extract EC, or salinity. It omits a
soil solid/surface-conductivity term and an independently calibrated tortuosity
function. Biochar could itself affect those terms.

## Empirical VWC exponents

{_markdown_table(exponent_display, ['pair', 'position', 'intervals', 'days', 'estimate_95_interval'])}

The empirical exponent is a descriptive drydown relationship, not a universal
soil constant. Differences between the biochar and control exponent estimates
are available in `moisture_exponents.csv` and are one way biochar-related pore
structure or persistent strip differences could enter the measurement.

## Paired apparent pore-EC contrasts

{_markdown_table(empirical_ratios, ['pair', 'position', 'intervals', 'ratio_95_interval'])}

A ratio above 1 means that the VWC-normalized index is higher in the biochar
strip than in its paired control. It does not by itself identify more total
salt, a biochar chemical effect, or an agronomic benefit. Compare these values
with `pair_ratio_summary.csv` to see whether conclusions change under the three
prespecified moisture exponents.

## Change during drydown

{_markdown_table(pore_trends, ['pair', 'position', 'treatment', 'intervals', 'median_percent_change_per_day'])}

If measured bulk EC declines while this index rises, the pattern is consistent
with the remaining pore water becoming more concentrated while bulk conduction
paths disappear. If both decline, salt removal, redistribution, adsorption, or
precipitation becomes more plausible, but cannot be distinguished here.

## Figures

- `ec25_vs_vwc_by_pair_position.png` shows every complete interval as a faint
  trajectory, separated by treatment pair and logger position.
- `apparent_pore_ec_drydown.png` shows the moisture-normalized index relative to
  days 1–2 of each interval.
- `biochar_control_ec_ratio_sensitivity.png` compares measured EC, EC25, three
  prespecified VWC models, and the empirical VWC model.

## Outputs and reproducibility

- `irrigation_intervals.csv`: event boundaries used in the analysis.
- `six_in_daily_drydowns.csv`: daily sensor-level analysis data.
- `six_in_daily_pair_contrasts.csv`: date-matched biochar/control comparisons.
- `moisture_exponents.csv`: pooled and treatment-specific drydown exponents.
- `pair_ratio_summary.csv`: all-years interval-equal treatment contrasts.
- `pair_ratio_by_year.csv`: year-specific treatment contrasts.
- `drydown_interval_trends.csv` and `drydown_trend_summary.csv`: interval and
  aggregate trends with time since irrigation.

Regenerate with:

```bash
.biochar_py313/bin/python biochar_app/scripts/research/analyze_ec_irrigation_intervals.py
```
"""
    REPORT_PATH.write_text(report, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    irrigation = pd.read_csv(IRRIGATION_CSV)
    intervals = build_irrigation_intervals(irrigation)
    daily = load_daily_drydowns(intervals)
    exponents = estimate_moisture_exponents(daily)
    daily = add_pore_ec_indices(daily, exponents)
    contrasts = make_daily_pair_contrasts(daily)
    ratio_summary = summarize_pair_ratios(contrasts)
    ratio_by_year = summarize_pair_ratios(
        contrasts,
        group_columns=("year", "pair", "position"),
    )
    interval_trends, trend_summary = summarize_drydown_trends(daily)

    intervals.to_csv(OUTPUT_DIR / "irrigation_intervals.csv", index=False)
    daily.to_csv(OUTPUT_DIR / "six_in_daily_drydowns.csv", index=False)
    contrasts.to_csv(OUTPUT_DIR / "six_in_daily_pair_contrasts.csv", index=False)
    exponents.to_csv(OUTPUT_DIR / "moisture_exponents.csv", index=False)
    ratio_summary.to_csv(OUTPUT_DIR / "pair_ratio_summary.csv", index=False)
    ratio_by_year.to_csv(OUTPUT_DIR / "pair_ratio_by_year.csv", index=False)
    interval_trends.to_csv(OUTPUT_DIR / "drydown_interval_trends.csv", index=False)
    trend_summary.to_csv(OUTPUT_DIR / "drydown_trend_summary.csv", index=False)

    plot_ec25_vs_vwc(daily, OUTPUT_DIR / "ec25_vs_vwc_by_pair_position.png")
    plot_pore_index_drydown(daily, OUTPUT_DIR / "apparent_pore_ec_drydown.png")
    plot_pair_ratio_summary(
        ratio_summary,
        OUTPUT_DIR / "biochar_control_ec_ratio_sensitivity.png",
    )
    write_report(intervals, daily, exponents, ratio_summary, trend_summary)

    print(
        f"Analyzed {daily['interval_id'].nunique()} complete irrigation intervals "
        f"and {len(daily):,} strip-position daily records."
    )
    print("\nPooled moisture exponents")
    print(
        exponents.loc[exponents["treatment"].eq("pooled")].to_string(index=False)
    )
    print("\nEmpirical apparent pore-EC treatment ratios")
    print(
        ratio_summary.loc[
            ratio_summary["metric"].eq("apparent_pore_ec_index")
        ].to_string(index=False)
    )
    print(f"\nReport: {REPORT_PATH}")


if __name__ == "__main__":
    main()

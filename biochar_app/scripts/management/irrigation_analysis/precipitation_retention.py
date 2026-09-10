"""Systematic retention analysis for independent precipitation events.

Weather-station precipitation identifies candidate events. Freeze screening is
performed separately for every VWC sensor with its co-located CS650 logger
temperature; remote weather-station soil temperatures are contextual only.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from biochar_app.config.experiment_config import (
    LOGGER_GEOMETRY,
    LOGGER_LOCATIONS,
    REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX,
    SENSOR_DEPTH_INDEX_TO_INCHES,
    STRIP_TREATMENT,
    STRIP_TREATMENT_PAIR,
    THREE_SENSOR_PROFILE_LOWER_BOUND_IN,
    THREE_SENSOR_PROFILE_NOMINAL_THICKNESS_IN,
    THREE_SENSOR_PROFILE_UPPER_BOUND_IN,
)
from biochar_app.config.field_management_metadata import (
    STRIP_WIDTH_FT,
    ZONE_AREA_SOURCE_BY_STRIP,
    ZONE_AREAS_SQFT_BY_STRIP,
    ZONE_GALLONS_PER_INCH_BY_STRIP,
    ZONE_LENGTHS_FT_BY_STRIP,
)

VWC_RE = re.compile(
    r"^VWC_(?P<depth>[123])_raw_(?P<strip>S[1-4])_(?P<position>[TMB])$"
)
PAIR_STRIPS = {"S1_S2": ("S1", "S2"), "S3_S4": ("S3", "S4")}
DEPTH_COLORS = {"1": "#1f77b4", "2": "#e69f00", "3": "#009e73"}


@dataclass(frozen=True)
class PrecipitationRetentionConfig:
    """Thresholds for the reproducible precipitation-event screen."""

    min_event_precip_in: float = 0.05
    allowed_dry_gap_days: int = 1
    irrigation_lookback_hours: float = 24.0
    irrigation_followup_hours: float = 72.0
    baseline_hours: float = 24.0
    peak_followup_hours: float = 24.0
    retained_window_hours: float = 6.0
    response_threshold_vwc: float = 0.25
    freeze_threshold_f: float = 32.5
    warm_event_min_temperature_f: float = 40.0
    min_window_coverage: float = 0.70
    max_vwc_pct: float = 80.0
    max_step_change_vwc: float = 15.0
    minimum_supported_events: int = 4
    maximum_p90_gap_review_pct: float = 10.0


def _with_datetime_index(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
        out = out.dropna(subset=["timestamp"]).set_index("timestamp")
    elif not isinstance(out.index, pd.DatetimeIndex):
        raise ValueError("Input must contain a timestamp column or DatetimeIndex.")
    out.index = pd.to_datetime(out.index)
    return out.sort_index()


def identify_precipitation_events(
    weather: pd.DataFrame,
    *,
    config: PrecipitationRetentionConfig = PrecipitationRetentionConfig(),
) -> pd.DataFrame:
    """Group wet days, allowing configured dry days inside one event."""

    data = _with_datetime_index(weather)
    if "precip_in" not in data:
        raise ValueError("Weather data do not contain precip_in.")
    precip = pd.to_numeric(data["precip_in"], errors="coerce").fillna(0).clip(lower=0)
    wet_days = precip.resample("D").sum().loc[lambda x: x.gt(0)].index
    columns = ["event_id", "year", "event_start", "event_end", "precip_total_in"]
    if wet_days.empty:
        return pd.DataFrame(columns=columns)

    groups: list[list[pd.Timestamp]] = [[wet_days[0]]]
    for day in wet_days[1:]:
        if (day - groups[-1][-1]).days <= config.allowed_dry_gap_days + 1:
            groups[-1].append(day)
        else:
            groups.append([day])

    rows: list[dict[str, object]] = []
    for days in groups:
        mask = (precip.index >= days[0]) & (precip.index < days[-1] + pd.Timedelta(days=1))
        event_precip = precip.loc[mask]
        positive = event_precip[event_precip.gt(0)]
        total = float(event_precip.sum())
        if positive.empty or total < config.min_event_precip_in:
            continue
        start, end = pd.Timestamp(positive.index.min()), pd.Timestamp(positive.index.max())
        rows.append({
            "event_id": f"precip_{start:%Y%m%d_%H%M}_{end:%Y%m%d_%H%M}",
            "year": start.year,
            "event_start": start,
            "event_end": end,
            "precip_total_in": total,
            "wet_day_count": len(days),
        })
    return pd.DataFrame(rows)


def flag_irrigation_overlap(
    events: pd.DataFrame,
    irrigation: pd.DataFrame,
    *,
    config: PrecipitationRetentionConfig = PrecipitationRetentionConfig(),
) -> pd.DataFrame:
    """Flag events whose baseline-to-72-hour window includes irrigation."""

    out = events.copy()
    starts = pd.to_datetime(irrigation["start_timestamp"], errors="coerce")
    ends = pd.to_datetime(irrigation["end_timestamp"], errors="coerce")
    counts: list[int] = []
    for event in out.itertuples(index=False):
        lo = pd.Timestamp(event.event_start) - pd.Timedelta(hours=config.irrigation_lookback_hours)
        hi = pd.Timestamp(event.event_end) + pd.Timedelta(hours=config.irrigation_followup_hours)
        counts.append(int((starts.le(hi) & ends.ge(lo)).fillna(False).sum()))
    out["irrigation_overlap_rows"] = counts
    out["irrigation_overlap"] = out["irrigation_overlap_rows"].gt(0)
    out["independent_event"] = ~out["irrigation_overlap"]
    return out


def _window_stats(
    series: pd.Series, start: pd.Timestamp, end: pd.Timestamp
) -> tuple[float, float]:
    values = pd.to_numeric(series.loc[(series.index >= start) & (series.index <= end)], errors="coerce").dropna()
    expected = max(1, int(round((end - start) / pd.Timedelta(minutes=15))) + 1)
    median = float(values.median()) if not values.empty else np.nan
    return median, min(1.0, len(values) / expected)


def analyze_precipitation_sensor_responses(
    logger: pd.DataFrame,
    events: pd.DataFrame,
    *,
    config: PrecipitationRetentionConfig = PrecipitationRetentionConfig(),
) -> pd.DataFrame:
    """Calculate VWC response, local temperature and QC for every sensor/event."""

    data = _with_datetime_index(logger)
    sensors = [column for column in data if VWC_RE.match(column)]
    half_window = pd.Timedelta(hours=config.retained_window_hours / 2)
    rows: list[dict[str, object]] = []
    for event in events.itertuples(index=False):
        start, end = pd.Timestamp(event.event_start), pd.Timestamp(event.event_end)
        baseline_start = start - pd.Timedelta(hours=config.baseline_hours)
        peak_end = end + pd.Timedelta(hours=config.peak_followup_hours)
        qc_end = end + pd.Timedelta(hours=72) + half_window
        for sensor in sensors:
            match = VWC_RE.match(sensor)
            assert match is not None
            depth, strip, position = match.group("depth", "strip", "position")
            temp_col = f"T_{depth}_raw_{strip}_{position}"
            vwc = pd.to_numeric(data[sensor], errors="coerce")
            baseline, baseline_cov = _window_stats(
                vwc, baseline_start, start - pd.Timedelta(minutes=15)
            )
            peak_values = vwc.loc[(vwc.index >= start) & (vwc.index <= peak_end)].dropna()
            peak = float(peak_values.max()) if not peak_values.empty else np.nan
            peak_time = peak_values.idxmax() if not peak_values.empty else pd.NaT
            retained: dict[int, float] = {}
            coverage: dict[int, float] = {}
            for hour in (24, 48, 72):
                center = end + pd.Timedelta(hours=hour)
                retained[hour], coverage[hour] = _window_stats(
                    vwc, center - half_window, center + half_window
                )

            temp_values = (
                pd.to_numeric(data[temp_col], errors="coerce").loc[baseline_start:qc_end].dropna()
                if temp_col in data else pd.Series(dtype=float)
            )
            expected_temp = max(1, int(round((qc_end - baseline_start) / pd.Timedelta(minutes=15))) + 1)
            temp_coverage = min(1.0, len(temp_values) / expected_temp)
            temp_min = float(temp_values.min()) if not temp_values.empty else np.nan
            qc_values = vwc.loc[baseline_start:qc_end].dropna()
            out_of_range = bool(((qc_values < 0) | (qc_values > config.max_vwc_pct)).any())
            max_step = float(qc_values.diff().abs().max()) if len(qc_values) > 1 else np.nan
            discontinuity = bool(np.isfinite(max_step) and max_step > config.max_step_change_vwc)
            freeze = bool(np.isfinite(temp_min) and temp_min <= config.freeze_threshold_f)
            missing_vwc = baseline_cov < config.min_window_coverage or any(
                value < config.min_window_coverage for value in coverage.values()
            )
            missing_temp = temp_coverage < config.min_window_coverage
            gain = peak - baseline if np.isfinite(peak) and np.isfinite(baseline) else np.nan
            responded = bool(np.isfinite(gain) and gain >= config.response_threshold_vwc)
            qc_pass = not (missing_vwc or missing_temp or freeze or out_of_range or discontinuity)
            independent = bool(event.independent_event)
            retention_eligible = independent and qc_pass and responded
            if missing_temp:
                temperature_class = "missing"
            elif freeze:
                temperature_class = "near_freezing"
            elif temp_min < config.warm_event_min_temperature_f:
                temperature_class = "cold_nonfreezing"
            else:
                temperature_class = "warm"
            empirical_max_eligible = retention_eligible and temperature_class == "warm"
            reasons: list[str] = []
            if not independent: reasons.append("irrigation_overlap")
            if missing_vwc: reasons.append("insufficient_vwc")
            if missing_temp: reasons.append("insufficient_local_temperature")
            if freeze: reasons.append("local_temperature_near_freezing")
            if out_of_range: reasons.append("vwc_out_of_range")
            if discontinuity: reasons.append("vwc_discontinuity")
            if not responded: reasons.append("no_material_response")

            rows.append({
                "event_id": event.event_id, "year": event.year,
                "event_start": start, "event_end": end,
                "precip_total_in": event.precip_total_in,
                "independent_event": independent,
                "sensor_col": sensor, "temperature_col": temp_col,
                "temperature_source": "co_located_cs650_logger",
                "strip": strip, "treatment": STRIP_TREATMENT[strip],
                "pair": STRIP_TREATMENT_PAIR[strip],
                "logger_position": position, "depth_index": int(depth),
                "depth_inches": SENSOR_DEPTH_INDEX_TO_INCHES[depth],
                "baseline_vwc": baseline, "peak_vwc": peak,
                "peak_timestamp": peak_time, "peak_gain_vwc": gain,
                "retained_24h_vwc": retained[24],
                "retained_48h_vwc": retained[48],
                "retained_72h_vwc": retained[72],
                "gain_24h_vwc": retained[24] - baseline,
                "gain_48h_vwc": retained[48] - baseline,
                "gain_72h_vwc": retained[72] - baseline,
                "decline_peak_to_72h_vwc": peak - retained[72],
                "local_temperature_min_f": temp_min,
                "local_temperature_coverage": temp_coverage,
                "freeze_affected": freeze, "vwc_max_step_change": max_step,
                "missing_vwc": missing_vwc,
                "missing_local_temperature": missing_temp,
                "vwc_out_of_range": out_of_range,
                "vwc_discontinuity": discontinuity,
                "material_response": responded, "sensor_qc_pass": qc_pass,
                "local_temperature_class": temperature_class,
                "retention_eligible": retention_eligible,
                "empirical_max_eligible": empirical_max_eligible,
                "eligibility_reason": "ok" if retention_eligible else ";".join(reasons),
                "empirical_max_reason": (
                    "ok" if empirical_max_eligible else
                    "cold_nonfreezing_corroborating_only" if retention_eligible else
                    ";".join(reasons)
                ),
            })
    return pd.DataFrame(rows)


def build_profile_retention(sensor_results: pd.DataFrame) -> pd.DataFrame:
    """Combine complete eligible 6/12/18-inch profiles into water inches."""

    metrics = ["baseline_vwc", "peak_vwc", "retained_24h_vwc", "retained_48h_vwc", "retained_72h_vwc"]
    keys = ["event_id", "year", "event_start", "event_end", "precip_total_in", "strip", "treatment", "pair", "logger_position"]
    rows: list[dict[str, object]] = []
    for key, group in sensor_results.groupby(keys, dropna=False):
        eligible = group.loc[group["retention_eligible"]]
        if set(eligible["depth_index"]) != {1, 2, 3}:
            continue
        row = dict(zip(keys, key))
        for metric in metrics:
            row[metric.replace("vwc", "profile_water_in")] = pd.to_numeric(
                eligible[metric], errors="coerce"
            ).sum() * 0.06
        row["gain_72h_profile_water_in"] = (
            row["retained_72h_profile_water_in"] - row["baseline_profile_water_in"]
        )
        rows.append(row)
    return pd.DataFrame(rows)


def build_paired_treatment_summary(sensor_results: pd.DataFrame) -> pd.DataFrame:
    """Create matched biochar-minus-control sensor comparisons by event."""

    eligible = sensor_results.loc[sensor_results["retention_eligible"]]
    metrics = ["baseline_vwc", "peak_vwc", "retained_24h_vwc", "retained_48h_vwc", "retained_72h_vwc", "gain_72h_vwc", "decline_peak_to_72h_vwc"]
    keys = ["event_id", "year", "event_start", "pair", "logger_position", "depth_index", "depth_inches"]
    rows: list[pd.DataFrame] = []
    for pair, (biochar_strip, control_strip) in PAIR_STRIPS.items():
        subset = eligible.loc[eligible["pair"].eq(pair)]
        bio = subset.loc[subset["strip"].eq(biochar_strip), keys + metrics]
        control = subset.loc[subset["strip"].eq(control_strip), keys + metrics]
        matched = bio.merge(control, on=keys, suffixes=("_biochar", "_control"))
        if matched.empty:
            continue
        matched["biochar_strip"], matched["control_strip"] = biochar_strip, control_strip
        for metric in metrics:
            matched[f"difference_{metric}"] = matched[f"{metric}_biochar"] - matched[f"{metric}_control"]
        rows.append(matched)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def summarize_empirical_maxima(sensor_results: pd.DataFrame) -> pd.DataFrame:
    """Summarize repeated upper retained VWC observations by physical sensor."""

    eligible = sensor_results.loc[sensor_results["empirical_max_eligible"]].copy()
    if eligible.empty:
        return pd.DataFrame()
    eligible["upper_retained_vwc"] = eligible[["retained_48h_vwc", "retained_72h_vwc"]].max(axis=1)
    keys = ["strip", "treatment", "pair", "logger_position", "depth_index", "depth_inches", "sensor_col"]
    summary = eligible.groupby(keys, as_index=False).agg(
        eligible_event_count=("event_id", "nunique"),
        empirical_max_retained_vwc=("upper_retained_vwc", "max"),
        retained_vwc_p90=("upper_retained_vwc", lambda s: s.quantile(0.90)),
        retained_vwc_median=("upper_retained_vwc", "median"),
        retained_vwc_sd=("upper_retained_vwc", "std"),
        first_eligible_year=("year", "min"), last_eligible_year=("year", "max"),
    )
    summary["max_minus_p90_vwc"] = (
        summary["empirical_max_retained_vwc"] - summary["retained_vwc_p90"]
    )
    summary["max_minus_p90_pct"] = np.where(
        summary["empirical_max_retained_vwc"].gt(0),
        100 * summary["max_minus_p90_vwc"] / summary["empirical_max_retained_vwc"],
        np.nan,
    )
    summary = _add_adoption_fields(summary)
    rounded = [
        "empirical_max_retained_vwc", "retained_vwc_p90",
        "retained_vwc_median", "retained_vwc_sd", "max_minus_p90_vwc",
        "max_minus_p90_pct", "recommended_upper_retained_vwc",
    ]
    summary[rounded] = summary[rounded].round(2)
    return summary


def _add_adoption_fields(summary: pd.DataFrame) -> pd.DataFrame:
    """Add common support and maximum-versus-P90 review fields."""

    out = summary.copy()
    config = PrecipitationRetentionConfig()
    out["max_p90_gap_review"] = out["max_minus_p90_pct"].ge(
        config.maximum_p90_gap_review_pct
    )

    def classify(row: pd.Series) -> str:
        count = int(row["eligible_event_count"])
        if count == 1:
            return "insufficient"
        if count < config.minimum_supported_events:
            if row["max_p90_gap_review"]:
                return "provisional_review_max_p90_gap"
            return "provisional"
        if row["max_p90_gap_review"]:
            return "review_max_p90_gap"
        return "supported"

    out["adoption_classification"] = out.apply(classify, axis=1)
    out["recommended_upper_retained_vwc"] = out["retained_vwc_p90"]
    return out


def build_irrigation_capacity_observations(
    event_results: pd.DataFrame,
    trustworthy_events: pd.DataFrame,
) -> pd.DataFrame:
    """Return trustworthy, physically plausible irrigation plateau observations."""

    if event_results.empty or trustworthy_events.empty:
        return pd.DataFrame()
    keys = ["year", "strip", "event_id", "sensor_col"]
    trusted = trustworthy_events.loc[
        trustworthy_events["trustworthy_event"].fillna(False).astype(bool), keys
    ].drop_duplicates()
    merged = event_results.merge(trusted, on=keys, how="inner")
    for column in ("baseline_vwc", "plateau_vwc", "depth_index", "depth_inches"):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    usable = (
        merged["plateau_vwc"].between(0, 80, inclusive="both")
        & merged["baseline_vwc"].notna()
        & merged["plateau_vwc"].ge(merged["baseline_vwc"])
        & ~merged["plateau_method"].astype("string").eq("no_peak")
    )
    out = merged.loc[usable].copy()
    out["capacity_source"] = "irrigation"
    out["capacity_vwc"] = out["plateau_vwc"]
    out["capacity_timestamp"] = pd.to_datetime(
        out["irrigation_start"], errors="coerce"
    ) + pd.to_timedelta(out["time_to_plateau_hours"], unit="h")
    out["treatment"] = out["strip"].map(STRIP_TREATMENT)
    out["pair"] = out["strip"].map(STRIP_TREATMENT_PAIR)
    keep = [
        "capacity_source", "capacity_vwc", "capacity_timestamp", "event_id",
        "year", "strip", "treatment", "pair", "logger_position",
        "depth_index", "depth_inches", "sensor_col", "baseline_vwc",
        "plateau_vwc", "plateau_method", "irrigation_start", "irrigation_end",
    ]
    return out[keep]


def build_combined_empirical_capacity(
    precipitation_sensor_results: pd.DataFrame,
    irrigation_observations: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Combine eligible precipitation and irrigation capacity evidence by sensor."""

    precip = precipitation_sensor_results.loc[
        precipitation_sensor_results["empirical_max_eligible"].fillna(False).astype(bool)
    ].copy()
    precip["capacity_source"] = "precipitation"
    precip["capacity_vwc"] = precip[["retained_48h_vwc", "retained_72h_vwc"]].max(axis=1)
    precip["capacity_timestamp"] = pd.to_datetime(precip["event_end"], errors="coerce")
    observation_columns = [
        "capacity_source", "capacity_vwc", "capacity_timestamp", "event_id",
        "year", "strip", "treatment", "pair", "logger_position",
        "depth_index", "depth_inches", "sensor_col",
    ]
    observations = pd.concat(
        [precip[observation_columns], irrigation_observations[observation_columns]],
        ignore_index=True,
    )
    if observations.empty:
        return observations, pd.DataFrame()

    keys = [
        "strip", "treatment", "pair", "logger_position", "depth_index",
        "depth_inches", "sensor_col",
    ]

    def summarize(group: pd.DataFrame) -> pd.Series:
        numeric_values = pd.to_numeric(group["capacity_vwc"], errors="coerce")
        values = numeric_values.dropna()
        maximum = float(values.max())
        p90 = float(values.quantile(0.90))
        maximum_row = group.loc[numeric_values.idxmax()]
        source_maxima = group.groupby("capacity_source")["capacity_vwc"].max()
        highest_source = "+".join(source_maxima.index[source_maxima.eq(source_maxima.max())])
        return pd.Series({
            "eligible_event_count": group["event_id"].nunique(),
            "empirical_max_retained_vwc": maximum,
            "retained_vwc_p90": p90,
            "retained_vwc_median": float(values.median()),
            "retained_vwc_sd": float(values.std()),
            "first_eligible_year": int(group["year"].min()),
            "last_eligible_year": int(group["year"].max()),
            "available_sources": "+".join(sorted(group["capacity_source"].unique())),
            "highest_observed_source": highest_source,
            "highest_observed_event_id": maximum_row["event_id"],
            "highest_observed_timestamp": maximum_row["capacity_timestamp"],
        })

    combined = observations.groupby(keys, as_index=False).apply(
        summarize, include_groups=False
    ).reset_index(drop=True)
    combined["max_minus_p90_vwc"] = (
        combined["empirical_max_retained_vwc"] - combined["retained_vwc_p90"]
    )
    combined["max_minus_p90_pct"] = np.where(
        combined["empirical_max_retained_vwc"].gt(0),
        100 * combined["max_minus_p90_vwc"] / combined["empirical_max_retained_vwc"],
        np.nan,
    )
    combined = _add_adoption_fields(combined)

    source_summary = observations.groupby(keys + ["capacity_source"], as_index=False).agg(
        source_event_count=("event_id", "nunique"),
        source_max_vwc=("capacity_vwc", "max"),
        source_p90_vwc=("capacity_vwc", lambda values: values.quantile(0.90)),
        source_median_vwc=("capacity_vwc", "median"),
        source_sd_vwc=("capacity_vwc", "std"),
    )
    source_wide = source_summary.pivot(index=keys, columns="capacity_source").reset_index()
    source_wide.columns = [
        column if isinstance(column, str) else
        column[0] if not column[1] else f"{column[1]}_{column[0]}"
        for column in source_wide.columns
    ]
    combined = combined.merge(source_wide, on=keys, how="left")
    combined["precip_minus_irrigation_p90_vwc"] = (
        combined.get("precipitation_source_p90_vwc")
        - combined.get("irrigation_source_p90_vwc")
    )
    combined["source_p90_absolute_difference_vwc"] = combined[
        "precip_minus_irrigation_p90_vwc"
    ].abs()
    combined["source_agreement_classification"] = np.select(
        [
            combined["source_p90_absolute_difference_vwc"].isna(),
            combined["source_p90_absolute_difference_vwc"].le(2),
            combined["source_p90_absolute_difference_vwc"].le(5),
        ],
        ["one_source_only", "within_2_vwc", "within_5_vwc"],
        default="divergent_over_5_vwc",
    )
    numeric = combined.select_dtypes(include=["number"]).columns
    combined[numeric] = combined[numeric].round(2)
    observations["capacity_vwc"] = pd.to_numeric(
        observations["capacity_vwc"], errors="coerce"
    ).round(2)
    return observations, combined


def build_zone_upper_retained_water(
    combined_capacity: pd.DataFrame,
) -> pd.DataFrame:
    """Convert three-depth P90 VWC profiles to water volume by logger zone.

    The horizontally installed sensors centered at 6, 12 and 18 inches
    represent three six-inch layers spanning approximately 3--21 inches. The
    resulting volume is an operational upper retained-water estimate for that
    represented 18-inch profile. It is not saturation, plant-available water,
    additional irrigation storage, or a direct 0--18-inch measurement.
    """

    columns = [
        "strip", "treatment", "pair", "logger_position", "zone_length_ft",
        "strip_width_ft", "zone_area_sqft", "zone_area_method", "profile_lower_bound_in",
        "profile_upper_bound_in", "represented_profile_thickness_in",
        "recommended_vwc_6in_pct", "recommended_vwc_12in_pct",
        "recommended_vwc_18in_pct", "profile_equivalent_water_in",
        "profile_weighted_mean_vwc_pct", "zone_gallons_per_water_inch",
        "estimated_upper_retained_water_gal", "estimated_upper_retained_water_acre_in",
        "strip_total_upper_retained_water_gal", "minimum_eligible_event_count",
        "review_depth_count", "divergent_source_depth_count", "estimate_status",
        "water_volume_basis",
    ]
    if combined_capacity.empty:
        return pd.DataFrame(columns=columns)

    required = {
        "strip", "treatment", "pair", "logger_position", "depth_index",
        "recommended_upper_retained_vwc", "eligible_event_count",
        "adoption_classification", "source_agreement_classification",
    }
    missing = sorted(required.difference(combined_capacity.columns))
    if missing:
        raise ValueError(
            "Combined empirical capacity is missing required columns: "
            + ", ".join(missing)
        )

    data = combined_capacity.copy()
    data["depth_index"] = pd.to_numeric(data["depth_index"], errors="coerce")
    data["recommended_upper_retained_vwc"] = pd.to_numeric(
        data["recommended_upper_retained_vwc"], errors="coerce"
    )
    keys = ["strip", "treatment", "pair", "logger_position"]
    records: list[dict[str, object]] = []
    for key, group in data.groupby(keys, sort=True, dropna=False):
        by_depth = group.dropna(
            subset=["depth_index", "recommended_upper_retained_vwc"]
        ).drop_duplicates("depth_index").set_index("depth_index")
        if not {1, 2, 3}.issubset(by_depth.index):
            continue
        strip, treatment, pair, position = key
        if strip not in ZONE_AREAS_SQFT_BY_STRIP or position not in {"T", "M", "B"}:
            continue
        layer_water = {
            depth: (
                float(by_depth.loc[depth, "recommended_upper_retained_vwc"])
                / 100.0
                * REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX[depth]
            )
            for depth in (1, 2, 3)
        }
        profile_water_in = sum(layer_water.values())
        zone_area_sqft = ZONE_AREAS_SQFT_BY_STRIP[strip][position]
        gallons_per_inch = ZONE_GALLONS_PER_INCH_BY_STRIP[strip][position]
        gallons = profile_water_in * gallons_per_inch
        review_count = int(
            by_depth.loc[[1, 2, 3], "adoption_classification"]
            .astype("string").str.contains("review", na=False).sum()
        )
        divergent_count = int(
            by_depth.loc[[1, 2, 3], "source_agreement_classification"]
            .astype("string").eq("divergent_over_5_vwc").sum()
        )
        if review_count:
            estimate_status = "review_component_max_p90_gap"
        elif divergent_count:
            estimate_status = "supported_with_source_disagreement"
        else:
            estimate_status = "supported"
        records.append({
            "strip": strip,
            "treatment": treatment,
            "pair": pair,
            "logger_position": position,
            "zone_length_ft": ZONE_LENGTHS_FT_BY_STRIP[strip][position],
            "strip_width_ft": STRIP_WIDTH_FT,
            "zone_area_sqft": zone_area_sqft,
            "zone_area_method": ZONE_AREA_SOURCE_BY_STRIP[strip][position],
            "profile_lower_bound_in": THREE_SENSOR_PROFILE_LOWER_BOUND_IN,
            "profile_upper_bound_in": THREE_SENSOR_PROFILE_UPPER_BOUND_IN,
            "represented_profile_thickness_in": THREE_SENSOR_PROFILE_NOMINAL_THICKNESS_IN,
            "recommended_vwc_6in_pct": by_depth.loc[1, "recommended_upper_retained_vwc"],
            "recommended_vwc_12in_pct": by_depth.loc[2, "recommended_upper_retained_vwc"],
            "recommended_vwc_18in_pct": by_depth.loc[3, "recommended_upper_retained_vwc"],
            "profile_equivalent_water_in": profile_water_in,
            "profile_weighted_mean_vwc_pct": (
                100 * profile_water_in / THREE_SENSOR_PROFILE_NOMINAL_THICKNESS_IN
            ),
            "zone_gallons_per_water_inch": gallons_per_inch,
            "estimated_upper_retained_water_gal": gallons,
            "estimated_upper_retained_water_acre_in": (
                profile_water_in * zone_area_sqft / 43_560.0
            ),
            "minimum_eligible_event_count": int(
                pd.to_numeric(
                    by_depth.loc[[1, 2, 3], "eligible_event_count"], errors="coerce"
                ).min()
            ),
            "review_depth_count": review_count,
            "divergent_source_depth_count": divergent_count,
            "estimate_status": estimate_status,
            "water_volume_basis": (
                "P90 combined precipitation+irrigation retained VWC; "
                "three 6-inch sensor-centered layers spanning 3-21 inches"
            ),
        })

    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=columns)
    out["strip_total_upper_retained_water_gal"] = out.groupby("strip")[
        "estimated_upper_retained_water_gal"
    ].transform("sum")
    two_decimal = [
        "zone_length_ft", "strip_width_ft", "zone_area_sqft",
        "recommended_vwc_6in_pct", "recommended_vwc_12in_pct",
        "recommended_vwc_18in_pct", "profile_equivalent_water_in",
        "profile_weighted_mean_vwc_pct", "zone_gallons_per_water_inch",
        "estimated_upper_retained_water_acre_in",
    ]
    out[two_decimal] = out[two_decimal].round(2)
    whole_gallons = [
        "estimated_upper_retained_water_gal",
        "strip_total_upper_retained_water_gal",
    ]
    out[whole_gallons] = out[whole_gallons].round().astype("Int64")
    position_order = pd.Categorical(
        out["logger_position"], categories=["T", "M", "B"], ordered=True
    )
    out = out.assign(_position_order=position_order).sort_values(
        ["strip", "_position_order"]
    ).drop(columns="_position_order")
    return out[columns].reset_index(drop=True)


def build_irrigation_available_storage_comparison(
    irrigation_events: pd.DataFrame,
    irrigation_qc: pd.DataFrame,
    combined_capacity: pd.DataFrame,
) -> pd.DataFrame:
    """Compare applied irrigation with pre-event available profile storage.

    Available storage is the positive difference between the recommended P90
    upper retained VWC and event baseline VWC in each of the three six-inch,
    sensor-centered layers. Whole-strip comparison requires all nine sensors.
    Water above this threshold is not retained in the represented 3--21-inch
    profile; it is not a direct measurement of surface runoff.
    """
    columns = [
        "year", "event_id", "strip", "pair", "treatment",
        "irrigation_start", "irrigation_end", "applied_irrigation_gal_strip",
        "comparison_eligible", "ineligibility_reason",
        "baseline_profile_water_gal_strip", "upper_retained_water_gal_strip",
        "available_storage_gal_strip", "baseline_above_upper_retained_gal_strip",
        "applied_minus_available_storage_gal_strip",
        "estimated_water_not_retained_gal_strip",
        "applied_fraction_of_available_storage",
        "irrigation_exceeds_available_storage", "zone_area_method",
        "represented_profile", "interpretation",
    ]
    if irrigation_events.empty:
        return pd.DataFrame(columns=columns)

    events = irrigation_events.copy()
    qc = irrigation_qc.copy()
    for frame in (events, qc):
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    qc_keys = ["year", "event_id", "strip", "sensor_col"]
    trusted = qc[qc_keys + ["trustworthy_event"]].drop_duplicates(qc_keys)
    events = events.merge(trusted, on=qc_keys, how="left")
    events["trustworthy_event"] = events["trustworthy_event"].eq(True)
    for column in ("depth_index", "baseline_vwc", "gallons_strip"):
        events[column] = pd.to_numeric(events[column], errors="coerce")

    capacity = combined_capacity.copy()
    capacity["depth_index"] = pd.to_numeric(capacity["depth_index"], errors="coerce")
    capacity["recommended_upper_retained_vwc"] = pd.to_numeric(
        capacity["recommended_upper_retained_vwc"], errors="coerce"
    )
    capacity_lookup = capacity.set_index(
        ["strip", "logger_position", "depth_index"]
    )["recommended_upper_retained_vwc"].to_dict()

    group_columns = [column for column in (
        "year", "event_id", "strip", "strip_group", "irrigation_start",
        "irrigation_end", "gallons_strip",
    ) if column in events.columns]
    expected = {(position, depth) for position in "TMB" for depth in (1, 2, 3)}
    records: list[dict[str, object]] = []
    for keys, group in events.groupby(group_columns, dropna=False, sort=True):
        keys = keys if isinstance(keys, tuple) else (keys,)
        row = dict(zip(group_columns, keys))
        strip = str(row["strip"])
        reasons: list[str] = []
        present = {
            (str(position), int(depth))
            for position, depth in zip(group["logger_position"], group["depth_index"])
            if pd.notna(depth)
        }
        if present != expected:
            reasons.append("incomplete_nine_sensor_profile")
        if not group["trustworthy_event"].all():
            reasons.append("one_or_more_sensor_events_not_trustworthy")
        if group["baseline_vwc"].isna().any():
            reasons.append("one_or_more_baselines_missing")

        baseline_total = upper_total = available_total = baseline_above_total = 0.0
        area_methods: set[str] = set()
        for position in "TMB":
            zone = group.loc[group["logger_position"].astype(str).eq(position)]
            zone = zone.drop_duplicates("depth_index").set_index("depth_index")
            if not {1, 2, 3}.issubset(zone.index):
                continue
            capacity_values = [
                capacity_lookup.get((strip, position, depth)) for depth in (1, 2, 3)
            ]
            if any(value is None or pd.isna(value) for value in capacity_values):
                reasons.append(f"missing_capacity_{position}")
                continue
            baselines = pd.to_numeric(
                zone.loc[[1, 2, 3], "baseline_vwc"], errors="coerce"
            )
            if baselines.isna().any():
                continue
            baseline_in = sum(
                float(baselines.loc[depth]) / 100
                * REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX[depth]
                for depth in (1, 2, 3)
            )
            upper_in = sum(
                float(capacity_values[depth - 1]) / 100
                * REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX[depth]
                for depth in (1, 2, 3)
            )
            available_in = sum(
                max(
                    float(capacity_values[depth - 1])
                    - float(baselines.loc[depth]),
                    0,
                ) / 100 * REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX[depth]
                for depth in (1, 2, 3)
            )
            baseline_above_in = sum(
                max(
                    float(baselines.loc[depth])
                    - float(capacity_values[depth - 1]),
                    0,
                ) / 100 * REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX[depth]
                for depth in (1, 2, 3)
            )
            gallons_per_inch = ZONE_GALLONS_PER_INCH_BY_STRIP[strip][position]
            baseline_total += baseline_in * gallons_per_inch
            upper_total += upper_in * gallons_per_inch
            available_total += available_in * gallons_per_inch
            baseline_above_total += baseline_above_in * gallons_per_inch
            area_methods.add(ZONE_AREA_SOURCE_BY_STRIP[strip][position])

        applied = pd.to_numeric(
            pd.Series([row.get("gallons_strip")]), errors="coerce"
        ).iloc[0]
        if pd.isna(applied):
            reasons.append("missing_applied_irrigation_volume")
        if available_total <= 0:
            reasons.append("no_positive_available_storage")
        reasons = sorted(set(reasons))
        eligible = not reasons
        excess = float(applied) - available_total if eligible else np.nan
        records.append({
            "year": row.get("year"), "event_id": row.get("event_id"),
            "strip": strip,
            "pair": row.get("strip_group", STRIP_TREATMENT_PAIR.get(strip, "")),
            "treatment": STRIP_TREATMENT.get(strip),
            "irrigation_start": row.get("irrigation_start"),
            "irrigation_end": row.get("irrigation_end"),
            "applied_irrigation_gal_strip": applied,
            "comparison_eligible": eligible,
            "ineligibility_reason": ";".join(reasons),
            "baseline_profile_water_gal_strip": baseline_total if eligible else np.nan,
            "upper_retained_water_gal_strip": upper_total if eligible else np.nan,
            "available_storage_gal_strip": available_total if eligible else np.nan,
            "baseline_above_upper_retained_gal_strip": baseline_above_total if eligible else np.nan,
            "applied_minus_available_storage_gal_strip": excess,
            "estimated_water_not_retained_gal_strip": max(excess, 0) if eligible else np.nan,
            "applied_fraction_of_available_storage": float(applied) / available_total if eligible else np.nan,
            "irrigation_exceeds_available_storage": bool(excess > 0) if eligible else pd.NA,
            "zone_area_method": ";".join(sorted(area_methods)),
            "represented_profile": "three 6-inch layers spanning approximately 3-21 inches",
            "interpretation": "modeled water not retained; not measured runoff",
        })

    out = pd.DataFrame(records, columns=columns)
    numeric = out.select_dtypes(include=["number"]).columns
    out[numeric] = out[numeric].round(2)
    return out.sort_values(["year", "event_id", "strip"]).reset_index(drop=True)


def build_irrigation_operational_diagnostics(
    available_storage: pd.DataFrame,
    arrival_times: pd.DataFrame,
    irrigation_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Add flow, duration, arrival, and candidate alert times to capacity rows.

    Threshold timestamps assume the calculated event-average strip flow. They
    are retrospective modeled times, not observed instantaneous meter flow.
    The candidate inspection time is the later of (a) earliest sustained
    bottom-zone VWC response and (b) 80 percent of modeled available storage.
    It is intended for field validation before any automated stopping rule.
    """
    if available_storage.empty:
        return available_storage.copy()

    out = available_storage.copy()
    key = ["year", "event_id", "strip"]
    for frame in (out, arrival_times, irrigation_metadata):
        if "year" in frame:
            frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")

    metadata_columns = key + [column for column in (
        "event_duration_hours", "avg_flow_gph_strip", "avg_flow_gpm_strip",
        "avg_flow_gpm_group", "start_flow_gpm", "end_flow_gpm",
        "flow_allocation_fraction", "notes",
    ) if column in irrigation_metadata.columns]
    metadata = irrigation_metadata[metadata_columns].drop_duplicates(key)
    out = out.merge(metadata, on=key, how="left", suffixes=("", "_workbook"))

    arrivals = arrival_times.copy()
    if not arrivals.empty:
        arrivals["arrival_time"] = pd.to_datetime(arrivals["arrival_time"], errors="coerce")
        arrivals["arrival_minutes_after_irrigation_start"] = pd.to_numeric(
            arrivals["arrival_minutes_after_irrigation_start"], errors="coerce"
        )
        arrivals["depth_index"] = pd.to_numeric(arrivals["depth_index"], errors="coerce")
        records: list[dict[str, object]] = []
        for keys, group in arrivals.groupby(key, dropna=False, sort=True):
            record = dict(zip(key, keys if isinstance(keys, tuple) else (keys,)))
            for position, label in (("T", "top"), ("M", "middle"), ("B", "bottom")):
                position_rows = group.loc[group["logger_position"].astype(str).eq(position)]
                valid = position_rows.dropna(subset=["arrival_time"])
                record[f"{label}_earliest_arrival_time"] = (
                    valid["arrival_time"].min() if not valid.empty else pd.NaT
                )
                record[f"{label}_earliest_arrival_delay_hr"] = (
                    valid["arrival_minutes_after_irrigation_start"].min() / 60
                    if not valid.empty else np.nan
                )
                depth_6 = valid.loc[valid["depth_index"].eq(1)]
                record[f"{label}_6in_arrival_time"] = (
                    depth_6["arrival_time"].min() if not depth_6.empty else pd.NaT
                )
                record[f"{label}_6in_arrival_delay_hr"] = (
                    depth_6["arrival_minutes_after_irrigation_start"].min() / 60
                    if not depth_6.empty else np.nan
                )
                complete = set(valid["depth_index"].dropna().astype(int)) == {1, 2, 3}
                record[f"{label}_all_depths_arrived"] = complete
                record[f"{label}_all_depths_arrival_time"] = (
                    valid["arrival_time"].max() if complete else pd.NaT
                )
                record[f"{label}_all_depths_arrival_delay_hr"] = (
                    valid["arrival_minutes_after_irrigation_start"].max() / 60
                    if complete else np.nan
                )
            records.append(record)
        out = out.merge(pd.DataFrame(records), on=key, how="left")

    out["irrigation_start"] = pd.to_datetime(out["irrigation_start"], errors="coerce")
    out["irrigation_end"] = pd.to_datetime(out["irrigation_end"], errors="coerce")
    out["avg_flow_gph_strip"] = pd.to_numeric(
        out.get("avg_flow_gph_strip"), errors="coerce"
    )
    out["available_storage_gal_strip"] = pd.to_numeric(
        out["available_storage_gal_strip"], errors="coerce"
    )
    model_valid = (
        out["comparison_eligible"].eq(True)
        & out["avg_flow_gph_strip"].gt(0)
        & out["available_storage_gal_strip"].gt(0)
        & out["irrigation_start"].notna()
    )
    for fraction in (0.50, 0.80, 0.90, 1.00):
        pct = int(fraction * 100)
        hours = (
            fraction * out["available_storage_gal_strip"]
            / out["avg_flow_gph_strip"]
        ).where(model_valid)
        out[f"modeled_hours_to_{pct}pct_available_storage"] = hours
        out[f"modeled_time_{pct}pct_available_storage"] = (
            out["irrigation_start"] + pd.to_timedelta(hours, unit="h")
        )
        out[f"{pct}pct_threshold_before_irrigation_end"] = (
            out[f"modeled_time_{pct}pct_available_storage"].le(out["irrigation_end"])
        ).where(model_valid)

    for label in ("top", "middle", "bottom"):
        delay = pd.to_numeric(
            out.get(f"{label}_earliest_arrival_delay_hr"), errors="coerce"
        )
        out[f"applied_gal_at_{label}_earliest_arrival"] = (
            delay.clip(lower=0) * out["avg_flow_gph_strip"]
        ).where(model_valid & delay.notna())
        out[f"applied_fraction_at_{label}_earliest_arrival"] = (
            out[f"applied_gal_at_{label}_earliest_arrival"]
            / out["available_storage_gal_strip"]
        )

    bottom_time = pd.to_datetime(out.get("bottom_earliest_arrival_time"), errors="coerce")
    threshold_80 = pd.to_datetime(out["modeled_time_80pct_available_storage"], errors="coerce")
    out["candidate_inspection_alert_time"] = pd.concat(
        [bottom_time.rename("bottom"), threshold_80.rename("threshold")], axis=1
    ).max(axis=1)
    out["candidate_alert_available"] = (
        model_valid & bottom_time.notna() & threshold_80.notna()
    )
    out.loc[~out["candidate_alert_available"], "candidate_inspection_alert_time"] = pd.NaT
    out["candidate_alert_before_irrigation_end"] = (
        out["candidate_inspection_alert_time"].le(out["irrigation_end"])
    ).where(out["candidate_alert_available"])
    out["runtime_after_candidate_alert_hr"] = (
        (out["irrigation_end"] - out["candidate_inspection_alert_time"])
        .dt.total_seconds().div(3600).clip(lower=0)
    ).where(out["candidate_alert_before_irrigation_end"].eq(True))
    out["applied_after_candidate_alert_gal"] = (
        out["runtime_after_candidate_alert_hr"] * out["avg_flow_gph_strip"]
    )
    out["runtime_after_modeled_full_storage_hr"] = (
        (out["irrigation_end"] - out["modeled_time_100pct_available_storage"])
        .dt.total_seconds().div(3600).clip(lower=0)
    ).where(out["100pct_threshold_before_irrigation_end"].eq(True))
    out["applied_after_modeled_full_storage_gal"] = (
        out["runtime_after_modeled_full_storage_hr"] * out["avg_flow_gph_strip"]
    )
    duration = pd.to_numeric(out.get("event_duration_hours"), errors="coerce")
    out["max_avg_flow_gph_to_fit_available_storage_at_actual_duration"] = (
        out["available_storage_gal_strip"] / duration
    ).where(model_valid & duration.gt(0))
    out["modeled_avg_flow_reduction_gph_needed_at_actual_duration"] = (
        out["avg_flow_gph_strip"]
        - out["max_avg_flow_gph_to_fit_available_storage_at_actual_duration"]
    ).clip(lower=0).where(model_valid & duration.gt(0))
    out["modeled_avg_flow_reduction_fraction_needed_at_actual_duration"] = (
        out["modeled_avg_flow_reduction_gph_needed_at_actual_duration"]
        / out["avg_flow_gph_strip"]
    ).where(model_valid & duration.gt(0))
    out["alert_basis"] = (
        "later of earliest sustained bottom response and modeled 80% available storage"
    )
    out["flow_model_caveat"] = (
        "threshold times use event-average strip flow; start/end readings are contextual"
    )

    numeric = out.select_dtypes(include=["number"]).columns
    out[numeric] = out[numeric].round(2)
    return out.sort_values(key).reset_index(drop=True)


def build_matched_complete_profiles(
    capacity_observations: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build complete three-depth profiles and matched treatment differences."""

    if capacity_observations.empty:
        return pd.DataFrame(), pd.DataFrame()
    observations = capacity_observations.copy()
    observations["depth_index"] = pd.to_numeric(
        observations["depth_index"], errors="coerce"
    )
    observations["capacity_vwc"] = pd.to_numeric(
        observations["capacity_vwc"], errors="coerce"
    )
    observations["layer_thickness_in"] = observations["depth_index"].map(
        REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX
    )
    observations["layer_water_in"] = (
        observations["capacity_vwc"] / 100 * observations["layer_thickness_in"]
    )
    profile_keys = [
        "capacity_source", "event_id", "year", "pair", "strip", "treatment",
        "logger_position",
    ]
    profiles = observations.groupby(profile_keys, as_index=False).agg(
        represented_depth_count=("depth_index", "nunique"),
        profile_water_in=("layer_water_in", "sum"),
        minimum_depth_vwc=("capacity_vwc", "min"),
        maximum_depth_vwc=("capacity_vwc", "max"),
    )
    profiles = profiles.loc[profiles["represented_depth_count"].eq(3)].copy()

    match_keys = [
        "capacity_source", "event_id", "year", "pair", "logger_position",
    ]
    values = profiles.pivot_table(
        index=match_keys,
        columns="treatment",
        values="profile_water_in",
        aggfunc="first",
    ).reset_index()
    if not {"biochar", "control"}.issubset(values.columns):
        return profiles, pd.DataFrame()
    matched = values.dropna(subset=["biochar", "control"]).copy()
    matched = matched.rename(columns={
        "biochar": "biochar_profile_water_in",
        "control": "control_profile_water_in",
    })
    matched["biochar_minus_control_profile_water_in"] = (
        matched["biochar_profile_water_in"]
        - matched["control_profile_water_in"]
    )
    numeric = matched.select_dtypes(include=["number"]).columns
    matched[numeric] = matched[numeric].round(3)
    return profiles, matched


def bootstrap_matched_profile_differences(
    matched_profiles: pd.DataFrame,
    *,
    iterations: int = 10_000,
    random_seed: int = 20260830,
    minimum_directional_events: int = 4,
) -> pd.DataFrame:
    """Bootstrap paired event-level profile differences with replacement."""

    if matched_profiles.empty:
        return pd.DataFrame()
    pooled = matched_profiles.copy()
    pooled["capacity_source"] = "combined"
    analysis = pd.concat([matched_profiles, pooled], ignore_index=True)
    rng = np.random.default_rng(random_seed)
    records: list[dict[str, object]] = []
    for (source, pair, position), group in analysis.groupby(
        ["capacity_source", "pair", "logger_position"], sort=True
    ):
        differences = pd.to_numeric(
            group["biochar_minus_control_profile_water_in"], errors="coerce"
        ).dropna().to_numpy()
        if not len(differences):
            continue
        samples = rng.choice(
            differences, size=(iterations, len(differences)), replace=True
        )
        bootstrap_means = samples.mean(axis=1)
        bootstrap_medians = np.median(samples, axis=1)
        mean_low, mean_high = np.quantile(bootstrap_means, [0.025, 0.975])
        median_low, median_high = np.quantile(bootstrap_medians, [0.025, 0.975])
        if len(differences) < minimum_directional_events:
            direction = "insufficient_events"
        elif mean_low > 0:
            direction = "biochar_higher"
        elif mean_high < 0:
            direction = "control_higher"
        else:
            direction = "inconclusive"
        records.append({
            "capacity_source": source,
            "pair": pair,
            "logger_position": position,
            "matched_event_count": len(differences),
            "first_year": int(group["year"].min()),
            "last_year": int(group["year"].max()),
            "mean_difference_in": differences.mean(),
            "median_difference_in": np.median(differences),
            "difference_sd_in": differences.std(ddof=1) if len(differences) > 1 else np.nan,
            "individual_events_biochar_higher_pct": 100 * np.mean(differences > 0),
            "bootstrap_mean_ci_95_low_in": mean_low,
            "bootstrap_mean_ci_95_high_in": mean_high,
            "bootstrap_median_ci_95_low_in": median_low,
            "bootstrap_median_ci_95_high_in": median_high,
            "bootstrap_mean_above_zero_pct": 100 * np.mean(bootstrap_means > 0),
            "bootstrap_median_above_zero_pct": 100 * np.mean(bootstrap_medians > 0),
            "bootstrap_direction_classification": direction,
            "bootstrap_iterations": iterations,
            "random_seed": random_seed,
            "minimum_directional_events": minimum_directional_events,
        })
    out = pd.DataFrame(records)
    numeric = out.select_dtypes(include=["number"]).columns
    out[numeric] = out[numeric].round(3)
    return out


def bootstrap_unmatched_profile_differences(
    complete_profiles: pd.DataFrame,
    *,
    iterations: int = 10_000,
    random_seed: int = 20260830,
    minimum_directional_events: int = 4,
) -> pd.DataFrame:
    """Independently bootstrap all complete biochar and control profiles."""

    if complete_profiles.empty:
        return pd.DataFrame()
    pooled = complete_profiles.copy()
    pooled["capacity_source"] = "combined"
    analysis = pd.concat([complete_profiles, pooled], ignore_index=True)
    rng = np.random.default_rng(random_seed)
    records: list[dict[str, object]] = []
    for (source, pair, position), group in analysis.groupby(
        ["capacity_source", "pair", "logger_position"], sort=True
    ):
        biochar = pd.to_numeric(
            group.loc[group["treatment"].eq("biochar"), "profile_water_in"],
            errors="coerce",
        ).dropna().to_numpy()
        control = pd.to_numeric(
            group.loc[group["treatment"].eq("control"), "profile_water_in"],
            errors="coerce",
        ).dropna().to_numpy()
        if not len(biochar) or not len(control):
            continue
        biochar_samples = rng.choice(
            biochar, size=(iterations, len(biochar)), replace=True
        )
        control_samples = rng.choice(
            control, size=(iterations, len(control)), replace=True
        )
        bootstrap_means = biochar_samples.mean(axis=1) - control_samples.mean(axis=1)
        bootstrap_medians = (
            np.median(biochar_samples, axis=1)
            - np.median(control_samples, axis=1)
        )
        mean_low, mean_high = np.quantile(bootstrap_means, [0.025, 0.975])
        median_low, median_high = np.quantile(bootstrap_medians, [0.025, 0.975])
        if min(len(biochar), len(control)) < minimum_directional_events:
            direction = "insufficient_events"
        elif mean_low > 0:
            direction = "biochar_higher"
        elif mean_high < 0:
            direction = "control_higher"
        else:
            direction = "inconclusive"
        records.append({
            "capacity_source": source,
            "pair": pair,
            "logger_position": position,
            "biochar_complete_profile_count": len(biochar),
            "control_complete_profile_count": len(control),
            "biochar_first_year": int(group.loc[
                group["treatment"].eq("biochar"), "year"
            ].min()),
            "biochar_last_year": int(group.loc[
                group["treatment"].eq("biochar"), "year"
            ].max()),
            "control_first_year": int(group.loc[
                group["treatment"].eq("control"), "year"
            ].min()),
            "control_last_year": int(group.loc[
                group["treatment"].eq("control"), "year"
            ].max()),
            "biochar_mean_profile_water_in": biochar.mean(),
            "control_mean_profile_water_in": control.mean(),
            "mean_difference_in": biochar.mean() - control.mean(),
            "biochar_median_profile_water_in": np.median(biochar),
            "control_median_profile_water_in": np.median(control),
            "median_difference_in": np.median(biochar) - np.median(control),
            "bootstrap_mean_ci_95_low_in": mean_low,
            "bootstrap_mean_ci_95_high_in": mean_high,
            "bootstrap_median_ci_95_low_in": median_low,
            "bootstrap_median_ci_95_high_in": median_high,
            "bootstrap_mean_above_zero_pct": 100 * np.mean(bootstrap_means > 0),
            "bootstrap_median_above_zero_pct": 100 * np.mean(bootstrap_medians > 0),
            "bootstrap_direction_classification": direction,
            "bootstrap_iterations": iterations,
            "random_seed": random_seed,
            "minimum_directional_events": minimum_directional_events,
        })
    out = pd.DataFrame(records)
    numeric = out.select_dtypes(include=["number"]).columns
    out[numeric] = out[numeric].round(3)
    return out


def compare_matched_and_unmatched_bootstraps(
    matched_summary: pd.DataFrame,
    unmatched_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Place pooled matched and unmatched profile results side by side."""

    keys = ["pair", "logger_position"]
    matched = matched_summary.loc[
        matched_summary["capacity_source"].eq("combined")
    ].copy()
    unmatched = unmatched_summary.loc[
        unmatched_summary["capacity_source"].eq("combined")
    ].copy()
    matched = matched.drop(columns="capacity_source").add_prefix("matched_")
    unmatched = unmatched.drop(columns="capacity_source").add_prefix("unmatched_")
    matched = matched.rename(columns={f"matched_{key}": key for key in keys})
    unmatched = unmatched.rename(columns={f"unmatched_{key}": key for key in keys})
    comparison = matched.merge(unmatched, on=keys, how="outer")
    comparison["unmatched_minus_matched_mean_difference_in"] = (
        comparison["unmatched_mean_difference_in"]
        - comparison["matched_mean_difference_in"]
    )
    matched_direction = comparison["matched_bootstrap_direction_classification"]
    unmatched_direction = comparison["unmatched_bootstrap_direction_classification"]
    comparison["direction_comparison"] = np.where(
        matched_direction.eq(unmatched_direction),
        "agreement",
        matched_direction + "__vs__" + unmatched_direction,
    )
    numeric = comparison.select_dtypes(include=["number"]).columns
    comparison[numeric] = comparison[numeric].round(3)
    return comparison


def build_profile_best_results(
    bootstrap_comparison: pd.DataFrame,
) -> pd.DataFrame:
    """Create a concise result table with statistical and causal confidence."""

    if bootstrap_comparison.empty:
        return pd.DataFrame()
    out = bootstrap_comparison.copy()
    matched_direction = out["matched_bootstrap_direction_classification"]
    unmatched_direction = out["unmatched_bootstrap_direction_classification"]
    matched_sign = np.sign(out["matched_mean_difference_in"])
    unmatched_sign = np.sign(out["unmatched_mean_difference_in"])
    same_directional_result = (
        matched_direction.eq(unmatched_direction)
        & matched_direction.isin(["biochar_higher", "control_higher"])
    )
    one_directional_same_sign = (
        matched_sign.eq(unmatched_sign)
        & (
            matched_direction.isin(["biochar_higher", "control_higher"])
            | unmatched_direction.isin(["biochar_higher", "control_higher"])
        )
    )
    both_inconclusive = (
        matched_direction.eq("inconclusive")
        & unmatched_direction.eq("inconclusive")
    )
    both_insufficient = (
        matched_direction.eq("insufficient_events")
        & unmatched_direction.eq("insufficient_events")
    )
    out["statistical_repeatability"] = np.select(
        [
            same_directional_result, one_directional_same_sign,
            both_inconclusive, both_insufficient,
        ],
        ["strong", "moderate", "low", "insufficient"],
        default="conflicting",
    )
    out["result_interpretation"] = np.select(
        [
            same_directional_result & matched_direction.eq("biochar_higher"),
            same_directional_result & matched_direction.eq("control_higher"),
            one_directional_same_sign & matched_sign.gt(0),
            one_directional_same_sign & matched_sign.lt(0),
        ],
        [
            "biochar_higher", "control_higher",
            "suggestive_biochar_higher", "suggestive_control_higher",
        ],
        default="no_clear_difference",
    )
    out["matched_bootstrap_support_pct"] = np.where(
        out["matched_mean_difference_in"].ge(0),
        out["matched_bootstrap_mean_above_zero_pct"],
        100 - out["matched_bootstrap_mean_above_zero_pct"],
    )
    out["causal_confidence"] = "limited_single_strip_per_treatment"
    context = {
        ("S1_S2", "B"): (
            "Historical LiDAR microtopography and later soil placement near S1B."
        ),
        ("S3_S4", "T"): (
            "Intermittently operated irrigation ditch east of S4 may affect S4."
        ),
        ("S3_S4", "M"): (
            "Intermittently operated irrigation ditch east of S4 may affect S4."
        ),
        ("S3_S4", "B"): (
            "Irrigation ditch east of S4 plus an intermittently leaking pipe near S4B."
        ),
    }
    out["known_site_context"] = [
        context.get((pair, position), "No additional location-specific issue documented.")
        for pair, position in zip(out["pair"], out["logger_position"])
    ]
    selected = [
        "pair", "logger_position", "result_interpretation",
        "statistical_repeatability", "matched_matched_event_count",
        "matched_mean_difference_in", "matched_bootstrap_mean_ci_95_low_in",
        "matched_bootstrap_mean_ci_95_high_in", "matched_bootstrap_support_pct",
        "unmatched_biochar_complete_profile_count",
        "unmatched_control_complete_profile_count", "unmatched_mean_difference_in",
        "unmatched_bootstrap_mean_ci_95_low_in",
        "unmatched_bootstrap_mean_ci_95_high_in", "direction_comparison",
        "causal_confidence", "known_site_context",
    ]
    result = out[selected].copy()
    result = result.rename(columns={
        "matched_matched_event_count": "matched_event_count",
        "matched_mean_difference_in": "best_mean_difference_in",
        "matched_bootstrap_mean_ci_95_low_in": "best_ci_95_low_in",
        "matched_bootstrap_mean_ci_95_high_in": "best_ci_95_high_in",
    })
    numeric = result.select_dtypes(include=["number"]).columns
    result[numeric] = result[numeric].round(3)
    return result.sort_values(["pair", "logger_position"]).reset_index(drop=True)


def save_empirical_maxima_map(
    maxima: pd.DataFrame,
    output_path: Path,
    *,
    title: str = "Empirical upper retained VWC from warm precipitation events",
    event_label: str = "eligible warm events",
    include_profile: bool = False,
) -> None:
    """Plot depth P90s and, optionally, equivalent three-depth profile water."""

    if maxima.empty:
        return
    strips = ["S1", "S2", "S3", "S4"]
    positions = ["T", "M", "B"]
    depths = [6, 12, 18]
    vmin = float(maxima["retained_vwc_p90"].min())
    vmax = float(maxima["retained_vwc_p90"].max())
    panel_count = 4 if include_profile else 3
    fig, axes = plt.subplots(
        1, panel_count, figsize=(18 if include_profile else 14, 6), sharey=True
    )
    image = None
    for ax, depth in zip(axes, depths):
        grid = np.full((len(positions), len(strips)), np.nan)
        counts = np.zeros_like(grid)
        classes = np.full(grid.shape, "", dtype=object)
        subset = maxima.loc[maxima["depth_inches"].eq(depth)]
        for row in subset.itertuples(index=False):
            y, x = positions.index(row.logger_position), strips.index(row.strip)
            grid[y, x] = row.retained_vwc_p90
            counts[y, x] = row.eligible_event_count
            classes[y, x] = row.adoption_classification
        image = ax.imshow(grid, cmap="YlGnBu", vmin=vmin, vmax=vmax, aspect="auto")
        for y in range(len(positions)):
            for x in range(len(strips)):
                if not np.isfinite(grid[y, x]):
                    continue
                flag = "*" if classes[y, x] != "supported" else ""
                ax.text(x, y, f"{grid[y, x]:.2f}{flag}\nn={int(counts[y, x])}",
                        ha="center", va="center", fontsize=10,
                        color="white" if grid[y, x] > (vmin + vmax) / 2 else "black")
        ax.set_title(f"{depth} in depth")
        ax.set_xticks(range(len(strips)), ["S1\nbiochar", "S2\ncontrol", "S3\nbiochar", "S4\ncontrol"])
        ax.set_yticks(range(len(positions)), [
            f"Top ({LOGGER_GEOMETRY['T']['distance_from_furrow_start_ft']} ft)",
            f"Middle ({LOGGER_GEOMETRY['M']['distance_from_furrow_start_ft']} ft)",
            f"Bottom ({LOGGER_GEOMETRY['B']['distance_from_furrow_start_ft']} ft)",
        ])
        ax.set_xlabel("Strip")
    assert image is not None
    if include_profile:
        profile = maxima.copy()
        profile["layer_thickness_in"] = profile["depth_index"].map(
            REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX
        )
        profile["layer_p90_water_in"] = (
            profile["retained_vwc_p90"] / 100 * profile["layer_thickness_in"]
        )
        profile = profile.groupby(["strip", "logger_position"], as_index=False).agg(
            represented_depth_count=("depth_index", "nunique"),
            profile_p90_water_in=("layer_p90_water_in", "sum"),
            minimum_component_event_count=("eligible_event_count", "min"),
            component_review=(
                "adoption_classification",
                lambda values: any(value != "supported" for value in values),
            ),
        )
        profile = profile.loc[profile["represented_depth_count"].eq(3)]
        profile_grid = np.full((len(positions), len(strips)), np.nan)
        profile_counts = np.zeros_like(profile_grid)
        profile_reviews = np.zeros_like(profile_grid, dtype=bool)
        for row in profile.itertuples(index=False):
            y, x = positions.index(row.logger_position), strips.index(row.strip)
            profile_grid[y, x] = row.profile_p90_water_in
            profile_counts[y, x] = row.minimum_component_event_count
            profile_reviews[y, x] = row.component_review
        profile_min = float(np.nanmin(profile_grid))
        profile_max = float(np.nanmax(profile_grid))
        profile_image = axes[3].imshow(
            profile_grid, cmap="PuBuGn", vmin=profile_min, vmax=profile_max,
            aspect="auto",
        )
        for y in range(len(positions)):
            for x in range(len(strips)):
                if not np.isfinite(profile_grid[y, x]):
                    continue
                flag = "*" if profile_reviews[y, x] else ""
                axes[3].text(
                    x, y,
                    f"{profile_grid[y, x]:.2f}{flag}\nmin n={int(profile_counts[y, x])}",
                    ha="center", va="center", fontsize=10,
                    color=(
                        "white"
                        if profile_grid[y, x] > (profile_min + profile_max) / 2
                        else "black"
                    ),
                )
        axes[3].set_title("Three-depth profile\n(equivalent water in)")
        axes[3].set_xticks(
            range(len(strips)),
            ["S1\nbiochar", "S2\ncontrol", "S3\nbiochar", "S4\ncontrol"],
        )
        axes[3].set_xlabel("Strip")
        profile_colorbar_axis = fig.add_axes((0.91, 0.16, 0.014, 0.29))
        profile_colorbar = fig.colorbar(profile_image, cax=profile_colorbar_axis)
        profile_colorbar.set_label("P90-derived profile water (in)")

    colorbar_axis = fig.add_axes(
        (0.91, 0.55, 0.014, 0.29)
        if include_profile else (0.89, 0.22, 0.018, 0.56)
    )
    colorbar = fig.colorbar(image, cax=colorbar_axis)
    colorbar.set_label("P90 retained VWC (%)")
    fig.suptitle(title, fontweight="bold")
    footer = (
        "* provisional, insufficient, or maximum–P90 gap requiring review; "
        f"n = {event_label}. "
    )
    if include_profile:
        footer += (
            "Profile = sum of the three depth P90 water layers; min n = smallest "
            "component-depth event count. "
        )
    footer += "Positions are schematic distances from the furrow start."
    fig.text(0.5, 0.02, footer, ha="center", fontsize=9)
    fig.subplots_adjust(
        left=0.10 if include_profile else 0.12,
        right=0.88 if include_profile else 0.86,
        bottom=0.17, top=0.84 if include_profile else 0.86, wspace=0.12,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def save_event_pair_plots(
    logger: pd.DataFrame, events: pd.DataFrame,
    sensor_results: pd.DataFrame, output_dir: Path,
) -> pd.DataFrame:
    """Write 3-position by 2-strip review plots for each event and pair."""

    data = _with_datetime_index(logger)
    output_dir.mkdir(parents=True, exist_ok=True)
    logs: list[dict[str, object]] = []
    for event in events.itertuples(index=False):
        start, end = pd.Timestamp(event.event_start), pd.Timestamp(event.event_end)
        plot_start, plot_end = start - pd.Timedelta(hours=24), end + pd.Timedelta(hours=78)
        for pair, strips in PAIR_STRIPS.items():
            fig, axes = plt.subplots(3, 2, figsize=(15, 10), sharex=True, sharey=True)
            for row_idx, position in enumerate(LOGGER_LOCATIONS):
                for col_idx, strip in enumerate(strips):
                    ax = axes[row_idx, col_idx]
                    for depth in ("1", "2", "3"):
                        sensor = f"VWC_{depth}_raw_{strip}_{position}"
                        if sensor in data:
                            window = data.loc[plot_start:plot_end]
                            ax.plot(window.index, pd.to_numeric(window[sensor], errors="coerce"),
                                    color=DEPTH_COLORS[depth], linewidth=1.4,
                                    label=f"{SENSOR_DEPTH_INDEX_TO_INCHES[depth]} in")
                    ax.axvspan(start, end, color="#9bd5f5", alpha=0.25)
                    ax.set_title(f"{strip} {position}")
                    ax.set_ylabel("VWC (%)")
                    ax.grid(alpha=0.25)
            handles, labels = axes[0, 0].get_legend_handles_labels()
            fig.legend(handles, labels, loc="upper right")
            fig.suptitle(f"{event.event_id} | {event.precip_total_in:.2f} in | {pair}", fontweight="bold")
            fig.autofmt_xdate()
            fig.tight_layout(rect=(0, 0, 0.94, 0.96))
            path = output_dir / f"{event.event_id}_{pair}.png"
            fig.savefig(path, dpi=150)
            plt.close(fig)
            subset = sensor_results.loc[sensor_results["event_id"].eq(event.event_id) & sensor_results["pair"].eq(pair)]
            logs.append({"event_id": event.event_id, "pair": pair,
                         "output_file": str(path), "sensor_rows": len(subset),
                         "retention_eligible_sensor_rows": int(subset["retention_eligible"].sum()),
                         "warm_empirical_max_sensor_rows": int(subset["empirical_max_eligible"].sum())})
    return pd.DataFrame(logs)

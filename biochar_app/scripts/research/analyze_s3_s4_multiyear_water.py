"""Audit S3/S4 timing, deep wetting, and event water balances across years.

The analysis is diagnostic rather than causal.  Absolute VWC differences among
depths can include sensor and soil-profile effects, so the deep-over-shallow
metric is used to identify persistence and changes, not to prove a water source.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw


REPO = Path(__file__).resolve().parents[3]
APP = REPO / "biochar_app"
IRRIGATION = APP / "data-processed/management/irrigation"
ANALYSIS = IRRIGATION / "analysis"
OUTPUT = APP / "docs/research/s3_s4_multiyear_water_audit"
PHOTO_INVENTORY = IRRIGATION / "photos/photo_inventory_unique.csv"
PHOTO_QC = IRRIGATION / "photos/meter_photo_workbook_qc.csv"
PHOTO_UNMATCHED = IRRIGATION / "photos/meter_photo_unmatched_clean_photos.csv"
EVENT_MULTIDEPTH = ANALYSIS / "figures/event_multidepth"
YEARS = (2023, 2024, 2025, 2026)
STRIPS = ("S3", "S4")
ALL_STRIPS = ("S1", "S2", "S3", "S4")
POSITIONS = ("T", "M", "B")
DEPTHS = {1: 6, 2: 12, 3: 18}
DEPTH_COLORS = {6: "#1673b1", 12: "#e39500", 18: "#159a73"}
YEAR_COLORS = {2023: "#5b8ff9", 2024: "#61d9a5", 2025: "#f6bd16", 2026: "#e8684a"}
STRIP_COLORS = {
    "S1": "#a9b4c2",
    "S2": "#68778a",
    "S3": "#d95f02",
    "S4": "#7b3294",
}


def load_irrigation() -> pd.DataFrame:
    data = pd.read_csv(IRRIGATION / "irrigation_clean.csv")
    for column in ("start_timestamp", "end_timestamp"):
        data[column] = pd.to_datetime(data[column], errors="coerce")
    return data


def build_daily_profiles() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    irrigation = load_irrigation()
    for year in YEARS:
        columns = ["timestamp"] + [
            f"VWC_{depth}_raw_{strip}_{position}"
            for strip in ALL_STRIPS
            for position in POSITIONS
            for depth in DEPTHS
        ]
        path = APP / f"data-processed/parquet/{year}/{year}_raw_logger.parquet"
        data = pd.read_parquet(path, columns=columns)
        data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
        data = data.dropna(subset=["timestamp"]).sort_values("timestamp")
        data = data.loc[
            data["timestamp"].between(
                pd.Timestamp(year, 5, 1), pd.Timestamp(year, 10, 31, 23, 59)
            )
        ].set_index("timestamp")
        daily = data.resample("D").mean(numeric_only=True)

        weather_path = (
            APP
            / f"data-processed/parquet/summary/weather/daily/{year}_daily.parquet"
        )
        weather = pd.read_parquet(weather_path, columns=["timestamp", "precip_in"])
        weather["date"] = pd.to_datetime(weather["timestamp"]).dt.normalize()
        precip = weather.set_index("date")["precip_in"]
        event_dates_by_group = {
            strip_group: set(
                irrigation.loc[
                    irrigation["year"].eq(year)
                    & irrigation["strip_group"].eq(strip_group),
                    "start_timestamp",
                ].dropna().dt.normalize()
            )
            for strip_group in ("S1_S2", "S3_S4")
        }

        for strip in ALL_STRIPS:
            strip_group = "S1_S2" if strip in ("S1", "S2") else "S3_S4"
            for position in POSITIONS:
                selected = daily[
                    [f"VWC_{depth}_raw_{strip}_{position}" for depth in DEPTHS]
                ].copy()
                selected.columns = ["vwc_6", "vwc_12", "vwc_18"]
                selected = selected.dropna(how="all")
                selected["year"] = year
                selected["strip"] = strip
                selected["position"] = position
                selected["date"] = selected.index.normalize()
                selected["day_of_year"] = selected["date"].dt.dayofyear
                selected["deep_minus_shallow_max"] = selected["vwc_18"] - selected[
                    ["vwc_6", "vwc_12"]
                ].max(axis=1)
                selected["precip_in"] = selected["date"].map(precip).fillna(0.0)
                selected["irrigation_day"] = selected["date"].isin(
                    event_dates_by_group[strip_group]
                )
                frames.append(selected.reset_index(drop=True))
    return pd.concat(frames, ignore_index=True)


def summarize_deep_dominance(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (year, strip, position), group in daily.groupby(
        ["year", "strip", "position"], sort=True
    ):
        if strip not in STRIPS:
            continue
        group = group.dropna(subset=["vwc_6", "vwc_12", "vwc_18"]).sort_values("date")
        delta = group["deep_minus_shallow_max"]
        flag = delta > 1.0
        persistent = flag.rolling(14, min_periods=10).sum() >= 10
        first_window = group.loc[persistent, "date"].min() if persistent.any() else pd.NaT
        first_date = group["date"].min() if not group.empty else pd.NaT
        present_at_start = bool(
            not group.empty and len(flag.iloc[:14]) >= 10 and flag.iloc[:14].sum() >= 10
        )
        rows.append(
            {
                "year": int(year),
                "strip": strip,
                "logger_position": position,
                "valid_daily_profiles": int(len(group)),
                "mean_vwc_6": group["vwc_6"].mean(),
                "mean_vwc_12": group["vwc_12"].mean(),
                "mean_vwc_18": group["vwc_18"].mean(),
                "median_deep_minus_shallow_max": delta.median(),
                "mean_deep_minus_shallow_max": delta.mean(),
                "days_deep_gt_shallow_by_1": int(flag.sum()),
                "fraction_days_deep_gt_shallow_by_1": flag.mean(),
                "persistent_at_first_available_data": present_at_start,
                "first_available_date": first_date,
                "first_persistent_window_date": first_window,
                "onset_interpretation": (
                    "present_at_or_before_first_available_date"
                    if present_at_start
                    else "first_detected_during_observed_period"
                    if pd.notna(first_window)
                    else "not_persistent"
                ),
            }
        )
    result = pd.DataFrame(rows)
    numeric = result.select_dtypes(include="number").columns
    result[numeric] = result[numeric].round(4)
    return result


def identify_unexplained_deep_rises(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for (_year, _strip, _position), group in daily.groupby(
        ["year", "strip", "position"], sort=True
    ):
        group = group.sort_values("date").copy()
        group["deep_daily_change"] = group["vwc_18"].diff()
        group["largest_shallow_daily_change"] = group[["vwc_6", "vwc_12"]].diff().max(axis=1)
        group["deep_change_beyond_shallow"] = (
            group["deep_daily_change"] - group["largest_shallow_daily_change"]
        )
        group["precip_current_and_previous_day_in"] = (
            group["precip_in"].rolling(2, min_periods=1).sum()
        )
        group["irrigation_current_or_previous_day"] = (
            group["irrigation_day"].rolling(2, min_periods=1).max().astype(bool)
        )
        complete_profile = group[["vwc_6", "vwc_12", "vwc_18"]].notna().all(axis=1)
        group["complete_profile_current_and_previous_day"] = (
            complete_profile & complete_profile.shift(1, fill_value=False)
        )
        selected = group.loc[
            group["deep_daily_change"].ge(1.0)
            & group["deep_change_beyond_shallow"].ge(0.75)
            & group["precip_current_and_previous_day_in"].le(0.05)
            & ~group["irrigation_current_or_previous_day"]
            & group["complete_profile_current_and_previous_day"]
        ].copy()
        rows.append(selected)
    result = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    west = result.loc[result["strip"].isin(("S1", "S2"))].groupby(
        ["year", "date", "position"], sort=False
    ).agg(
        west_comparable_deep_rise=("strip", "size"),
        west_candidate_strips=("strip", lambda values: ",".join(sorted(set(values)))),
    ).reset_index()
    result = result.loc[result["strip"].isin(STRIPS)].merge(
        west, on=["year", "date", "position"], how="left"
    )
    result["west_comparable_deep_rise"] = (
        result["west_comparable_deep_rise"].fillna(0).astype(int).gt(0)
    )
    result["west_candidate_strips"] = result["west_candidate_strips"].fillna("")
    keep = [
        "year", "date", "strip", "position", "vwc_6", "vwc_12", "vwc_18",
        "deep_daily_change", "largest_shallow_daily_change",
        "deep_change_beyond_shallow", "precip_current_and_previous_day_in",
        "west_comparable_deep_rise", "west_candidate_strips",
    ]
    result = result[keep].sort_values(["year", "date", "strip", "position"])
    numeric = result.select_dtypes(include="number").columns
    result[numeric] = result[numeric].round(4)
    return result


def group_deep_wetting_episodes(rises: pd.DataFrame) -> pd.DataFrame:
    """Combine candidate days separated by at most two days into episodes."""
    if rises.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for year, group in rises.groupby("year", sort=True):
        group = group.sort_values("date").copy()
        dates = pd.to_datetime(group["date"])
        group["episode_number"] = dates.diff().gt(pd.Timedelta(days=2)).cumsum() + 1
        for episode_number, episode in group.groupby("episode_number", sort=True):
            episode_dates = pd.to_datetime(episode["date"])
            first_date = episode_dates.min()
            first = episode.loc[episode_dates.eq(first_date)]
            s3_dates = episode_dates.loc[episode["strip"].eq("S3")]
            s4_dates = episode_dates.loc[episode["strip"].eq("S4")]
            starts_s4 = bool(first["strip"].eq("S4").any())
            reaches_s3_later = bool(
                starts_s4
                and not s3_dates.empty
                and not s4_dates.empty
                and s3_dates.min() > s4_dates.min()
            )
            rows.append(
                {
                    "year": int(year),
                    "episode_id": f"{int(year)}-{int(episode_number):02d}",
                    "start_date": first_date,
                    "end_date": episode_dates.max(),
                    "candidate_rows": int(len(episode)),
                    "positions": ",".join(sorted(episode["position"].unique())),
                    "first_day_sites": ",".join(
                        sorted((first["strip"] + first["position"]).unique())
                    ),
                    "all_sites": ",".join(
                        sorted((episode["strip"] + episode["position"]).unique())
                    ),
                    "starts_in_s4": starts_s4,
                    "reaches_s3_later": reaches_s3_later,
                    "days_s4_to_s3": (
                        int((s3_dates.min() - s4_dates.min()).days)
                        if reaches_s3_later else np.nan
                    ),
                    "maximum_deep_daily_change": episode["deep_daily_change"].max(),
                    "maximum_deep_change_beyond_shallow": episode[
                        "deep_change_beyond_shallow"
                    ].max(),
                    "west_comparable_deep_rise": bool(
                        episode["west_comparable_deep_rise"].any()
                    ),
                }
            )
    result = pd.DataFrame(rows)
    numeric = result.select_dtypes(include="number").columns
    result[numeric] = result[numeric].round(4)
    return result


def build_2026_event_audit() -> pd.DataFrame:
    path = ANALYSIS / "diagnostics/timestamp_diagnostics/s3m_event_classification.csv"
    classified = pd.read_csv(path)
    classified = classified.loc[classified["year"].eq(2026)].copy()
    classified["irrigation_start"] = pd.to_datetime(
        classified["irrigation_start"], errors="coerce"
    )
    photo_qc = pd.read_csv(PHOTO_QC)
    photo_qc = photo_qc.loc[
        photo_qc["year"].eq(2026) & photo_qc["strip_group"].eq("S3_S4")
    ].copy()
    photo_qc["workbook_start_timestamp"] = pd.to_datetime(
        photo_qc["workbook_start_timestamp"], errors="coerce"
    )
    photo_columns = [
        "workbook_start_timestamp",
        "start_photo_filename",
        "start_photo_counter",
        "end_photo_filename",
        "end_photo_datetime",
        "end_photo_counter",
        "end_qc_status",
    ]
    classified = classified.merge(
        photo_qc[photo_columns],
        how="left",
        left_on="irrigation_start",
        right_on="workbook_start_timestamp",
        validate="one_to_one",
    )
    classified["start_photo_available"] = classified["start_photo_filename"].notna()
    classified["end_photo_available"] = classified["end_photo_filename"].notna()
    classified["photo_absolute_difference_min"] = pd.to_numeric(
        classified["photo_minus_workbook_min"], errors="coerce"
    ).abs()
    classified["absolute_start_anchor"] = np.select(
        [
            classified["photo_start_qc"].eq("camera_workbook_agreement"),
            classified["photo_start_qc"].eq("no_camera_match"),
        ],
        ["photo_verified", "no_photo_anchor"],
        default="photo_workbook_disagreement",
    )
    classified["timing_reliability"] = np.select(
        [
            classified["classification"].eq("possible_lateral_wetting"),
            classified["classification"].eq("field_wide_pre_start_drift"),
            classified["classification"].eq("irrigation_boundary_needs_review"),
            classified["classification"].eq("ambiguous_or_incomplete_sensor_response"),
        ],
        [
            "investigate_east_side_wetting",
            "review_field_wide_pre_start_drift",
            "review_irrigation_boundary",
            "exclude_incomplete",
        ],
        default="usable_no_clock_flag",
    )
    keep = [
        "event_id", "irrigation_start", "start_photo_available",
        "start_photo_filename", "photo_start", "start_photo_counter",
        "photo_minus_workbook_min", "end_photo_available", "end_photo_filename",
        "end_photo_datetime", "end_photo_counter", "end_qc_status",
        "photo_start_qc", "absolute_start_anchor", "classification",
        "timing_reliability", "possible_lateral_entry",
        "field_wide_pre_start_drift",
        "s3m_standard_arrival_depth_count", "s3m_missing_standard_depth_count",
        "s3m_median_standard_delay_min", "s3_reference_median_top_bottom_delay_min",
        "timestamp_correction_recommendation", "field_notes",
    ]
    return classified[keep].sort_values("irrigation_start")


def summarize_photo_source(event_audit: pd.DataFrame) -> dict[str, object]:
    inventory = pd.read_csv(PHOTO_INVENTORY, dtype=str)
    inventory["effective_datetime_parsed"] = pd.to_datetime(
        inventory["effective_datetime"], errors="coerce", format="mixed", utc=True
    )
    readable = inventory["review_status"].fillna("").str.casefold().eq("readable")
    valid_reading = inventory["meter_reading"].fillna("").str.fullmatch(r"\d{6}")
    confident = ~inventory["timestamp_confidence"].fillna("").str.casefold().eq("low")
    selected = inventory.loc[readable & valid_reading & confident].copy()
    inventory_2026 = selected.loc[
        selected["effective_datetime_parsed"].dt.year.eq(2026)
    ].copy()
    review_by_sha = selected.set_index("sha256")["meter_reading"]
    qc = pd.read_csv(PHOTO_QC)
    qc = qc.loc[qc["year"].eq(2026)].copy()
    alignment_checks: list[bool] = []
    qc_photo_datetimes: list[pd.Series] = []
    for boundary in ("start", "end"):
        photo_ids = qc[f"{boundary}_photo_id"]
        counters = pd.to_numeric(qc[f"{boundary}_photo_counter"], errors="coerce")
        reviewed_counters = pd.to_numeric(photo_ids.map(review_by_sha), errors="coerce")
        referenced = photo_ids.notna()
        alignment_checks.extend(
            (
                referenced
                & reviewed_counters.notna()
                & counters.notna()
                & np.isclose(counters, reviewed_counters)
            ).loc[referenced].tolist()
        )
        qc_photo_datetimes.append(
            pd.to_datetime(qc[f"{boundary}_photo_datetime"], errors="coerce")
        )
    latest_qc_photo = pd.concat(qc_photo_datetimes, ignore_index=True).max()
    unmatched = pd.read_csv(PHOTO_UNMATCHED)
    unmatched["photo_datetime"] = pd.to_datetime(
        unmatched["photo_datetime"], errors="coerce"
    )
    unmatched_2026 = unmatched.loc[unmatched["photo_datetime"].dt.year.eq(2026)].copy()
    unmatched_latest = unmatched_2026.loc[
        unmatched_2026["photo_datetime"].dt.date
        == unmatched_2026["photo_datetime"].dt.date.max()
    ]
    return {
        "reviewed_2026_photos": int(len(inventory_2026)),
        "latest_reviewed_2026_photo": inventory_2026["effective_datetime_parsed"].max(),
        "s3_s4_events": int(len(event_audit)),
        "s3_s4_events_with_start_photo": int(event_audit["start_photo_available"].sum()),
        "s3_s4_events_with_end_photo": int(event_audit["end_photo_available"].sum()),
        "s3_s4_events_with_exact_start_agreement": int(
            event_audit["absolute_start_anchor"].eq("photo_verified").sum()
        ),
        "latest_derived_2026_photo": latest_qc_photo,
        "all_2026_referenced_photos_match_review": bool(all(alignment_checks)),
        "latest_unmatched_2026_photo_count": int(len(unmatched_latest)),
        "latest_unmatched_2026_photo_date": unmatched_latest["photo_datetime"].max(),
        "latest_unmatched_2026_filenames": unmatched_latest["photo_filename"].tolist(),
    }


def build_paired_water_balance() -> tuple[pd.DataFrame, pd.DataFrame]:
    source = pd.read_csv(
        ANALYSIS / "holding_capacity/first_pass_water_balance_all_years.csv"
    )
    pairs = {"S1_S2": ("S1", "S2"), "S3_S4": ("S3", "S4")}
    rows: list[dict[str, object]] = []
    for (event_id, strip_group), group in source.groupby(["event_id", "strip_group"], sort=True):
        if strip_group not in pairs:
            continue
        biochar, control = pairs[strip_group]
        by_strip = group.set_index("strip")
        if biochar not in by_strip.index or control not in by_strip.index:
            continue
        b = by_strip.loc[biochar]
        c = by_strip.loc[control]
        eligible = bool(b["unretained_eligible"]) and bool(c["unretained_eligible"])
        row: dict[str, object] = {
            "year": int(float(b["year"])),
            "strip_group": strip_group,
            "event_id": event_id,
            "irrigation_start": b["irrigation_start"],
            "irrigation_end": b["irrigation_end"],
            "event_duration_hours": b["event_duration_hours"],
            "biochar_strip": biochar,
            "control_strip": control,
            "biochar_applied_gal": b["gallons_strip"],
            "control_applied_gal": c["gallons_strip"],
            "equal_strip_allocation_assumed": bool(
                np.isclose(float(b["gallons_strip"]), float(c["gallons_strip"]), equal_nan=False)
            ),
            "paired_water_balance_eligible": eligible,
            "biochar_eligibility_reason": b["unretained_reason"],
            "control_eligibility_reason": c["unretained_reason"],
        }
        for label, column in (
            ("storage_gal", "estimated_storage_gal_strip_0_18in"),
            ("signed_residual_gal", "water_balance_residual_gal_strip"),
            ("nonnegative_unretained_proxy_gal", "unretained_gal_strip"),
            ("stored_fraction", "estimated_storage_fraction_0_18in"),
            ("post_bottom_applied_gal", "post_bottom_6in_arrival_applied_gal"),
        ):
            b_value = pd.to_numeric(pd.Series([b[column]]), errors="coerce").iloc[0]
            c_value = pd.to_numeric(pd.Series([c[column]]), errors="coerce").iloc[0]
            row[f"biochar_{label}"] = b_value
            row[f"control_{label}"] = c_value
            row[f"biochar_minus_control_{label}"] = b_value - c_value
        rows.append(row)
    paired = pd.DataFrame(rows)
    paired["signed_residual_difference_identity_error_gal"] = (
        paired["biochar_minus_control_signed_residual_gal"]
        + paired["biochar_minus_control_storage_gal"]
    )
    numeric = paired.select_dtypes(include="number").columns
    paired[numeric] = paired[numeric].round(4)

    eligible = paired.loc[paired["paired_water_balance_eligible"]].copy()
    summary = eligible.groupby(["year", "strip_group"], sort=True).agg(
        eligible_matched_events=("event_id", "size"),
        median_applied_gal=("biochar_applied_gal", "median"),
        median_biochar_storage_gal=("biochar_storage_gal", "median"),
        median_control_storage_gal=("control_storage_gal", "median"),
        median_biochar_minus_control_storage_gal=(
            "biochar_minus_control_storage_gal", "median"
        ),
        median_biochar_minus_control_nonnegative_unretained_proxy_gal=(
            "biochar_minus_control_nonnegative_unretained_proxy_gal", "median"
        ),
        median_biochar_minus_control_stored_fraction=(
            "biochar_minus_control_stored_fraction", "median"
        ),
        median_biochar_minus_control_post_bottom_applied_gal=(
            "biochar_minus_control_post_bottom_applied_gal", "median"
        ),
    ).reset_index()
    summary[summary.select_dtypes(include="number").columns] = summary.select_dtypes(
        include="number"
    ).round(4)
    return paired, summary


def build_questionable_event_contact_sheets(event_audit: pd.DataFrame) -> list[str]:
    """Combine the existing S3/S4 multidepth plots for each flagged event."""
    names: list[str] = []
    flagged = event_audit.loc[
        ~event_audit["timing_reliability"].eq("usable_no_clock_flag")
    ]
    for row in flagged.itertuples():
        event_id = str(row.event_id)
        source_paths: list[Path] = []
        for position in POSITIONS:
            for strip in STRIPS:
                matches = sorted(
                    (EVENT_MULTIDEPTH / position).glob(
                        f"*_{strip}_{position}_event_{event_id}.png"
                    )
                )
                if matches:
                    source_paths.append(matches[0])
        if not source_paths:
            continue
        thumb_width, thumb_height = 900, 470
        title_height = 75
        canvas = Image.new(
            "RGB", (thumb_width * 2, title_height + thumb_height * 3), "white"
        )
        draw = ImageDraw.Draw(canvas)
        draw.text(
            (25, 20),
            f"Questionable event {event_id}: S3/S4 multidepth plots",
            fill="#1f3557",
        )
        for index, source in enumerate(source_paths):
            image = Image.open(source).convert("RGB")
            image.thumbnail((thumb_width, thumb_height))
            x = (index % 2) * thumb_width + (thumb_width - image.width) // 2
            y = (
                title_height
                + (index // 2) * thumb_height
                + (thumb_height - image.height) // 2
            )
            canvas.paste(image, (x, y))
        name = f"questionable_event_{event_id[:10]}_multidepth.png"
        canvas.save(OUTPUT / name, optimize=True)
        names.append(name)
    return names


def plot_directional_episode_multilocation(episodes: pd.DataFrame) -> list[str]:
    """Plot 15-minute 18-inch VWC at all S3/S4 logger positions."""
    names: list[str] = []
    for episode in episodes.loc[episodes["reaches_s3_later"]].itertuples():
        start = pd.Timestamp(episode.start_date) - pd.Timedelta(days=2)
        stop = pd.Timestamp(episode.end_date) + pd.Timedelta(days=2)
        columns = ["timestamp"] + [
            f"VWC_3_raw_{strip}_{position}"
            for strip in STRIPS
            for position in POSITIONS
        ]
        data = pd.read_parquet(
            APP
            / f"data-processed/parquet/{int(episode.year)}/{int(episode.year)}_raw_logger.parquet",
            columns=columns,
        )
        data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
        data = data.loc[data["timestamp"].between(start, stop)].copy()
        figure, axes = plt.subplots(2, 1, figsize=(15, 8), sharex=True)
        for axis, strip in zip(axes, STRIPS):
            for position, color in zip(
                POSITIONS, ("#2b6cb0", "#dd8a00", "#159a73")
            ):
                axis.plot(
                    data["timestamp"],
                    data[f"VWC_3_raw_{strip}_{position}"],
                    label={"T": "Top", "M": "Middle", "B": "Bottom"}[position],
                    color=color,
                    linewidth=1.6,
                )
            axis.axvspan(
                pd.Timestamp(episode.start_date),
                pd.Timestamp(episode.end_date) + pd.Timedelta(days=1),
                color="#f3c969",
                alpha=0.22,
                label="Daily-screen episode",
            )
            axis.set_ylabel("18-inch VWC (%)")
            axis.set_title(f"{strip}: Top, Middle, and Bottom loggers", weight="bold")
            axis.grid(axis="y", alpha=0.22)
        handles, labels = axes[0].get_legend_handles_labels()
        figure.legend(
            handles,
            labels,
            loc="upper center",
            ncol=4,
            frameon=False,
            bbox_to_anchor=(0.5, 0.955),
        )
        figure.suptitle(
            f"Directional deep-rise screen {episode.episode_id}: 15-minute multilocation view",
            fontsize=18,
            weight="bold",
            y=0.995,
        )
        axes[-1].set_xlabel("Date")
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%b %d\n%H:%M"))
        figure.tight_layout(rect=(0.02, 0.03, 1, 0.88))
        name = f"directional_episode_{episode.episode_id}_multilocation.png"
        figure.savefig(OUTPUT / name, dpi=180, bbox_inches="tight")
        plt.close(figure)
        names.append(name)
    return names


def plot_multiyear_top_profiles(daily: pd.DataFrame, path: Path) -> None:
    figure, axes = plt.subplots(len(YEARS), 2, figsize=(16, 14), sharey=True)
    for row, year in enumerate(YEARS):
        for column, strip in enumerate(STRIPS):
            axis = axes[row, column]
            data = daily.loc[
                daily["year"].eq(year)
                & daily["strip"].eq(strip)
                & daily["position"].eq("T")
            ].sort_values("date")
            for depth in (6, 12, 18):
                axis.plot(
                    data["date"], data[f"vwc_{depth}"],
                    color=DEPTH_COLORS[depth], linewidth=1.8, label=f"{depth} in",
                )
            dominant = data["deep_minus_shallow_max"].gt(1.0)
            axis.fill_between(
                data["date"], 0, 1, where=dominant,
                transform=axis.get_xaxis_transform(), color="#7d3c98", alpha=0.08,
                label="18 in > both shallow depths by >1 point",
            )
            for event_date in data.loc[data["irrigation_day"], "date"]:
                axis.axvline(event_date, color="#555555", linewidth=0.7, linestyle=":", alpha=0.55)
            rain = data.loc[data["precip_in"].gt(0.05)]
            axis.scatter(rain["date"], np.full(len(rain), 1.5), marker="|", s=70,
                         color="#78c4ee", label="Rain >0.05 in")
            axis.set_ylim(0, 65)
            axis.set_title(f"{year} · {strip} Top", fontsize=13, weight="bold")
            axis.grid(axis="y", alpha=0.22)
            axis.xaxis.set_major_locator(mdates.MonthLocator())
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
            if column == 0:
                axis.set_ylabel("Daily mean VWC (%)")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    figure.legend(
        unique.values(), unique.keys(), loc="upper center", ncol=5,
        frameon=False, bbox_to_anchor=(0.5, 0.969),
    )
    figure.suptitle(
        "S3 and S4 Top loggers: multi-year depth profiles",
        fontsize=20, weight="bold", y=0.995,
    )
    figure.text(0.5, 0.006, "Dotted vertical lines are recorded S3/S4 irrigation starts", ha="center")
    figure.tight_layout(rect=(0.02, 0.025, 1, 0.925))
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)


def plot_multiyear_deep_dominance(daily: pd.DataFrame, path: Path) -> None:
    figure, axes = plt.subplots(3, 2, figsize=(16, 11), sharex=True, sharey=True)
    anchor_year = 2000
    for row, position in enumerate(POSITIONS):
        for column, strip in enumerate(STRIPS):
            axis = axes[row, column]
            for year in YEARS:
                data = daily.loc[
                    daily["year"].eq(year)
                    & daily["strip"].eq(strip)
                    & daily["position"].eq(position)
                ].sort_values("date").copy()
                data["aligned_date"] = data["date"].map(
                    lambda value: pd.Timestamp(anchor_year, value.month, value.day)
                )
                axis.plot(
                    data["aligned_date"], data["deep_minus_shallow_max"],
                    color=YEAR_COLORS[year], linewidth=1.6, label=str(year),
                )
            axis.axhline(0, color="#333333", linewidth=1)
            axis.axhline(1, color="#7d3c98", linewidth=0.9, linestyle="--")
            axis.set_title(f"{strip} · {position}", fontsize=13, weight="bold")
            axis.grid(axis="y", alpha=0.22)
            axis.xaxis.set_major_locator(mdates.MonthLocator())
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
            if column == 0:
                axis.set_ylabel("18 in − max(6 in, 12 in)\nVWC points")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    figure.legend(
        handles, labels, loc="upper center", ncol=4, frameon=False,
        bbox_to_anchor=(0.5, 0.966),
    )
    figure.suptitle(
        "Deep-over-shallow VWC ordering across years",
        fontsize=20, weight="bold", y=0.995,
    )
    figure.text(
        0.5, 0.005,
        "Positive values mean the 18-inch sensor exceeds both shallower sensors; dashed line = +1 VWC point",
        ha="center",
    )
    figure.tight_layout(rect=(0.02, 0.035, 1, 0.92))
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)


def plot_lateral_episode_comparison(
    daily: pd.DataFrame, episodes: pd.DataFrame, path: Path
) -> None:
    """Compare east and west 18-inch traces during the strongest candidate years."""
    years = (2025, 2026)
    positions = ("M", "B")
    figure, axes = plt.subplots(2, 2, figsize=(16, 9), sharex=False, sharey=False)
    for row, year in enumerate(years):
        year_episodes = episodes.loc[
            episodes["year"].eq(year) & episodes["reaches_s3_later"]
        ]
        for column, position in enumerate(positions):
            axis = axes[row, column]
            for strip in ALL_STRIPS:
                data = daily.loc[
                    daily["year"].eq(year)
                    & daily["strip"].eq(strip)
                    & daily["position"].eq(position)
                ].sort_values("date")
                axis.plot(
                    data["date"], data["vwc_18"],
                    color=STRIP_COLORS[strip],
                    linewidth=2.0 if strip in STRIPS else 1.2,
                    alpha=1.0 if strip in STRIPS else 0.75,
                    label=strip,
                )
            for episode in year_episodes.itertuples():
                axis.axvspan(
                    pd.Timestamp(episode.start_date) - pd.Timedelta(hours=12),
                    pd.Timestamp(episode.end_date) + pd.Timedelta(hours=36),
                    color="#f3c969", alpha=0.22,
                )
            axis.set_title(f"{year} · {position} position", fontsize=13, weight="bold")
            axis.set_ylabel("18-inch daily mean VWC (%)")
            axis.grid(axis="y", alpha=0.22)
            axis.xaxis.set_major_locator(mdates.MonthLocator())
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    handles, labels = axes[0, 0].get_legend_handles_labels()
    figure.legend(
        handles, labels, loc="upper center", ncol=4, frameon=False,
        bbox_to_anchor=(0.5, 0.95),
    )
    figure.suptitle(
        "18-inch VWC during S4-first, S3-later wetting episodes",
        fontsize=20, weight="bold", y=0.995,
    )
    figure.text(
        0.5, 0.008,
        "Gold shading marks screened episodes; S1/S2 are western comparisons and S3/S4 are eastern strips",
        ha="center",
    )
    figure.tight_layout(rect=(0.02, 0.04, 1, 0.91))
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)


def write_report(
    dominance: pd.DataFrame,
    rises: pd.DataFrame,
    episodes: pd.DataFrame,
    event_audit: pd.DataFrame,
    pair_summary: pd.DataFrame,
    photo_source: dict[str, object],
    questionable_plot_names: list[str],
    directional_plot_names: list[str],
    path: Path,
) -> None:
    top = dominance.loc[dominance["logger_position"].eq("T")]
    top_lines = []
    for strip in STRIPS:
        values = top.loc[top["strip"].eq(strip)].sort_values("year")
        rendered = ", ".join(
            f"{int(row.year)}: {row.median_deep_minus_shallow_max:.2f}"
            for row in values.itertuples()
        )
        top_lines.append(f"- {strip} Top median deep-over-shallow difference (VWC points): {rendered}.")
    verified = int(event_audit["absolute_start_anchor"].eq("photo_verified").sum())
    starts_available = int(event_audit["start_photo_available"].sum())
    ends_available = int(event_audit["end_photo_available"].sum())
    review = int(event_audit["timing_reliability"].eq("review_irrigation_boundary").sum())
    lateral = int(event_audit["possible_lateral_entry"].sum())
    field_wide = int(event_audit["field_wide_pre_start_drift"].sum())
    usable = int(event_audit["timing_reliability"].eq("usable_no_clock_flag").sum())
    pair_2026 = pair_summary.loc[pair_summary["year"].eq(2026)]
    directional = episodes.loc[episodes["reaches_s3_later"]].copy()

    timing_rows = [
        "| Event | Start photo? | Start photo | Photo − workbook (min) | Timing decision |",
        "|---|---|---|---:|---|",
    ]
    for row in event_audit.itertuples():
        offset = (
            "—" if pd.isna(row.photo_minus_workbook_min)
            else f"{row.photo_minus_workbook_min:.1f}"
        )
        timing_rows.append(
            f"| {str(row.event_id)[:10]} | {'yes' if row.start_photo_available else 'no'} | "
            f"{row.start_photo_filename if row.start_photo_available else '—'} | {offset} | "
            f"{row.timing_reliability.replace('_', ' ')} |"
        )

    episode_rows = [
        "| Episode | Start–end | First sites | Later sites | S4→S3 lag | West response? |",
        "|---|---|---|---|---:|---|",
    ]
    for row in directional.itertuples():
        episode_rows.append(
            f"| {row.episode_id} | {pd.Timestamp(row.start_date):%b %-d}–"
            f"{pd.Timestamp(row.end_date):%b %-d} | {row.first_day_sites} | "
            f"{row.all_sites} | {int(row.days_s4_to_s3)} day | "
            f"{'yes' if row.west_comparable_deep_rise else 'no'} |"
        )

    balance_rows = [
        "| Year | Pair | Eligible events | Median applied/strip (gal) | Biochar storage (gal) | Control storage (gal) | Biochar − control storage (gal) | Stored-fraction difference |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in pair_summary.itertuples():
        balance_rows.append(
            f"| {int(row.year)} | {row.strip_group.replace('_', '/')} | "
            f"{int(row.eligible_matched_events)} | {row.median_applied_gal:,.0f} | "
            f"{row.median_biochar_storage_gal:,.0f} | {row.median_control_storage_gal:,.0f} | "
            f"{row.median_biochar_minus_control_storage_gal:,.0f} | "
            f"{row.median_biochar_minus_control_stored_fraction:.3f} |"
        )

    questionable_images: list[str] = []
    for name in questionable_plot_names:
        questionable_images.extend(["", f"![Questionable irrigation event plots]({name})"])
    directional_images: list[str] = []
    for name in directional_plot_names:
        directional_images.extend(["", f"![Directional deep-rise multilocation plot]({name})"])
    if photo_source["all_2026_referenced_photos_match_review"]:
        photo_alignment_text = (
            "A content-alignment check found that every 2026 photo referenced by the derived QC "
            "file exists in the finalized canonical inventory with the same manual counter reading. "
            "The event-level QC is therefore current with the reviewed photo content."
        )
    else:
        photo_alignment_text = (
            "The derived QC file does not fully align with the current Excel review and should be "
            "rebuilt before the photo-dependent conclusions are used."
        )

    lines = [
        "# Multi-year S3/S4 water-source and event water-balance audit",
        "",
        "## Bottom line",
        "",
        "The 18-inch sensor exceeds both shallower sensors at the Top positions of S3 and S4 "
        "from the first available 2023 growing-season observations. The ordering therefore did "
        "not first appear in 2025 or 2026. It becomes stronger at S3T and appears at additional "
        "positions over time, so a later external source could amplify the pattern, but a recently "
        "started ditch or recent pipe break cannot by itself explain its original presence.",
        "",
        *top_lines,
        "",
        "Because each depth uses a different CS650 sensor, absolute depth ordering can also reflect "
        "sensor calibration, installation contact, and soil texture. These plots are evidence for "
        "where and when to investigate; they do not by themselves prove a ditch or pipe leak.",
        "",
        "![Multi-year S3 and S4 Top depth profiles](multiyear_top_depth_profiles.png)",
        "",
        "![Deep-over-shallow VWC ordering across years](multiyear_deep_dominance.png)",
        "",
        "## Definitions used in this audit",
        "",
        "- **Shared canonical irrigation boundary:** one recorded start and end time applies to "
        "both S3 and S4 because they are irrigated as one metered pair. It is the workbook/photo "
        "reference time, not a claim that water reached every logger simultaneously.",
        "- **Sensor arrival:** the first sustained VWC increase after the recorded boundary that "
        "meets the standard +0.25 VWC-point response rule. Arrival delay is sensor arrival minus "
        "the shared boundary.",
        "- **Deep-over-shallow difference:** 18-inch VWC minus the larger of the simultaneous "
        "6- and 12-inch VWC values. A positive value describes sensor ordering; it does not by "
        "itself demonstrate upward or lateral water movement.",
        "- **Deep-rise candidate:** an exploratory daily observation outside recorded irrigation "
        "and material rain when 18-inch mean VWC rose by at least 1 point and at least 0.75 point "
        "more than either shallower depth. It is a screening flag, not a confirmed leak.",
        "- **Directional episode:** deep-rise candidates no more than two days apart, grouped as "
        "one episode, where the screen fires in S4 first and subsequently in S3. The term describes "
        "the observed east-to-west sequence only.",
        "- **Eligible event:** an event with the required complete sensor coverage and established "
        "quality-control checks for the reported calculation. Failed events remain in the detailed "
        "table as an audit trail but are omitted from eligible-event summaries.",
        "",
        "## 2026 timing audit",
        "",
        f"There are {len(event_audit)} S3/S4 irrigation events in 2026. {starts_available} have a "
        f"reviewed start photograph, {ends_available} have a reviewed end photograph, and {verified} "
        f"start photographs agree exactly with the workbook boundary. {review} need boundary review, "
        f"{field_wide} has field-wide pre-start drift, {lateral} has a directional pre-start flag, and "
        f"{usable} are usable with no clock flag. No event "
        "supports an additional S3M clock correction.",
        "",
        "### Photo source and coverage",
        "",
        "The audit does **not** treat either review workbook as its final authority. The source "
        "chain is `meter_photo_review.xlsx` plus `meter_photo_unresolved_review.xlsx` → "
        "`photo_inventory_unique.csv` → `meter_photo_workbook_qc.csv` → this timing audit. "
        "The canonical inventory contains "
        f"{photo_source['reviewed_2026_photos']} dated 2026 photographs through "
        f"{pd.Timestamp(photo_source['latest_reviewed_2026_photo']):%B %-d, %Y}. For the nine S3/S4 "
        f"events, the derived QC file contains {starts_available} start photographs and "
        f"{ends_available} end photographs. Thus the earlier count of {verified} meant exact start-time "
        f"agreement, not that only that many photographs existed. {photo_alignment_text} The August 14 "
        "event now has reviewed start and end photographs.",
        "",
        "S3 and S4 use one shared canonical irrigation boundary for each S3/S4 event. The June 27 "
        "photo is about 54 minutes later than the workbook start and lacks a confirming counter, so "
        "that event should not anchor absolute timing.",
        "",
        f"The latest date also has {photo_source['latest_unmatched_2026_photo_count']} reviewed photos "
        f"({', '.join(photo_source['latest_unmatched_2026_filenames'])}) dated "
        f"{pd.Timestamp(photo_source['latest_unmatched_2026_photo_date']):%B %-d, %Y}. They are retained "
        "as unmatched evidence because the irrigation event table does not yet provide an authoritative "
        "strip assignment for them. They are not assigned to S3/S4 or used in event calculations.",
        "",
        *timing_rows,
        *questionable_images,
        "",
        "### Interpretation of the supplied 15-minute plots",
        "",
        "**June 27:** the workbook boundary is 11:05, while the start photograph is 54.4 minutes "
        "later and the flowmeter was not turning. All plotted responses occur after the workbook "
        "boundary. S4 Top responds quickly at all three depths; S3 Top responds later at 6 inches, "
        "then 12 inches, with little 18-inch response in this window. This supports a real irrigation "
        "response and different advance between the strips, but it does not resolve which of the two "
        "candidate start times is exact. The event remains a boundary-review event, not a clock error.",
        "",
        "**April 21:** the workbook boundary (14:54) and photograph (14:54:55) agree. At S3 Top, "
        "the main response is after that boundary and progresses from 6 to 12 to 18 inches. The "
        "automated pre-start screen nevertheless found a smaller six-hour rise at S3 Top (6 inch, "
        "+0.586 VWC point) and S4 Bottom (6 inch, +1.692). Crucially, S1 Bottom (+0.625) and S2 "
        "Bottom (+0.623) also rose before the same start. The corrected interpretation is therefore "
        "field-wide pre-start drift or an event-boundary/field-condition issue, not evidence of a "
        "particular external source.",
        "",
        "## Unexplained daily deep rises",
        "",
        f"The exploratory screen found {len(rises)} daily deep-rise candidates across all S3/S4 "
        "positions and years after excluding recorded irrigation on the current/previous day and "
        "precipitation above 0.05 inch. A candidate requires the 18-inch daily mean to rise by at "
        "least 1 VWC point and by at least 0.75 point more than the larger shallow-depth change. "
        "No ditch-use, north-boundary inflow, or pipe-leak record is currently available. These "
        "candidates identify dates on which such external records or field observations would be useful.",
        "",
        f"The candidates form {len(episodes)} episodes. "
        f"{int(episodes['reaches_s3_later'].sum())} begin at S4 and reach S3 later; "
        f"{int(episodes['west_comparable_deep_rise'].sum())} have a comparable screened response "
        "at S1 or S2. The recurring S4-first, S3-later episodes in 2025 and June 2026 establish a "
        "directional sensor-response pattern only. They are compatible with several mechanisms, "
        "including redistribution of recorded irrigation, unrecorded entry from the east or north, "
        "a subsurface pipe leak, or sensor/site effects. With no independent record of those sources, "
        "the water source remains unknown.",
        "",
        *episode_rows,
        "",
        "![East-versus-west 18-inch VWC during directional episodes](lateral_episode_comparison.png)",
        *directional_images,
        "",
        "## Event water balance",
        "",
        "### Why calculate it?",
        "",
        "The purpose is to test whether the observed increase in monitored soil-water storage can "
        "plausibly account for the water assigned to each strip, and whether the paired biochar and "
        "control strips differ consistently. It is also a way to identify events that deserve closer "
        "review. It is **not** yet a direct estimate of runoff, deep drainage, or water savings.",
        "",
        "### Calculation sequence",
        "",
        "1. **Assigned applied water:** the shared meter total for a strip pair is divided equally "
        "between its two strips. This is an assumption because strip-specific inflow was not measured.",
        "2. **Modeled 0–18-inch storage change:** pre-event VWC is compared with the accepted "
        "post-event response at 6, 12, and 18 inches for the Top, Middle, and Bottom field zones; "
        "VWC changes are converted to gallons using the modeled soil and zone volumes and summed.",
        "3. **Signed residual:** assigned applied water minus modeled storage change. A negative value "
        "means modeled storage exceeded assigned water and signals measurement, timing, allocation, "
        "or spatial-representation error rather than creation of water.",
        "4. **Nonnegative unretained proxy:** max(signed residual, 0). This legacy descriptive field "
        "prevents negative 'unretained water,' but clipping makes it unsuitable as the sole paired "
        "treatment comparison.",
        "5. **Paired difference:** the biochar-strip value minus the control-strip value is calculated "
        "within the same irrigation event. Pairing controls for the shared event but not for persistent "
        "soil, topographic, lateral-water, or unequal-delivery differences between strips.",
        "",
        "Because the calculation assigns exactly the same applied-water amount, *A*, to the two "
        "strips, the paired signed-residual difference is not an independent result. Algebraically, "
        "`(A − storage_biochar) − (A − storage_control) = −(storage_biochar − storage_control)`. "
        "That is why the two former columns had equal magnitudes and opposite signs. The redundant "
        "signed-residual-difference column has been removed from this summary; the detailed CSV retains "
        "the strip-level residuals for auditing.",
        "",
        "The event table retains both eligible and failed rows so exclusions can be audited. Neither "
        "residual separates runoff from deep drainage, lateral redistribution, continued infiltration, "
        "unequal gate delivery, or model error.",
        "",
        *balance_rows,
        "",
        "No 2023 pair met the complete matched-event criteria used for this table.",
        "",
        "For 2026, fully eligible matched-event counts are:",
        "",
    ]
    if pair_2026.empty:
        lines.append("- None.")
    else:
        for row in pair_2026.itertuples():
            lines.append(f"- {row.strip_group}: {int(row.eligible_matched_events)} events.")
    lines.extend(
        [
            "",
            "The small matched-event counts—especially for 2026 S3/S4—mean the present residual "
            "differences are diagnostics, not defensible runoff or deep-drainage treatment effects.",
            "",
            "## Consolidated checks still needed",
            "",
            "Historical ditch-use dates and pipe-pressure, shutoff, repair, or leak-observation "
            "records are not available. The ditch and buried-pipe explanations therefore cannot be "
            "tested retrospectively against operational records. They remain unverified hypotheses, "
            "not pending record requests.",
            "",
            "1. Resolve the June 27 start boundary using the original start/end photographs, file "
            "metadata, field notes, and any valve-opening record; retain the 11:05 and 11:59 alternatives "
            "until that review is complete.",
            "2. Review April 21 field operations across both irrigation pairs. The pre-start drift is "
            "visible in all four strips, so test common causes before interpreting it as external water.",
            "3. Inspect 15-minute S4 Middle/Bottom and S3 Middle/Bottom traces for each directional "
            "episode. Daily means identify candidates but cannot establish hour-scale direction.",
            "4. Check sensor-specific offsets and installation context, especially the persistent S3T "
            "and S4T 18-inch-over-shallow ordering that is already present in 2023.",
            "5. Verify strip-specific delivery in future irrigation events. Temporary inflow measurements "
            "at both gates would test "
            "the equal-allocation assumption used in the water balance.",
            "6. In future field work, measure or bound tailwater at the south end and add deeper-than-18-inch observations "
            "if runoff and deep drainage are to be separated rather than combined in a residual.",
            "7. Recalculate treatment effects after excluding unresolved boundary and directional-pattern "
            "episodes, then compare the result with the full-data estimate as a sensitivity analysis.",
            "",
            "## Files",
            "",
            "- `multiyear_top_depth_profiles.png`: four years of S3/S4 Top depth traces.",
            "- `multiyear_deep_dominance.png`: the depth-order metric for all positions and years.",
            "- `lateral_episode_comparison.png`: 2025–2026 east-versus-west 18-inch traces with S4-first episodes highlighted.",
            "- `questionable_event_*.png`: contact sheets made from the existing S3/S4 event-multidepth plots.",
            "- `directional_episode_*_multilocation.png`: 15-minute Top/Middle/Bottom views for each S4-first, S3-later episode.",
            "- `deep_dominance_summary.csv`: annual persistence and magnitude statistics.",
            "- `unexplained_deep_rise_candidates.csv`: dry/non-irrigation deep-rise screen.",
            "- `unexplained_deep_wetting_episodes.csv`: grouped candidate episodes and direction.",
            "- `s3_s4_2026_event_timing_audit.csv`: event timing and photo-anchor decisions.",
            "- `paired_event_water_balance.csv`: complete S1/S2 and S3/S4 paired event table.",
            "- `paired_event_water_balance_summary.csv`: eligible annual paired summaries.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    daily = build_daily_profiles()
    dominance = summarize_deep_dominance(daily)
    rises = identify_unexplained_deep_rises(daily)
    episodes = group_deep_wetting_episodes(rises)
    event_audit = build_2026_event_audit()
    photo_source = summarize_photo_source(event_audit)
    paired, pair_summary = build_paired_water_balance()

    dominance.to_csv(OUTPUT / "deep_dominance_summary.csv", index=False)
    rises.to_csv(OUTPUT / "unexplained_deep_rise_candidates.csv", index=False)
    episodes.to_csv(OUTPUT / "unexplained_deep_wetting_episodes.csv", index=False)
    event_audit.to_csv(OUTPUT / "s3_s4_2026_event_timing_audit.csv", index=False)
    paired.to_csv(OUTPUT / "paired_event_water_balance.csv", index=False)
    pair_summary.to_csv(OUTPUT / "paired_event_water_balance_summary.csv", index=False)
    plot_multiyear_top_profiles(daily, OUTPUT / "multiyear_top_depth_profiles.png")
    plot_multiyear_deep_dominance(daily, OUTPUT / "multiyear_deep_dominance.png")
    plot_lateral_episode_comparison(daily, episodes, OUTPUT / "lateral_episode_comparison.png")
    questionable_plot_names = build_questionable_event_contact_sheets(event_audit)
    directional_plot_names = plot_directional_episode_multilocation(episodes)
    write_report(
        dominance,
        rises,
        episodes,
        event_audit,
        pair_summary,
        photo_source,
        questionable_plot_names,
        directional_plot_names,
        OUTPUT / "s3_s4_multiyear_water_audit.md",
    )
    print(f"Wrote multi-year S3/S4 audit to {OUTPUT}")
    print(f"Unexplained daily deep-rise candidates: {len(rises)}")
    print(episodes.to_string(index=False))
    print(pair_summary.to_string(index=False))


if __name__ == "__main__":
    main()

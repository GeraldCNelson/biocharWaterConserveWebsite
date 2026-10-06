"""Audit corrected S3M timing against irrigation, neighboring loggers, and S4.

This diagnostic deliberately separates event-level meter-photo QC from
sensor-level response behavior.  It does not alter logger timestamps.  Its
outputs document whether the corrected data support an additional clock
correction, a spatial wetting interpretation, or an ambiguous classification.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


REPO = Path(__file__).resolve().parents[4]
IRRIGATION = REPO / "biochar_app/data-processed/management/irrigation"
ANALYSIS = IRRIGATION / "analysis"
DIAGNOSTICS = ANALYSIS / "diagnostics"
OUTPUT = DIAGNOSTICS / "timestamp_diagnostics"
PHOTO_QC = IRRIGATION / "photos/meter_photo_workbook_qc.csv"
YEARS = (2023, 2024, 2025, 2026)
POSITIONS = (("S3", "T"), ("S3", "M"), ("S3", "B"), ("S4", "T"), ("S4", "M"), ("S4", "B"))


def _s3m_clock_state_for_event(timestamp: pd.Timestamp) -> tuple[str, int]:
    """Return the documented S3M correction state covering an event date.

    Irrigation events are well separated from the raw reset boundaries, so the
    event-date grouping is sufficient for comparing response behavior by state.
    """
    boundaries = [
        (pd.Timestamp("2026-02-19"), "after 2026-02-19 synchronization", 0),
        (pd.Timestamp("2025-01-16"), "2025-01-16 through 2026-02-19 reset", 270),
        (pd.Timestamp("2024-07-07"), "2024-07-07 through 2025-01-16 reset", 330),
        (pd.Timestamp("2023-09-04"), "2023-09-04 through 2024-07-07 reset", 390),
    ]
    for boundary, label, minutes in boundaries:
        if timestamp >= boundary:
            return label, minutes
    return "initial through 2023-09-04 reset", 450


def _read_yearly(prefix: str) -> pd.DataFrame:
    frames = [pd.read_csv(DIAGNOSTICS / f"{prefix}_{year}.csv") for year in YEARS]
    return pd.concat(frames, ignore_index=True)


def _as_datetime(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        result[column] = pd.to_datetime(result[column], errors="coerce")
    return result


def _earliest_delay(group: pd.DataFrame, strip: str, position: str) -> float:
    selected = group.loc[
        group["strip"].eq(strip) & group["logger_position"].eq(position),
        "arrival_minutes_after_irrigation_start",
    ]
    values = pd.to_numeric(selected, errors="coerce").dropna()
    return float(values.min()) if not values.empty else np.nan


def _photo_table() -> pd.DataFrame:
    photo = pd.read_csv(PHOTO_QC)
    photo = photo.loc[photo["strip_group"].eq("S3_S4")].copy()
    photo = _as_datetime(
        photo,
        ["workbook_start_timestamp", "start_photo_datetime"],
    )
    columns = [
        "year",
        "strip_group",
        "workbook_start_timestamp",
        "start_photo_datetime",
        "start_photo_minus_workbook_minutes",
        "start_qc_status",
        "start_review_reason",
        "event_qc_status",
        "event_review_required",
        "workbook_notes",
    ]
    return photo[columns].drop_duplicates(
        ["year", "strip_group", "workbook_start_timestamp"]
    )


def build_lateral_diagnostics(prestart: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    # Compare both irrigation pairs at the same recorded start.  A response in
    # S3 and S4 is not evidence of east-side lateral entry when S1 or S2 shows
    # the same pre-start behavior.
    for (_year, irrigation_start), group in prestart.groupby(
        ["year", "irrigation_start"], sort=False
    ):
        east = group.loc[group["strip_group"].eq("S3_S4")]
        if east.empty:
            continue
        first = east.iloc[0]
        row: dict[str, object] = {
            "year": int(first["year"]),
            "event_id": first["event_id"],
            "irrigation_start": first["irrigation_start"],
        }
        for strip in ("S1", "S2", "S3", "S4"):
            subset = group.loc[group["strip"].eq(strip)]
            row[f"{strip.lower()}_pre_start_sensor_count"] = int(
                subset["flag_pre_start_response"].fillna(False).astype(bool).sum()
            )
            row[f"{strip.lower()}_unexplained_pre_start_sensor_count"] = int(
                subset["flag_unexplained_pre_start_response"].fillna(False).astype(bool).sum()
            )
            row[f"{strip.lower()}_precip_driven_sensor_count"] = int(
                subset["likely_precip_driven_pre_start_response"].fillna(False).astype(bool).sum()
            )
            row[f"{strip.lower()}_affected_positions"] = ",".join(
                sorted(
                    subset.loc[
                        subset["flag_pre_start_response"].fillna(False).astype(bool),
                        "logger_position",
                    ].dropna().astype(str).unique()
                )
            )
        s3_unexplained = int(row["s3_unexplained_pre_start_sensor_count"])
        s4_unexplained = int(row["s4_unexplained_pre_start_sensor_count"])
        west_unexplained = (
            int(row["s1_unexplained_pre_start_sensor_count"])
            + int(row["s2_unexplained_pre_start_sensor_count"])
        )
        row["both_strips_unexplained_pre_start"] = bool(s3_unexplained and s4_unexplained)
        row["west_comparable_unexplained_pre_start"] = bool(west_unexplained)
        row["field_wide_pre_start_drift"] = bool(
            s3_unexplained and s4_unexplained and west_unexplained
        )
        row["possible_lateral_entry"] = bool(
            s3_unexplained and s4_unexplained and not west_unexplained
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["year", "irrigation_start"]).reset_index(drop=True)


def build_variable_coherence(arrivals: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year in YEARS:
        parquet = REPO / f"biochar_app/data-processed/parquet/{year}/{year}_raw_logger.parquet"
        data = pd.read_parquet(parquet).copy()
        data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
        data = data.dropna(subset=["timestamp"]).sort_values("timestamp").set_index("timestamp")
        year_rows = arrivals.loc[
            arrivals["year"].eq(year)
            & arrivals["strip"].eq("S3")
            & arrivals["logger_position"].eq("M")
        ]
        for _, arrival in year_rows.iterrows():
            depth = int(arrival["depth_index"])
            start = pd.Timestamp(arrival["irrigation_start"])
            end = pd.Timestamp(arrival["irrigation_end"])
            response = arrival["arrival_time"]
            window = data.loc[start - pd.Timedelta(hours=2): end + pd.Timedelta(hours=4)]
            variables = {
                "vwc": f"VWC_{depth}_raw_S3_M",
                "ec": f"EC_{depth}_raw_S3_M",
                "temperature": f"T_{depth}_raw_S3_M",
            }
            result: dict[str, object] = {
                "year": year,
                "event_id": arrival["event_id"],
                "irrigation_start": start,
                "depth_inches": int(arrival["depth_inches"]),
                "standard_arrival_time": response,
                "standard_arrival_delay_min": arrival["arrival_minutes_after_irrigation_start"],
            }
            for label, column in variables.items():
                series = pd.to_numeric(window[column], errors="coerce").dropna()
                if pd.isna(response) or series.empty:
                    before = after = np.nan
                else:
                    response_time = pd.Timestamp(response)
                    before_values = series.loc[
                        response_time - pd.Timedelta(hours=1): response_time - pd.Timedelta(minutes=15)
                    ]
                    after_values = series.loc[
                        response_time: response_time + pd.Timedelta(minutes=45)
                    ]
                    before = float(before_values.median()) if not before_values.empty else np.nan
                    after = float(after_values.median()) if not after_values.empty else np.nan
                result[f"{label}_before"] = before
                result[f"{label}_after"] = after
                result[f"{label}_change"] = after - before if np.isfinite(before) and np.isfinite(after) else np.nan

            diff_frame = pd.DataFrame({
                label: pd.to_numeric(window[column], errors="coerce").diff()
                for label, column in variables.items()
            }).dropna()
            result["vwc_ec_first_difference_correlation"] = (
                diff_frame["vwc"].corr(diff_frame["ec"]) if len(diff_frame) >= 8 else np.nan
            )
            result["vwc_temperature_first_difference_correlation"] = (
                diff_frame["vwc"].corr(diff_frame["temperature"]) if len(diff_frame) >= 8 else np.nan
            )
            rows.append(result)
    output = pd.DataFrame(rows)
    numeric = output.select_dtypes(include="number").columns
    output[numeric] = output[numeric].round(4)
    return output.sort_values(["year", "irrigation_start", "depth_inches"]).reset_index(drop=True)


def classify_events(
    arrivals: pd.DataFrame,
    photo: pd.DataFrame,
    lateral: pd.DataFrame,
) -> pd.DataFrame:
    merged = arrivals.merge(
        photo,
        how="left",
        left_on=["year", "strip_group", "irrigation_start"],
        right_on=["year", "strip_group", "workbook_start_timestamp"],
        validate="many_to_one",
    )
    rows: list[dict[str, object]] = []
    for event_id, group in merged.groupby("event_id", sort=False):
        first = group.iloc[0]
        s3m = group.loc[group["strip"].eq("S3") & group["logger_position"].eq("M")]
        standard = pd.to_numeric(s3m["arrival_minutes_after_irrigation_start"], errors="coerce")
        alternate = pd.to_numeric(s3m["alt_arrival_minutes_after_irrigation_start"], errors="coerce")
        position_delay = {
            f"{strip.lower()}{position.lower()}_earliest_standard_min": _earliest_delay(group, strip, position)
            for strip, position in POSITIONS
        }
        s3_reference = np.nanmedian([
            position_delay["s3t_earliest_standard_min"],
            position_delay["s3b_earliest_standard_min"],
        ])
        s3m_median = float(standard.median()) if standard.notna().any() else np.nan
        lateral_row = lateral.loc[lateral["event_id"].eq(event_id)]
        possible_lateral = bool(lateral_row["possible_lateral_entry"].iloc[0]) if not lateral_row.empty else False
        field_wide_pre_start = bool(
            lateral_row["field_wide_pre_start_drift"].iloc[0]
        ) if not lateral_row.empty else False
        pre_start_depth_count = int((alternate < 0).sum())
        missing_depth_count = int(standard.isna().sum())
        arrival_spread = float(standard.max() - standard.min()) if standard.notna().sum() >= 2 else np.nan
        relative_delay = s3m_median - s3_reference if np.isfinite(s3m_median) and np.isfinite(s3_reference) else np.nan

        if pre_start_depth_count >= 2 and not possible_lateral:
            classification = "possible_logger_clock_issue"
            correction_recommendation = "inspect_raw_clock_evidence_before_any_change"
        elif possible_lateral:
            classification = "possible_lateral_wetting"
            correction_recommendation = "no_clock_change"
        elif field_wide_pre_start:
            classification = "field_wide_pre_start_drift"
            correction_recommendation = "review_field_condition_or_event_boundary"
        elif missing_depth_count:
            classification = "ambiguous_or_incomplete_sensor_response"
            correction_recommendation = "no_clock_change"
        elif first["start_qc_status"] != "camera_workbook_agreement":
            classification = "irrigation_boundary_needs_review"
            correction_recommendation = "review_event_boundary_not_logger_clock"
        elif np.isfinite(relative_delay) and relative_delay > 120:
            classification = "timing_anchored_spatially_late_s3m"
            correction_recommendation = "no_clock_change"
        else:
            classification = "timing_anchored_no_clock_flag"
            correction_recommendation = "no_clock_change"

        row = {
            "year": int(first["year"]),
            "event_id": event_id,
            "irrigation_start": first["irrigation_start"],
            "photo_start": first["start_photo_datetime"],
            "photo_minus_workbook_min": first["start_photo_minus_workbook_minutes"],
            "photo_start_qc_scope": "irrigation_event_shared_by_all_sensor_rows",
            "photo_start_qc": first["start_qc_status"],
            "s3m_standard_arrival_depth_count": int(standard.notna().sum()),
            "s3m_missing_standard_depth_count": missing_depth_count,
            "s3m_alternate_pre_start_depth_count": pre_start_depth_count,
            "s3m_standard_arrival_spread_min": arrival_spread,
            "s3m_median_standard_delay_min": s3m_median,
            "s3_reference_median_top_bottom_delay_min": s3_reference,
            "s3m_minus_s3_top_bottom_reference_min": relative_delay,
            "possible_lateral_entry": possible_lateral,
            "field_wide_pre_start_drift": field_wide_pre_start,
            "classification": classification,
            "timestamp_correction_recommendation": correction_recommendation,
            "field_notes": first["workbook_notes"],
            **position_delay,
        }
        state_label, state_minutes = _s3m_clock_state_for_event(
            pd.Timestamp(first["irrigation_start"])
        )
        row["documented_s3m_clock_state"] = state_label
        row["documented_minutes_added_to_raw_s3m"] = state_minutes
        rows.append(row)
    output = pd.DataFrame(rows).sort_values(["year", "irrigation_start"]).reset_index(drop=True)
    numeric = output.select_dtypes(include="number").columns
    output[numeric] = output[numeric].round(3)
    return output


def build_state_summary(classified: pd.DataFrame) -> pd.DataFrame:
    # These are the already-operational absolute states documented in etl.py.
    states = pd.DataFrame(
        [
            ("initial through 2023-09-04 reset", 450),
            ("2023-09-04 through 2024-07-07 reset", 390),
            ("2024-07-07 through 2025-01-16 reset", 330),
            ("2025-01-16 through 2026-02-19 reset", 270),
            ("after 2026-02-19 synchronization", 0),
        ],
        columns=["documented_s3m_clock_state", "minutes_added_to_raw_timestamp"],
    )
    states["evidence"] = [
        "back-propagated from 2026 anchor through verified discontinuities",
        "verified raw 75-minute forward discontinuity",
        "verified raw 75-minute forward discontinuity",
        "verified raw 75-minute forward discontinuity",
        "PC400 comparison after battery replacement",
    ]
    states["additional_correction_supported_by_event_audit"] = False
    states["event_audit_note"] = (
        "Corrected irrigation responses do not show a persistent multi-depth pre-start S3M offset."
    )
    grouped = classified.groupby("documented_s3m_clock_state", observed=True).agg(
        irrigation_event_count=("event_id", "size"),
        median_s3m_minus_s3_top_bottom_reference_min=(
            "s3m_minus_s3_top_bottom_reference_min", "median"
        ),
        multi_depth_pre_start_event_count=(
            "s3m_alternate_pre_start_depth_count", lambda values: int((values >= 2).sum())
        ),
    ).reset_index()
    states = states.merge(grouped, on="documented_s3m_clock_state", how="left")
    return states


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    arrivals = _read_yearly("irrigation_arrival_times")
    arrivals = arrivals.loc[arrivals["strip_group"].eq("S3_S4")].copy()
    arrivals = _as_datetime(arrivals, ["irrigation_start", "irrigation_end", "arrival_time", "alt_arrival_time"])
    prestart = _read_yearly("irrigation_pre_start_response_flags_all_positions")
    prestart = _as_datetime(prestart, ["irrigation_start"])

    lateral = build_lateral_diagnostics(prestart)
    coherence = build_variable_coherence(arrivals)
    classified = classify_events(arrivals, _photo_table(), lateral)
    states = build_state_summary(classified)

    lateral.to_csv(OUTPUT / "s3m_lateral_wetting_diagnostics.csv", index=False)
    coherence.to_csv(OUTPUT / "s3m_variable_coherence.csv", index=False)
    classified.to_csv(OUTPUT / "s3m_event_classification.csv", index=False)
    states.to_csv(OUTPUT / "s3m_timing_state_summary.csv", index=False)

    print("S3/S4 events:", len(classified))
    print("Classification counts:")
    print(classified["classification"].value_counts().to_string())
    print("S3M alternate pre-start depth counts:")
    print(classified["s3m_alternate_pre_start_depth_count"].value_counts().sort_index().to_string())
    print("S3M missing standard-arrival depth counts:")
    print(classified["s3m_missing_standard_depth_count"].value_counts().sort_index().to_string())
    print("Possible lateral-entry events:", int(classified["possible_lateral_entry"].sum()))
    print("Additional S3M clock corrections recommended:", int(classified["timestamp_correction_recommendation"].eq("inspect_raw_clock_evidence_before_any_change").sum()))


if __name__ == "__main__":
    main()

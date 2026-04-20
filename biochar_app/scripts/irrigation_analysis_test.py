import pandas as pd
from pathlib import Path

from biochar_app.config.core import (
    DEFAULT_YEAR,
    SENSOR_DEPTH_CODES,
    SENSOR_DEPTH_VALUES,
    STRIPS,
)
from biochar_app.scripts.data_loading import (
    load_irrigation_data,
    load_logger_data,
    prepare_irrigation_input,
)

from biochar_app.scripts.irrigation_analysis import (
    analyze_irrigation_events,
    build_event_debug_table,
    summarize_targets_and_runtimes,
    build_bottom_control_sensor_map,
    analyze_bottom_logger_controls,
    build_variable_definitions_table,
    build_variable_definitions_with_sources,
    save_irrigation_event_multidepth_plots,
)

DEPTH_INDEX_TO_INCHES: dict[str, int] = {
    depth_code: int(SENSOR_DEPTH_VALUES[depth_code]["us"])
    for depth_code in SENSOR_DEPTH_CODES
}

year = DEFAULT_YEAR


def run_irrigation_analysis_for_year(
    year: int,
    strip_to_sensor_map: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run the irrigation analysis workflow for one year and return:
    1. event-level debug table
    2. targets table
    3. rec_runtime table
    """
    df_15min = load_logger_data(year=year, granularity="15min")

    if "timestamp" in df_15min.columns:
        df_15min["timestamp"] = pd.to_datetime(df_15min["timestamp"], errors="coerce")
        df_15min = prepare_irrigation_input(df_15min)
    elif isinstance(df_15min.index, pd.DatetimeIndex):
        df_15min.index = pd.to_datetime(df_15min.index, errors="coerce")
        df_15min = df_15min[~df_15min.index.isna()].copy()
        df_15min = df_15min.sort_index()
        df_15min = df_15min[~df_15min.index.duplicated(keep="last")].copy()
    else:
        raise ValueError(f"Could not prepare timestamp index for year {year}")

    results = analyze_bottom_logger_controls(
        df_15min=df_15min,
        strips=STRIPS,
        year=year,
        strip_to_bottom_sensor=strip_to_sensor_map,
    )

    debug_table = build_event_debug_table(results)
    targets, rec_runtimes = summarize_targets_and_runtimes(results)

    if not debug_table.empty:
        debug_table["year"] = year
    if not targets.empty:
        targets["year"] = year
    if not rec_runtimes.empty:
        rec_runtimes["year"] = year

    return debug_table, targets, rec_runtimes


def build_bottom_logger_profile_map() -> dict[str, list[str]]:
    """
    Return all bottom-logger VWC columns for each strip and depth.
    """
    return {
        strip: [f"VWC_{depth_code}_raw_{strip}_B" for depth_code in SENSOR_DEPTH_CODES]
        for strip in STRIPS
    }


def add_derived_event_fields(event_results: pd.DataFrame) -> pd.DataFrame:
    """
    Add:
    - depth_inches
    - lag_after_irrigation_hr
    - avg_flow_gph
    """
    if event_results.empty:
        return event_results.copy()

    out = event_results.copy()

    if "depth_index" in out.columns:
        out["depth_index"] = out["depth_index"].astype("string")
        out["depth_inches"] = out["depth_index"].map(DEPTH_INDEX_TO_INCHES)
    else:
        out["depth_inches"] = pd.NA

    if "time_to_plateau_hours" in out.columns and "event_duration_hours" in out.columns:
        out["lag_after_irrigation_hr"] = (
            pd.to_numeric(out["time_to_plateau_hours"], errors="coerce")
            - pd.to_numeric(out["event_duration_hours"], errors="coerce")
        )
    else:
        out["lag_after_irrigation_hr"] = pd.NA

    if "volume_gal" in out.columns and "event_duration_hours" in out.columns:
        volume = pd.to_numeric(out["volume_gal"], errors="coerce")
        duration = pd.to_numeric(out["event_duration_hours"], errors="coerce")
        out["avg_flow_gph"] = volume / duration
        out.loc[duration <= 0, "avg_flow_gph"] = pd.NA
    else:
        out["avg_flow_gph"] = pd.NA

    return out


def analyze_bottom_logger_all_depths(
    df_15min: pd.DataFrame,
    strips: list[str],
    year: int,
    strip_to_profile_sensors: dict[str, list[str]],
) -> pd.DataFrame:
    """
    Run irrigation-event analysis for all bottom-logger depths in each strip.
    """
    all_results: list[pd.DataFrame] = []
    all_events = load_irrigation_data()

    for strip in strips:
        sensor_cols = strip_to_profile_sensors.get(strip, [])
        if not sensor_cols:
            print(f"Skipping {strip}: no bottom logger profile configured.")
            continue

        available_sensor_cols = [col for col in sensor_cols if col in df_15min.columns]
        if not available_sensor_cols:
            print(f"Skipping {strip}: no configured bottom-logger columns found.")
            continue

        events = all_events.loc[
            (all_events["strip"] == strip) & (all_events["year"] == year),
            ["start_timestamp", "end_timestamp", "gallons"],
        ].copy()

        if events.empty:
            print(f"Skipping {strip}: no irrigation events returned.")
            continue

        events = events.rename(
            columns={
                "start_timestamp": "start",
                "end_timestamp": "end",
                "gallons": "volume_gal",
            }
        )

        strip_results = analyze_irrigation_events(
            df=df_15min,
            events=events,
            sensor_cols=available_sensor_cols,
            start_col="start",
            end_col="end",
            volume_col="volume_gal",
            strip=strip,
            year=year,
        )

        if not strip_results.empty:
            all_results.append(strip_results)

    if not all_results:
        return pd.DataFrame()

    out = pd.concat(all_results, ignore_index=True)
    out = add_derived_event_fields(out)
    return out


def build_enhanced_event_debug_table(
    event_results: pd.DataFrame,
    decimals: int = 2,
) -> pd.DataFrame:
    """
    Event-level diagnostics with added practical columns.
    """
    if event_results.empty:
        return pd.DataFrame()

    keep_cols = [
        "event_id",
        "strip",
        "year",
        "sensor_col",
        "depth_index",
        "depth_inches",
        "logger_position",
        "irrigation_start",
        "irrigation_end",
        "volume_gal",
        "event_duration_hours",
        "avg_flow_gph",
        "baseline_vwc",
        "peak_vwc",
        "peak_increase",
        "plateau_vwc",
        "plateau_method",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "lag_after_irrigation_hr",
    ]
    keep_cols = [c for c in keep_cols if c in event_results.columns]

    out = event_results[keep_cols].copy()

    numeric_cols = out.select_dtypes(include=["number"]).columns
    out[numeric_cols] = out[numeric_cols].round(decimals)

    return out


def build_enhanced_runtime_table(
    event_results: pd.DataFrame,
    min_events: int = 3,
) -> pd.DataFrame:
    """
    Grouped rec_runtime summary using time_to_plateau_hours, plus actual runtime
    and supporting irrigation metrics.
    """
    if event_results.empty:
        return pd.DataFrame()

    required_group_cols = ["strip", "depth_inches"]

    df = event_results.copy()

    for col in [
        "time_to_plateau_hours",
        "event_duration_hours",
        "avg_flow_gph",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    runtime_rows: list[dict[str, object]] = []
    grouped = df.groupby(required_group_cols, dropna=False)

    for group_key, sub in grouped:
        rec_runtime_vals = pd.to_numeric(
            sub["time_to_plateau_hours"], errors="coerce"
        ).dropna()
        n_events = int(rec_runtime_vals.shape[0])

        if n_events == 0:
            continue

        actual_runtime_vals = pd.to_numeric(
            sub["event_duration_hours"], errors="coerce"
        ).dropna()
        flow_vals = pd.to_numeric(sub["avg_flow_gph"], errors="coerce").dropna()

        if "lag_after_irrigation_hr" in sub.columns:
            lag_vals = pd.to_numeric(
                sub["lag_after_irrigation_hr"], errors="coerce"
            ).dropna()
        else:
            lag_vals = pd.Series(dtype="float64")

        rec_runtime_hours = float(rec_runtime_vals.median())
        actual_runtime_hours = (
            float(actual_runtime_vals.median())
            if not actual_runtime_vals.empty
            else pd.NA
        )

        row: dict[str, object] = {
            "n_events": n_events,
            "rec_runtime_hours": rec_runtime_hours,
            "rec_runtime_minutes": rec_runtime_hours * 60.0,
            "rec_runtime_is_trustworthy": n_events >= min_events,
            "actual_runtime_hours": actual_runtime_hours,
            "actual_runtime_minutes": (
                actual_runtime_hours * 60.0
                if actual_runtime_hours is not pd.NA
                else pd.NA
            ),
            "source_time_col": "time_to_plateau_hours",
            "summary_stat": "median",
            "median_avg_flow_gph": (
                float(flow_vals.median()) if not flow_vals.empty else pd.NA
            ),
            "median_lag_after_irrigation_hr": (
                float(lag_vals.median()) if not lag_vals.empty else pd.NA
            ),
        }

        if not isinstance(group_key, tuple):
            group_key = (group_key,)

        for col_name, value in zip(required_group_cols, group_key):
            row[col_name] = value

        runtime_rows.append(row)

    out = pd.DataFrame(runtime_rows)
    if not out.empty:
        numeric_cols = out.select_dtypes(include=["number"]).columns
        out[numeric_cols] = out[numeric_cols].round(6)

    return out


# ---------------------------------------------------------------------
# Main test workflow
# ---------------------------------------------------------------------

df_15min = load_logger_data(year=year, granularity="15min")

print("Rows before index:", len(df_15min))
print("Columns:", df_15min.columns.tolist()[:20])
print("Index type:", type(df_15min.index))
print("Index name:", df_15min.index.name)

if "timestamp" in df_15min.columns:
    df_15min["timestamp"] = pd.to_datetime(df_15min["timestamp"], errors="coerce")

    print("Unique timestamps:", df_15min["timestamp"].nunique())
    print("Duplicate timestamp rows:", df_15min["timestamp"].duplicated().sum())

    dupes = df_15min[df_15min["timestamp"].duplicated(keep=False)].copy()
    if not dupes.empty:
        print("\n=== DUPLICATE TIMESTAMP SAMPLE ===")
        print(dupes.head(20).to_string(index=False))

    df_15min = prepare_irrigation_input(df_15min)

elif isinstance(df_15min.index, pd.DatetimeIndex):
    print("Unique timestamps:", df_15min.index.nunique())
    print("Duplicate timestamp rows:", df_15min.index.duplicated().sum())

    dupes = df_15min[df_15min.index.duplicated(keep=False)].copy()
    if not dupes.empty:
        print("\n=== DUPLICATE TIMESTAMP SAMPLE ===")
        print(dupes.head(20).to_string())

    df_15min.index = pd.to_datetime(df_15min.index, errors="coerce")
    df_15min = df_15min[~df_15min.index.isna()].copy()
    df_15min = df_15min.sort_index()
    df_15min = df_15min[~df_15min.index.duplicated(keep="last")].copy()

else:
    raise ValueError("Could not find timestamp column or DatetimeIndex in df_15min")

print("Rows after timestamp prep:", len(df_15min))
if isinstance(df_15min.index, pd.DatetimeIndex):
    print("Index type after prep:", type(df_15min.index))
    print("Index name after prep:", df_15min.index.name)
    print("Unique index timestamps:", df_15min.index.nunique())
    print("Duplicate index rows:", df_15min.index.duplicated().sum())

strip_to_profile_sensors = build_bottom_logger_profile_map()

results = analyze_bottom_logger_all_depths(
    df_15min=df_15min,
    strips=STRIPS,
    year=year,
    strip_to_profile_sensors=strip_to_profile_sensors,
)

print("\n=== RESULTS COLUMNS ===")
if results.empty:
    print("No results returned.")
else:
    print(results.columns.tolist())

debug_table = build_enhanced_event_debug_table(results)
targets, _ = summarize_targets_and_runtimes(results)
runtimes = build_enhanced_runtime_table(results)
definitions = build_variable_definitions_table()

if not targets.empty and "depth_index" in results.columns:
    sensor_depth_lookup = (
        results[["sensor_col", "depth_index", "depth_inches"]]
        .drop_duplicates()
        .copy()
    )
    targets = targets.merge(sensor_depth_lookup, on="sensor_col", how="left")

print("\n=== DEBUG TABLE ===")
if debug_table.empty:
    print("No event-level results returned.")
else:
    print(debug_table.head(40).to_string(index=False))

print("\n=== TARGETS ===")
if targets.empty:
    print("No targets returned.")
else:
    print(targets.to_string(index=False))

print("\n=== RUNTIMES ===")
if runtimes.empty:
    print("No runtimes returned.")
else:
    print(runtimes.to_string(index=False))

print("\n=== VARIABLE DEFINITIONS ===")
print(definitions.to_string(index=False))

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "biochar_app" / "data-processed" / "management"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def force_float(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].astype(float)
    return df


def move_id_columns_left(df: pd.DataFrame) -> pd.DataFrame:
    left_cols = ["year", "strip", "sensor_col", "depth_index", "depth_inches"]
    left_cols = [col for col in left_cols if col in df.columns]
    other_cols = [col for col in df.columns if col not in left_cols]
    return df[left_cols + other_cols]


debug_table["year"] = year
targets["year"] = year
runtimes["year"] = year

debug_table = move_id_columns_left(debug_table)
targets = move_id_columns_left(targets)
runtimes = move_id_columns_left(runtimes)

runtimes = force_float(runtimes)
targets = force_float(targets)
debug_table = force_float(debug_table)

debug_table.to_csv(
    OUTPUT_DIR / f"debug_irrigation_events_{year}_all_depths.csv",
    index=False,
    float_format="%.4f",
)
targets.to_csv(
    OUTPUT_DIR / f"irrigation_targets_{year}_all_depths.csv",
    index=False,
    float_format="%.4f",
)
runtimes.to_csv(
    OUTPUT_DIR / f"irrigation_runtimes_{year}_all_depths.csv",
    index=False,
    float_format="%.4f",
)
definitions.to_csv(
    OUTPUT_DIR / f"irrigation_variable_definitions_{year}.csv",
    index=False,
)

expanded_definitions = build_variable_definitions_with_sources(
    output_dir=str(OUTPUT_DIR),
    year=year,
)

MULTIDEPTH_PLOT_DIR = OUTPUT_DIR / "irrigation_event_multidepth_plots"
MULTIDEPTH_PLOT_LOG = OUTPUT_DIR / f"irrigation_event_multidepth_plot_log_{year}.csv"

multidepth_plot_log = save_irrigation_event_multidepth_plots(
    df=df_15min,
    event_results=results,
    output_dir=MULTIDEPTH_PLOT_DIR,
    strip_filter=["S1", "S2", "S3", "S4"],
    event_ids=None,
    logger_position="B",
    depths=(1, 2, 3),
    hours_before=4.0,
    hours_after=36.0,
    max_plots=None,
    precip_col="precip_in",
    use_common_y_axis=True,
)

multidepth_plot_log.to_csv(
    MULTIDEPTH_PLOT_LOG,
    index=False,
)

print("\n=== FILES WRITTEN ===")
print(OUTPUT_DIR / f"debug_irrigation_events_{year}_all_depths.csv")
print(OUTPUT_DIR / f"irrigation_targets_{year}_all_depths.csv")
print(OUTPUT_DIR / f"irrigation_runtimes_{year}_all_depths.csv")
print(OUTPUT_DIR / f"irrigation_variable_definitions_{year}.csv")
print(OUTPUT_DIR / f"irrigation_variable_definitions_{year}_expanded.csv")
print(MULTIDEPTH_PLOT_LOG)
print(MULTIDEPTH_PLOT_DIR)
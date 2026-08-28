# Function Catalog

This file is generated from Python AST metadata and JavaScript source patterns.
Regenerate both the Markdown and JSON catalogs with:

```bash
python biochar_app/scripts/dev-tools/build_function_catalog.py
```

- Python functions and methods: **1618**
- JavaScript functions: **326**
- Files with Python parse errors: **4**

The JavaScript inventory is intentionally heuristic; dynamically created functions
and some class/object method syntaxes may require manual review.

## Python

### `biochar_app/config/biochar_lab_reports.py`

- **`biochar_lab_report_path(report_key: str) -> Path`** — line 28; function; public/exported. Return the configured source PDF path for a public report key.

### `biochar_app/config/core.py`

- **`format_gseason_date(mm_dd: str) -> str`** — line 135; function; public/exported. No docstring.
- **`build_gseason_period_label(period: dict) -> str`** — line 140; function; public/exported. No docstring.
- **`build_gseason_period_labels(periods: dict) -> list[str]`** — line 147; function; public/exported. No docstring.
- **`cylinder_volume_m3(length_cm: float=SWC_CYLINDER_LENGTH_CM, radius_cm: float=SWC_CYLINDER_RADIUS_CM) -> float`** — line 284; function; public/exported. Compute the legacy reference-cylinder volume in cubic metres.

### `biochar_app/config/deployment_manifest.py`

- **`build_deployment_requirements() -> tuple[DeploymentRequirement, ...]`** — line 42; function; public/exported. Return deployment requirements assembled from feature configuration.

### `biochar_app/config/field_management_metadata.py`

- **`build_zone_lengths_ft(strip: str) -> dict[str, float]`** — line 108; function; public/exported. No docstring.

### `biochar_app/config/geospatial/lidar.py`

- **`get_lidar_product(product_key: str \| None=None) -> dict`** — line 63; function; public/exported. No docstring.

### `biochar_app/config/irrigation_config.py`

- **`get_irrigation_analysis_options(variant: str \| None=None) -> IrrigationAnalysisOptions`** — line 126; function; public/exported. Return validated options for the requested irrigation-analysis variant.

### `biochar_app/config/lab_reference_data.py`

- **`combine_reference_bundles(*bundles: VariableReferenceBundle) -> VariableReferenceBundle`** — line 11; function; public/exported. No docstring.

### `biochar_app/config/lab_reference_models.py`

- **`InterpretationBand.matches(self, value: float) -> bool`** — line 25; method; public/exported. No docstring.

### `biochar_app/config/lab_variable_metadata.py`

- **`get_lab_variable_metadata(key: str) -> dict[str, Any]`** — line 1448; function; public/exported. No docstring.
- **`get_display_label(key: str) -> str`** — line 1452; function; public/exported. No docstring.
- **`get_units(key: str) -> str \| None`** — line 1456; function; public/exported. No docstring.

### `biochar_app/config/pakbus.py`

- **`parse_ids(s: str) -> List[int]`** — line 38; function; public/exported. Supports '2-13' or '2,3,5-7'. Defaults to an empty list on bad input.

### `biochar_app/config/paths.py`

- **`irrigation_analysis_paths(variant: str \| None=None) -> dict[str, Path]`** — line 123; function; public/exported. Return output paths for one irrigation-analysis variant.
- **`ensure_analysis_output_directories(paths: dict[str, Path]) -> None`** — line 143; function; public/exported. Create an irrigation-analysis output directory tree if it does not exist.

### `biochar_app/config/thresholds.py`

- **`_coerce_numeric(series: pd.Series) -> pd.Series`** — line 106; function; internal. No docstring.
- **`_compile_family_rules() -> List[ColumnFamilyRule]`** — line 110; function; internal. No docstring.
- **`_compile_explicit_rules() -> Dict[str, BoundRule]`** — line 128; function; internal. No docstring.
- **`_collect_examples(df: pd.DataFrame, is_violation: pd.Series, example_col: str, limit: int) -> List[Dict[str, Any]]`** — line 141; function; internal. No docstring.
- **`apply_value_bounds(df: pd.DataFrame, *, year: int, bad_value_threshold: Optional[float]=DEFAULT_BAD_VALUE_THRESHOLD, bad_value_cols: Optional[Sequence[str]]=None, family_rules: Optional[Sequence[ColumnFamilyRule]]=None, explicit_rules: Optional[Dict[str, BoundRule]]=None, collect_examples: int=0) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]`** — line 164; function; public/exported. Apply sentinel masking + bounds enforcement.
- **`apply_value_bounds._apply_bound_rule_to_column(col_to_check: str, rule_to_apply: BoundRule, rule_tag: str) -> None`** — line 228; nested function; internal. No docstring.

### `biochar_app/config/units.py`

- **`validate_unit_system(value: str \| None) -> UnitSystemKey`** — line 30; function; public/exported. Strict validation: unknown values raise ValueError. Use at boundaries (API request parsing, UI bootstrap).
- **`human_label(col_name: str, unit_system: UnitSystemKey) -> str`** — line 172; function; public/exported. Given a column like 'precip_mm', 'precip_in', 'temp_air_degF', or 'soil_temp_6in_degF', return a human-friendly label.
- **`conversion_for_column(colname: str) -> Optional[Callable[[float], float]]`** — line 190; function; public/exported. Returns a conversion lambda (us_to_metric) based on known suffixes, otherwise None.

### `biochar_app/diagnostics/battery_replacements.py`

- **`_ensure_dir(p: Path) -> None`** — line 94; function; internal. No docstring.
- **`_normalize_logger_key(x: str) -> str`** — line 98; function; internal. Accept "S3_M", "S3M", "S3-M", "S3 M" -> "S3_M"
- **`_read_raw_logger_parquet(year: int) -> pd.DataFrame`** — line 109; function; internal. No docstring.
- **`_list_batt_cols(df: pd.DataFrame) -> List[str]`** — line 125; function; internal. No docstring.
- **`_logger_from_batt_col(col: str) -> str`** — line 129; function; internal. No docstring.
- **`_safe_numeric(s: pd.Series) -> pd.Series`** — line 136; function; internal. No docstring.
- **`_window_df(df: pd.DataFrame, start: Optional[str], end: Optional[str]) -> pd.DataFrame`** — line 140; function; internal. No docstring.
- **`extract_predawn_series(df: pd.DataFrame, batt_col: str, *, target_hour: int=5, window_minutes: int=20) -> pd.DataFrame`** — line 158; function; public/exported. Return a small DataFrame with one row per day (where present): date, time, value
- **`detect_upward_steps(predawn: pd.DataFrame, *, min_step_v: float=0.6, require_stable_days: int=5, stable_floor_v: float=12.0) -> pd.DataFrame`** — line 202; function; public/exported. Detect candidate replacement events as large upward steps in the pre-dawn series.
- **`detect_upward_steps.stable_streak_from(i: int) -> int`** — line 235; nested function; public/exported. No docstring.
- **`summarize_predawn_streaks(predawn: pd.DataFrame, *, floor_v: float=12.0, min_days: int=5) -> List[Tuple[pd.Timestamp, pd.Timestamp, int]]`** — line 270; function; public/exported. Find streaks of days where predawn value >= floor_v for at least min_days. Returns list of (start_time, end_time, n_days).
- **`load_manual_inventory_csv(path: Path) -> pd.DataFrame`** — line 321; function; public/exported. Expected CSV columns:
- **`validate_intervals(man: pd.DataFrame) -> None`** — line 433; function; public/exported. No docstring.
- **`get_active_battery(man: pd.DataFrame, logger: str, ts: pd.Timestamp) -> pd.Series \| None`** — line 448; function; public/exported. No docstring.
- **`print_logger_summary(logger_key: str, predawn: pd.DataFrame, steps: pd.DataFrame, *, floor_v: float, streak_days: int) -> None`** — line 464; function; public/exported. No docstring.
- **`candidates_to_rows(logger_key: str, steps: pd.DataFrame, *, min_step_v: float, floor_v: float, streak_days: int) -> List[Dict[str, object]]`** — line 497; function; public/exported. No docstring.
- **`merge_manual_with_candidates(candidates: pd.DataFrame, manual_df: pd.DataFrame, *, max_days_apart: int=10) -> pd.DataFrame`** — line 526; function; public/exported. Merge manual replacements onto nearest candidate in time (per logger) if within max_days_apart. Also keep manual entries even if no candidate matched.
- **`main() -> int`** — line 645; function; public/exported. No docstring.

### `biochar_app/diagnostics/check_dat_ranges.py`

- **`RangeRule.violates(self, s: pd.Series) -> pd.Series`** — line 48; method; public/exported. No docstring.
- **`extract_logger_tag(path: Path) -> str`** — line 83; function; public/exported. Extract logger tag from filename. Example: S3M_Table1.dat -> S3M
- **`_clean_col_name(s: str) -> str`** — line 94; function; internal. No docstring.
- **`_read_toa5(datfile: Path) -> pd.DataFrame`** — line 102; function; internal. No docstring.
- **`_parse_timestamp(series: pd.Series) -> pd.Series`** — line 125; function; internal. No docstring.
- **`iter_project_datfiles(root: Path, glob_pattern: str) -> Iterable[Path]`** — line 140; function; public/exported. If root is biochar_app/data-raw, scan ONLY datfiles_2023..datfiles_2026 and do a NON-recursive glob inside each folder. This avoids scanning archived duplicates (old/, datsForPC/, etc.) unless you explicitly point root at those directories.
- **`mask_sentinels(df: pd.DataFrame, columns: List[str], threshold: float) -> None`** — line 166; function; public/exported. No docstring.
- **`find_rule(col: str) -> Optional[RangeRule]`** — line 177; function; public/exported. No docstring.
- **`scan_file(datfile: Path, bad_value_threshold: float) -> List[Dict[str, str]]`** — line 190; function; public/exported. No docstring.
- **`build_summary(df_viol: pd.DataFrame) -> pd.DataFrame`** — line 252; function; public/exported. Group by logger_tag/column/rule and count. Also report first/last timestamp where available.
- **`main() -> None`** — line 294; function; public/exported. No docstring.

### `biochar_app/diagnostics/check_dst_transitions_parquet.py`

- **`load_year_parquet(year: int) -> pd.DataFrame`** — line 48; function; public/exported. No docstring.
- **`inspect_window(df: pd.DataFrame, *, year: int, label: str, center_date: str, hours_before: int=12, hours_after: int=12, write_csv: bool=True) -> None`** — line 59; function; public/exported. No docstring.
- **`main(years: Iterable[int]=(2023, 2024, 2025)) -> None`** — line 100; function; public/exported. No docstring.

### `biochar_app/diagnostics/clock_mode_analysis.py`

- **`_read_raw_logger_parquet(year: int) -> pd.DataFrame`** — line 38; function; internal. No docstring.
- **`_find_denver_dst_window(year: int, tz: str='America/Denver') -> DstWindow`** — line 66; function; internal. Find the two DST transitions for a given year in America/Denver by scanning hourly. Returns (dst_start, dst_end) for that year as both tz-aware and naive wall-times.
- **`_is_mst_window_naive(ts: pd.Series, dstw: DstWindow) -> pd.Series`** — line 98; function; internal. Within a single year's data, treat MST (standard-time) window as: timestamps < dst_start OR >= dst_end (All naive wall-time.)
- **`_is_mdt_window_naive(ts: pd.Series, dstw: DstWindow) -> pd.Series`** — line 107; function; internal. No docstring.
- **`_logger_keys() -> List[str]`** — line 113; function; internal. No docstring.
- **`_normalize_logger_key(logger_key: str) -> str`** — line 117; function; internal. No docstring.
- **`_suffix_for_logger(logger_key: str) -> str`** — line 123; function; internal. No docstring.
- **`_batt_col_for_logger(df: pd.DataFrame, logger_key: str) -> Optional[str]`** — line 131; function; internal. No docstring.
- **`_logger_timestamp_set(df: pd.DataFrame, value_cols: List[str]) -> pd.Index`** — line 142; function; internal. Return timestamps where ANY of value_cols is non-null.
- **`_find_jump_events(ts: pd.Series, *, step_min: float=15.0) -> List[JumpEvent]`** — line 163; function; internal. Detect likely Set Clock events from timestamp diffs.
- **`_compute_overlap(a: pd.Index, b: pd.Index) -> float`** — line 220; function; internal. No docstring.
- **`_seasonal_shift_index(idx: pd.Index, ts_mask: pd.Series, shift: pd.Timedelta) -> pd.Index`** — line 227; function; internal. Shift only the subset of timestamps where ts_mask is True. idx is an Index of timestamps from the logger. ts_mask is a boolean Series aligned to idx values (same length).
- **`overlap_seasonal_test(df: pd.DataFrame, *, year: int, logger_key: str, ref_key: str, dstw: DstWindow) -> OverlapResult`** — line 247; function; public/exported. No docstring.
- **`classify_logger(df: pd.DataFrame, *, year: int, logger_key: str, ref_key: str, dstw: DstWindow, overlap_gain_threshold: float=0.1) -> ClockMode`** — line 300; function; public/exported. Heuristics: - If many jump events -> irregular_manual - If one forward jump after DST start (spring) and no backward -> seasonal_manual - If no jumps: * if overlap improves with MST -1h shift -> always_mdt * elif overlap improves with MDT +1h shift -> always_mst * else unknown
- **`_pick_default_reference_logger(df: pd.DataFrame) -> str`** — line 386; function; internal. Pick a logger with the most non-null BattV_Min points as a stable reference for overlap tests.
- **`_resolve_reference_logger(df: pd.DataFrame, requested: Optional[str]) -> str`** — line 403; function; internal. If requested is provided and appears in the dataset, use it. Otherwise fall back to auto-pick.
- **`run_year(year: int, *, overlap_gain_threshold: float=0.1, requested_ref_logger: Optional[str]=None) -> Tuple[pd.DataFrame, pd.DataFrame, str]`** — line 418; function; public/exported. No docstring.
- **`main() -> int`** — line 469; function; public/exported. No docstring.

### `biochar_app/diagnostics/extract_pre2024_battv_from_2024.py`

- **`_clean_col_name(col: object) -> str`** — line 38; function; internal. No docstring.
- **`_read_first_four_lines_raw(path: Path) -> List[str]`** — line 45; function; internal. No docstring.
- **`_read_toa5_data_frame(path: Path) -> Tuple[pd.DataFrame, List[str]]`** — line 59; function; internal. ETL-consistent TOA5: line 1: TOA5 metadata line 2: colnames line 3: units line 4: agg/processing data starts line 5 Returns (df, cleaned_colnames)
- **`_coerce_timestamp(ts: pd.Series) -> pd.Series`** — line 95; function; internal. No docstring.
- **`infer_logger_tag(path: Path) -> str`** — line 104; function; public/exported. No docstring.
- **`build_out_name(tag: str, suffix: str) -> str`** — line 109; function; public/exported. No docstring.
- **`write_toa5_subset(*, src_path: Path, dst_path: Path, header_lines: List[str], df_subset: pd.DataFrame) -> None`** — line 114; function; public/exported. No docstring.
- **`main() -> None`** — line 138; function; public/exported. No docstring.

### `biochar_app/diagnostics/inspect_config_exports.py`

- **`ModuleSymbols.all_symbols(self) -> list[str]`** — line 86; method; public/exported. No docstring.
- **`ModuleSymbols.candidate_symbols(self) -> list[str]`** — line 89; method; public/exported. No docstring.
- **`is_public_constant_name(name: str) -> bool`** — line 113; function; public/exported. No docstring.
- **`extract_module_symbols(py_file: Path) -> ModuleSymbols`** — line 117; function; public/exported. No docstring.
- **`iter_config_modules(config_dir: Path) -> Iterable[Path]`** — line 153; function; public/exported. No docstring.
- **`iter_project_python_files(root: Path) -> Iterable[Path]`** — line 164; function; public/exported. No docstring.
- **`is_config_file(path: Path) -> bool`** — line 174; function; public/exported. No docstring.
- **`import_hit_matches_symbol(line: str, symbol: str) -> bool`** — line 182; function; public/exported. No docstring.
- **`collect_import_hits_for_symbol(symbol: str) -> list[UsageHit]`** — line 199; function; public/exported. No docstring.
- **`collect_text_hits_for_symbol(symbol: str) -> list[UsageHit]`** — line 218; function; public/exported. No docstring.
- **`build_import_hit_index(modules: list[ModuleSymbols]) -> dict[str, list[UsageHit]]`** — line 237; function; public/exported. No docstring.
- **`print_grouped_inventory(modules: list[ModuleSymbols]) -> None`** — line 249; function; public/exported. No docstring.
- **`build_init_block(modules: list[ModuleSymbols], import_hit_index: dict[str, list[UsageHit]]) -> str`** — line 277; function; public/exported. No docstring.
- **`print_suggested_init(modules: list[ModuleSymbols], import_hit_index: dict[str, list[UsageHit]]) -> None`** — line 320; function; public/exported. No docstring.
- **`print_usage_report(modules: list[ModuleSymbols]) -> None`** — line 328; function; public/exported. No docstring.
- **`main() -> None`** — line 364; function; public/exported. No docstring.

### `biochar_app/diagnostics/logger_health.py`

- **`_load_parquet(path: Path) -> pd.DataFrame \| None`** — line 20; function; internal. No docstring.
- **`_battery_columns(df: pd.DataFrame) -> list[str]`** — line 34; function; internal. No docstring.
- **`_continuity(ts: pd.Series, max_gap: pd.Timedelta)`** — line 38; function; internal. No docstring.
- **`evaluate_logger_health(start_date: pd.Timestamp=DEFAULT_START_DATE, voltage_threshold: float=DEFAULT_VOLTAGE_THRESHOLD, max_gap: pd.Timedelta=DEFAULT_MAX_GAP) -> pd.DataFrame`** — line 48; function; public/exported. No docstring.
- **`print_health_summary(df: pd.DataFrame)`** — line 110; function; public/exported. No docstring.
- **`run_health_check(write_csv: bool=True)`** — line 130; function; public/exported. No docstring.

### `biochar_app/diagnostics/plot_voltage_window.py`

- **`_list_dat_files(path: Path) -> list[Path]`** — line 46; function; internal. No docstring.
- **`_clean_col_name(col: object) -> str`** — line 54; function; internal. No docstring.
- **`_normalize_columns(df: pd.DataFrame) -> pd.DataFrame`** — line 61; function; internal. No docstring.
- **`_find_toa5_header_offset(path: Path, *, max_scan_lines: int=50) -> int`** — line 67; function; internal. Return the 0-based line index where the TOA5 metadata row occurs. Handles: - quoted "TOA5" - UTF-8 BOM - blank lines before TOA5 Raises ValueError if not found quickly.
- **`_read_toa5_dat(path: Path) -> pd.DataFrame`** — line 90; function; internal. Robust TOA5 reader: - locate the TOA5 row (even if quoted / after blank lines) - read the next row as column names - skip 4 header rows total (TOA5 + colnames + units + aggs) - read data with those names
- **`_read_generic_csvish(path: Path) -> pd.DataFrame`** — line 138; function; internal. No docstring.
- **`_read_one_dat(path: Path) -> pd.DataFrame`** — line 145; function; internal. No docstring.
- **`_detect_timestamp_col(columns: Sequence[str]) -> str`** — line 153; function; internal. No docstring.
- **`_resolve_col_name(df: pd.DataFrame, wanted: str) -> Optional[str]`** — line 164; function; internal. No docstring.
- **`_detect_voltage_col(columns: Sequence[str]) -> Optional[str]`** — line 179; function; internal. No docstring.
- **`_detect_voltage_col.score(name: str) -> int`** — line 190; nested function; public/exported. No docstring.
- **`_coerce_timestamp(series: pd.Series) -> pd.Series`** — line 201; function; internal. No docstring.
- **`load_dat_files(path: Path, voltage_col: Optional[str]=None) -> LoadedData`** — line 216; function; public/exported. No docstring.
- **`plot_voltage_window(loaded: LoadedData, event_ts: pd.Timestamp, months: int=2, out_png: Optional[Path]=None) -> None`** — line 268; function; public/exported. No docstring.
- **`main() -> None`** — line 322; function; public/exported. No docstring.

### `biochar_app/diagnostics/remove_duplicate_block.py`

- **`hash_line(line)`** — line 13; function; public/exported. No docstring.
- **`find_duplicate_block(lines)`** — line 17; function; public/exported. Efficiently finds the largest contiguous duplicate block using hashing. Hashes each line, then uses a rolling hash approach to find matching blocks. Returns (start, end, block_len) of the SECOND occurrence.
- **`remove_duplicate_block(input_path, output_path=None)`** — line 59; function; public/exported. No docstring.

### `biochar_app/diagnostics/scan_battv_events.py`

- **`_clean_col_name(col: object) -> str`** — line 73; function; internal. No docstring.
- **`read_toa5_dat(path: Path) -> pd.DataFrame`** — line 80; function; public/exported. No docstring.
- **`detect_voltage_col(cols: Iterable[str]) -> Optional[str]`** — line 107; function; public/exported. No docstring.
- **`coerce_timestamp(s: pd.Series) -> pd.Series`** — line 117; function; public/exported. No docstring.
- **`find_gap_events(ts: pd.Series, *, gap_minutes: int=60) -> list[tuple[pd.Timestamp, pd.Timestamp, float]]`** — line 146; function; public/exported. Return list of (prev_ts, next_ts, gap_minutes) where gap exceeds threshold.
- **`find_brownout_runs(df: pd.DataFrame, ts_col: str, vcol: str, threshold: float) -> list[tuple[pd.Timestamp, pd.Timestamp, float]]`** — line 162; function; public/exported. Contiguous runs where voltage < threshold; returns (start,end,minV).
- **`find_high_voltage_runs(df: pd.DataFrame, ts_col: str, vcol: str, threshold: float) -> list[tuple[pd.Timestamp, pd.Timestamp, float]]`** — line 183; function; public/exported. Contiguous runs where voltage >= threshold; returns (start,end,maxV).
- **`detect_replacement_steps(df: pd.DataFrame, ts_col: str, vcol: str, *, step_v: float=0.6) -> list[tuple[pd.Timestamp, float]]`** — line 204; function; public/exported. Heuristic: compute daily min series, then look for upward steps >= step_v. Returns list of (day, deltaV) where step occurs.
- **`plot_event_window(df: pd.DataFrame, ts_col: str, vcol: str, *, logger_tag: str, kind: str, event_ts: pd.Timestamp, months: int, out_path: Path, brownout_threshold: float \| None=None, hv_warn: float \| None=None, hv_critical: float \| None=None) -> None`** — line 227; function; public/exported. Save a per-event plot: - light raw BattV - daily minimum trend - vertical event line - optional brownout threshold line - optional high-voltage warn/critical lines
- **`infer_logger_tag(path: Path) -> str`** — line 292; function; public/exported. No docstring.
- **`infer_year_from_parent(path: Path) -> int`** — line 297; function; public/exported. No docstring.
- **`iter_dat_files(data_root: Path) -> list[tuple[Path, str]]`** — line 308; function; public/exported. Only scan "authoritative" locations: biochar_app/data-raw/datfiles_2023..datfiles_2026 and only Table1 .dat files.
- **`main() -> None`** — line 343; function; public/exported. No docstring.

### `biochar_app/diagnostics/scan_dat_clock_events.py`

- **`iter_table1_files(years: List[int]) -> Iterable[Tuple[int, Path]]`** — line 99; function; public/exported. Yield (year, datfile) for *non-recursive* datfiles_{year} directories. Only files containing "Table1" and ending in ".dat".
- **`logger_tag_from_filename(datfile: Path) -> str`** — line 117; function; public/exported. Expect names like 'S3M_Table1.dat' -> 'S3M' If it doesn't match, return stem up to first underscore.
- **`_read_toa5_timestamp_column(datfile: Path) -> pd.Series`** — line 128; function; internal. Read TIMESTAMP column in ORIGINAL FILE ORDER (no sorting).
- **`filter_to_year_window(ts: pd.Series, year: int) -> pd.Series`** — line 176; function; public/exported. Keep timestamps within [Jan 1 year, Jan 1 year+1), preserving file order.
- **`_has_consecutive(seq: List[str], target: str) -> bool`** — line 192; function; internal. No docstring.
- **`_fmt(ts: Optional[pd.Timestamp]) -> str`** — line 198; function; internal. No docstring.
- **`detect_events(ts: pd.Series, *, logger_tag: str, year: int, file_rel: str, fwd_min_minutes: float, fwd_max_minutes: float, bwd_min_minutes: float, bwd_max_minutes: float, out_of_order_max_abs_minutes: float, downtime_hours: float) -> Tuple[List[EventRow], SummaryRow, List[EventRow]]`** — line 206; function; public/exported. Returns: events: all detected events summary: per file/year summary ooo: out-of-order subset (small backward steps)
- **`main() -> None`** — line 382; function; public/exported. No docstring.

### `biochar_app/diagnostics/scan_dat_state_tracker.py`

- **`_logger_tags() -> List[str]`** — line 30; function; internal. No docstring.
- **`_dat_path(year: int, logger_tag: str) -> Path`** — line 34; function; internal. No docstring.
- **`_mode_from_offset_minutes(offset_min: int) -> str`** — line 38; function; internal. No docstring.
- **`_find_colname_ci(cols: List[str], name: str) -> Optional[str]`** — line 49; function; internal. No docstring.
- **`filter_to_year_window_ts(ts: pd.Series, year: int) -> pd.Series`** — line 57; function; public/exported. No docstring.
- **`_filter_to_year_window_df(df: pd.DataFrame, year: int, ts_col: str) -> pd.DataFrame`** — line 67; function; internal. No docstring.
- **`_read_toa5_timestamps(datfile: Path) -> pd.Series`** — line 77; function; internal. No docstring.
- **`_read_toa5_time_record(datfile: Path) -> Tuple[pd.DataFrame, str, str]`** — line 94; function; internal. No docstring.
- **`detect_events_for_year(logger_tag: str, year: int, ts_year: pd.Series, *, fwd_min_minutes: float, fwd_max_minutes: float, bwd_min_minutes: float, bwd_max_minutes: float, downtime_minutes: float, start_offset_min: int) -> Tuple[List[DetectedEvent], Dict[str, int]]`** — line 127; function; public/exported. No docstring.
- **`classify_logger_year(*, year: int, forward_setclock: int, backward_setclock: int, has_ff: bool, has_bb: bool) -> Tuple[str, bool, str]`** — line 187; function; public/exported. No docstring.
- **`compute_has_ff_bb(events: List[DetectedEvent]) -> Tuple[bool, bool]`** — line 203; function; public/exported. No docstring.
- **`scan(years: List[int], *, fwd_min_minutes: float, fwd_max_minutes: float, bwd_min_minutes: float, bwd_max_minutes: float, downtime_minutes: float) -> Tuple[pd.DataFrame, pd.DataFrame]`** — line 222; function; public/exported. No docstring.
- **`main() -> int`** — line 402; function; public/exported. No docstring.

### `biochar_app/diagnostics/timestamp_health.py`

- **`_infer_timestamp_column(df: pd.DataFrame) -> Optional[str]`** — line 51; function; internal. Returns the name of a timestamp column if present; else if DatetimeIndex exists, returns "__index__" to indicate index should be used.
- **`_load_with_timestamp(path: Path) -> Optional[pd.DataFrame]`** — line 65; function; internal. No docstring.
- **`_battery_cols(df: pd.DataFrame) -> list[str]`** — line 83; function; internal. No docstring.
- **`_logger_from_batt_col(col: str) -> str`** — line 87; function; internal. Extract logger id like S4_B from a column like BattV_Min_S4_B. Falls back to the full column name if no match.
- **`_continuity_from_timestamps(ts: pd.Series, compute_grid: bool) -> ContinuityStats`** — line 110; function; internal. ts: timestamps (may contain duplicates). Must be datetime-like. compute_grid: if True, compute expected count on a perfect 15-min grid between start and end (inclusive), and compare to actual.
- **`main() -> int`** — line 193; function; public/exported. No docstring.
- **`main._to_timedelta(x)`** — line 302; nested function; internal. No docstring.

### `biochar_app/diagnostics/weekly_health.py`

- **`_ensure_dir(p: Path) -> None`** — line 45; function; internal. No docstring.
- **`_read_raw_logger_parquet(year: int) -> pd.DataFrame`** — line 49; function; internal. Expected path (per your etl.py): PARQUET_DIR/<year>/<year>_raw_logger.parquet
- **`_safe_numeric(s: pd.Series) -> pd.Series`** — line 72; function; internal. No docstring.
- **`_pct(x: float) -> str`** — line 76; function; internal. No docstring.
- **`_fmt_float(x: float) -> str`** — line 80; function; internal. No docstring.
- **`_md_table(headers: List[str], rows: List[List[str]]) -> str`** — line 86; function; internal. No docstring.
- **`_infer_step_minutes(ts: pd.Series) -> Optional[float]`** — line 97; function; internal. Infer the median positive timestep (minutes) from a timestamp series.
- **`check_time_health(df: pd.DataFrame, gap_multiplier: float=3.0) -> TimeHealth`** — line 129; function; public/exported. No docstring.
- **`compute_missingness(df: pd.DataFrame, top_n: int=20) -> List[MissingnessRow]`** — line 177; function; public/exported. No docstring.
- **`battery_health(df: pd.DataFrame, vmin_ok: float=11.0, vmax_ok: float=13.0, top_n: int=50) -> Tuple[List[BatterySummary], pd.DataFrame]`** — line 206; function; public/exported. Returns: - per-column battery summaries - a "violations" dataframe with timestamp/column/value for out-of-range rows (capped to top_n per column)
- **`write_report(report_path: Path, year_blocks: List[Tuple[int, TimeHealth, List[MissingnessRow], List[BatterySummary]]], batt_vmin_ok: float, batt_vmax_ok: float) -> None`** — line 280; function; public/exported. No docstring.
- **`main() -> int`** — line 347; function; public/exported. No docstring.

### `biochar_app/etf.py`

- **`campbell_seconds_to_datetime(seconds: int) -> dt.datetime`** — line 11; function; public/exported. Convert Campbell 'seconds since 1990-01-01' to a naive datetime.
- **`decode_row(raw_bytes: bytes) -> Dict[str, Any]`** — line 22; function; public/exported. Decode one ETF record from Table1.

### `biochar_app/geospatial/analyze_fruita_dem_profiles.py`

- **`load_layers() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]`** — line 59; function; public/exported. No docstring.
- **`get_row_y(control_points: gpd.GeoDataFrame, feature_ids: list[str]) -> float`** — line 75; function; public/exported. No docstring.
- **`build_horizontal_transect(boundary_geom, y: float) -> LineString`** — line 85; function; public/exported. No docstring.
- **`sample_line(line: LineString, raster_path: Path, sample_spacing_m: float) -> list[dict]`** — line 110; function; public/exported. No docstring.
- **`main() -> None`** — line 143; function; public/exported. No docstring.

### `biochar_app/geospatial/build_fruita_field_layout.py`

- **`load_control_points(path: Path) -> gpd.GeoDataFrame`** — line 55; function; public/exported. No docstring.
- **`get_named_points(gdf: gpd.GeoDataFrame) -> dict[str, object]`** — line 73; function; public/exported. No docstring.
- **`build_field_boundary(points: dict[str, object]) -> Polygon`** — line 90; function; public/exported. No docstring.
- **`build_field_edges(points: dict[str, object]) -> list[dict[str, object]]`** — line 102; function; public/exported. No docstring.
- **`interpolate_point(a: Point, b: Point, fraction: float) -> Point`** — line 126; function; public/exported. Return point a + fraction * (b - a).
- **`build_strip_polygons(points: dict[str, Point]) -> list[dict[str, object]]`** — line 134; function; public/exported. Build S1-S4 polygons by dividing the north and south field edges into four equal-width strips.
- **`build_strip_centerlines(points: dict[str, Point]) -> list[dict[str, object]]`** — line 176; function; public/exported. Build strip centerlines from midpoint of each strip on the north edge to midpoint of each strip on the south edge.
- **`write_layers(control_points: gpd.GeoDataFrame) -> None`** — line 206; function; public/exported. No docstring.
- **`main() -> None`** — line 279; function; public/exported. No docstring.

### `biochar_app/geospatial/lidar/analyze_field_topography.py`

- **`build_logger_rows() -> dict[str, list[str]]`** — line 40; function; public/exported. Return datalogger IDs grouped by logger row. Returns ------- { "top":    ["S1T", "S2T", "S3T", "S4T"], "middle": ["S1M", "S2M", "S3M", "S4M"], "bottom": ["S1B", "S2B", "S3B", "S4B"], }
- **`load_layers() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]`** — line 61; function; public/exported. No docstring.
- **`get_row_y(control_points: gpd.GeoDataFrame, feature_ids: list[str]) -> float`** — line 67; function; public/exported. No docstring.
- **`build_horizontal_transect(boundary_geom, y: float) -> LineString`** — line 75; function; public/exported. No docstring.
- **`sample_line(line: LineString, raster_path: Path, spacing_m: float) -> list[dict]`** — line 88; function; public/exported. No docstring.
- **`build_profiles() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]`** — line 114; function; public/exported. No docstring.
- **`write_outputs(profiles_gdf: gpd.GeoDataFrame, transects_gdf: gpd.GeoDataFrame, summary_df: pd.DataFrame) -> None`** — line 176; function; public/exported. No docstring.
- **`get_logger_distance_positions(transects_gdf: gpd.GeoDataFrame) -> dict[str, float]`** — line 194; function; public/exported. No docstring.
- **`add_logger_position_lines(ax, transects_gdf: gpd.GeoDataFrame) -> None`** — line 223; function; public/exported. No docstring.
- **`make_relative_profile_plot(profiles_gdf: gpd.GeoDataFrame, transects_gdf: gpd.GeoDataFrame) -> None`** — line 249; function; public/exported. No docstring.
- **`make_absolute_profile_plot(profiles_gdf: gpd.GeoDataFrame, transects_gdf: gpd.GeoDataFrame) -> None`** — line 301; function; public/exported. No docstring.
- **`write_report(summary_df: pd.DataFrame) -> None`** — line 339; function; public/exported. No docstring.
- **`main() -> None`** — line 403; function; public/exported. No docstring.

### `biochar_app/markdown/tools/convert_word_to_html.py`

- **`_safe_directory_name(stem: str) -> str`** — line 116; function; internal. No docstring.
- **`_ensure_head(soup: BeautifulSoup) -> Tag`** — line 121; function; internal. No docstring.
- **`_inject_css(soup: BeautifulSoup) -> None`** — line 130; function; internal. No docstring.
- **`inject_tab_links(soup: BeautifulSoup) -> None`** — line 136; function; public/exported. Replace known application-tab labels in paragraph and list text.
- **`_clean_caption_remainder(text: str, label: str) -> str`** — line 158; function; internal. No docstring.
- **`_format_numbered_caption(label: str, number: int, text: str) -> str`** — line 172; function; internal. No docstring.
- **`_number_caption_tag(caption: Tag, label: str, number: int) -> None`** — line 177; function; internal. Number a caption without discarding hyperlinks or inline formatting.
- **`_promote_table_cell_figures(soup: BeautifulSoup) -> None`** — line 197; function; internal. Turn Word table-cell image/caption pairs into semantic figures.
- **`_normalize_captions(soup: BeautifulSoup) -> None`** — line 237; function; internal. No docstring.
- **`_caption_for_image(image_tag: Tag) -> str`** — line 253; function; internal. No docstring.
- **`_convert_to_webp(source: Path, destination: Path) -> None`** — line 265; function; internal. No docstring.
- **`_rewrite_extracted_images(soup: BeautifulSoup, *, document_stem: str) -> list[Path]`** — line 281; function; internal. Convert Pandoc-extracted images and rewrite their browser URLs.
- **`_add_image_accessibility_text(soup: BeautifulSoup) -> None`** — line 318; function; internal. No docstring.
- **`_document_specs(selected_sources: Iterable[str]) -> list[DocumentSpec]`** — line 330; function; internal. Build conversion specs from explicit names or every DOCX in the folder.
- **`convert_document(spec: DocumentSpec) -> tuple[Path, list[Path]]`** — line 367; function; public/exported. No docstring.
- **`parse_args() -> argparse.Namespace`** — line 407; function; public/exported. No docstring.
- **`main() -> int`** — line 422; function; public/exported. No docstring.

### `biochar_app/markdown/tools/markdown_config.py`

- **`output_name_for_spec(spec: DocumentSpec) -> str`** — line 42; function; public/exported. Return the configured output name or derive it from the DOCX stem.
- **`iter_document_specs() -> Iterable[DocumentSpec]`** — line 50; function; public/exported. Yield each configured source document once, in application order.
- **`build_markdown_mapping() -> dict[str, str]`** — line 62; function; public/exported. Map application container IDs to generated content URLs.

### `biochar_app/pakbus/core/client.py`

- **`ping6(host: str) -> bool`** — line 54; function; public/exported. ICMPv6 probe (macOS: ping6 or ping -6). Returns True on any reply. Advisory only; many networks rate-limit ICMPv6.
- **`quick_port_check_ipv6(host: str, port: int, timeout: float=3.0) -> tuple[bool, str]`** — line 76; function; public/exported. Small TCP handshake to verify the PakBus/TCP port is listening.
- **`_compute_window(hours: int, tz_name: str) -> tuple[datetime, datetime]`** — line 95; function; internal. No docstring.
- **`_fetch_window(dev: CR1000, table: str, start: datetime, stop: datetime) -> Iterator[pd.DataFrame]`** — line 104; function; internal. No docstring.
- **`fetch_batch(table: str, hours: int, tz_name: str) -> Iterator[tuple[int, pd.DataFrame]]`** — line 111; function; public/exported. Keep one IPv6/TCP socket to the CR800 open and walk all logger IDs. Yields (logger_id, DataFrame) pages.
- **`main() -> None`** — line 170; function; public/exported. No docstring.

### `biochar_app/pakbus/core/cr200_client.py`

- **`_iso_utc(ts: float \| None) -> str \| None`** — line 40; function; internal. No docstring.
- **`normalize_host(host: str \| None) -> str \| None`** — line 45; function; public/exported. Accept hostnames, literal IPv6 (with/without brackets), or full URLs, return bare host.
- **`_decode_name(x: Any) -> str`** — line 57; function; internal. No docstring.
- **`_resolve_table_entry(tdefs: List[Dict[str, Any]], requested: str) -> Tuple[Dict[str, Any], str, int]`** — line 65; function; internal. Find a table entry whose decoded name matches requested (case-sensitive), return (entry, wire_name, signature).
- **`_flatten_records(recs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]`** — line 84; function; internal. Accept a list of CollectData responses (dicts with RecFrag) and normalize to rows.
- **`normalize_to_parquet_schema(df: pd.DataFrame, drop_record: bool=False) -> pd.DataFrame`** — line 128; function; public/exported. No docstring.
- **`do_collect(*, sock, leaf: int, src: int, router: Optional[int], tdefs: List[Dict[str, Any]], table_name_req: str, num: int, collect_mode: str, tdef_sig_override: Optional[int], timeout: float, max_age_hours: Optional[float]=None, drop_record: bool=False) -> pd.DataFrame`** — line 162; function; public/exported. Perform CollectData with robust signature+mode fallback. Returns a normalized DataFrame (optionally filtered by max_age_hours).
- **`do_collect._collect_once(mode_code: int, p1, p2=0, sig_val: int=0, note: str='') -> List[Dict[str, Any]]`** — line 192; nested function; internal. No docstring.
- **`do_collect._try_all(sig_val: int) -> List[Dict[str, Any]]`** — line 223; nested function; internal. No docstring.
- **`main()`** — line 271; function; public/exported. No docstring.

### `biochar_app/pakbus/core/cr200_client_utils.py`

- **`new_tran_nbr() -> int`** — line 95; function; public/exported. No docstring.
- **`calc_sig_for(buff: BytesLike, seed: int=43690) -> int`** — line 107; function; public/exported. No docstring.
- **`calc_sig_nullifier(sig: int) -> bytes`** — line 118; function; public/exported. No docstring.
- **`quote(pkt: BytesLike) -> bytes`** — line 128; function; public/exported. No docstring.
- **`unquote(pkt: BytesLike) -> bytes`** — line 132; function; public/exported. No docstring.
- **`send(sock: socket.socket, pkt: BytesLike) -> None`** — line 136; function; public/exported. No docstring.
- **`_recv_byte(sock: socket.socket) -> int`** — line 141; function; internal. Receive exactly one byte and return it as an int (0..255). Raises TimeoutError on socket timeout; raises RuntimeError on EOF.
- **`recv(sock: socket.socket) -> Optional[bytes]`** — line 154; function; public/exported. Receive one framed PakBus packet (bytes) or None if signature check fails. Uses _recv_byte() which returns ints, making FRAME comparisons correct.
- **`pakbus_hdr(DstNodeId: int, SrcNodeId: int, HiProtoCode: int, ExpMoreCode: int=0, LinkState: int=0, Priority: int=0, HopCnt: int=0, DstPhyAddr: Optional[int]=None, SrcPhyAddr: Optional[int]=None) -> bytes`** — line 179; function; public/exported. No docstring.
- **`decode_bin(Types: Sequence[str], buff: BytesLike, length: int=1) -> Tuple[List[Any], int]`** — line 205; function; public/exported. Decode a sequence of BMP5 field types from a bytes-like buffer.
- **`decode_bin._require(n: int) -> None`** — line 214; nested function; internal. No docstring.
- **`encode_bin(Types: Sequence[str], Values: Sequence[Any]) -> bytes`** — line 268; function; public/exported. No docstring.
- **`decode_pkt(pkt: BytesLike) -> Tuple[Header, Message]`** — line 299; function; public/exported. No docstring.
- **`pkt_hello_cmd(DstNodeId: int, SrcNodeId: int, IsRouter: int=0, HopMetric: int=2, VerifyIntv: int=1800) -> Tuple[bytes, int]`** — line 325; function; public/exported. No docstring.
- **`msg_hello(msg: Message) -> Message`** — line 337; function; public/exported. No docstring.
- **`wait_pkt(s: socket.socket, DstNodeId: int, SrcNodeId: int, TranNbr: int, timeout: float=5.0) -> Tuple[Header, Message]`** — line 342; function; public/exported. Wait for a PakBus packet matching the expected Src/Dst/TranNbr. Returns (hdr, msg) upon match; if the deadline expires, returns the last decoded (hdr, msg) seen (which may be empty dicts) so callers can decide how to proceed.
- **`ping_node(s: socket.socket, *, DstNodeId: int, SrcNodeId: int, RouterPhyAddr: Optional[int]=None, timeout: float=5.0) -> Message`** — line 410; function; public/exported. No docstring.
- **`pkt_collectdata_cmd(DstNodeId: int, SrcNodeId: int, TableNbr: int, TableDefSig: int, FieldNbr: Sequence[int]=(), CollectMode: int=4, P1: int \| Tuple[int, int]=1, P2: int \| Tuple[int, int]=0, SecurityCode: int=0) -> Tuple[bytes, int]`** — line 439; function; public/exported. No docstring.
- **`msg_collectdata_response(msg: Message) -> Message`** — line 482; function; public/exported. No docstring.
- **`parse_tabledef(raw: BytesLike) -> List[Dict[str, Any]]`** — line 495; function; public/exported. Parse Table Definitions blob from 0x17 response into a structured list.
- **`parse_collectdata(raw: BytesLike, tabledef: List[Dict[str, Any]], FieldNbr: Sequence[int]=()) -> Tuple[List[Dict[str, Any]], bool]`** — line 561; function; public/exported. Parse the payload from a 0x89 response using the given table definition.
- **`nsec_to_time(nsec: Tuple[int, int], epoch: int=nsec_base, tick: float=nsec_tick) -> float`** — line 669; function; public/exported. No docstring.
- **`time_to_nsec(timestamp: float, epoch: int=nsec_base, tick: float=nsec_tick) -> Tuple[int, int]`** — line 672; function; public/exported. No docstring.
- **`open_socket(host: str, *, Port: int=6785, Timeout: float=20.0) -> Optional[socket.socket]`** — line 678; function; public/exported. Open a TCP socket to the CR800 (router) host:Port. Tries all address families returned by getaddrinfo and returns the first connected socket with Timeout applied. Returns None if all attempts fail.
- **`get_tabledefs_bmp5(s: socket.socket, *, DstNodeId: int, SrcNodeId: int, RouterPhyAddr: Optional[int]=None, timeout: float=10.0) -> bytes`** — line 709; function; public/exported. No docstring.
- **`get_tabledefs_bmp5._try_once(hdr_opt: Dict[str, Any], flag_byte: int) -> Optional[bytes]`** — line 723; nested function; internal. No docstring.
- **`ensure_tabledefs(s: socket.socket, *, DstNodeId: int, SrcNodeId: int, RouterPhyAddr: Optional[int]=None, timeout: float=10.0) -> List[Dict[str, Any]]`** — line 763; function; public/exported. No docstring.
- **`_get_table_number(tabledef: List[Dict[str, Any]], table_name: str) -> Optional[int]`** — line 774; function; internal. No docstring.
- **`collect_data(s: socket.socket, *, DstNodeId: int, SrcNodeId: int, TableDef: List[Dict[str, Any]], TableName: str, FieldNames: Sequence[str]=(), CollectMode: int=4, P1: Any=1, P2: Any=0, SecurityCode: int=0, RouterPhyAddr: Optional[int]=None, timeout: float=20.0, TableDefSigOverride: Optional[int]=None) -> Tuple[List[Dict[str, Any]], bool]`** — line 783; function; public/exported. Send CollectData and parse response into record fragments using TableDef.
- **`collect_by_time(s: socket.socket, *, DstNodeId: int, SrcNodeId: int, TableDef: List[Dict[str, Any]], TableName: str, BeginUnixUTC: float, EndUnixUTC: float, FieldNames: Sequence[str]=(), RouterPhyAddr: Optional[int]=None, timeout: float=20.0) -> Tuple[List[Dict[str, Any]], bool]`** — line 916; function; public/exported. Convenience: CollectData by time range (inclusive start, exclusive end).
- **`collect_most_recent(s: socket.socket, *, DstNodeId: int, SrcNodeId: int, TableDef: List[Dict[str, Any]], TableName: str, Count: int=1, FieldNames: Sequence[str]=(), RouterPhyAddr: Optional[int]=None, timeout: float=20.0) -> Tuple[List[Dict[str, Any]], bool]`** — line 943; function; public/exported. Convenience: most recent Count records from TableName.
- **`flatten_records(rec_frags: List[Dict[str, Any]]) -> List[Dict[str, Any]]`** — line 967; function; public/exported. Flatten parsed fragments into a simple list of row dicts: { "TableName": ..., "RecNbr": ..., "TimeOfRec": (sec,nsec), **fields }
- **`msg_fileupload_response(msg: Message) -> Message`** — line 991; function; public/exported. No docstring.
- **`fileupload(s: socket.socket, *, DstNodeId: int, SrcNodeId: int, FileName: str, SecurityCode: int=0, RouterPhyAddr: Optional[int]=None, timeout: float=12.0) -> Tuple[bytes, int]`** — line 1003; function; public/exported. No docstring.
- **`fileupload._hdr_variants()`** — line 1013; nested function; internal. No docstring.
- **`fileupload._try_one(req_name: str, hdr_opt: Dict[str, Any]) -> Tuple[bytes, int]`** — line 1033; nested function; internal. No docstring.

### `biochar_app/pakbus/core/link.py`

- **`link_from_url(url: str)`** — line 33; function; public/exported. Parse a URL into the appropriate pylink.Link class: - tcp://host:port    → TCPLink(host, port) - udp://host:port    → UDPLink(host, port) - serial:///dev/...  → SerialLink(device)
- **`IPv6TCPLink.__init__(self, host: str, port: int, timeout: float \| None=None, tcp_keepalive: bool=True)`** — line 70; method; internal. No docstring.
- **`IPv6TCPLink.open(self)`** — line 90; method; public/exported. No docstring.
- **`IPv6TCPLink.close(self)`** — line 110; method; public/exported. No docstring.
- **`_link_override(url: str)`** — line 128; function; internal. No docstring.
- **`install_url_override() -> None`** — line 137; function; public/exported. Override pylink and pycampbellcr1000 URL factory to support IPv6. Call this once at startup.
- **`pakbus_url(host: str, port: int) -> str`** — line 149; function; public/exported. Construct a pakbus:// URL for IPv6 hosts.
- **`open_pakbus_link(host: str, port: int, connect_timeout: float=10.0, tcp_keepalive: bool=True) -> Iterator[IPv6TCPLink]`** — line 155; function; public/exported. No docstring.

### `biochar_app/pakbus/examples/bintools.py`

- **`ByteToHex(byteStr)`** — line 26; function; public/exported. No docstring.
- **`ByteToInt(byteStr)`** — line 33; function; public/exported. No docstring.
- **`str2int(S)`** — line 43; function; public/exported. No docstring.

### `biochar_app/pakbus/examples/show_tabledef.py`

- **`hexdump(b: bytes, n: int=96) -> str`** — line 24; function; public/exported. No docstring.
- **`try_fileupload_defs(s, *, leaf: int, src: int, router: int, timeout: float) -> Tuple[bytes, int]`** — line 29; function; public/exported. Try a few common file names that sometimes hold table defs.
- **`print_defs(tabledef: List[Dict[str, Any]]) -> None`** — line 58; function; public/exported. No docstring.
- **`main() -> int`** — line 80; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/bd_getdata_focus_downloader.py`

- **`load_seeds(path)`** — line 4; function; public/exported. No docstring.
- **`crc_ibm(data: bytes) -> bytes`** — line 17; function; public/exported. No docstring.
- **`crc_ccitt(data: bytes) -> bytes`** — line 29; function; public/exported. No docstring.
- **`wrap(seed: bytes, mode: str) -> bytes`** — line 42; function; public/exported. No docstring.
- **`try_send(addr, port, frame, connect_timeout=5, idle_timeout=2, fresh=True)`** — line 52; function; public/exported. No docstring.
- **`main()`** — line 72; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/bd_getdata_probe.py`

- **`crc16_ibm(data: bytes) -> int`** — line 36; function; public/exported. CRC-16/IBM (x16 + x15 + x2 + 1), init 0xFFFF, ref in/out.
- **`crc16_ccitt(data: bytes) -> int`** — line 48; function; public/exported. CRC-16/CCITT-FALSE (poly 0x1021), init 0xFFFF, no ref, no xorout.
- **`split_frames(raw: bytes) -> List[bytes]`** — line 61; function; public/exported. Split raw stream on 0xBD boundaries, return payload-only slices (bytes between flags). Empty slices are filtered.
- **`hexify(b: bytes) -> str`** — line 69; function; public/exported. No docstring.
- **`wrap_with_flag(payload: bytes) -> bytes`** — line 72; function; public/exported. No docstring.
- **`send_hello(sock: socket.socket, repeats: int, gap_ms: int) -> None`** — line 81; function; public/exported. No docstring.
- **`strip_flags_and_crc(seed: bytes) -> bytes`** — line 89; function; public/exported. Remove leading/trailing 0xBD if present; strip last two bytes as CRC if len>=3.
- **`guess_and_patch_addresses(core: bytes, src_id: int, dst_id: int) -> bytes`** — line 100; function; public/exported. Very conservative patch: if we find a pair of consecutive bytes that look like 'src,dst' near the start of the frame (within first ~8 bytes), replace them. If we don't find anything plausible, just return the core unchanged.
- **`append_crc_and_flag(core: bytes, flavor: str) -> bytes`** — line 118; function; public/exported. No docstring.
- **`recv_with_idle_timeout(sock: socket.socket, idle_timeout: float) -> bytes`** — line 129; function; public/exported. Receive until the socket is idle for idle_timeout seconds. Returns the concatenated bytes (possibly zero length).
- **`main()`** — line 155; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/bd_minimal_getdata_v8.py`

- **`build_getdata_frame(our_id, dest_id, table, count, start_key)`** — line 6; function; public/exported. No docstring.
- **`parse_pkt(pkt: bytes)`** — line 14; function; public/exported. Return (label, detail) for a received logger packet.
- **`recv_with_gaps(sock, n_reads, gap_ms, rx_limit, suppress_neighbor=False, prefix='')`** — line 32; function; public/exported. Read up to n_reads packets, spaced by gap_ms, classify & print; return counts and if reply-89 was seen.
- **`main()`** — line 62; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/bd_read_table1_downloader.py`

- **`crc16_ibm(data: bytes) -> int`** — line 30; function; public/exported. No docstring.
- **`crc16_ccitt_false(data: bytes) -> int`** — line 41; function; public/exported. No docstring.
- **`split_bd_frames(buf: bytes) -> List[bytes]`** — line 52; function; public/exported. No docstring.
- **`is_data_frame(b: bytes) -> bool`** — line 62; function; public/exported. No docstring.
- **`recv_all(sock: socket.socket, idle_timeout=0.6, max_wait=1.5) -> bytes`** — line 65; function; public/exported. No docstring.
- **`to_epoch1990_from_local(ts_str: str, tz_name: Optional[str]) -> int`** — line 83; function; public/exported. No docstring.
- **`iter_fd09_slots(core_wo_crc: bytes) -> List[Tuple[int, int]]`** — line 96; function; public/exported. Find all positions matching: FD 09 ?? 00 00  [then at least 6 bytes available] Return list of (pos_count, pos_startkey).
- **`make_patched_variants(seed_hex: str, count: int, start_key_le32: int) -> List[bytes]`** — line 115; function; public/exported. From a seed hex frame, create patched frames (for each FD09 slot) with placeholder CRC. Returns list of BD-framed bytes with 0 CRC (we'll fill later).
- **`apply_crc(frame_with_placeholder: bytes, flavor: str) -> bytes`** — line 138; function; public/exported. No docstring.
- **`decode_table1_blocks_from_reply(buf: bytes) -> List[Tuple[str, list]]`** — line 146; function; public/exported. Scan for (epoch1990 UInt32 BE + 10 * float32 BE) blocks. Return [(ISO8601Z, values[10]), ...]
- **`try_send(addr, port, frame_bytes: bytes, hello_gap_ms: int, post_wait_ms: int, connect_timeout: float, idle_timeout: float, af_family: int, retries: int, retry_sleep: float)`** — line 171; function; public/exported. Send one frame with retries. Returns (raw_rx, frames, acks, datas). On connect/IO errors, returns (None, [], [], []).
- **`main()`** — line 218; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/bd_replay_raw_v2.py`

- **`u8(x: int) -> bytes`** — line 32; function; public/exported. No docstring.
- **`parse_hex_string(s: str) -> bytes`** — line 37; function; public/exported. No docstring.
- **`parse_hex_or_int(s: str) -> int`** — line 43; function; public/exported. No docstring.
- **`parse_key_ffffxxxx(s: str) -> int`** — line 47; function; public/exported. No docstring.
- **`build_getdata_payload_leaf(table_code: int, count: int, leaf_id: int, key_lo16: int) -> bytes`** — line 55; function; public/exported. No docstring.
- **`build_getdata_payload_router(table_code: int, table_index: int, count: int, key_lo16: int) -> bytes`** — line 60; function; public/exported. No docstring.
- **`build_outer_frame(host_id: int, router_id: int, payload: bytes) -> bytes`** — line 65; function; public/exported. No docstring.
- **`classify(pkt: bytes) -> str`** — line 71; function; public/exported. No docstring.
- **`main()`** — line 84; function; public/exported. No docstring.
- **`main.do_read_loop(label: str)`** — line 187; nested function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/compare_table1_magnitudes.py`

- **`clean_numeric_series(s: pd.Series) -> pd.Series`** — line 34; function; public/exported. Strip quotes/whitespace, map blanks to NaN, then coerce to numeric. Uses element-wise try/float to avoid deprecated to_numeric(...) options.
- **`clean_numeric_series._to_num(x)`** — line 41; nested function; internal. No docstring.
- **`read_toa5_dat(path: str) -> pd.DataFrame`** — line 48; function; public/exported. Read a Campbell TOA5 .dat and return a dataframe with TIMESTAMP (datetime) and numeric fields. Handles the standard 4-line header: row0: "TOA5", row1: column names, row2: units, row3: types
- **`read_bd_csv(path: str) -> pd.DataFrame`** — line 104; function; public/exported. Read BD-decoded table1_master.csv, coerce numerics, parse TIMESTAMP (UTC) if present.
- **`choose_fields(df: pd.DataFrame) -> List[str]`** — line 128; function; public/exported. Pick fields to compare (prioritize EXPECTED_FIELDS, else first 10 numeric).
- **`basic_stats(df: pd.DataFrame, fields: List[str], prefix: str) -> pd.DataFrame`** — line 136; function; public/exported. No docstring.
- **`format_df_numeric(df: pd.DataFrame, precision: int=6) -> pd.DataFrame`** — line 151; function; public/exported. Round numeric columns for pretty CSVs (no forced quoting).
- **`_normalize_df_ts(df: pd.DataFrame, is_dat: bool, tz: Optional[str]) -> pd.DataFrame`** — line 159; function; internal. Make all timestamps tz-aware UTC. - DAT: naive -> localize to `tz` (e.g., America/Denver) with DST rules, then convert to UTC. - BD: already UTC-aware; if naive, localize to UTC.
- **`_to_utc_bounds(since_str: Optional[str], until_str: Optional[str]) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]`** — line 197; function; internal. Parse window bounds; if naive, localize to UTC for safe comparison.
- **`_to_utc_bounds._one(x: Optional[str]) -> Optional[pd.Timestamp]`** — line 199; nested function; internal. No docstring.
- **`resample_and_join(dat: pd.DataFrame, bd: pd.DataFrame, fields_common: List[str], bin_freq: str, dat_tz: Optional[str], precision: int, out_dir: str)`** — line 208; function; public/exported. Resample both sides on bin_freq and inner-join. Produces time_aligned_15min.csv and summaries.
- **`compare_magnitudes(dat: pd.DataFrame, bd: pd.DataFrame, out_dir: str, precision: int, bin_freq: str, dat_tz: Optional[str])`** — line 331; function; public/exported. No docstring.
- **`main()`** — line 386; function; public/exported. No docstring.
- **`main._apply_window_utc(df: pd.DataFrame) -> pd.DataFrame`** — line 410; nested function; internal. No docstring.

### `biochar_app/pakbus/scripts/decode_bd_data_frames.py`

- **`load_hex_bytes(p: pathlib.Path) -> bytes`** — line 11; function; public/exported. No docstring.
- **`find_payload(frame: bytes) -> bytes`** — line 16; function; public/exported. Heuristic payload finder: 1) locate any data prefix inside the frame 2) start payload a few bytes *after* the prefix to skip response header 3) stop before CRC+terminator if present
- **`floats_from_bytes(buf: bytes, endian: str)`** — line 49; function; public/exported. Convert a buffer to float32s with the given endianness ('<' or '>'). Return a contiguous run of “sane” floats (finite, abs < 1e9), preferring the longest run.
- **`floats_from_bytes.sane(x)`** — line 62; nested function; public/exported. No docstring.
- **`decode_frame(frame_bytes: bytes)`** — line 79; function; public/exported. No docstring.
- **`decode_frame.spread(v)`** — line 88; nested function; public/exported. No docstring.
- **`main()`** — line 96; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/decode_hex_candidates.py`

- **`load_hex_file(path: Path) -> bytes`** — line 51; function; public/exported. Load a hex dump from `path` and return the concatenated raw bytes.
- **`get_epoch_base(name: str) -> _dt.datetime`** — line 143; function; public/exported. No docstring.
- **`decode_timestamp(raw: bytes, offset: int, endian: str, ts_type: str, epoch_base: _dt.datetime) -> Optional[Tuple[float, _dt.datetime]]`** — line 153; function; public/exported. Decode a 4-byte timestamp at `raw[offset:offset+4]` according to: - endian: ">" or "<" - ts_type: "uint32" or "float32"
- **`decode_float_run(raw: bytes, offset: int, endian: str, n_floats: int, value_min: float=-1000.0, value_max: float=1000.0) -> Optional[Tuple[float, ...]]`** — line 198; function; public/exported. Decode `n_floats` from raw[offset:offset+4*n_floats] using `endian`. Reject if any are non-finite or outside [value_min, value_max].
- **`find_candidates(raw: bytes, epoch: str, min_year: int, max_year: int, min_floats: int, max_floats: int) -> Iterable[CandidateHit]`** — line 233; function; public/exported. Scan the raw byte stream for candidate ETF-like records.
- **`write_candidates_csv(path: Path, hits: Sequence[CandidateHit]) -> None`** — line 312; function; public/exported. Write candidate hits to CSV.
- **`parse_args() -> argparse.Namespace`** — line 348; function; public/exported. No docstring.
- **`main() -> None`** — line 425; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/decode_table1_from_bd.py`

- **`in_range(name: str, v: float) -> bool`** — line 29; function; public/exported. No docstring.
- **`read_hex_bytes(p: pathlib.Path) -> bytes`** — line 35; function; public/exported. No docstring.
- **`plausible_timestamp(sec: int) -> datetime.datetime \| None`** — line 39; function; public/exported. Return UTC datetime if seconds are in a sane range and aligned to 15-min boundary.
- **`score_values(vals: list[float]) -> tuple[int, float]`** — line 50; function; public/exported. Score a 10-float vector: - count_in_range: how many fields pass plausibility ranges - penalty: sum of absolute distance outside ranges (0 if in range) Higher count_in_range is better; if tied, lower penalty is better.
- **`find_best_table1(b: bytes) -> tuple[int, datetime.datetime, list[float]] \| None`** — line 73; function; public/exported. Scan a bounded window for (epoch BE + 10 float32 BE) and pick the single best hit. We limit scanning to offsets [scan_start .. scan_end) to avoid false positives.
- **`main()`** — line 125; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/fetch_http_last.py`

- **`norm_host(h: str) -> str`** — line 25; function; public/exported. No docstring.
- **`check_root(base_url: str) -> None`** — line 32; function; public/exported. No docstring.
- **`dataquery_csv(base_url: str, table: str, n: int) -> pd.DataFrame \| None`** — line 37; function; public/exported. No docstring.
- **`newestrecord_html(base_url: str, table: str, n: int) -> pd.DataFrame`** — line 54; function; public/exported. No docstring.
- **`normalize_to_target(df: pd.DataFrame) -> pd.DataFrame`** — line 65; function; public/exported. No docstring.
- **`write_outputs(df: pd.DataFrame, site: str, table: str, parquet: str \| None)`** — line 103; function; public/exported. No docstring.
- **`main()`** — line 125; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/fetch_leaf_records.py`

- **`build_arg_parser() -> argparse.ArgumentParser`** — line 58; function; public/exported. No docstring.
- **`normalize_collect_mode(mode: str) -> str`** — line 108; function; public/exported. Normalize collect mode string.
- **`slice_last_n(records: Iterable[Dict[str, Any]], n: int) -> List[Dict[str, Any]]`** — line 117; function; public/exported. Return the last N records from any iterable of mapping-like rows.
- **`write_csv(path: Path, rows: List[Dict[str, Any]]) -> None`** — line 129; function; public/exported. Write rows (List[dict]) to CSV at the given path.
- **`fetch_records(host: str, port: int, router_id: int, src_id: int, leaf_id: int, table: str, mode: str, num: int) -> List[Dict[str, Any]]`** — line 162; function; public/exported. Connect to the CR800 via IPv6 and return a list of dict records.
- **`main(argv: list[str] \| None=None) -> int`** — line 236; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/fetch_leaf_records_bd.py`

- **`_decode_campbell_ts(ts_raw: int) -> _dt.datetime`** — line 85; function; internal. Convert Campbell 'seconds since 1990-01-01' into a UTC datetime.
- **`_looks_reasonable(dt: _dt.datetime, floats: Sequence[float], min_year: int, max_year: int) -> bool`** — line 90; function; internal. Basic sanity checks for a candidate record.
- **`iter_candidate_records(raw: bytes, *, min_year: int, max_year: int) -> Iterable[CandidateRecord]`** — line 127; function; public/exported. Slide a 44-byte window over `raw` and yield records that match our layout.
- **`dedupe_and_sort(candidates: Iterable[CandidateRecord]) -> List[CandidateRecord]`** — line 157; function; public/exported. De-duplicate candidate records based on (ts_raw, rounded floats) and return them sorted by ts_raw (ascending).
- **`connect_and_fetch_bytes(host: str, port: int, timeout: float=10.0) -> bytes`** — line 178; function; public/exported. Open a TCP connection to (host, port), send TX_GETDATA, and read reply bytes.
- **`write_csv(path: Path, records: Sequence[CandidateRecord], *, include_raw: bool) -> None`** — line 219; function; public/exported. Write decoded records to CSV.
- **`parse_args(argv: Sequence[str] \| None=None) -> argparse.Namespace`** — line 281; function; public/exported. No docstring.
- **`main(argv: Sequence[str] \| None=None) -> None`** — line 356; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/fetch_s1b.py`

- **`check_root_page()`** — line 27; function; public/exported. Verify we can reach the CR800 home page.
- **`list_tables()`** — line 35; function; public/exported. Try to discover available tables by polling known command variants. Returns True if any variant returned 200; otherwise False.
- **`fetch_last_records(table: str='Table1', num: int=DEFAULT_NUM_RECORDS) -> pd.DataFrame`** — line 55; function; public/exported. Fetch the last `num` records from `table`. Falls back to NewestRecord if record_count mode fails.
- **`main()`** — line 93; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/fetch_table1_live.py`

- **`bd_hello(inner: bytes) -> bytes`** — line 33; function; public/exported. Wrap a raw HELLO inner payload in BD markers, no CRC (PC400 style): BD <inner> BD
- **`make_read_table1(leaf: int, table_id: int, start_rec: int, count: int) -> bytes`** — line 44; function; public/exported. Build the 11-byte PC400‐style read payload: 2C <leaf> 00 00 <table_id_hi> <table_id_lo> 00 <start_hi> <start_lo> <count> 00
- **`setup_run_dir(base: str) -> str`** — line 62; function; public/exported. No docstring.
- **`setup_logging(run_dir: str, verbose: bool)`** — line 69; function; public/exported. No docstring.
- **`connect_ipv6(addr: str, port: int, timeout: float) -> socket.socket`** — line 87; function; public/exported. No docstring.
- **`send_pkt(sock: socket.socket, pkt: bytes, label: str, run_dir: str, fname: str)`** — line 94; function; public/exported. No docstring.
- **`run(args) -> int`** — line 105; function; public/exported. No docstring.
- **`_build_argparser()`** — line 215; function; internal. No docstring.
- **`main(argv=None)`** — line 252; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/forge_and_fetch_table1.py`

- **`split_bd_frames(buf: bytes)`** — line 36; function; public/exported. No docstring.
- **`is_data_frame(frame: bytes) -> bool`** — line 45; function; public/exported. No docstring.
- **`iter_table1_blocks(b: bytes)`** — line 48; function; public/exported. No docstring.
- **`to_epoch1990(ts_utc: datetime.datetime) -> int`** — line 57; function; public/exported. No docstring.
- **`send_one(addr, port, tx: bytes, hello_gap_ms=120, wait_ms=1200) -> bytes`** — line 61; function; public/exported. No docstring.
- **`main()`** — line 84; function; public/exported. No docstring.
- **`main.parse_utc(x: str) -> datetime.datetime`** — line 114; nested function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/ingest_runner.py`

- **`load_state()`** — line 33; function; public/exported. No docstring.
- **`save_state(state)`** — line 38; function; public/exported. No docstring.
- **`ensure_parquet_dir(path: Path)`** — line 41; function; public/exported. No docstring.
- **`run_fetch(leaf, table, host, port, tsv, dat) -> Path \| None`** — line 44; function; public/exported. No docstring.
- **`append_to_parquet(leaf, table, csv_path: Path, last_seen_ts: str \| None)`** — line 81; function; public/exported. No docstring.
- **`main()`** — line 121; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/pakbus_collect00_latest.py`

- **`frame(payload: bytes) -> bytes`** — line 6; function; public/exported. No docstring.
- **`build_inner(dst=1, src=15, tran=66, table=6, pbytes=b'')`** — line 9; function; public/exported. No docstring.
- **`send_recv(inner: bytes, wait_s=4.0)`** — line 19; function; public/exported. No docstring.
- **`deframe_all(buf: bytes)`** — line 37; function; public/exported. No docstring.
- **`main()`** — line 47; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/pakbus_get_fields.py`

- **`recv_some(sock: socket.socket, timeout: float) -> bytes`** — line 40; function; public/exported. No docstring.
- **`build_field_list_payload(selector: bytes, pairs: List[Tuple[int, int]]) -> bytes`** — line 50; function; public/exported. Build: 2C <selector>  <tbl_le, fld_le>...  00 00 selector is typically b'y ' in your capture.
- **`main()`** — line 67; function; public/exported. No docstring.
- **`main.parse_pair(tok: str) -> Tuple[int, int]`** — line 93; nested function; public/exported. No docstring.
- **`main.parse_pair.to_int(x: str) -> int`** — line 95; nested function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/pcap_pakbus_miner.py`

- **`hexify(b: bytes, maxlen: Optional[int]=None) -> str`** — line 15; function; public/exported. No docstring.
- **`find_bd_frames(payload: bytes) -> List[bytes]`** — line 19; function; public/exported. Split PakBus frames delimited by 0xBD...0xBD and drop very short fragments.
- **`classify_frame(hx: str) -> str`** — line 31; function; public/exported. No docstring.
- **`scapy_iter_frames(pcap_path: str) -> Iterable[Tuple[float, str, bytes]]`** — line 46; function; public/exported. Yields (epoch_ts, "src->dst", payload_bytes) for TCP packets. Requires scapy; if not installed, caller should choose tshark path.
- **`tshark_iter_frames(pcap_path: str) -> Iterable[Tuple[float, str, bytes]]`** — line 74; function; public/exported. Yields (epoch_ts, "src->dst", payload_bytes) for TCP packets via tshark. Requires tshark in PATH.
- **`mine_pcap(pcap_path: str, out_csv: str, prefer_scapy: bool=True, min_frame_len: int=8, only_bd: bool=False) -> None`** — line 122; function; public/exported. No docstring.
- **`main()`** — line 203; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/probe_and_pull.py`

- **`_drain_bd_frames_from_buffer() -> list[bytes]`** — line 13; function; internal. Pull complete BD-framed packets out of _FRAMER_BUF. A BD frame starts with 0xBD and ends with 0xBD. Leaves any trailing partial frame bytes in _FRAMER_BUF.
- **`split_frames(raw: bytes) -> list[bytes]`** — line 47; function; public/exported. Stateless splitter if you already have the entire payload containing complete frames. For streamed sockets, prefer recv_some() below which uses the rolling buffer.
- **`recv_some(sock, idle_timeout: float=5.0)`** — line 68; function; public/exported. Read from socket and return (raw_bytes, frames_list). Never returns None. Uses a rolling buffer so frames can span reads.
- **`hex_to_bytes(s: str) -> bytes`** — line 110; function; public/exported. No docstring.
- **`send_all(sock: socket.socket, data: bytes)`** — line 113; function; public/exported. No docstring.
- **`open_tcp(addr: str, port: int, connect_timeout: float=5.0) -> socket.socket`** — line 121; function; public/exported. No docstring.
- **`write_frame_log_row(fh, ts_iso: str, fr: bytes)`** — line 128; function; public/exported. No docstring.
- **`longest_float_run(b: bytes, step: int=1, endian: Literal['>', '<']='>', min_count: int=6) -> tuple[Optional[int], list[float]]`** — line 131; function; public/exported. Sliding scan for the longest plausible run of 32-bit floats. step: advance per probe (1=every byte, 4=word aligned) endian: '>' big-endian, '<' little-endian min_count: only return if run meets threshold
- **`_preview_batch(frames: list[bytes], batch_idx: int, total_frames: int) -> int`** — line 169; function; internal. Print the standard RX batch preview and return updated total_frames.
- **`_log_and_classify_frames(frames: list[bytes], frame_fh, hex_fh, summary: Optional[dict]=None)`** — line 178; function; internal. Common logging + optional summary classification for probe phase.
- **`_write_raw_hex_once(hex_fh, raw: bytes)`** — line 204; function; internal. No docstring.
- **`run_probe_once(addr: str, port: int, tx_hex: str, *, hello: bool=False, hello_gap_ms: int=100, reads_per_tx: int=4, read_gap_ms: int=400, tx_gap_ms: int=3000, idle_timeout: float=8.0, rx_frame_log: Optional[str]=None, rx_hex_log: Optional[str]=None) -> dict`** — line 213; function; public/exported. Sends hello (optional), then tx_hex once; listens in a small loop with idle gaps. Returns a summary: counts of short acks, neighbor-70, etc.
- **`pull_and_decode(addr: str, port: int, *, tx_hex: str, hello: bool, reads: int, read_gap_ms: int, resend_every: float, idle_timeout: float, reply_sig_hex: str, seek_after_sig: int, scan_step: int, min_floats: int, endian: Literal['>', '<'], include_raw: bool, out_csv: str, rx_frame_log: Optional[str], rx_hex_log: Optional[str]) -> Tuple[int, int]`** — line 285; function; public/exported. Sends hello (optional) + repeated TXs and collects frames. Looks for frames that contain reply_sig_hex, then scans after that offset for plausible float runs. Writes CSV of decoded rows. Returns (#rows_written, max_cols).
- **`main()`** — line 428; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/replay_after_hello.py`

- **`main()`** — line 28; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/replay_bd_frame.py`

- **`parse_hex_stream(s: str) -> bytes`** — line 6; function; public/exported. No docstring.
- **`recv_all_quiet(sock: socket.socket, first_timeout=2.0, grace=1.5) -> bytes`** — line 10; function; public/exported. No docstring.
- **`main()`** — line 33; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/replay_bd_frames_capture_reads.py`

- **`is_data_frame(frame: bytes) -> bool`** — line 14; function; public/exported. No docstring.
- **`hexline_to_bytes(s: str) -> bytes`** — line 18; function; public/exported. No docstring.
- **`recv_all(sock: socket.socket, idle_timeout=0.25, max_wait=2.0) -> bytes`** — line 26; function; public/exported. No docstring.
- **`split_bd_frames(buf: bytes)`** — line 43; function; public/exported. Simple terminator-based splitter (kept for ACKs).
- **`find_embedded_data_frames(buf: bytes)`** — line 55; function; public/exported. More robust extractor: scan for any known data prefix anywhere in buf, then cut up to and including the next 0xBD terminator.
- **`main()`** — line 85; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/replay_bd_frames_no_crc.py`

- **`hexline_to_bytes(s: str) -> bytes`** — line 6; function; public/exported. No docstring.
- **`recv_all(sock: socket.socket, idle_timeout=0.25, max_wait=2.0) -> bytes`** — line 14; function; public/exported. No docstring.
- **`main()`** — line 34; function; public/exported. No docstring.
- **`main.log(msg)`** — line 51; nested function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/replay_bd_frames_v1.py`

- **`hexd(b: bytes) -> str`** — line 6; function; public/exported. No docstring.
- **`classify_inner(inner: bytes) -> str`** — line 8; function; public/exported. No docstring.
- **`load_frames(path: str)`** — line 16; function; public/exported. No docstring.
- **`run(addr: str, port: int, frames_file: str, gap_ms: int, timeout: float, out_log: str)`** — line 40; function; public/exported. No docstring.
- **`main()`** — line 104; function; public/exported. No docstring.

### `biochar_app/pakbus/scripts/serial_collect_tail.py`

- **`SerialSock.__init__(self, ser: serial.Serial)`** — line 26; method; internal. No docstring.
- **`SerialSock.sendall(self, b: bytes) -> None`** — line 30; method; public/exported. No docstring.
- **`SerialSock.recv(self, n: int) -> bytes`** — line 33; method; public/exported. No docstring.
- **`SerialSock.settimeout(self, t: float) -> None`** — line 36; method; public/exported. No docstring.
- **`SerialSock.close(self) -> None`** — line 39; method; public/exported. No docstring.
- **`fmt_iso_pairs(sec_nsec: tuple[int, int]) -> tuple[str, str]`** — line 48; function; public/exported. No docstring.
- **`default_serial_port() -> Optional[str]`** — line 54; function; public/exported. Try to guess a USB-serial device on macOS/Linux/Windows.
- **`main()`** — line 67; function; public/exported. No docstring.
- **`main.g(d: dict, k: str, default: str \| float='NaN') -> str`** — line 131; nested function; public/exported. No docstring.

### `biochar_app/scripts/aggregation.py`

- **`build_summary(df: pd.DataFrame, year: int, get_period_specs: PeriodSpecFn, variables: Iterable[str], strips: Iterable[str], depths: Iterable[str], compute_fn: ComputeFn, zero_ratio_for: Iterable[str]=()) -> dict[str, Dict]`** — line 19; function; public/exported. A generic driver that: - asks get_period_specs(year) for { code: (start_ts,end_ts) } - for each period code, slices df on that interval - for each var/strip/depth calls compute_fn → (raw,ratio) - zeroes out ratio if var in zero_ratio_for - returns nested dict[period][var][strip_depth] = {raw_statistics, ratio_statistics}

### `biochar_app/scripts/app.py`

- **`_cached_load_logger_data(year: int, granularity: str) -> pd.DataFrame`** — line 44; function; internal. Thin wrapper around routes_utils.load_logger_year to cache (year, granularity) in memory so all routes benefit.
- **`serve_spa(request: Request) -> HTMLResponse`** — line 82; async function; public/exported. No docstring.

### `biochar_app/scripts/archive_dataset.py`

- **`archive_dataset(path: Path) -> None`** — line 9; function; public/exported. No docstring.

### `biochar_app/scripts/bulk_download_utils.py`

- **`_inject_year_if_missing(df: pd.DataFrame, year: Optional[int]) -> pd.DataFrame`** — line 40; function; internal. No docstring.
- **`load_sheet_as_dataframe(xlsx_path: str \| Path, spec: BulkSheetSpec) -> pd.DataFrame`** — line 48; function; public/exported. No docstring.
- **`load_csv_as_dataframe(csv_path: str \| Path, spec: BulkSheetSpec) -> pd.DataFrame`** — line 55; function; public/exported. No docstring.
- **`load_spec_as_dataframe(xlsx_path: str \| Path, spec: BulkSheetSpec) -> pd.DataFrame`** — line 63; function; public/exported. No docstring.
- **`dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes`** — line 68; function; public/exported. No docstring.
- **`build_zip_for_selection(xlsx_path: str \| Path, selected_keys: list[str], registry: Optional[list[BulkSheetSpec]]=None) -> bytes`** — line 77; function; public/exported. No docstring.
- **`default_bulk_registry() -> list[BulkSheetSpec]`** — line 115; function; public/exported. Add new datasets by appending new BulkSheetSpec entries. Keep sheet_name EXACT (including trailing spaces).
- **`build_manifest(xlsx_path: str \| Path) -> dict[str, Any]`** — line 162; function; public/exported. Compatibility wrapper for routes.py. Returns the full bulk download manifest, including: - entries - years - granularities

### `biochar_app/scripts/bulk_downloads.py`

- **`_safe_int(v: Any) -> Optional[int]`** — line 84; function; internal. No docstring.
- **`_list_years_on_disk() -> list[int]`** — line 90; function; internal. No docstring.
- **`_list_resolutions_on_disk(year: int) -> list[str]`** — line 111; function; internal. No docstring.
- **`_summary_logger_parquet_path(year: int, resolution: str) -> Path`** — line 120; function; internal. No docstring.
- **`_summary_logger_ratios_parquet_path(year: int, resolution: str) -> Path`** — line 123; function; internal. No docstring.
- **`_summary_weather_base_dir(resolution: str) -> Path`** — line 126; function; internal. No docstring.
- **`_summary_weather_parquet_candidates(year: int, resolution: str) -> list[Path]`** — line 135; function; internal. No docstring.
- **`_logger_parquet_path(year: int, resolution: str) -> Path`** — line 143; function; internal. No docstring.
- **`_logger_ratios_parquet_path(year: int, resolution: str) -> Optional[Path]`** — line 149; function; internal. No docstring.
- **`_weather_parquet_path(year: int, resolution: str) -> Optional[Path]`** — line 160; function; internal. No docstring.
- **`_read_parquet_df(path: Path) -> pd.DataFrame`** — line 177; function; internal. No docstring.
- **`_read_workbook_sheet_df(sheet_name: str) -> pd.DataFrame`** — line 185; function; internal. No docstring.
- **`_load_logger_download_df(year: int, resolution: str) -> pd.DataFrame`** — line 212; function; internal. No docstring.
- **`_load_weather_download_df(year: int, resolution: str) -> pd.DataFrame`** — line 225; function; internal. No docstring.
- **`_zip_bytes(files: list[tuple[str, bytes]]) -> bytes`** — line 241; function; internal. No docstring.
- **`_build_biomass_hay_files() -> list[tuple[str, bytes]]`** — line 250; function; internal. Build the combined all-years field-biomass and hay-NIR archive files.
- **`bulk_download_manifest() -> dict[str, Any]`** — line 314; function; public/exported. No docstring.
- **`bulk_download(payload: dict[str, Any])`** — line 428; async function; public/exported. No docstring.

### `biochar_app/scripts/cache.py`

- **`sizeof_df(df: pd.DataFrame) -> int`** — line 10; function; public/exported. Return the approximate in-memory size of a DataFrame in bytes, summing all its columns (deep=True counts object-dtypes accurately).
- **`available_memory_bytes() -> int`** — line 17; function; public/exported. Return how many bytes of RAM are currently available on this machine.
- **`MemoryBoundedCache.__init__(self, max_bytes: int, size_fn: Callable[[pd.DataFrame], int]=sizeof_df) -> None`** — line 24; method; internal. No docstring.
- **`MemoryBoundedCache.get(self, key: Hashable) -> pd.DataFrame \| None`** — line 35; method; public/exported. No docstring.
- **`MemoryBoundedCache.set(self, key: Hashable, value: pd.DataFrame) -> None`** — line 43; method; public/exported. No docstring.

### `biochar_app/scripts/csv_validation.py`

- **`normalize_dates(df: pd.DataFrame, *, source: str='') -> pd.DataFrame`** — line 5; function; public/exported. Enforce canonical date handling for Ward / biomass / soil CSVs.

### `biochar_app/scripts/custom_app.py`

- **`root()`** — line 16; async function; public/exported. No docstring.
- **`announce()`** — line 21; async function; public/exported. No docstring.

### `biochar_app/scripts/data_loading.py`

- **`load_logger_data(year: int, granularity: Optional[str]=None) -> pd.DataFrame`** — line 22; function; public/exported. Canonical loader for logger summary parquet data.
- **`load_logger_data._normalize_timestamp_column(df_in: pd.DataFrame, col: str='timestamp') -> pd.DataFrame`** — line 32; nested function; internal. Force a clean, numpy-backed datetime64[ns] timestamp column.
- **`load_logger_data._code_to_dt(code: str) -> datetime`** — line 76; nested function; internal. No docstring.
- **`_weather_base_dir(granularity: str) -> Path`** — line 117; function; internal. No docstring.
- **`_weather_parquet_candidates(year: int, granularity: str) -> list[Path]`** — line 127; function; internal. No docstring.
- **`load_weather_data(year: int, granularity: Optional[str]=None) -> pd.DataFrame`** — line 139; function; public/exported. Canonical loader for processed weather parquet data.
- **`_parse_irrigation_timestamp(series: pd.Series) -> pd.Series`** — line 169; function; internal. No docstring.
- **`load_irrigation_data(path: Path \| str \| None=None) -> pd.DataFrame`** — line 187; function; public/exported. Canonical loader for cleaned irrigation management data.
- **`load_irrigation_data.expand_group(strip_group: object) -> list[str]`** — line 335; nested function; public/exported. No docstring.
- **`prepare_irrigation_input(df: pd.DataFrame) -> pd.DataFrame`** — line 547; function; public/exported. Prepare a dataframe for irrigation-analysis functions that require a DatetimeIndex with unique timestamps.

### `biochar_app/scripts/date_ranges.py`

- **`parquet_timestamp_range(path: Path) -> Optional[dict[str, str]]`** — line 7; function; public/exported. No docstring.
- **`parquet_gseason_range(path: Path) -> Optional[dict[str, str]]`** — line 28; function; public/exported. No docstring.
- **`build_date_ranges(base_dir: Path, years: list[int], granularities: list[str]) -> dict[int, dict[str, dict[str, str]]]`** — line 51; function; public/exported. Scan parquet files and return:

### `biochar_app/scripts/dev-tools/audit_unit_suffixes.py`

- **`iter_files(root: Path)`** — line 32; function; public/exported. No docstring.
- **`main()`** — line 45; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/build_documentation_catalog.py`

- **`clean_markdown(text: str) -> str`** — line 36; function; public/exported. No docstring.
- **`document_title(path: Path, text: str) -> str`** — line 43; function; public/exported. No docstring.
- **`document_description(text: str) -> str`** — line 51; function; public/exported. No docstring.
- **`classify(path: Path) -> tuple[str, str]`** — line 76; function; public/exported. No docstring.
- **`collect_documents() -> list[DocumentEntry]`** — line 102; function; public/exported. No docstring.
- **`render_catalog(entries: list[DocumentEntry]) -> str`** — line 121; function; public/exported. No docstring.
- **`main() -> int`** — line 156; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/build_function_catalog.py`

- **`summary_line(text: str) -> str`** — line 58; function; public/exported. No docstring.
- **`safe_unparse(node: ast.AST \| None) -> str`** — line 66; function; public/exported. No docstring.
- **`PythonFunctionVisitor.__init__(self, relative_path: str) -> None`** — line 76; method; internal. No docstring.
- **`PythonFunctionVisitor.visit_ClassDef(self, node: ast.ClassDef) -> None`** — line 81; method; public/exported. No docstring.
- **`PythonFunctionVisitor.visit_FunctionDef(self, node: ast.FunctionDef) -> None`** — line 86; method; public/exported. No docstring.
- **`PythonFunctionVisitor.visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None`** — line 89; method; public/exported. No docstring.
- **`PythonFunctionVisitor._record_function(self, node: ast.FunctionDef \| ast.AsyncFunctionDef, *, is_async: bool) -> None`** — line 92; method; internal. No docstring.
- **`iter_source_files(pattern: str) -> Iterable[Path]`** — line 133; function; public/exported. No docstring.
- **`collect_python_functions() -> tuple[list[FunctionEntry], list[str]]`** — line 140; function; public/exported. No docstring.
- **`nearest_jsdoc(lines: list[str], line_index: int) -> str`** — line 174; function; public/exported. No docstring.
- **`collect_javascript_from_text(text: str, relative_path: str, *, line_offset: int=0) -> list[FunctionEntry]`** — line 201; function; public/exported. No docstring.
- **`collect_javascript_functions() -> list[FunctionEntry]`** — line 267; function; public/exported. No docstring.
- **`markdown_safe(text: str) -> str`** — line 288; function; public/exported. No docstring.
- **`render_markdown(entries: list[FunctionEntry], errors: list[str]) -> str`** — line 292; function; public/exported. No docstring.
- **`main() -> int`** — line 340; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/check_battery_continuity.py`

- **`_load_year(path) -> pd.DataFrame \| None`** — line 21; function; internal. No docstring.
- **`_battery_cols(df: pd.DataFrame) -> list[str]`** — line 42; function; internal. No docstring.
- **`_continuity(ts: pd.Series) -> tuple[bool, pd.Timedelta \| None]`** — line 45; function; internal. No docstring.
- **`main() -> int`** — line 60; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/check_deployment_requirements.py`

- **`is_git_tracked(path: Path) -> bool`** — line 45; function; public/exported. Return whether Git tracks ``path`` relative to the repository root.
- **`directory_has_files(path: Path) -> bool`** — line 62; function; public/exported. No docstring.
- **`looks_like_readable_pdf(path: Path) -> bool`** — line 66; function; public/exported. No docstring.
- **`check_requirement(requirement: dict[str, str]) -> CheckResult`** — line 75; function; public/exported. No docstring.
- **`run_checks(*, git_only: bool=False) -> list[CheckResult]`** — line 115; function; public/exported. No docstring.
- **`parse_args() -> argparse.Namespace`** — line 124; function; public/exported. No docstring.
- **`main() -> int`** — line 139; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/check_irrigation_timing_alignment.py`

- **`load_logger_year(year: int) -> pd.DataFrame`** — line 27; function; public/exported. No docstring.
- **`first_positive_jump(series: pd.Series, irrigation_start: pd.Timestamp) -> dict[str, object]`** — line 44; function; public/exported. No docstring.
- **`main() -> None`** — line 96; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/check_lab_reference_anchors.py`

- **`ok(msg: str) -> None`** — line 58; function; public/exported. No docstring.
- **`warn(msg: str) -> None`** — line 61; function; public/exported. No docstring.
- **`fail(msg: str) -> None`** — line 64; function; public/exported. No docstring.
- **`IdCollector.__init__(self) -> None`** — line 70; method; internal. No docstring.
- **`IdCollector.handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None`** — line 74; method; public/exported. No docstring.
- **`normalize_table_number(text: Optional[str]) -> Optional[str]`** — line 96; function; public/exported. Convert 'Table 18' -> 'table-18' Convert 'Table 7'  -> 'table-7'
- **`fetch_html(url: str, timeout: int=15) -> str`** — line 108; function; public/exported. No docstring.
- **`extract_ids(html: str) -> list[str]`** — line 113; function; public/exported. No docstring.
- **`simple_slug(text: str) -> str`** — line 118; function; public/exported. No docstring.
- **`score_candidate(fragment: str, candidate: str, table_hint: Optional[str], title_hint: Optional[str]) -> int`** — line 124; function; public/exported. Rough ranking for suggested ids. Higher is better.
- **`suggest_matches(fragment: str, ids_on_page: Sequence[str], table_number: Optional[str], table_title: Optional[str], limit: int=5) -> list[str]`** — line 155; function; public/exported. No docstring.
- **`build_anchor_records(lab_references: Mapping[str, object], only_page: Optional[str]=None) -> tuple[list[AnchorRecord], list[tuple[str, int, str]]]`** — line 181; function; public/exported. Returns: - records with fragments to validate - skipped refs without fragments: (lab_key, ref_index, source_url)
- **`semantic_checks(rec: AnchorRecord) -> list[SemanticIssue]`** — line 230; function; public/exported. Checks for anchors that exist but may still be poor semantic matches.
- **`parse_args() -> argparse.Namespace`** — line 362; function; public/exported. No docstring.
- **`main() -> int`** — line 386; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/check_lab_variable_metadata.py`

- **`check_csv_coverage(csv_path: Path, dataset_family: str, label: str, ignore_columns: set[str], ignore_compatibility_columns: set[str] \| None=None) -> list[str]`** — line 92; function; public/exported. No docstring.
- **`check_required_fields() -> list[str]`** — line 129; function; public/exported. No docstring.
- **`check_duplicate_aliases() -> list[str]`** — line 145; function; public/exported. No docstring.
- **`check_soil_bio_coverage() -> list[str]`** — line 168; function; public/exported. No docstring.
- **`check_nir_coverage() -> list[str]`** — line 176; function; public/exported. No docstring.
- **`check_soil_chem_coverage() -> list[str]`** — line 184; function; public/exported. No docstring.
- **`main() -> None`** — line 202; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/check_tables_soil_bio.py`

- **`main() -> None`** — line 7; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/check_tables_soil_chem.py`

- **`main() -> None`** — line 7; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/convert_images_to_webp.py`

- **`image_path(value: str \| Path) -> Path`** — line 42; function; public/exported. Resolve relative image paths beneath the static image directory.
- **`crop_white_border(img: Image.Image, padding: int=20) -> Image.Image`** — line 47; function; public/exported. No docstring.
- **`parse_args() -> argparse.Namespace`** — line 66; function; public/exported. No docstring.
- **`main() -> None`** — line 92; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/convert_kml_to_webp.py`

- **`main() -> None`** — line 19; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/convert_tex_to_word.py`

- **`run(cmd: list[str])`** — line 7; function; public/exported. Run a subprocess, printing its output on error.
- **`main()`** — line 15; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/convert_ward_docx_to_html.py`

- **`slugify(text: str) -> str`** — line 288; function; public/exported. No docstring.
- **`to_snake_case_filename(text: str) -> str`** — line 298; function; public/exported. No docstring.
- **`unique_slug(base: str, seen: set[str]) -> str`** — line 305; function; public/exported. No docstring.
- **`normalize_text(text: str) -> str`** — line 319; function; public/exported. No docstring.
- **`ensure_head(soup: BeautifulSoup) -> Tag`** — line 326; function; public/exported. No docstring.
- **`ensure_body(soup: BeautifulSoup) -> Tag`** — line 338; function; public/exported. No docstring.
- **`inject_css(soup: BeautifulSoup) -> None`** — line 350; function; public/exported. No docstring.
- **`wrap_body_content(soup: BeautifulSoup) -> None`** — line 356; function; public/exported. No docstring.
- **`collect_existing_ids(soup: BeautifulSoup) -> set[str]`** — line 366; function; public/exported. No docstring.
- **`add_heading_ids(soup: BeautifulSoup) -> None`** — line 376; function; public/exported. No docstring.
- **`add_explicit_named_anchors(soup: BeautifulSoup, output_name: str) -> None`** — line 394; function; public/exported. No docstring.
- **`_get_block_tags_in_order(soup: BeautifulSoup) -> list[Tag]`** — line 423; function; internal. No docstring.
- **`_find_first_matching_block(blocks: list[Tag], predicate: Callable[[str], bool]) -> tuple[Optional[int], Optional[Tag]]`** — line 430; function; internal. No docstring.
- **`_find_next_matching_block(blocks: list[Tag], start_idx: int, predicate: Callable[[str], bool], max_scan: int=80, used_indexes: Optional[set[int]]=None) -> tuple[Optional[int], Optional[Tag]]`** — line 440; function; internal. No docstring.
- **`_insert_anchor_before(tag: Tag, wanted_id: str, seen_ids: set[str]) -> bool`** — line 457; function; internal. No docstring.
- **`add_biological_report_anchors(soup: BeautifulSoup, output_name: str) -> None`** — line 490; function; public/exported. No docstring.
- **`add_biological_report_anchors.is_scale_rating(text: str) -> bool`** — line 529; nested function; public/exported. No docstring.
- **`add_biological_report_anchors.is_diversity_scale_text(text: str) -> bool`** — line 532; nested function; public/exported. No docstring.
- **`add_table_ids_from_captions(soup: BeautifulSoup) -> None`** — line 574; function; public/exported. No docstring.
- **`replace_word_bookmark_ids_with_table_ids(soup: BeautifulSoup) -> None`** — line 605; function; public/exported. No docstring.
- **`remove_empty_paragraphs(soup: BeautifulSoup) -> None`** — line 668; function; public/exported. No docstring.
- **`_basename_from_src(src: str) -> str`** — line 678; function; internal. No docstring.
- **`rewrite_image_paths(soup: BeautifulSoup, media_url_prefix: str) -> None`** — line 681; function; public/exported. No docstring.
- **`_remove_empty_ancestors(start_tag: Optional[Tag]) -> None`** — line 716; function; internal. No docstring.
- **`_paragraph_text_without_images(tag: Tag) -> str`** — line 738; function; internal. No docstring.
- **`_is_probable_page_footer_or_header(text: str) -> bool`** — line 747; function; internal. No docstring.
- **`remove_images_inside_headings(soup: BeautifulSoup) -> None`** — line 759; function; public/exported. No docstring.
- **`remove_page_header_footer_paragraphs(soup: BeautifulSoup) -> None`** — line 768; function; public/exported. No docstring.
- **`remove_decorative_transition_images(soup: BeautifulSoup, output_name: str) -> None`** — line 777; function; public/exported. No docstring.
- **`normalize_image_classes(soup: BeautifulSoup) -> None`** — line 806; function; public/exported. No docstring.
- **`apply_image_replacements(soup: BeautifulSoup, output_name: str) -> None`** — line 833; function; public/exported. No docstring.
- **`cleanup_html(soup: BeautifulSoup, output_name: str) -> None`** — line 951; function; public/exported. No docstring.
- **`output_name_for_docx(docx_name: str) -> str`** — line 978; function; public/exported. No docstring.
- **`convert_docx_to_html(docx_path: Path, media_dir: Path) -> str`** — line 987; function; public/exported. No docstring.
- **`iter_docx_files(directory: Path) -> Iterable[Path]`** — line 1007; function; public/exported. No docstring.
- **`main() -> int`** — line 1017; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/directory_map.py`

- **`generate_directory_map(start_path, level=0)`** — line 3; function; public/exported. Generate a map of the directory structure starting from the given path.

### `biochar_app/scripts/dev-tools/extract_field_photo_locations.py`

- **`strip_from_description(description: str) -> str`** — line 121; function; public/exported. No docstring.
- **`add_compass_rose(ax) -> None`** — line 133; function; public/exported. No docstring.
- **`write_png(rows: list[dict[str, object]]) -> None`** — line 157; function; public/exported. No docstring.
- **`write_png.find_by_description(pattern: str) -> pd.Series \| None`** — line 200; nested function; public/exported. No docstring.
- **`write_png.find_logger(label: str) -> pd.Series \| None`** — line 210; nested function; public/exported. No docstring.
- **`guess_pakbus_from_filename(path: Path) -> str`** — line 456; function; public/exported. Guess PakBus ID from filenames such as: PB10_IMG_7385.jpeg pb_10.jpg logger_PB02.jpeg
- **`photo_group_from_path(path: Path) -> str`** — line 470; function; public/exported. No docstring.
- **`load_existing_manual_fields() -> dict[str, dict[str, str]]`** — line 478; function; public/exported. No docstring.
- **`find_photos() -> list[Path]`** — line 502; function; public/exported. No docstring.
- **`read_exif(photo: Path) -> dict[str, Any]`** — line 517; function; public/exported. No docstring.
- **`build_rows(photos: list[Path]) -> list[dict[str, object]]`** — line 541; function; public/exported. No docstring.
- **`distance_ft(lat1: float, lon1: float, lat2: float, lon2: float) -> float`** — line 566; function; public/exported. Approximate distance between two lat/lon points in feet. Good enough for this small field-scale map.
- **`add_distance_label(ax, p1: pd.Series, p2: pd.Series, label_offset: tuple[float, float]=(0, 0), fontsize: int=7, draw_line: bool=True, linestyle: str='--') -> None`** — line 583; function; public/exported. No docstring.
- **`add_furrow_reference_lines(ax, nw: pd.Series \| None, ne: pd.Series \| None, sw: pd.Series \| None, se: pd.Series \| None, logger_df: pd.DataFrame) -> None`** — line 619; function; public/exported. No docstring.
- **`add_distance_tick(ax, edge_point: pd.Series, logger_point: pd.Series, tick_fraction: float=0.45) -> None`** — line 742; function; public/exported. No docstring.
- **`add_horizontal_tick_from_logger_to_edge(ax, edge_point: pd.Series, logger_point: pd.Series, tick_fraction: float=0.45) -> None`** — line 760; function; public/exported. No docstring.
- **`add_width_arrow_between_corners(ax, sw: pd.Series, se: pd.Series, y_offset: float=-5.5e-05) -> None`** — line 779; function; public/exported. No docstring.
- **`write_csv(rows: list[dict[str, object]]) -> None`** — line 811; function; public/exported. No docstring.
- **`write_geojson(rows: list[dict[str, object]]) -> None`** — line 833; function; public/exported. No docstring.
- **`main() -> int`** — line 870; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/find_max_vwc.py`

- **`get_vwc_columns(df: pd.DataFrame) -> list[str]`** — line 28; function; public/exported. No docstring.
- **`count_bad_episodes(mask: pd.Series) -> int`** — line 34; function; public/exported. No docstring.
- **`episode_stats(values: pd.Series, threshold: float) -> tuple[int, float]`** — line 41; function; public/exported. Return the longest continuous episode above threshold.
- **`get_episode_details(sub: pd.DataFrame, threshold: float) -> list[dict[str, Any]]`** — line 67; function; public/exported. Return detailed contiguous episodes where value > threshold.
- **`summarize_sensor_qa(all_values: pd.DataFrame) -> None`** — line 120; function; public/exported. No docstring.
- **`summarize_high_values_by_hour(all_values: pd.DataFrame) -> None`** — line 185; function; public/exported. No docstring.
- **`summarize_high_vwc_episodes(all_values: pd.DataFrame) -> None`** — line 206; function; public/exported. No docstring.
- **`main() -> None`** — line 243; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/find_unused_functions.py`

- **`get_py_files(root: Path)`** — line 21; function; public/exported. No docstring.
- **`collect_definitions(py_files)`** — line 24; function; public/exported. No docstring.
- **`collect_references(py_files)`** — line 40; function; public/exported. No docstring.
- **`main()`** — line 58; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/find_unused_scripts.py`

- **`iter_py_files(root: Path) -> Iterator[Path]`** — line 29; function; public/exported. No docstring.
- **`safe_read_text(path: Path) -> Optional[str]`** — line 39; function; public/exported. No docstring.
- **`parse_imports(py_file: Path) -> tuple[set[str], set[str]]`** — line 50; function; public/exported. Returns: (import_modules, from_modules)
- **`file_to_module(project_root: Path, file_path: Path) -> str`** — line 84; function; public/exported. Convert a file path like: <root>/biochar/scripts/convert_word_to_html.py into a module-ish name: biochar.scripts.convert_word_to_html
- **`module_prefixes(mod: str) -> set[str]`** — line 95; function; public/exported. "a.b.c" -> {"a", "a.b", "a.b.c"} Useful because code might import only "a.b" while the file is "a.b.c".
- **`is_likely_manual_script(path: Path) -> bool`** — line 106; function; public/exported. Heuristic: if the script looks like a CLI entrypoint, it may not be imported. We'll still report it, but you can optionally filter these out in output.
- **`main() -> None`** — line 121; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/function_doc_checker.py`

- **`extract_functions_with_doc(file_path, file_type='py')`** — line 4; function; public/exported. No docstring.
- **`scan_directory(base_path)`** — line 38; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/logger_health_report.py`

- **`find_battery_columns(df)`** — line 53; function; public/exported. No docstring.
- **`logger_name_from_column(col)`** — line 60; function; public/exported. No docstring.
- **`load_year(year)`** — line 64; function; public/exported. No docstring.
- **`monthly_summary(df, batt_cols)`** — line 78; function; public/exported. No docstring.
- **`daily_flags(df, batt_cols)`** — line 126; function; public/exported. No docstring.
- **`summary_table(monthly, daily)`** — line 178; function; public/exported. No docstring.
- **`recent_summary_table(daily: pd.DataFrame) -> pd.DataFrame`** — line 236; function; public/exported. Summarize current logger battery condition from daily records.
- **`classify_health(row)`** — line 288; function; public/exported. No docstring.
- **`round_report_columns(df)`** — line 305; function; public/exported. No docstring.
- **`main()`** — line 329; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/smoke_downloads.py`

- **`print_banner(title: str) -> None`** — line 154; function; public/exported. No docstring.
- **`content_disposition_filename(response: requests.Response) -> str`** — line 159; function; public/exported. No docstring.
- **`normalize_trace_option_for_test(trace_option: str) -> str`** — line 166; function; public/exported. No docstring.
- **`assert_filename_rules(filename: str, trace_option: str) -> None`** — line 174; function; public/exported. No docstring.
- **`preview_text(text: str, max_lines: int=35) -> str`** — line 193; function; public/exported. No docstring.
- **`validate_filename_rules(filename: str, trace_option: str) -> None`** — line 200; function; public/exported. No docstring.
- **`validate_zip(response: requests.Response, *, trace_option: str \| None=None) -> None`** — line 216; function; public/exported. No docstring.
- **`validate_csv_response(response: requests.Response) -> None`** — line 251; function; public/exported. No docstring.
- **`test_plot_download(case: dict[str, str]) -> None`** — line 271; function; public/exported. No docstring.
- **`get_summary_stats(variable: str=DEFAULT_VARIABLE) -> dict`** — line 300; function; public/exported. No docstring.
- **`test_summary_download(case: dict[str, str]) -> None`** — line 319; function; public/exported. No docstring.
- **`validate_smoke_metadata() -> None`** — line 352; function; public/exported. No docstring.
- **`main() -> None`** — line 366; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/summarize_diagnostics_metadata.py`

- **`get_module_docstring(path: Path) -> str`** — line 21; function; public/exported. No docstring.
- **`get_imports(path: Path) -> list[str]`** — line 29; function; public/exported. No docstring.
- **`get_top_level_functions(path: Path) -> list[str]`** — line 47; function; public/exported. No docstring.
- **`get_constants(path: Path) -> list[str]`** — line 59; function; public/exported. No docstring.
- **`find_keywords(text: str) -> list[str]`** — line 79; function; public/exported. No docstring.
- **`summarize_file(path: Path) -> str`** — line 108; function; public/exported. No docstring.
- **`main() -> None`** — line 145; function; public/exported. No docstring.

### `biochar_app/scripts/dev-tools/view_parquet.py`

- **`summarize_file(path: Path) -> None`** — line 13; function; public/exported. Print diagnostics for a single parquet file: filename, columns, shape, dtypes, and first rows.
- **`main()`** — line 31; function; public/exported. No docstring.

### `biochar_app/scripts/errors.py`

- **`UserFacingError.__str__(self) -> str`** — line 17; method; internal. No docstring.

### `biochar_app/scripts/etl.py`

- **`update_dataset_metadata(metadata: DatasetMetadata, key: str, values: pd.Series) -> None`** — line 185; function; public/exported. No docstring.
- **`write_dataset_metadata(metadata: DatasetMetadata, years: list[int]) -> None`** — line 214; function; public/exported. Write automatically generated dataset metadata constants.
- **`tz_name(tz_like: Any) -> str`** — line 619; function; public/exported. Return a pandas-friendly timezone name string.
- **`apply_logger_clock_corrections(ts: pd.Series, logger_tag: str) -> pd.Series`** — line 638; function; public/exported. Apply piecewise absolute clock offsets to a naive timestamp series.
- **`build_logger_clock_corrections_audit() -> pd.DataFrame`** — line 674; function; public/exported. Build an audit table documenting logger clock corrections.
- **`write_logger_clock_corrections_audit(output_path: Path) -> None`** — line 750; function; public/exported. Write logger clock correction metadata audit CSV.
- **`apply_logger_seasonal_civil_time(ts: pd.Series, *, fixed_tz: str=LOGGER_FIXED_STANDARD_TZ, local_tz: Any=DEFAULT_TIMEZONE) -> pd.Series`** — line 774; function; public/exported. Convert corrected logger timestamps from a fixed MST base into America/Denver civil time.
- **`ts_to_iso_minute(ts_any: Any) -> str`** — line 802; function; public/exported. No docstring.
- **`ts_to_iso_date(ts_any: Any) -> str`** — line 813; function; public/exported. No docstring.
- **`make_timestamp_or_raise(value: str, *, context: str='') -> pd.Timestamp`** — line 824; function; public/exported. No docstring.
- **`force_datetime64_ns(s: pd.Series) -> pd.Series`** — line 831; function; public/exported. No docstring.
- **`normalize_logger_timestamp_series(ts: Series) -> Series`** — line 836; function; public/exported. Parse raw logger timestamp text to naive datetime.
- **`normalize_weather_timestamp_series(ts: pd.Series, tz: Any=DEFAULT_TIMEZONE) -> pd.Series`** — line 846; function; public/exported. Normalize CoAgMet timestamps: - parse - localize naive to tz, shifting DST gaps forward - convert any tz-aware to tz - drop tz info (timezone-naive)
- **`make_timestamp_column_naive(df_in: pd.DataFrame, col: str='timestamp') -> pd.DataFrame`** — line 864; function; public/exported. If df[col] is timezone-aware, convert to DEFAULT_TIMEZONE and drop tz info.
- **`make_datetimeindex_naive(df_in: pd.DataFrame, copy: bool=True) -> pd.DataFrame`** — line 877; function; public/exported. If df.index is a tz-aware DatetimeIndex, convert to DEFAULT_TIMEZONE and drop tz info.
- **`convert_soil_t_to_fahrenheit(df_in: pd.DataFrame, copy: bool=True) -> pd.DataFrame`** — line 896; function; public/exported. No docstring.
- **`rename_logger_columns(df: pd.DataFrame, logger_name: str) -> pd.DataFrame`** — line 909; function; public/exported. No docstring.
- **`_clean_col_name(s: object) -> str`** — line 930; function; internal. No docstring.
- **`_read_toa5_table1_dat(datfile: Path) -> pd.DataFrame`** — line 933; function; internal. No docstring.
- **`_candidate_logger_files(tag: str, year: int) -> list[Path]`** — line 956; function; internal. Resolve which .dat files should contribute to a (tag,year).
- **`read_logger_data(tag: str, year: int) -> Optional[pd.DataFrame]`** — line 983; function; public/exported. No docstring.
- **`merge_all_loggers(year: int) -> Optional[pd.DataFrame]`** — line 1057; function; public/exported. No docstring.
- **`replace_bad_values(df_in: pd.DataFrame, threshold: float=DEFAULT_BAD_VALUE_THRESHOLD, copy: bool=True) -> pd.DataFrame`** — line 1075; function; public/exported. No docstring.
- **`scale_vwc_to_percent(df_in: pd.DataFrame, *, copy: bool=True) -> pd.DataFrame`** — line 1089; function; public/exported. No docstring.
- **`add_swc_cylinder_volumes(df_in: pd.DataFrame, copy: bool=True) -> pd.DataFrame`** — line 1098; function; public/exported. Retain legacy VWC-scaled reference-cylinder water volumes.
- **`add_cs650_sensing_volume_water(df_in: pd.DataFrame, copy: bool=True) -> pd.DataFrame`** — line 1123; function; public/exported. Estimate local water volume within the documented CS650 sensing volume.
- **`add_temperature_differences(df_in: pd.DataFrame, *, copy: bool=True) -> pd.DataFrame`** — line 1158; function; public/exported. No docstring.
- **`add_swc_differences(df_in: pd.DataFrame, *, copy: bool=True) -> pd.DataFrame`** — line 1187; function; public/exported. No docstring.
- **`unpack_gseason_period(period_code: str, period_meta: Any) -> tuple[str, str, str]`** — line 1228; function; public/exported. No docstring.
- **`write_gseason_summary(year: int, df_daily: pd.DataFrame) -> None`** — line 1244; function; public/exported. No docstring.
- **`write_logger_download_zip(year: int, df_15min: pd.DataFrame) -> None`** — line 1352; function; public/exported. No docstring.
- **`write_weather_download_zip(year: int, df_15min: pd.DataFrame, download_url: str='', builder_url: str='') -> None`** — line 1424; function; public/exported. No docstring.
- **`aggregate_and_write(year: int, df: pd.DataFrame) -> None`** — line 1463; function; public/exported. Aggregate logger data.
- **`clean_weather_frame(dfw: pd.DataFrame) -> pd.DataFrame`** — line 1516; function; public/exported. No docstring.
- **`validate_datfiles_for_year(year: int) -> None`** — line 1542; function; public/exported. No docstring.
- **`_read_backup_state() -> dict[str, Any]`** — line 1569; function; internal. No docstring.
- **`_write_backup_state(state: dict[str, Any]) -> None`** — line 1578; function; internal. No docstring.
- **`maybe_backup_raw_data(force: bool=False) -> Path \| None`** — line 1585; function; public/exported. No docstring.
- **`generate_summaries(years: list[int]) -> None`** — line 1633; function; public/exported. No docstring.
- **`generate_summaries._fmt_ts(x: Any) -> str`** — line 1662; nested function; internal. No docstring.
- **`resolve_target_year(cli_year: Optional[int]=None) -> int`** — line 1820; function; public/exported. Determine the year to process.
- **`safe_series_ratio(num: pd.Series, denom: pd.Series, eps: float=0.001) -> pd.Series`** — line 1834; function; public/exported. Compute num / denom but avoid blow-ups when denom ≈ 0.
- **`calculate_ratios(df_in: pd.DataFrame) -> pd.DataFrame`** — line 1852; function; public/exported. Build a ratio-only dataframe.
- **`update_plot_metadata(weather_frames: dict[int, pd.DataFrame]) -> None`** — line 1895; function; public/exported. Generate plot metadata from processed weather data.
- **`refresh_master_workbook_snapshot() -> dict[str, Any]`** — line 2006; function; public/exported. Validate and install the latest locally synchronized master workbook.
- **`main() -> None`** — line 2044; function; public/exported. No docstring.

### `biochar_app/scripts/field_utils.py`

- **`get_strip_width_ft(strip: str) -> float`** — line 9; function; public/exported. No docstring.
- **`get_strip_length_ft(strip: str) -> float`** — line 12; function; public/exported. No docstring.
- **`get_strip_area_acres(strip: str) -> float`** — line 15; function; public/exported. No docstring.
- **`total_to_lb_ac(total_lb: float, strip: str) -> float`** — line 18; function; public/exported. No docstring.
- **`lb_ac_to_total(lb_ac: float, strip: str) -> float`** — line 21; function; public/exported. No docstring.
- **`gallons_to_inches_applied(gallons: float, strip: str) -> float`** — line 24; function; public/exported. No docstring.
- **`gpm_per_foot(gpm: float, strip: str) -> float`** — line 28; function; public/exported. No docstring.

### `biochar_app/scripts/get_weather_data.py`

- **`get_weather_column_labels(units: str=DEFAULT_UNITS) -> dict[str, str]`** — line 94; function; public/exported. Build a mapping from base 15-min column names (e.g. "soil_temp_5cm", "precip") to final column names with proper unit suffixes (e.g. "_in" or "_mm"), and convert 5 cm / 15 cm soil depths to 2 in / 6 in when using US units.
- **`fetch_weather_data(year: int) -> pd.DataFrame`** — line 133; function; public/exported. Fetch a full year of CoAgMet weather data for `year` and return a 15-minute-aggregated DataFrame.

### `biochar_app/scripts/gseason.py`

- **`_slice_and_mean(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series`** — line 19; function; internal. Column-wise mean of df[start:end] (inclusive). Assumes DatetimeIndex.
- **`compute_seasons(df: pd.DataFrame, year: int, periods: Mapping[str, Mapping[str, Any]]=DEFAULT_GSEASON_PERIODS, *, include_precip: bool=True) -> pd.DataFrame`** — line 26; function; public/exported. Compute one row per custom period.
- **`assign_gseason_periods(ts: pd.Timestamp, year: int) -> str \| None`** — line 139; function; public/exported. Return the period code in DEFAULT_GSEASON_PERIODS that contains timestamp `ts`. Handles wrap-around windows (e.g., Nov–Feb maps to the given `year`).

### `biochar_app/scripts/gseason_utils.py`

- **`generate_gseason_summary(year: int, periods: dict[str, dict[str, str]] \| None=None, overwrite: bool=False) -> None`** — line 39; function; public/exported. Build and persist growing-season summary JSON for a given year directly from the 15-min logger parquet + 15-min ratio parquet.
- **`generate_gseason_summary.stats(series: pd.Series) -> dict[str, float]`** — line 145; nested function; public/exported. No docstring.
- **`load_or_generate_gseason_summary(year: int, overwrite: bool=False) -> dict`** — line 224; function; public/exported. Load the nested JSON summary (period → variable → strip_depth → stats). Returns the raw dict, not a DataFrame.
- **`compute_summary_statistics(df: pd.DataFrame, variable: str, strip: str, depth: str)`** — line 239; function; public/exported. Compute summary statistics for raw and ratio values filtered by variable, strip, and depth. Returns two dictionaries: raw_stats and ratio_stats.
- **`get_flat_gseason_summary(year: int) -> pd.DataFrame`** — line 352; function; public/exported. Flatten the nested gseason JSON into a wide, analysis-friendly table.
- **`format_gseason_label(code: str) -> str`** — line 485; function; public/exported. No docstring.
- **`add_gseason_precip_from_daily(df_gs: pd.DataFrame, year: int, periods_raw) -> pd.DataFrame`** — line 495; function; public/exported. Attach seasonal precip sums to the growing-season dataframe.
- **`periods_to_list_of_dicts(periods: Any) -> list[dict[str, str]]`** — line 564; function; public/exported. Normalize various PeriodSpec shapes to a list of dicts: [{"code":..., "label":..., "start":"MM-DD", "end":"MM-DD"}, ...]
- **`add_gseason_irrigation_from_events(df_gs: pd.DataFrame, year: int, strip: str, periods_raw=None) -> pd.DataFrame`** — line 630; function; public/exported. Attach seasonal strip-level irrigation totals to the growing-season dataframe.
- **`build_gseason_frame_for_strip_depth(year: int, variable: str, strip: str, depth: str) -> pd.DataFrame`** — line 713; function; public/exported. Build the 3-row dataframe used for gseason plots & CSV downloads for a specific variable / strip / depth.

### `biochar_app/scripts/lab/biomass_field_tables.py`

- **`_input_csv_path() -> Path`** — line 55; function; internal. Default CSV location for the cleaned wide-form Biomass Field dataset. Adjust this path if you move the processed CSV.
- **`_as_period_obj(p: str) -> dict[str, str]`** — line 62; function; internal. No docstring.
- **`get_biomass_field_table_payload(csv_path: str \| Path \| None=None, min_year: int \| None=None) -> dict[str, Any]`** — line 69; function; public/exported. Build the wide-form Biomass Field table payload.

### `biochar_app/scripts/lab/build_field_biomass_from_master.py`

- **`_normalize_location(value: object) -> str \| None`** — line 34; function; internal. No docstring.
- **`_parse_date(value: object) -> str \| None`** — line 39; function; internal. No docstring.
- **`extract_biomass_sheet(workbook_path: Path, sheet_name: str) -> pd.DataFrame`** — line 51; function; public/exported. Return location/sampling_date/dry_g rows from one biomass worksheet.
- **`read_historical_biomass(path: Path) -> pd.DataFrame`** — line 99; function; public/exported. Normalize the reviewed two-header 2023-2025 CSV to wide form.
- **`_observations_to_wide(observations: pd.DataFrame) -> pd.DataFrame`** — line 111; function; internal. No docstring.
- **`build_field_biomass(*, workbook_path: Path=BIOCHAR_MASTER_WORKBOOK, historical_path: Path=BIOMASS_HISTORICAL_CSV) -> pd.DataFrame`** — line 118; function; public/exported. Build wide biomass data, with reviewed historical values taking priority.
- **`_atomic_write_csv(dataframe: pd.DataFrame, path: Path) -> None`** — line 147; function; internal. No docstring.
- **`build_and_install_field_biomass(*, workbook_path: Path=BIOCHAR_MASTER_WORKBOOK, historical_path: Path=BIOMASS_HISTORICAL_CSV, output_path: Path=BIOMASS_FIELD_CSV) -> dict[str, Any]`** — line 158; function; public/exported. No docstring.
- **`main() -> None`** — line 175; function; public/exported. Compatibility entry point; ETL is now the preferred orchestrator.

### `biochar_app/scripts/lab/clean_ward_master_common.py`

- **`make_machine_name(name: str) -> str`** — line 105; function; public/exported. Convert headers like: "CEC/Sum of Cations me/100g" -> "cec_sum_of_cations_me_100g" "1:1 Soil pH"               -> "1_1_soil_ph" "Gram(+):Gram(-)"           -> "gram_gram"
- **`normalize_strip(value: object) -> Optional[str]`** — line 128; function; public/exported. Convert common strip/sample-id variants to canonical lab-master strings: strip_1 .. strip_4
- **`canonical_strip_to_display(value: object) -> str`** — line 160; function; public/exported. strip_1 -> STRIP 1
- **`canonical_strip_to_etl(value: object) -> Optional[str]`** — line 170; function; public/exported. strip_1 -> S1
- **`normalize_strip_column(df: pd.DataFrame, *, strip_col: str='strip') -> pd.DataFrame`** — line 180; function; public/exported. Return a copy with df[strip_col] normalized to canonical strip_n strings.
- **`derive_strip_column(df: pd.DataFrame, *, source_candidates: Sequence[str], target_col: str='strip') -> pd.DataFrame`** — line 188; function; public/exported. Create/overwrite a canonical `strip` column from the first available source column among `source_candidates`.
- **`parse_to_iso_date(value: object) -> Optional[str]`** — line 208; function; public/exported. Parse a value into an ISO date (YYYY-MM-DD) string, or None.
- **`normalize_date_columns(df: pd.DataFrame, *, date_cols: Mapping[str, str]) -> pd.DataFrame`** — line 224; function; public/exported. Rename + normalize date columns to ISO 'YYYY-MM-DD'.
- **`normalize_special_ward_value(value: ScalarValue, *, below_detection_to_zero: bool=True) -> ScalarValue`** — line 250; function; public/exported. Normalize special strings used in Ward/Lobato files.
- **`normalize_special_ward_values(df: pd.DataFrame, *, below_detection_to_zero: bool=True, exclude_cols: Iterable[str]=('strip', 'date_rec', 'date_rept', 'date_recd')) -> pd.DataFrame`** — line 278; function; public/exported. Apply special-value normalization across most columns.
- **`normalize_special_ward_values._normalize_cell(x: ScalarValue) -> ScalarValue`** — line 290; nested function; internal. No docstring.
- **`coerce_numeric_series(s: pd.Series) -> pd.Series`** — line 303; function; public/exported. Best-effort numeric coercion for columns that might include commas, whitespace, or stray symbols.
- **`coerce_numeric_columns(df: pd.DataFrame, *, exclude_cols: Iterable[str]=('strip', 'date_rec', 'date_rept', 'date_recd')) -> pd.DataFrame`** — line 327; function; public/exported. Apply numeric coercion to all non-excluded columns. Leaves excluded columns untouched.
- **`first_present(columns: Iterable[str], *candidates: str) -> Optional[str]`** — line 349; function; public/exported. No docstring.
- **`ensure_compatibility_columns(df: pd.DataFrame) -> pd.DataFrame`** — line 356; function; public/exported. Add stable/legacy column names expected by downstream code/UI if they are missing but can be derived from newer/alternative Ward/Lobato naming.
- **`add_fixed_depth_columns(df: pd.DataFrame, *, begin_in: int, end_in: int) -> pd.DataFrame`** — line 398; function; public/exported. No docstring.
- **`drop_admin_columns(df: pd.DataFrame, *, extra_drop: Iterable[str] \| None=None, preserve_cols: Iterable[str] \| None=None) -> pd.DataFrame`** — line 407; function; public/exported. Drop known admin columns (case-insensitive) if present.
- **`print_strip_summary(df: pd.DataFrame, *, strip_col: str='strip') -> None`** — line 430; function; public/exported. No docstring.
- **`print_date_summary(df: pd.DataFrame, *, date_col: str='date_rept') -> None`** — line 440; function; public/exported. No docstring.
- **`report_missing_columns(df: pd.DataFrame, expected: Iterable[str]) -> list[str]`** — line 460; function; public/exported. No docstring.
- **`report_unmatched_source_columns(df: pd.DataFrame, *, matched_output_columns: Iterable[str], ignore_columns: Iterable[str]=()) -> list[str]`** — line 470; function; public/exported. No docstring.
- **`read_ward_two_header_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]`** — line 491; function; public/exported. Ward export format: row 0 = human headers row 1 = machine headers row 2+ = data
- **`read_clean_one_header_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]`** — line 527; function; public/exported. Clean export format: single header row (already machine-readable)
- **`read_ward_master_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]`** — line 549; function; public/exported. Auto-detect: - Try one-header clean CSV first (works if file is already cleaned) - If that doesn't look like a cleaned file, fall back to two-header reader
- **`clean_compiled_workbook(df: pd.DataFrame, *, admin_drop_cols: Iterable[str]=ADMIN_DROP_COLS, preserve_cols: Iterable[str] \| None=None) -> tuple[pd.DataFrame, dict[str, str]]`** — line 576; function; public/exported. Normalize a compiled one-header workbook.
- **`standardize_ward_dataframe(df: pd.DataFrame, *, strip_source_candidates: Sequence[str]=('strip', 'sample_id', 'sample_id_1'), date_cols: Mapping[str, str] \| None=None, below_detection_to_zero: bool=True, extra_drop_cols: Iterable[str] \| None=None, fixed_depth: tuple[int, int] \| None=None, numeric_exclude_cols: Iterable[str]=('strip', 'date_rec', 'date_rept', 'date_recd'), add_compatibility_aliases: bool=True) -> pd.DataFrame`** — line 626; function; public/exported. Apply the common normalization steps used by Ward/Lobato cleaners.
- **`validate_and_report(df: pd.DataFrame, *, strip_col: str='strip', date_col: str='date_rept', expected_columns: Iterable[str] \| None=None, matched_output_columns: Iterable[str] \| None=None, ignore_unmatched_columns: Iterable[str]=()) -> dict[str, list[str]]`** — line 677; function; public/exported. Convenience reporting wrapper for cleaners.
- **`write_clean_outputs(df: pd.DataFrame, header_map: dict[str, str], *, out_csv: Path, out_headers_json: Optional[Path]=None) -> None`** — line 713; function; public/exported. No docstring.

### `biochar_app/scripts/lab/merge_nir_into_master_v2.py`

- **`_as_str_cell(x) -> str`** — line 46; function; internal. Exact-ish string conversion without numeric normalization. We DO normalize NaN/None to empty string so that blank cells compare cleanly.
- **`signature_from_col(df: pd.DataFrame, col_idx: int) -> str`** — line 60; function; public/exported. Build the signature string for a column from the 4 data rows, preserving exact string forms (except NaN->"").
- **`load_master_test(master_test_csv: Path) -> tuple[list[str], pd.DataFrame]`** — line 68; function; public/exported. Master_test format: row 0 = human headers row 1 = machine headers  <-- we want these as mapping targets rows 2-5 = 4 data rows
- **`is_unnamed_col(col: object) -> bool`** — line 85; function; public/exported. No docstring.
- **`load_old_reference(old_csv: Path) -> tuple[list[str], pd.DataFrame]`** — line 89; function; public/exported. Old NIR reference file: row 0 = headers rows 1-4 = 4 data rows
- **`build_mapping(master_machine_headers: list[str], master_data: pd.DataFrame, old_headers: list[str], old_data: pd.DataFrame) -> tuple[dict[str, str], dict[str, list[str]], list[str]]`** — line 117; function; public/exported. Returns: mapping: old_name -> master_machine_name ambiguous: old_name -> list of possible master columns (if signature matches >1) unmapped: list of old columns with no matches
- **`main() -> None`** — line 161; function; public/exported. No docstring.

### `biochar_app/scripts/lab/merge_soilchem_supplemental.py`

- **`parse_date(value: Any) -> str`** — line 85; function; public/exported. No docstring.
- **`coerce_value(col: str, value: Any) -> Any`** — line 91; function; public/exported. No docstring.
- **`build_supplemental_clean(raw_csv: Path, clean_columns: list[str]) -> pd.DataFrame`** — line 100; function; public/exported. No docstring.
- **`main() -> None`** — line 137; function; public/exported. No docstring.

### `biochar_app/scripts/lab/reference_helpers.py`

- **`get_reference_bundle(reference_key: Optional[str]) -> Optional[VariableReferenceBundle]`** — line 17; function; public/exported. No docstring.
- **`get_reference_for_varspec(var_spec: LabVarSpec) -> Optional[VariableReferenceBundle]`** — line 22; function; public/exported. No docstring.
- **`has_reference(var_spec: LabVarSpec) -> bool`** — line 25; function; public/exported. No docstring.
- **`get_matching_band(value: float, interpretation: Optional[InterpretationInfo]) -> Optional[InterpretationBand]`** — line 32; function; public/exported. No docstring.
- **`get_band_label_for_value(value: Optional[float], bundle: Optional[VariableReferenceBundle]) -> Optional[str]`** — line 45; function; public/exported. No docstring.

### `biochar_app/scripts/lab/serializers.py`

- **`serialize_reference_bundle(bundle: Optional[VariableReferenceBundle]) -> Optional[dict[str, Any]]`** — line 8; function; public/exported. No docstring.

### `biochar_app/scripts/lab/suggest_glossary_aliases.py`

- **`normalize_text(value: str) -> str`** — line 81; function; public/exported. No docstring.
- **`humanize_column(col: str) -> str`** — line 84; function; public/exported. No docstring.
- **`load_glossary() -> dict`** — line 107; function; public/exported. No docstring.
- **`existing_aliases(item: dict) -> set[str]`** — line 110; function; public/exported. No docstring.
- **`item_matches_column(item: dict, col: str) -> bool`** — line 118; function; public/exported. No docstring.
- **`suggest_for_item(section_key: str, item: dict, columns: list[str]) -> list[dict]`** — line 159; function; public/exported. No docstring.
- **`main() -> None`** — line 194; function; public/exported. No docstring.

### `biochar_app/scripts/lab/update_ward_master_nir.py`

- **`_apply_raw_to_canonical_map(df: pd.DataFrame) -> pd.DataFrame`** — line 117; function; internal. No docstring.
- **`_canonicalize_header_map(header_map: dict[str, str]) -> dict[str, str]`** — line 131; function; internal. Keep the label map synchronized with source-to-canonical renames.
- **`_read_supplemental_nir_csv(path: Path, header_map: dict[str, str]) -> pd.DataFrame`** — line 139; function; internal. Read one-header Ward NIR data and return canonical cleaned rows.
- **`_merge_supplemental_nir_rows(dataframe: pd.DataFrame, header_map: dict[str, str]) -> pd.DataFrame`** — line 194; function; internal. No docstring.
- **`_rename_nir_date(df: pd.DataFrame) -> pd.DataFrame`** — line 210; function; internal. No docstring.
- **`_canonicalize_strip(value: object) -> str \| None`** — line 216; function; internal. No docstring.
- **`_find_sheet_name(xls: pd.ExcelFile, patterns: tuple[str, ...]) -> str`** — line 238; function; internal. No docstring.
- **`_read_one_2024_mineral_workbook(path: Path) -> pd.DataFrame`** — line 249; function; internal. No docstring.
- **`_drop_as_received_pct_when_dry_basis_exists(df: pd.DataFrame) -> pd.DataFrame`** — line 317; function; internal. Drop as-received/wet-basis percentage columns when a dry-basis equivalent exists.
- **`_read_2024_mineral_supplements() -> pd.DataFrame`** — line 356; function; internal. No docstring.
- **`_patch_missing_minerals(df: pd.DataFrame, supplement_df: pd.DataFrame) -> pd.DataFrame`** — line 377; function; internal. No docstring.
- **`update_ward_master_nir() -> None`** — line 409; function; public/exported. No docstring.

### `biochar_app/scripts/lab/update_ward_master_soilbio.py`

- **`_apply_rename_map(df: pd.DataFrame, rename_map: dict[str, str]) -> pd.DataFrame`** — line 102; function; internal. No docstring.
- **`_print_date_counts(csv_path, label: str) -> None`** — line 108; function; internal. No docstring.
- **`_merge_supplemental_raw_files_if_present(supplemental_csvs: Sequence) -> None`** — line 126; function; internal. Merge known supplemental raw Ward soil-bio files into the canonical cleaned CSV.
- **`_resolve_master_input() -> Path`** — line 167; function; internal. Return the preferred compiled master file path, with CSV fallback.
- **`_read_compiled_master(path: Path) -> pd.DataFrame`** — line 185; function; internal. Read Ward's compiled PLFA file from CSV or Excel.
- **`update_ward_master_soilbio() -> None`** — line 204; function; public/exported. No docstring.

### `biochar_app/scripts/lab/update_ward_master_soilchem.py`

- **`_snake_col(name: str) -> str`** — line 65; function; internal. No docstring.
- **`_apply_raw_to_canonical_map(df: pd.DataFrame) -> pd.DataFrame`** — line 78; function; internal. Apply RAW_TO_CANONICAL by coalescing source values into canonical columns.
- **`_find_machine_col_by_human_header(header_map: dict[str, str], human_name: str) -> Optional[str]`** — line 110; function; internal. No docstring.
- **`_ensure_sample_id_column(df_clean: pd.DataFrame, header_map: dict[str, str]) -> pd.DataFrame`** — line 123; function; internal. No docstring.
- **`_filter_to_project_rows(df_clean: pd.DataFrame) -> pd.DataFrame`** — line 168; function; internal. No docstring.
- **`_drop_blank_key_rows(df_clean: pd.DataFrame) -> pd.DataFrame`** — line 186; function; internal. Remove footer/comment/blank rows that do not have both strip and date_rec.
- **`_ensure_expected_soilchem_columns(df_clean: pd.DataFrame) -> pd.DataFrame`** — line 206; function; internal. No docstring.
- **`_standardize_soilchem_dataframe(df_raw: pd.DataFrame, compiled_master: bool=True) -> tuple[pd.DataFrame, dict[str, str]]`** — line 258; function; internal. Shared cleaning logic for both the compiled master and supplemental files.
- **`_read_supplemental_file(path: Path) -> pd.DataFrame`** — line 324; function; internal. No docstring.
- **`_print_date_counts(df: pd.DataFrame, label: str) -> None`** — line 335; function; internal. No docstring.
- **`_prepare_soilchem_csv(clean_csv: Path, output_csv: Path, supplemental_raw_csv: Optional[Path]=None) -> Path`** — line 351; function; internal. Prepare the canonical soil chemistry CSV.
- **`update_ward_master_soilchem(sheet: Optional[str]=None) -> None`** — line 414; function; public/exported. No docstring.

### `biochar_app/scripts/lab/ward_find_image_context.py`

- **`clean_text(text: str) -> str`** — line 31; function; public/exported. No docstring.
- **`collect_text_blocks(soup: BeautifulSoup) -> list[tuple[Tag, str]]`** — line 34; function; public/exported. Build a linear list of meaningful text-containing block tags in document order.
- **`find_nearest_block_index(img_tag: Tag, blocks: list[tuple[Tag, str]]) -> int \| None`** — line 50; function; public/exported. Find the first text block that is the image's parent, next sibling, or nearby ancestor context.
- **`main() -> None`** — line 72; function; public/exported. No docstring.

### `biochar_app/scripts/lab/ward_search_reference_terms.py`

- **`parse_args() -> argparse.Namespace`** — line 234; function; public/exported. No docstring.
- **`normalize_text(text: str) -> str`** — line 256; function; public/exported. No docstring.
- **`build_pattern(term: str, case_sensitive: bool, whole_word: bool) -> re.Pattern[str]`** — line 263; function; public/exported. No docstring.
- **`should_skip(path: Path, include_hidden: bool) -> bool`** — line 269; function; public/exported. No docstring.
- **`iter_files(root: Path, extensions: Sequence[str], include_hidden: bool) -> Iterable[Path]`** — line 274; function; public/exported. No docstring.
- **`extract_docx_text(path: Path) -> str \| None`** — line 289; function; public/exported. No docstring.
- **`read_file(path: Path) -> str \| None`** — line 305; function; public/exported. No docstring.
- **`search_file(path: Path, terms: Sequence[str], case_sensitive: bool, whole_word: bool, context: int, max_matches: int) -> list[MatchRecord]`** — line 318; function; public/exported. No docstring.
- **`filename_matches(path: Path, terms: Sequence[str], case_sensitive: bool) -> list[str]`** — line 367; function; public/exported. No docstring.
- **`print_match(m: MatchRecord) -> None`** — line 378; function; public/exported. No docstring.
- **`search_schema_key_in_sources(schema_key: str, source_files: Sequence[Path], case_sensitive: bool, whole_word: bool, context: int, max_matches_per_file: int) -> dict[str, list[MatchRecord]]`** — line 390; function; public/exported. Returns dict term -> matches for that schema key.
- **`summarize_schema_audit(source_files: Sequence[Path], case_sensitive: bool, whole_word: bool, context: int, max_matches_per_file: int) -> int`** — line 418; function; public/exported. No docstring.
- **`main() -> int`** — line 491; function; public/exported. No docstring.

### `biochar_app/scripts/logger_toa5.py`

- **`normalize_logger_timestamp_series(ts: Series) -> Series`** — line 71; function; public/exported. Parse TOA5 TIMESTAMP strings into pandas datetimes (timezone-naive). Expected format: 'YYYY-MM-DD HH:MM:SS'
- **`_read_toa5_table1_dat(datfile: Path) -> pd.DataFrame`** — line 79; function; internal. Read a Campbell Scientific TOA5 Table1 .dat file and return a DataFrame with column names derived from the TOA5 header row.
- **`read_dat_timestamps(datfile: Path) -> pd.Series`** — line 108; function; public/exported. Convenience helper: - reads TOA5 Table1 .dat - normalizes TIMESTAMP -> 'timestamp' if needed - parses timestamps using normalize_logger_timestamp_series() - drops NaT - returns Series[datetime64[ns]] suitable for diff/gap analysis

### `biochar_app/scripts/management/apply_duplicate_actions.py`

- **`parse_args() -> argparse.Namespace`** — line 75; function; public/exported. No docstring.
- **`calculate_sha256(path: Path, chunk_size: int=1024 * 1024) -> str`** — line 115; function; public/exported. No docstring.
- **`normalize_boolean_series(values: pd.Series) -> pd.Series`** — line 128; function; public/exported. No docstring.
- **`load_inventory(inventory_csv: Path) -> pd.DataFrame`** — line 148; function; public/exported. No docstring.
- **`validate_duplicate_decisions(inventory: pd.DataFrame) -> pd.DataFrame`** — line 206; function; public/exported. No docstring.
- **`resolve_source_path(photo_dir: Path, relative_path: str) -> Path`** — line 288; function; public/exported. No docstring.
- **`validate_files(delete_rows: pd.DataFrame, photo_dir: Path) -> list[tuple[str, Path]]`** — line 323; function; public/exported. No docstring.
- **`print_plan(validated_files: list[tuple[str, Path]], photo_dir: Path, apply_changes: bool) -> None`** — line 382; function; public/exported. No docstring.
- **`main() -> int`** — line 410; function; public/exported. No docstring.

### `biochar_app/scripts/management/audit_irrigation_event_ids.py`

- **`audit_text_files() -> None`** — line 27; function; public/exported. No docstring.
- **`audit_filenames() -> None`** — line 48; function; public/exported. No docstring.
- **`audit_csv_event_id_columns() -> None`** — line 57; function; public/exported. No docstring.
- **`audit_sqlite_databases() -> None`** — line 79; function; public/exported. No docstring.
- **`main() -> None`** — line 113; function; public/exported. No docstring.

### `biochar_app/scripts/management/build_irrigation_from_master.py`

- **`stable_event_id(date_value: object, start_value: object, strip_group: str) -> str`** — line 135; function; public/exported. Create a deterministic ID for a newly encountered workbook event.
- **`existing_event_ids(path: Path) -> dict[tuple[str, str, str], str]`** — line 148; function; public/exported. Read prior event IDs so unchanged events retain stable identifiers.
- **`concurrent_group_counts(events: pd.DataFrame) -> pd.Series`** — line 185; function; public/exported. Count strip groups sharing one physical meter interval.
- **`build_candidate_from_events(events: pd.DataFrame, *, prior_event_ids: dict[tuple[str, str, str], str] \| None=None, source_workbook_name: str='biochar-data-master.xlsx') -> tuple[pd.DataFrame, pd.DataFrame]`** — line 196; function; public/exported. Expand valid group-level workbook events into canonical strip-level rows.
- **`build_qc_candidate(candidate: pd.DataFrame) -> pd.DataFrame`** — line 394; function; public/exported. Apply established photo-supported corrections and validate them.
- **`validate_production(production: pd.DataFrame, *, previous_production: pd.DataFrame \| None=None) -> None`** — line 423; function; public/exported. Validate schema, uniqueness, volumes, and accidental row loss.
- **`atomic_write_csv(path: Path, dataframe: pd.DataFrame) -> None`** — line 464; function; public/exported. Write one CSV through a temporary file and atomically replace it.
- **`atomic_write_json(path: Path, payload: dict[str, Any]) -> None`** — line 488; function; public/exported. Write one JSON audit through a temporary file.
- **`build_and_install_irrigation(*, workbook_path: Path=BIOCHAR_MASTER_WORKBOOK, candidate_path: Path=DEFAULT_CANDIDATE_CSV, qc_candidate_path: Path=IRRIGATION_QC_CSV, production_path: Path=IRRIGATION_PRODUCTION_CSV, invalid_rows_path: Path=DEFAULT_INVALID_ROWS_CSV, audit_path: Path=DEFAULT_AUDIT_JSON, dry_run: bool=False) -> dict[str, Any]`** — line 509; function; public/exported. Build, validate, and optionally install all irrigation products.
- **`parse_args() -> argparse.Namespace`** — line 568; function; public/exported. No docstring.
- **`main() -> None`** — line 588; function; public/exported. No docstring.

### `biochar_app/scripts/management/build_irrigation_qc_candidate.py`

- **`parse_args() -> argparse.Namespace`** — line 194; function; public/exported. No docstring.
- **`clean_text(value: object) -> str`** — line 240; function; public/exported. No docstring.
- **`normalize_strip_group(value: object) -> str`** — line 257; function; public/exported. No docstring.
- **`parse_datetime_column(series: pd.Series) -> pd.Series`** — line 285; function; public/exported. No docstring.
- **`require_columns(df: pd.DataFrame, required: list[str], source_name: str) -> None`** — line 295; function; public/exported. No docstring.
- **`make_event_id(start_timestamp: pd.Timestamp, end_timestamp: pd.Timestamp, strip_group: str) -> str`** — line 313; function; public/exported. Create an event ID from the corrected physical event identity.
- **`initialize_qc_columns(df: pd.DataFrame) -> pd.DataFrame`** — line 348; function; public/exported. No docstring.
- **`match_correction_rows(df: pd.DataFrame, correction: TimestampCorrection) -> pd.Series`** — line 387; function; public/exported. No docstring.
- **`apply_timestamp_corrections(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]`** — line 404; function; public/exported. No docstring.
- **`apply_shared_meter_classifications(df: pd.DataFrame) -> pd.DataFrame`** — line 619; function; public/exported. No docstring.
- **`validate_corrected_events(df: pd.DataFrame) -> None`** — line 657; function; public/exported. No docstring.
- **`format_output(df: pd.DataFrame) -> pd.DataFrame`** — line 735; function; public/exported. No docstring.
- **`build_pending_events(input_path: Path) -> pd.DataFrame`** — line 836; function; public/exported. No docstring.
- **`print_summary(input_df: pd.DataFrame, output_df: pd.DataFrame, audit: pd.DataFrame, pending: pd.DataFrame, output_path: Path) -> None`** — line 1010; function; public/exported. No docstring.
- **`main() -> int`** — line 1141; function; public/exported. No docstring.

### `biochar_app/scripts/management/build_meter_photo_inventory.py`

- **`parse_args() -> argparse.Namespace`** — line 176; function; public/exported. No docstring.
- **`resolve_exiftool(explicit_path: Path \| None) -> str`** — line 232; function; public/exported. No docstring.
- **`find_photos(photo_dir: Path) -> list[Path]`** — line 260; function; public/exported. No docstring.
- **`calculate_sha256(path: Path, chunk_size: int=1024 * 1024) -> str`** — line 290; function; public/exported. No docstring.
- **`read_exif_batch(exiftool: str, photo_dir: Path) -> dict[str, dict[str, Any]]`** — line 303; function; public/exported. No docstring.
- **`clean_text(value: object) -> str`** — line 394; function; public/exported. No docstring.
- **`optional_number(value: object) -> object`** — line 412; function; public/exported. No docstring.
- **`optional_integer(value: object) -> object`** — line 425; function; public/exported. No docstring.
- **`normalize_exif_datetime(value: object) -> str`** — line 438; function; public/exported. Convert common EXIF date formatting into an ISO-like string.
- **`select_timezone_offset(metadata: dict[str, Any]) -> tuple[str, str]`** — line 467; function; public/exported. No docstring.
- **`select_timestamp(metadata: dict[str, Any]) -> tuple[str, str, str]`** — line 496; function; public/exported. No docstring.
- **`derive_year(timestamp: object) -> object`** — line 514; function; public/exported. Derive a four-digit year from one timestamp value.
- **`infer_source_collection(relative_path: Path) -> str`** — line 547; function; public/exported. Infer provenance when originals are stored in source subdirectories.
- **`load_manual_values(output_csv: Path) -> dict[str, dict[str, str]]`** — line 562; function; public/exported. No docstring.
- **`build_inventory_rows(photos: list[Path], photo_dir: Path, metadata_by_path: dict[str, dict[str, Any]], preserved_manual: dict[str, dict[str, str]], progress_every: int) -> list[dict[str, object]]`** — line 612; function; public/exported. No docstring.
- **`apply_manual_datetime(inventory: pd.DataFrame) -> pd.DataFrame`** — line 815; function; public/exported. Build effective datetime and year using an optional manual timestamp.
- **`duplicate_filename_preference(filename: str) -> tuple[int, int, str]`** — line 887; function; public/exported. Return a deterministic archival-preference key.
- **`add_duplicate_information(inventory: pd.DataFrame) -> pd.DataFrame`** — line 961; function; public/exported. Add duplicate-group information and choose one preferred archival copy.
- **`order_columns(inventory: pd.DataFrame) -> pd.DataFrame`** — line 1176; function; public/exported. No docstring.
- **`print_summary(inventory: pd.DataFrame, output_csv: Path) -> None`** — line 1245; function; public/exported. No docstring.
- **`main() -> int`** — line 1384; function; public/exported. No docstring.

### `biochar_app/scripts/management/build_meter_review_workbook.py`

- **`parse_args() -> argparse.Namespace`** — line 102; function; public/exported. No docstring.
- **`find_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str \| None`** — line 177; function; public/exported. No docstring.
- **`normalize_filename(value: object) -> str`** — line 194; function; public/exported. No docstring.
- **`clean_text(value: object) -> str`** — line 200; function; public/exported. No docstring.
- **`valid_meter_reading(value: object) -> bool`** — line 209; function; public/exported. No docstring.
- **`load_corrections(corrections_csv: Path \| None) -> dict[str, dict[str, str]]`** — line 213; function; public/exported. No docstring.
- **`apply_corrections(metadata: pd.DataFrame, corrections: dict[str, dict[str, str]]) -> pd.DataFrame`** — line 257; function; public/exported. No docstring.
- **`load_metadata(metadata_csv: Path, photo_dir: Path) -> pd.DataFrame`** — line 289; function; public/exported. No docstring.
- **`select_inventory_photos(metadata: pd.DataFrame) -> pd.DataFrame`** — line 359; function; public/exported. Return every canonical inventory photo, ordered by timestamp and filename.
- **`register_heic_support() -> bool`** — line 396; function; public/exported. No docstring.
- **`read_image(image_path: Path, heic_available: bool) -> np.ndarray`** — line 404; function; public/exported. No docstring.
- **`resize_for_detection(image: np.ndarray, max_dimension: int=1800) -> tuple[np.ndarray, float]`** — line 423; function; public/exported. No docstring.
- **`detect_meter_face(image: np.ndarray) -> tuple[int, int, int] \| None`** — line 445; function; public/exported. Detect the circular meter face using a Hough-circle search.
- **`detect_meter_face.candidate_score(circle: np.ndarray) -> float`** — line 475; nested function; public/exported. No docstring.
- **`fixed_meter_head_crop(image: np.ndarray) -> np.ndarray`** — line 489; function; public/exported. Broad fallback crop for photographs where circle detection fails.
- **`crop_meter_head(image: np.ndarray) -> tuple[np.ndarray, str]`** — line 503; function; public/exported. Crop the complete meter head, retaining enough dial context for a human.
- **`save_workbook_crop(image_path: Path, crop_dir: Path, heic_available: bool) -> tuple[Path, str]`** — line 536; function; public/exported. No docstring.
- **`fit_thumbnail(image_path: Path, max_width: int, max_height: int) -> tuple[int, int]`** — line 580; function; public/exported. No docstring.
- **`add_instructions_sheet(workbook: Workbook) -> None`** — line 600; function; public/exported. No docstring.
- **`build_workbook(selected_photos: pd.DataFrame, output_xlsx: Path, crop_dir: Path, heic_available: bool, keep_crops: bool) -> None`** — line 643; function; public/exported. No docstring.
- **`count_reviewed_readings(workbook_path: Path) -> int`** — line 882; function; public/exported. No docstring.
- **`count_metadata_readings(metadata: pd.DataFrame) -> int`** — line 914; function; public/exported. No docstring.
- **`main() -> None`** — line 920; function; public/exported. No docstring.

### `biochar_app/scripts/management/compare_meter_photos_to_irrigation.py`

- **`parse_args() -> argparse.Namespace`** — line 125; function; public/exported. No docstring.
- **`clean_text(value: object) -> str`** — line 236; function; public/exported. No docstring.
- **`normalize_column_name(value: object) -> str`** — line 254; function; public/exported. No docstring.
- **`normalize_columns(df: pd.DataFrame) -> pd.DataFrame`** — line 260; function; public/exported. No docstring.
- **`require_columns(df: pd.DataFrame, required: tuple[str, ...], source_name: str) -> None`** — line 271; function; public/exported. No docstring.
- **`find_required_column(columns: list[str], candidates: tuple[str, ...], display_name: str, source_name: str) -> str`** — line 289; function; public/exported. No docstring.
- **`parse_datetime_series(series: pd.Series) -> pd.Series`** — line 307; function; public/exported. No docstring.
- **`parse_local_wall_datetime_series(series: pd.Series) -> pd.Series`** — line 323; function; public/exported. Parse local observations and discard offsets without shifting time.
- **`parse_local_wall_datetime_series.parse_one(value: object) -> pd.Timestamp`** — line 328; nested function; public/exported. No docstring.
- **`parse_numeric_series(series: pd.Series) -> pd.Series`** — line 350; function; public/exported. No docstring.
- **`join_unique_text(values: pd.Series) -> str`** — line 359; function; public/exported. No docstring.
- **`unique_numeric_values(values: pd.Series) -> list[float]`** — line 373; function; public/exported. No docstring.
- **`nullable_float(value: object) -> float \| None`** — line 389; function; public/exported. No docstring.
- **`normalize_strip_group(value: object) -> str \| None`** — line 403; function; public/exported. No docstring.
- **`normalize_location(value: object) -> str \| None`** — line 438; function; public/exported. No docstring.
- **`infer_location_from_strip_group(strip_group: str \| None) -> str \| None`** — line 452; function; public/exported. No docstring.
- **`parse_excel_date(value: object) -> date \| None`** — line 464; function; public/exported. No docstring.
- **`parse_excel_time(value: object) -> time \| None`** — line 490; function; public/exported. No docstring.
- **`combine_excel_date_and_time(date_value: object, time_value: object) -> pd.Timestamp`** — line 549; function; public/exported. No docstring.
- **`correct_overnight_end_times(start: pd.Series, end: pd.Series) -> pd.Series`** — line 575; function; public/exported. No docstring.
- **`load_photo_readings(path: Path) -> pd.DataFrame`** — line 599; function; public/exported. No docstring.
- **`find_irrigation_sheets(workbook_path: Path) -> list[tuple[int, str]]`** — line 755; function; public/exported. No docstring.
- **`load_master_irrigation_sheet(workbook_path: Path, sheet_name: str, year: int) -> pd.DataFrame`** — line 797; function; public/exported. No docstring.
- **`load_master_irrigation_events(workbook_path: Path) -> pd.DataFrame`** — line 1164; function; public/exported. No docstring.
- **`build_boundary_table(events: pd.DataFrame) -> pd.DataFrame`** — line 1208; function; public/exported. No docstring.
- **`candidate_photos_for_boundary(boundary: pd.Series, photos: pd.DataFrame, max_match_hours: float) -> pd.DataFrame`** — line 1395; function; public/exported. No docstring.
- **`select_best_photo(boundary: pd.Series, photos: pd.DataFrame, max_match_hours: float, time_tolerance_minutes: float, counter_tolerance_units: int) -> pd.Series \| None`** — line 1456; function; public/exported. No docstring.
- **`classify_boundary_comparison(photo_found: bool, within_time_tolerance: bool, workbook_counter_present: bool, within_counter_tolerance: bool) -> tuple[str, str]`** — line 1527; function; public/exported. No docstring.
- **`compare_boundary_to_camera(boundary: pd.Series, photos: pd.DataFrame, max_match_hours: float, time_tolerance_minutes: float, counter_tolerance_units: int) -> dict[str, object]`** — line 1599; function; public/exported. No docstring.
- **`build_boundary_comparison(boundaries: pd.DataFrame, photos: pd.DataFrame, max_match_hours: float, time_tolerance_minutes: float, counter_tolerance_units: int) -> pd.DataFrame`** — line 1837; function; public/exported. No docstring.
- **`boundary_record_for_event(event_key: str, boundary_type: str, boundary_qc: pd.DataFrame) -> pd.Series \| None`** — line 1875; function; public/exported. No docstring.
- **`prefixed_boundary_fields(prefix: str, row: pd.Series \| None) -> dict[str, object]`** — line 1905; function; public/exported. No docstring.
- **`classify_event_qc(start_status: str, end_status: str, volume_difference: float \| None, volume_tolerance_gallons: float) -> tuple[str, str]`** — line 1993; function; public/exported. No docstring.
- **`build_event_qc(events: pd.DataFrame, boundary_qc: pd.DataFrame, volume_tolerance_gallons: float) -> pd.DataFrame`** — line 2084; function; public/exported. No docstring.
- **`build_unmatched_photo_table(photos: pd.DataFrame, boundary_qc: pd.DataFrame) -> pd.DataFrame`** — line 2344; function; public/exported. No docstring.
- **`round_numeric_columns(df: pd.DataFrame) -> pd.DataFrame`** — line 2375; function; public/exported. No docstring.
- **`write_csv(df: pd.DataFrame, path: Path) -> None`** — line 2412; function; public/exported. No docstring.
- **`print_console_summary(photos: pd.DataFrame, events: pd.DataFrame, boundaries: pd.DataFrame, boundary_qc: pd.DataFrame, event_qc: pd.DataFrame, unmatched_photos: pd.DataFrame) -> None`** — line 2428; function; public/exported. No docstring.
- **`main() -> None`** — line 2573; function; public/exported. No docstring.

### `biochar_app/scripts/management/estimate_irrigation_holding_capacity.py`

- **`prune_stale_multidepth_figures(*, year: int, plot_log: pd.DataFrame, multidepth_plot_dir: Path) -> list[Path]`** — line 201; function; public/exported. Remove obsolete generated event plots for one successfully built year.
- **`concat_nonempty_informative_frames(frames: list[pd.DataFrame]) -> pd.DataFrame`** — line 247; function; public/exported. Concatenate nonempty frames without pandas' all-NA dtype warning.
- **`logger_order_flag_summary(arrival_order_table: pd.DataFrame) -> pd.DataFrame`** — line 271; function; public/exported. Return one console-summary row per flagged event and strip.
- **`build_irrigation_event_response_summary(arrival_times: pd.DataFrame, event_results: pd.DataFrame) -> pd.DataFrame`** — line 305; function; public/exported. Build one summary row per logger position for each irrigation event.
- **`analyze_loggers_all_depths(df_15min: pd.DataFrame, irrigation_events: pd.DataFrame, strips: list[str], year: int, logger_positions: list[str] \| None=None) -> pd.DataFrame`** — line 635; function; public/exported. Analyze irrigation responses for all available depths and selected loggers.
- **`build_enhanced_event_debug_table(event_results: pd.DataFrame, decimals: int=2) -> pd.DataFrame`** — line 798; function; public/exported. No docstring.
- **`build_enhanced_runtime_table(event_results: pd.DataFrame, min_events: int=3) -> pd.DataFrame`** — line 861; function; public/exported. No docstring.
- **`write_year_outputs(year: int, df_15min: pd.DataFrame, results: pd.DataFrame, zone_storage_table: pd.DataFrame, plot_results: pd.DataFrame \| None=None, *, diagnostics_dir: Path=IRRIGATION_DIAGNOSTICS_DIR, holding_capacity_dir: Path=HOLDING_CAPACITY_DIR, figures_dir: Path=IRRIGATION_FIGURES_DIR) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]`** — line 941; function; public/exported. Write yearly irrigation-analysis outputs.
- **`_append_if_not_empty(collection: list[pd.DataFrame], df: pd.DataFrame) -> None`** — line 1676; function; internal. No docstring.
- **`_write_zone_storage_outputs(all_event_storage_zone_tables: list[pd.DataFrame], *, holding_capacity_dir: Path) -> pd.DataFrame`** — line 1680; function; internal. No docstring.
- **`write_all_logger_debug_table(*, year: int, all_logger_results: pd.DataFrame, holding_capacity_dir: Path) -> pd.DataFrame`** — line 1771; function; public/exported. Write depth-level irrigation diagnostics for all logger positions.
- **`_write_combined_year_outputs(all_pre_start_flags: list[pd.DataFrame], all_trustworthy_tables: list[pd.DataFrame], all_holding_capacity_tables: list[pd.DataFrame], all_water_balance_tables: list[pd.DataFrame], combined_zone_storage: pd.DataFrame, *, diagnostics_dir: Path, holding_capacity_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]`** — line 1837; function; internal. No docstring.
- **`main() -> None`** — line 1985; function; public/exported. No docstring.

### `biochar_app/scripts/management/extract_irrigation_photo_events.py`

- **`parse_args() -> argparse.Namespace`** — line 84; function; public/exported. No docstring.
- **`get_exif_datetime_image(path: Path) -> tuple[str \| None, str]`** — line 138; function; public/exported. Extract DateTimeOriginal/DateTimeDigitized/DateTime from image EXIF.
- **`parse_exif_datetime(raw: str) -> str \| None`** — line 168; function; public/exported. No docstring.
- **`get_video_metadata_with_exiftool(path: Path) -> tuple[str \| None, str]`** — line 189; function; public/exported. Try to read video capture metadata using exiftool if available.
- **`parse_video_datetime(raw: str) -> str \| None`** — line 229; function; public/exported. Parse common exiftool video date strings.
- **`get_file_modified_timestamp(path: Path) -> tuple[str, str]`** — line 258; function; public/exported. No docstring.
- **`get_capture_timestamp(path: Path) -> tuple[str \| None, str]`** — line 262; function; public/exported. No docstring.
- **`list_media_files(photo_dir: Path) -> list[Path]`** — line 279; function; public/exported. No docstring.
- **`build_initial_photo_index(photo_dir: Path) -> pd.DataFrame`** — line 286; function; public/exported. No docstring.
- **`load_or_create_review_csv(photo_dir: Path, year: int, use_review: bool) -> pd.DataFrame`** — line 327; function; public/exported. No docstring.
- **`normalize_review_df(df: pd.DataFrame) -> pd.DataFrame`** — line 346; function; public/exported. No docstring.
- **`suggest_pairs(df: pd.DataFrame, max_event_hours: float, min_gallons: float) -> pd.DataFrame`** — line 383; function; public/exported. Greedy pairing: - Sort by timestamp. - Treat each reading as a possible start. - Pair with the next later reading that increases by at least min_gallons and is within max_event_hours. - Skip readings already consumed by a pair.
- **`enrich_with_irrigation_comparison(events_df: pd.DataFrame, irrigation_csv: Path)`** — line 466; function; public/exported. No docstring.
- **`suggest_pairs_from_known_irrigation_events(photo_df: pd.DataFrame, irrigation_csv: Path, max_start_window_hours: float=4.0, max_end_window_hours: float=4.0) -> pd.DataFrame`** — line 525; function; public/exported. Event-driven matching.
- **`append_unmatched_rows(events_df: pd.DataFrame, photo_df: pd.DataFrame) -> pd.DataFrame`** — line 691; function; public/exported. No docstring.
- **`attach_irrigation_matches(photo_df: pd.DataFrame, irrigation_csv: Path, max_match_minutes: float=90.0) -> pd.DataFrame`** — line 775; function; public/exported. No docstring.
- **`main() -> int`** — line 879; function; public/exported. No docstring.
- **`detect_irrigation_events_from_meter(df: pd.DataFrame, min_event_gallons: float=20000, max_gap_minutes: float=180) -> pd.DataFrame`** — line 965; function; public/exported. Detect irrigation events based purely on meter reading increases.

### `biochar_app/scripts/management/finalize_meter_photo_inventory.py`

- **`clean(value: object) -> str`** — line 83; function; public/exported. No docstring.
- **`normalize_reading(value: object) -> str`** — line 92; function; public/exported. No docstring.
- **`valid_reading(value: object) -> bool`** — line 99; function; public/exported. No docstring.
- **`photo_family(filename: str) -> str`** — line 103; function; public/exported. No docstring.
- **`integer(value: object) -> int`** — line 108; function; public/exported. No docstring.
- **`read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]`** — line 115; function; public/exported. No docstring.
- **`write_csv_rows(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> None`** — line 121; function; public/exported. No docstring.
- **`workbook_rows(path: Path, sheet_name: str) -> list[dict[str, object]]`** — line 129; function; public/exported. No docstring.
- **`read_main_review(path: Path) -> list[ReviewReading]`** — line 139; function; public/exported. No docstring.
- **`read_full_resolution_review(path: Path) -> list[ReviewReading]`** — line 159; function; public/exported. No docstring.
- **`read_corrections(path: Path \| None) -> dict[str, ReviewReading]`** — line 182; function; public/exported. No docstring.
- **`main_family_consensus(main_rows: list[ReviewReading]) -> tuple[dict[str, str], dict[str, list[str]]]`** — line 206; function; public/exported. No docstring.
- **`choose_reading(inventory_row: dict[str, str], main_by_sha: dict[str, ReviewReading], followup_by_sha: dict[str, ReviewReading], corrections: dict[str, ReviewReading], family_consensus: dict[str, str]) -> tuple[str, str, str, list[dict[str, str]]]`** — line 226; function; public/exported. No docstring.
- **`preferred_record(rows: list[dict[str, str]]) -> dict[str, str]`** — line 282; function; public/exported. No docstring.
- **`finalize_inventory(inventory_csv: Path, main_workbook: Path, full_resolution_workbook: Path, corrections_csv: Path \| None, output_csv: Path, manifest_csv: Path, audit_json: Path, copy_unique_dir: Path \| None=None, originals_dir: Path \| None=None) -> dict[str, object]`** — line 293; function; public/exported. No docstring.
- **`parse_args() -> argparse.Namespace`** — line 429; function; public/exported. No docstring.
- **`main() -> None`** — line 443; function; public/exported. No docstring.

### `biochar_app/scripts/management/generate_fertilizer_clean.py`

- **`parse_args() -> argparse.Namespace`** — line 49; function; public/exported. No docstring.
- **`make_backup(path: Path, backup_dir: Path) -> Path \| None`** — line 57; function; public/exported. No docstring.
- **`seasonal_date(year: int) -> str`** — line 67; function; public/exported. No docstring.
- **`strip_group_for_strip(strip: str) -> str`** — line 70; function; public/exported. No docstring.
- **`location_for_strip(strip: str) -> str`** — line 77; function; public/exported. No docstring.
- **`strip_area_acres(strip: str) -> float \| None`** — line 84; function; public/exported. No docstring.
- **`normalize_product_name(value: object) -> str`** — line 94; function; public/exported. No docstring.
- **`clean_product_name(value: object) -> str`** — line 100; function; public/exported. No docstring.
- **`numeric_value(value: object) -> float`** — line 103; function; public/exported. No docstring.
- **`product_analysis_for(product: str) -> dict[str, float]`** — line 109; function; public/exported. No docstring.
- **`product_analysis_note(product: str) -> str`** — line 121; function; public/exported. No docstring.
- **`add_nutrient_totals(row: dict[str, object]) -> dict[str, object]`** — line 134; function; public/exported. No docstring.
- **`add_rate_columns(df: pd.DataFrame) -> pd.DataFrame`** — line 148; function; public/exported. No docstring.
- **`strip_amounts_from_row_values(raw: pd.DataFrame, row_index: int, strips: list[str], value_start_col: int=1) -> dict[str, float]`** — line 190; function; public/exported. No docstring.
- **`parse_2023_sheet(workbook: Path, sheet_name: str) -> pd.DataFrame`** — line 220; function; public/exported. No docstring.
- **`parse_2024_2025_sheet(workbook: Path, sheet_name: str, year: int) -> pd.DataFrame`** — line 281; function; public/exported. No docstring.
- **`build_fertilizer_clean(input_workbook: Path) -> pd.DataFrame`** — line 337; function; public/exported. No docstring.
- **`main() -> int`** — line 388; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/arrival.py`

- **`detect_sustained_baseline_arrival(vwc_series: pd.Series, baseline_vwc: float, irrigation_start: pd.Timestamp, response_threshold_vwc: float, min_persist_points: int=4, min_followup_rise_vwc: float=0.5) -> tuple[pd.Timestamp \| None, float \| None]`** — line 13; function; public/exported. Standard arrival: first time after irrigation start that VWC exceeds baseline by threshold and is followed by a sustained rise.
- **`detect_alt_arrival_from_vwc_step(vwc_series: pd.Series, response_threshold_vwc: float, min_persist_points: int=4, min_total_rise_vwc: float \| None=None) -> tuple[pd.Timestamp \| None, float \| None, float \| None]`** — line 65; function; public/exported. Detect alternate arrival from the VWC trace itself.
- **`build_irrigation_arrival_times(df_15min: pd.DataFrame, event_results: pd.DataFrame, response_threshold_vwc: float=ARRIVAL_RESPONSE_THRESHOLD_VWC, hours_before: float=EVENT_PLOT_HOURS_BEFORE, hours_after: float=EVENT_PLOT_HOURS_AFTER) -> pd.DataFrame`** — line 130; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/diagnostics.py`

- **`_first_existing_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str \| None`** — line 30; function; internal. Return the first candidate column present in a DataFrame.
- **`_normalize_event_id(series: pd.Series) -> pd.Series`** — line 40; function; internal. Normalize event IDs for reliable joins.
- **`build_photo_recorded_timestamp_audit(irrigation_events: pd.DataFrame, photo_events: pd.DataFrame, *, year: int \| None=None, review_threshold_min: float=PHOTO_TIMESTAMP_REVIEW_THRESHOLD_MIN) -> pd.DataFrame`** — line 51; function; public/exported. Compare photo-derived irrigation timestamps with recorded event timestamps.
- **`build_photo_recorded_timestamp_audit.build_review_reason(row: pd.Series) -> str`** — line 325; nested function; public/exported. No docstring.
- **`_logger_distance_ft(logger_position: object) -> float \| None`** — line 400; function; internal. No docstring.
- **`add_vertical_velocity_fields(response_summary: pd.DataFrame) -> pd.DataFrame`** — line 406; function; public/exported. Add vertical wetting-front velocity estimates to logger-level response table. Units: inches per minute.
- **`build_irrigation_horizontal_advance_summary(arrival_times: pd.DataFrame) -> pd.DataFrame`** — line 443; function; public/exported. Build one row per event × strip × depth summarizing movement along the furrow.
- **`build_irrigation_horizontal_advance_summary._val(loc: str, key: str) -> float \| None`** — line 496; nested function; internal. No docstring.
- **`build_irrigation_horizontal_advance_summary._meta(key: str) -> object`** — line 501; nested function; internal. No docstring.
- **`build_irrigation_horizontal_advance_summary._delta_time(a: float \| None, b: float \| None) -> float \| None`** — line 519; nested function; internal. No docstring.
- **`build_irrigation_horizontal_advance_summary._velocity(d1: float \| None, d2: float \| None, t1: float \| None, t2: float \| None) -> float \| None`** — line 524; nested function; internal. No docstring.
- **`battery_col_for_sensor(sensor_col: str) -> str \| None`** — line 597; function; public/exported. No docstring.
- **`battery_window_summary(df_15min: pd.DataFrame, battery_col: str \| None, start: pd.Timestamp, end: pd.Timestamp, vmin_ok: float=BATTERY_MIN_OK, vmax_ok: float=BATTERY_MAX_OK) -> dict[str, object]`** — line 603; function; public/exported. No docstring.
- **`detect_pre_start_response(df_15min: pd.DataFrame, event_results: pd.DataFrame, lookback_hours: float=6.0, min_increase: float=0.5, precip_col: str='precip_in', min_precip_in: float=MIN_PRECIP_IN) -> pd.DataFrame`** — line 648; function; public/exported. No docstring.
- **`classify_trustworthy_irrigation_events(pre_start_table: pd.DataFrame, min_bottom_response_delay_hr: float=MIN_BOTTOM_RESPONSE_DELAY_HR) -> pd.DataFrame`** — line 860; function; public/exported. No docstring.
- **`build_arrival_order_diagnostics(arrival_times: pd.DataFrame) -> pd.DataFrame`** — line 950; function; public/exported. No docstring.
- **`build_arrival_order_diagnostics._as_float_or_none(value: object) -> float \| None`** — line 956; nested function; internal. No docstring.
- **`build_arrival_order_diagnostics._as_bool(value: object) -> bool`** — line 960; nested function; internal. No docstring.
- **`build_arrival_order_diagnostics._depth_order_class(v6: float \| None, v12: float \| None, v18: float \| None) -> str`** — line 972; nested function; internal. No docstring.
- **`build_arrival_order_diagnostics._logger_order_class(top: float \| None, middle: float \| None, bottom: float \| None) -> str`** — line 989; nested function; internal. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/diagnostics/analyze_post_irrigation_retention_statistics.py`

- **`print_section(title: str) -> None`** — line 100; function; public/exported. No docstring.
- **`bh_adjust(p_values: pd.Series) -> pd.Series`** — line 106; function; public/exported. Benjamini-Hochberg adjustment, preserving missing values and index.
- **`stratified_bootstrap_mean_ci(values: np.ndarray, years: np.ndarray) -> tuple[float, float]`** — line 120; function; public/exported. Resample events within each year, retaining observed year weights.
- **`make_matched_events(events: pd.DataFrame) -> pd.DataFrame`** — line 135; function; public/exported. No docstring.
- **`paired_summary(matched: pd.DataFrame) -> pd.DataFrame`** — line 162; function; public/exported. No docstring.
- **`fit_hc3_model(data: pd.DataFrame) -> dict[str, float] \| None`** — line 218; function; public/exported. Fit difference ~ centered precipitation + year FE with HC3 covariance.
- **`adjusted_models(matched: pd.DataFrame) -> pd.DataFrame`** — line 294; function; public/exported. No docstring.
- **`write_csv(df: pd.DataFrame, filename: str) -> None`** — line 330; function; public/exported. No docstring.
- **`main() -> None`** — line 336; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/diagnostics/inspect_post_irrigation_retention.py`

- **`print_section(title: str) -> None`** — line 218; function; public/exported. No docstring.
- **`require_columns(df: pd.DataFrame, columns: list[str], *, table_name: str) -> None`** — line 225; function; public/exported. No docstring.
- **`first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str \| None`** — line 244; function; public/exported. No docstring.
- **`write_csv(df: pd.DataFrame, filename: str) -> None`** — line 255; function; public/exported. No docstring.
- **`load_irrigation_events() -> pd.DataFrame`** — line 276; function; public/exported. No docstring.
- **`find_raw_logger_parquet(year: int) -> Path`** — line 517; function; public/exported. Locate the raw logger parquet for one year.
- **`load_logger_year(year: int) -> pd.DataFrame`** — line 568; function; public/exported. No docstring.
- **`find_weather_parquet(year: int) -> Path`** — line 648; function; public/exported. Locate the canonical processed 15-minute CoAgMet parquet.
- **`load_weather_year(year: int) -> pd.DataFrame`** — line 668; function; public/exported. Load cleaned precipitation increments; do not fetch external data.
- **`accumulated_precipitation(weather_df: pd.DataFrame, *, start: pd.Timestamp, end: pd.Timestamp) -> float`** — line 691; function; public/exported. Sum CoAgMet precipitation increments in the half-open [start, end).
- **`build_logger_profile_water(logger_df: pd.DataFrame, *, strip: str, logger_position: str) -> pd.DataFrame`** — line 712; function; public/exported. Calculate actual 0-18 inch profile water for one logger.
- **`build_strip_profile_water(logger_df: pd.DataFrame, *, strip: str) -> pd.DataFrame`** — line 846; function; public/exported. Build strip-level actual 0-18 inch soil-water storage.
- **`extract_event_retention_series(profile_df: pd.DataFrame, event: pd.Series) -> pd.DataFrame`** — line 972; function; public/exported. No docstring.
- **`nearest_checkpoint_row(event_df: pd.DataFrame, *, target_timestamp: pd.Timestamp) -> pd.Series \| None`** — line 1088; function; public/exported. No docstring.
- **`build_event_checkpoints(event_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame`** — line 1141; function; public/exported. No docstring.
- **`trapezoid_auc(hours: pd.Series, water: pd.Series) -> float`** — line 1408; function; public/exported. No docstring.
- **`build_event_summary(event_df: pd.DataFrame, checkpoints: pd.DataFrame, weather_df: pd.DataFrame) -> dict[str, object] \| None`** — line 1444; function; public/exported. No docstring.
- **`build_event_summary.cp_value(label: str, col: str) -> float`** — line 1498; nested function; public/exported. No docstring.
- **`treatment_name(strip: str) -> str`** — line 1676; function; public/exported. No docstring.
- **`build_pair_summary(event_summary: pd.DataFrame) -> pd.DataFrame`** — line 1689; function; public/exported. Compare matched biochar/control intervals.
- **`main() -> None`** — line 1888; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/diagnostics/inspect_water_balance_outputs.py`

- **`require_columns(df: pd.DataFrame, columns: list[str], *, table_name: str) -> None`** — line 152; function; public/exported. Raise a clear error if required columns are missing.
- **`numeric_if_present(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame`** — line 173; function; public/exported. Convert listed columns to numeric where present.
- **`round_numeric(df: pd.DataFrame, decimals: int=2) -> pd.DataFrame`** — line 191; function; public/exported. Round numeric reporting columns.
- **`print_section(title: str) -> None`** — line 210; function; public/exported. No docstring.
- **`bool_series_if_present(df: pd.DataFrame, column: str, *, default: bool=False) -> pd.Series`** — line 217; function; public/exported. Return a boolean Series for a column when present.
- **`load_water_balance() -> pd.DataFrame`** — line 248; function; public/exported. No docstring.
- **`build_overall_summary(df: pd.DataFrame) -> pd.DataFrame`** — line 308; function; public/exported. No docstring.
- **`build_strip_summary(df: pd.DataFrame) -> pd.DataFrame`** — line 374; function; public/exported. No docstring.
- **`build_zone_summary(df: pd.DataFrame) -> pd.DataFrame`** — line 445; function; public/exported. No docstring.
- **`build_arrival_summary(df: pd.DataFrame) -> pd.DataFrame`** — line 504; function; public/exported. No docstring.
- **`build_correlation_table(df: pd.DataFrame) -> pd.DataFrame`** — line 593; function; public/exported. No docstring.
- **`build_correlations_by_strip(df: pd.DataFrame) -> pd.DataFrame`** — line 647; function; public/exported. No docstring.
- **`build_largest_residuals(df: pd.DataFrame) -> pd.DataFrame`** — line 718; function; public/exported. No docstring.
- **`build_storage_exceeds_applied_events(df: pd.DataFrame) -> pd.DataFrame`** — line 770; function; public/exported. No docstring.
- **`build_incomplete_zone_events(df: pd.DataFrame) -> pd.DataFrame`** — line 812; function; public/exported. No docstring.
- **`build_negative_zone_storage_events(df: pd.DataFrame) -> pd.DataFrame`** — line 854; function; public/exported. No docstring.
- **`build_complete_zone_strip_summary(df: pd.DataFrame) -> pd.DataFrame`** — line 905; function; public/exported. No docstring.
- **`build_complete_vs_all_summary(all_summary: pd.DataFrame, complete_summary: pd.DataFrame) -> pd.DataFrame`** — line 983; function; public/exported. No docstring.
- **`build_consistency_checks(df: pd.DataFrame) -> pd.DataFrame`** — line 1098; function; public/exported. No docstring.
- **`write_csv(df: pd.DataFrame, filename: str) -> None`** — line 1194; function; public/exported. No docstring.
- **`main() -> None`** — line 1210; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/holding_capacity.py`

- **`build_event_storage_by_event(zone_df: pd.DataFrame) -> pd.DataFrame`** — line 72; function; public/exported. Build one whole-strip storage row per irrigation event.
- **`build_zone_storage_summary(zone_df: pd.DataFrame) -> pd.DataFrame`** — line 341; function; public/exported. Summarize event storage by year, strip, and logger influence zone.
- **`build_flow_storage_correlation_summary(zone_df: pd.DataFrame) -> pd.DataFrame`** — line 453; function; public/exported. Summarize the relationship between average irrigation flow rate and estimated 0-18 inch zone storage.
- **`build_zone_ordering_frequency(zone_df: pd.DataFrame) -> pd.DataFrame`** — line 538; function; public/exported. Summarize the frequency of relative top/middle/bottom zone-storage ordering across complete events.
- **`build_zone_ordering_frequency.ordering(row: pd.Series) -> str`** — line 586; nested function; public/exported. No docstring.
- **`build_zone_anomaly_diagnostics(zone_df: pd.DataFrame) -> pd.DataFrame`** — line 635; function; public/exported. Identify selected historical zone-storage patterns that were previously flagged for closer review.
- **`build_event_storage_by_zone(event_results: pd.DataFrame) -> pd.DataFrame`** — line 752; function; public/exported. Estimate 0-18 inch irrigation-induced soil-water storage by logger zone.
- **`add_response_delta_fields(df: pd.DataFrame) -> pd.DataFrame`** — line 1405; function; public/exported. Add simple response (peak - baseline) fields.
- **`build_first_pass_water_balance_table(trustworthy_table: pd.DataFrame, zone_storage_table: pd.DataFrame, arrival_times: pd.DataFrame) -> pd.DataFrame`** — line 1432; function; public/exported. Build a first-pass whole-strip irrigation water balance.
- **`build_first_pass_water_balance_table.combine_qc_reasons(values: pd.Series) -> str`** — line 1534; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary(water_balance: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]`** — line 1923; function; public/exported. Build compact biochar irrigation-performance summaries.
- **`build_biochar_performance_summary.numeric_values(frame: pd.DataFrame, column: str) -> pd.Series`** — line 2184; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.mean_value(frame: pd.DataFrame, column: str) -> float \| object`** — line 2201; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.median_value(frame: pd.DataFrame, column: str) -> float \| object`** — line 2217; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.pct_true(values: pd.Series) -> float \| object`** — line 2233; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.compact_round(frame: pd.DataFrame) -> pd.DataFrame`** — line 2248; nested function; public/exported. Apply reporting-specific rounding.
- **`build_biochar_performance_summary.numeric_values(frame: pd.DataFrame, column: str) -> pd.Series`** — line 3423; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.mean_value(frame: pd.DataFrame, column: str) -> float \| object`** — line 3440; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.median_value(frame: pd.DataFrame, column: str) -> float \| object`** — line 3456; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.pct_true(values: pd.Series) -> float \| object`** — line 3472; nested function; public/exported. No docstring.
- **`build_biochar_performance_summary.compact_round(frame: pd.DataFrame) -> pd.DataFrame`** — line 3487; nested function; public/exported. Apply reporting-specific rounding.
- **`summarize_holding_capacity_from_trustworthy_events(trustworthy_table: pd.DataFrame) -> pd.DataFrame`** — line 4411; function; public/exported. Estimate logger-location holding capacity from trustworthy irrigation events.
- **`summarize_holding_capacity_from_trustworthy_events.capacity_confidence(row: pd.Series) -> str`** — line 4551; nested function; public/exported. No docstring.
- **`build_trustworthy_holding_capacity_summary(trustworthy_table: pd.DataFrame, event_results: pd.DataFrame) -> pd.DataFrame`** — line 4597; function; public/exported. Summarize trustworthy bottom-logger irrigation responses by sensor depth.
- **`add_scaled_storage_fields(results: pd.DataFrame) -> pd.DataFrame`** — line 4900; function; public/exported. Add explicit layer-scale storage fields to irrigation event results.

### `biochar_app/scripts/management/irrigation_analysis/irrigation_response_analysis.py`

- **`build_bottom_control_sensor_map() -> dict[str, str]`** — line 121; function; public/exported. No docstring.
- **`build_bottom_logger_profile_map() -> dict[str, list[str]]`** — line 129; function; public/exported. No docstring.
- **`build_bottom_profile_sensor_map(depth_index: int=3) -> dict[str, str]`** — line 137; function; public/exported. No docstring.
- **`build_all_bottom_sensor_cols() -> list[str]`** — line 148; function; public/exported. No docstring.
- **`parse_vwc_sensor_column(sensor_col: str) -> SensorMeta`** — line 155; function; public/exported. No docstring.
- **`_build_profile_swc_gal_cols(strip: str, logger_position: str) -> list[str]`** — line 175; function; internal. Return legacy reference-cylinder SWC columns for compatibility.
- **`_build_profile_cs650_water_gal_cols(strip: str, logger_position: str) -> list[str]`** — line 184; function; internal. Return local-water columns based on the documented CS650 volume.
- **`_is_missing(value: object) -> bool`** — line 198; function; internal. No docstring.
- **`_as_float_or_none(value: object) -> Optional[float]`** — line 201; function; internal. No docstring.
- **`_coerce_optional_timestamp(value: object) -> Optional[pd.Timestamp]`** — line 207; function; internal. No docstring.
- **`_validate_datetime_index(df: pd.DataFrame) -> None`** — line 220; function; internal. No docstring.
- **`_coerce_datetime_column(events: pd.DataFrame, column: str) -> pd.Series`** — line 228; function; internal. No docstring.
- **`_hours_from_timedelta(td: pd.Timedelta) -> float`** — line 233; function; internal. No docstring.
- **`_infer_step_minutes(index: pd.DatetimeIndex) -> float`** — line 236; function; internal. No docstring.
- **`_run_lengths(mask: pd.Series) -> pd.Series`** — line 242; function; internal. No docstring.
- **`_group_iter(df: pd.DataFrame, group_cols: Sequence[str]) -> Iterable[tuple[Any, pd.DataFrame]]`** — line 247; function; internal. No docstring.
- **`_safe_value_at_timestamp(df: pd.DataFrame, timestamp: Optional[pd.Timestamp], column: str) -> Optional[float]`** — line 258; function; internal. No docstring.
- **`build_variable_definitions_table() -> pd.DataFrame`** — line 272; function; public/exported. No docstring.
- **`compute_event_storage_metrics(df: pd.DataFrame, sensor_meta: SensorMeta, baseline_time: Optional[pd.Timestamp], plateau_time: Optional[pd.Timestamp], gallons_strip: Optional[float]) -> dict[str, object]`** — line 465; function; public/exported. Build local sensor-volume diagnostics for an event.
- **`compute_event_storage_metrics.values_at(columns: Sequence[str], timestamp: Optional[pd.Timestamp]) -> list[float]`** — line 508; nested function; public/exported. No docstring.
- **`find_event_baseline(series: pd.Series, irrigation_start: pd.Timestamp, baseline_lookback_hours: float=2.0) -> tuple[Optional[float], Optional[pd.Timestamp]]`** — line 593; function; public/exported. No docstring.
- **`find_event_peak(series: pd.Series, irrigation_start: pd.Timestamp, peak_search_hours_after_start: float=24.0, min_peak_increase: float=0.5, baseline_vwc: Optional[float]=None) -> dict[str, object]`** — line 607; function; public/exported. No docstring.
- **`find_post_peak_plateau(series: pd.Series, peak_time: pd.Timestamp, config: PlateauConfig) -> dict[str, object]`** — line 638; function; public/exported. No docstring.
- **`analyze_single_event_sensor(df: pd.DataFrame, sensor_col: str, irrigation_start: pd.Timestamp, irrigation_end: Optional[pd.Timestamp]=None, gallons_strip: Optional[float]=None, gallons_group: Optional[float]=None, strip: Optional[str]=None, year: Optional[int]=None, event_id: Optional[object]=None, search_config: Optional[EventSearchConfig]=None, plateau_config: Optional[PlateauConfig]=None) -> dict[str, object]`** — line 713; function; public/exported. No docstring.
- **`analyze_irrigation_events(df: pd.DataFrame, events: pd.DataFrame, sensor_cols: Sequence[str], start_col: str='start', end_col: str='end', gallons_strip_col: str='gallons_strip', gallons_group_col: Optional[str]='gallons_group', strip: Optional[str]=None, year: Optional[int]=None, event_id_col: Optional[str]=None, search_config: Optional[EventSearchConfig]=None, plateau_config: Optional[PlateauConfig]=None, layer_thickness_inches: float=6.0) -> pd.DataFrame`** — line 855; function; public/exported. Analyze irrigation events for selected VWC sensors.
- **`analyze_irrigation_events.count_numeric_values(s: pd.Series) -> int`** — line 1024; nested function; public/exported. No docstring.
- **`analyze_bottom_logger_controls(df_15min: pd.DataFrame, strips: Sequence[str], year: int, strip_to_bottom_sensor: Optional[dict[str, str]]=None, search_config: Optional[EventSearchConfig]=None, plateau_config: Optional[PlateauConfig]=None) -> pd.DataFrame`** — line 1117; function; public/exported. No docstring.
- **`add_derived_event_fields(event_results: pd.DataFrame) -> pd.DataFrame`** — line 1198; function; public/exported. No docstring.
- **`build_event_debug_table(event_results: pd.DataFrame, decimals: int=2) -> pd.DataFrame`** — line 1244; function; public/exported. No docstring.
- **`estimate_statistical_target(event_results: pd.DataFrame, value_col: str='plateau_vwc', group_cols: Optional[Sequence[str]]=None, target_config: Optional[TargetConfig]=None) -> pd.DataFrame`** — line 1293; function; public/exported. No docstring.
- **`recommend_runtime_from_history(event_results: pd.DataFrame, target_time_col: str='time_to_plateau_hours', group_cols: Optional[Sequence[str]]=None, min_events: int=3, summary_stat: str='median') -> pd.DataFrame`** — line 1344; function; public/exported. No docstring.
- **`summarize_targets_and_runtimes(event_results: pd.DataFrame, group_cols: Sequence[str]=('strip', 'sensor_col'), min_events: int=3, k_std: float=0.5, runtime_summary_stat: str='median') -> tuple[pd.DataFrame, pd.DataFrame]`** — line 1395; function; public/exported. No docstring.
- **`build_depth_target_runtime_summary(event_results: pd.DataFrame, min_events: int=3, k_std: float=0.5) -> pd.DataFrame`** — line 1419; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/plotting.py`

- **`_depth_color_for_sensor(sensor_col: str) -> str`** — line 54; function; internal. No docstring.
- **`_is_missing(value: object) -> bool`** — line 61; function; internal. No docstring.
- **`_as_float_or_none(value: object) -> float \| None`** — line 64; function; internal. No docstring.
- **`_safe_filename(text: str) -> str`** — line 70; function; internal. No docstring.
- **`_fmt1(value: object) -> str`** — line 75; function; internal. No docstring.
- **`_fmt_event_id(value: object) -> str`** — line 81; function; internal. No docstring.
- **`coerce_optional_timestamp(value: object) -> Optional[pd.Timestamp]`** — line 99; function; public/exported. No docstring.
- **`_datetime_index_to_mpl_nums(index: pd.Index) -> np.ndarray`** — line 112; function; internal. No docstring.
- **`_timestamp_to_mpl_num(ts: pd.Timestamp) -> float`** — line 117; function; internal. No docstring.
- **`_get_strip_volume_and_flow(first_row: pd.Series) -> tuple[object, object, object]`** — line 120; function; internal. No docstring.
- **`_get_plot_window_series(df: pd.DataFrame, sensor_col: str, irrigation_start: pd.Timestamp, hours_before: float, hours_after: float) -> pd.Series`** — line 139; function; internal. No docstring.
- **`_prepare_plot_window_df(df: pd.DataFrame, start: pd.Timestamp \| str, end: pd.Timestamp \| str) -> pd.DataFrame`** — line 152; function; internal. No docstring.
- **`_collect_multidepth_cols(strip: str, logger_position: str='B', depths: Sequence[int]=(1, 2, 3)) -> list[tuple[str, str]]`** — line 160; function; internal. No docstring.
- **`_event_id_mask(series: pd.Series, event_id: object) -> pd.Series`** — line 176; function; internal. No docstring.
- **`_event_label_for_filename(event_id: object, irrigation_start: pd.Timestamp, strip: str) -> str`** — line 181; function; internal. No docstring.
- **`compute_event_plot_ylim(df: pd.DataFrame, event_results: pd.DataFrame, hours_before: float=6.0, hours_after: float=36.0, strip_filter: Optional[Sequence[str]]=None, sensor_filter: Optional[Sequence[str]]=None, pad_fraction: float=0.05) -> tuple[float, float] \| None`** — line 190; function; public/exported. No docstring.
- **`plot_irrigation_event_inspection(df: pd.DataFrame, event_row: pd.Series, output_path: Optional[str \| Path]=None, hours_before: float=6.0, hours_after: float=36.0, show: bool=False, y_limits: Optional[tuple[float, float]]=None, precip_col: Optional[str]='precip_in') -> None`** — line 248; function; public/exported. No docstring.
- **`save_irrigation_event_inspection_plots(df: pd.DataFrame, event_results: pd.DataFrame, output_dir: str \| Path, hours_before: float=6.0, hours_after: float=36.0, strip_filter: Optional[Sequence[str]]=None, sensor_filter: Optional[Sequence[str]]=None, max_plots: Optional[int]=None, skip_no_peak: bool=False, use_common_y_axis: bool=True, precip_col: Optional[str]='precip_in') -> pd.DataFrame`** — line 440; function; public/exported. No docstring.
- **`plot_event_multidepth(df: pd.DataFrame, cols: Sequence[tuple[str, str]], start: pd.Timestamp \| str, end: pd.Timestamp \| str, event_id: Optional[object]=None, strip: Optional[str]=None, logger_position: Optional[str]=None, year: Optional[int]=None, irrigation_start: Optional[pd.Timestamp \| str]=None, irrigation_end: Optional[pd.Timestamp \| str]=None, peaks: Optional[Mapping[str, pd.Timestamp \| str]]=None, baselines: Optional[Mapping[str, pd.Timestamp \| str]]=None, plateaus: Optional[Mapping[str, pd.Timestamp \| str]]=None, arrivals: Optional[Mapping[str, pd.Timestamp \| str]]=None, alt_arrivals: Optional[Mapping[str, pd.Timestamp \| str]]=None, output_path: Optional[str \| Path]=None, show: bool=False, precip_col: Optional[str]='precip_in', y_limits: Optional[tuple[float, float]]=None, title_prefix: str='Event Multi-depth VWC', response_threshold: float=ARRIVAL_RESPONSE_THRESHOLD_VWC, workbook_start_flow_gpm: Optional[float]=None, workbook_end_flow_gpm: Optional[float]=None, calculated_avg_flow_gpm_group: Optional[float]=None, flow_rate_comparison_status: Optional[str]=None) -> None`** — line 523; function; public/exported. No docstring.
- **`plot_event_multidepth.flow_text(value: Optional[float]) -> str`** — line 834; nested function; public/exported. No docstring.
- **`plot_event_multidepth_from_results(df: pd.DataFrame, event_results: pd.DataFrame, strip: str, event_id: object, logger_position: str='B', depths: Sequence[int]=(1, 2, 3), hours_before: float=12.0, hours_after: float=30.0, output_path: Optional[str \| Path]=None, show: bool=False, precip_col: Optional[str]='precip_in', y_limits: Optional[tuple[float, float]]=None) -> None`** — line 898; function; public/exported. No docstring.
- **`save_irrigation_event_multidepth_plots(df: pd.DataFrame, event_results: pd.DataFrame, output_dir: str \| Path, strip_filter: Optional[Sequence[str]]=None, event_ids: Optional[Sequence[object]]=None, logger_position: str='B', depths: Sequence[int]=(1, 2, 3), hours_before: float=12.0, hours_after: float=30.0, max_plots: Optional[int]=None, precip_col: Optional[str]='precip_in', use_common_y_axis: bool=True) -> pd.DataFrame`** — line 1037; function; public/exported. No docstring.
- **`save_failed_event_pair_qc_plots(df: pd.DataFrame, trustworthy_table: pd.DataFrame, output_dir: str \| Path, hours_before: float=6.0, hours_after: float=24.0) -> pd.DataFrame`** — line 1254; function; public/exported. Write paired-strip T/M/B comparison figures for failed QC events.
- **`plot_mean_storage_depth_by_zone_by_year(zone_df: pd.DataFrame, HOLDING_CAPACITY_DIR: Path) -> None`** — line 1344; function; public/exported. No docstring.
- **`plot_mean_storage_by_zone(zone_df: pd.DataFrame, HOLDING_CAPACITY_DIR: Path) -> None`** — line 1387; function; public/exported. No docstring.
- **`plot_mean_storage_by_zone_by_year(zone_df: pd.DataFrame, HOLDING_CAPACITY_DIR: Path) -> None`** — line 1449; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/reporting.py`

- **`validate_reporting_inputs() -> None`** — line 37; function; public/exported. Verify that the selected analysis variant has reporting inputs.
- **`_fmt_minutes(value: object) -> str`** — line 60; function; internal. No docstring.
- **`_fmt_datetime(value: object) -> str`** — line 65; function; internal. No docstring.
- **`_fmt_bool_flag(value: object) -> bool`** — line 75; function; internal. No docstring.
- **`add_arrival_definitions_section(doc: Document) -> None`** — line 84; function; public/exported. No docstring.
- **`build_arrival_diagnostics_report(year: int, include_expected: bool=False, output_path: Optional[str \| Path]=None) -> Path`** — line 126; function; public/exported. No docstring.
- **`build_holding_capacity_report() -> None`** — line 335; function; public/exported. No docstring.
- **`build_trustworthy_events_report() -> None`** — line 338; function; public/exported. No docstring.
- **`main() -> None`** — line 341; function; public/exported. No docstring.

### `biochar_app/scripts/management/irrigation_analysis/utils.py`

- **`force_float(df: pd.DataFrame) -> pd.DataFrame`** — line 18; function; public/exported. No docstring.
- **`move_id_columns_left(df: pd.DataFrame) -> pd.DataFrame`** — line 24; function; public/exported. No docstring.
- **`round_for_reporting(df: pd.DataFrame) -> pd.DataFrame`** — line 39; function; public/exported. No docstring.
- **`build_bottom_logger_profile_map() -> dict[str, list[str]]`** — line 105; function; public/exported. No docstring.
- **`prepare_15min_logger_data(year: int, VERBOSE=None) -> pd.DataFrame`** — line 111; function; public/exported. No docstring.
- **`add_derived_event_fields(event_results: pd.DataFrame) -> pd.DataFrame`** — line 140; function; public/exported. No docstring.
- **`attach_event_metadata(results: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame`** — line 183; function; public/exported. No docstring.
- **`add_flow_rate_comparison_fields(df: pd.DataFrame) -> pd.DataFrame`** — line 246; function; public/exported. Compare workbook boundary GPM readings with calculated event-average GPM.
- **`add_flow_rate_comparison_fields.numeric(column: str) -> pd.Series`** — line 258; nested function; public/exported. No docstring.
- **`load_irrigation_photo_review(year: int) -> pd.DataFrame`** — line 312; function; public/exported. No docstring.

### `biochar_app/scripts/management/management_db.py`

- **`utc_now_iso() -> str`** — line 11; function; public/exported. Return current UTC time in ISO format.
- **`get_connection() -> sqlite3.Connection`** — line 15; function; public/exported. Get SQLite connection, creating the parent directory if needed.
- **`initialize_management_db() -> None`** — line 22; function; public/exported. Create management tables if they do not exist.
- **`insert_irrigation_event(row: dict[str, Any]) -> None`** — line 62; function; public/exported. Insert a new irrigation event.
- **`update_irrigation_event(event_id: str, updates: dict[str, Any]) -> None`** — line 82; function; public/exported. Update an existing irrigation event.
- **`get_irrigation_event(event_id: str) -> dict[str, Any] \| None`** — line 101; function; public/exported. Fetch a single irrigation event.
- **`list_irrigation_events(limit: int=100) -> list[dict[str, Any]]`** — line 111; function; public/exported. List recent irrigation events.
- **`delete_irrigation_event(event_id: str) -> None`** — line 126; function; public/exported. Delete an irrigation event.

### `biochar_app/scripts/management/management_routes.py`

- **`_year_from_timestamp(ts: str) -> int`** — line 40; function; internal. No docstring.
- **`_date_from_timestamp(ts: str) -> str`** — line 43; function; internal. No docstring.
- **`_normalize_strip_group(value: str) -> str`** — line 46; function; internal. No docstring.
- **`_location_for_group(strip_group: str, fallback: str) -> str`** — line 56; function; internal. No docstring.
- **`_compute_total_meter_gallons(start_totalizer_gal_x100: Optional[float], end_totalizer_gal_x100: Optional[float]) -> Optional[float]`** — line 63; function; internal. No docstring.
- **`_compute_gallons_group(total_meter_gallons: Optional[float], flow_allocation_fraction: float \| int \| str \| None) -> Optional[float]`** — line 82; function; internal. No docstring.
- **`_compute_avg_flow_gpm_group(start_flow_gpm: Optional[float], end_flow_gpm: Optional[float], flow_allocation_fraction: float \| int \| str \| None) -> Optional[float]`** — line 99; function; internal. No docstring.
- **`startup_management_db() -> None`** — line 121; async function; public/exported. No docstring.
- **`start_irrigation_event(req: StartIrrigationRequest)`** — line 125; async function; public/exported. No docstring.
- **`finish_irrigation_event(event_id: str, req: FinishIrrigationRequest)`** — line 155; async function; public/exported. No docstring.
- **`api_list_irrigation_events(limit: int=100)`** — line 207; async function; public/exported. No docstring.
- **`api_get_irrigation_event(event_id: str)`** — line 211; async function; public/exported. No docstring.
- **`_photo_path(event_id: str, photo_type: str, original_filename: str) -> Path`** — line 224; function; internal. No docstring.
- **`upload_irrigation_photo(event_id: str, photo_type: str, file: UploadFile=File(...))`** — line 240; async function; public/exported. No docstring.

### `biochar_app/scripts/management/update_master_workbook_snapshot.py`

- **`require_onedrive_desktop_app() -> None`** — line 67; function; public/exported. Stop when the macOS OneDrive synchronization application is not running.
- **`sha256_file(path: Path) -> str`** — line 105; function; public/exported. Return the SHA-256 digest of a file.
- **`normalize_sheet_name(name: str) -> str`** — line 116; function; public/exported. Normalize harmless worksheet-name whitespace for validation.
- **`validate_workbook(path: Path, required_sheets: tuple[str, ...]) -> dict[str, Any]`** — line 126; function; public/exported. Validate that ``path`` is an XLSX workbook with required worksheets.
- **`write_audit(audit_path: Path, audit: dict[str, Any]) -> None`** — line 210; function; public/exported. Write the audit JSON atomically.
- **`update_snapshot(*, source: Path, destination: Path, required_sheets: tuple[str, ...], audit_path: Path, validate_only: bool) -> dict[str, Any]`** — line 241; function; public/exported. Copy, validate, and optionally install the master-workbook snapshot.
- **`parse_args() -> argparse.Namespace`** — line 351; function; public/exported. Parse command-line arguments.
- **`main() -> None`** — line 406; function; public/exported. Run the master-workbook snapshot update.

### `biochar_app/scripts/plot_builder.py`

- **`round_axis_limit(value: float, step: float) -> float`** — line 86; function; public/exported. Round a positive axis limit up to the next multiple of `step`.
- **`bad_request(msg: str) -> None`** — line 94; function; public/exported. No docstring.
- **`coerce_unit_system(unit_system: str) -> UnitSystem`** — line 97; function; public/exported. No docstring.
- **`_depth_color(depth_key: str) -> Optional[str]`** — line 105; function; internal. No docstring.
- **`_plot_margin(preset: str) -> dict[str, int]`** — line 108; function; internal. No docstring.
- **`_ensure_timestamp_datetime(df_in: pd.DataFrame) -> pd.DataFrame`** — line 116; function; internal. No docstring.
- **`_x_time_strings(df: pd.DataFrame) -> list[str]`** — line 122; function; internal. No docstring.
- **`prepare_plot_for_json(fig: go.Figure) -> dict[str, Any]`** — line 128; function; public/exported. No docstring.
- **`_compact_unit_phrase(label: str) -> str`** — line 132; function; internal. No docstring.
- **`_depth_display_label(depth_key: str \| int, usys: UnitSystem, *, compact: bool=False) -> str`** — line 144; function; internal. No docstring.
- **`_logger_display_label(logger_location: str) -> str`** — line 152; function; internal. No docstring.
- **`_normalize_trace_grouping(trace_option: str) -> str`** — line 155; function; internal. No docstring.
- **`build_raw_plot_title(*, granularity: str, human_var: str, strip: str, year: int, trace_option: str, logger_location: str, depth: str \| int, unit_system: str, is_gseason: bool=False) -> str`** — line 159; function; public/exported. No docstring.
- **`build_ratio_plot_title(*, granularity: str, variable: str, logger_location: str, depth: str \| int, unit_system: str, year: int, is_gseason: bool=False, no_data: bool=False) -> str`** — line 184; function; public/exported. No docstring.
- **`add_precipitation_bars(fig: go.Figure, df: pd.DataFrame, unit_system: str, granularity: str, precipitation_axis_max_in: float=DAILY_PRECIPITATION_MAX_IN) -> None`** — line 211; function; public/exported. No docstring.
- **`add_irrigation_shapes(fig: go.Figure, strip: str, year: int, unit_system: str, sum_only: bool=False, periods: Optional[Sequence[Any]]=None, category_labels: Optional[list[str]]=None) -> None`** — line 337; function; public/exported. No docstring.
- **`configure_primary_yaxis(fig: go.Figure, df: pd.DataFrame, y_cols: list[str], variable: str, unit_system: str, kind: str) -> None`** — line 455; function; public/exported. No docstring.
- **`make_raw_figure(*, df: pd.DataFrame, variable: str, strip: str, logger_location: str, depth: str, unit_system: str, year: int, granularity: str, start: str, end: str, trace_option: str) -> dict[str, Any]`** — line 498; function; public/exported. No docstring.
- **`make_raw_figure.swc_from_vwc(series: pd.Series, depth_key: str) -> pd.Series`** — line 531; nested function; public/exported. No docstring.
- **`make_ratio_figure(df: pd.DataFrame, variable: str, strip: str, logger_location: str, unit_system: str, granularity: str, year: int, start: str, end: str, depth: str) -> dict[str, Any]`** — line 682; function; public/exported. No docstring.
- **`make_temperature_delta_figure(df: pd.DataFrame, depth: int, logger_location: str, unit_system: str, granularity: str, year: int, start: str, end: str) -> dict[str, Any]`** — line 824; function; public/exported. No docstring.
- **`make_raw_gseason_figure(*, df: pd.DataFrame, periods: list[Any], variable: str, strip: str, logger_location: str, depth: int, unit_system: str, year: int, trace_option: str) -> dict[str, Any]`** — line 939; function; public/exported. No docstring.
- **`make_ratio_gseason_figure(*, df: pd.DataFrame, periods: list[Any], variable: str, strip: str, logger_location: str, depth: int, unit_system: str, year: int) -> dict[str, Any]`** — line 1154; function; public/exported. No docstring.

### `biochar_app/scripts/plot_components.py`

- **`bad_request(msg: str) -> None`** — line 62; function; public/exported. No docstring.
- **`sanitize_json(obj: Any) -> Any`** — line 65; function; public/exported. No docstring.
- **`compute_global_min_max(df: pd.DataFrame, cols: list[str]) -> tuple[float, float]`** — line 97; function; public/exported. No docstring.
- **`common_xaxis_config(_granularity: str, start: str, end: str) -> dict[str, Any]`** — line 118; function; public/exported. No docstring.
- **`common_yaxis_config(kind: str, variable: str, unit_system: UnitSystem, global_min: Optional[float], global_max: Optional[float], padding_fraction: float=0.05) -> dict[str, Any]`** — line 191; function; public/exported. No docstring.
- **`common_yaxis2_config(unit_system: UnitSystem='us') -> dict[str, Any]`** — line 258; function; public/exported. No docstring.
- **`common_legend_config(title: str) -> dict`** — line 278; function; public/exported. No docstring.
- **`get_unit_aware_label(variable: str, unit_system: UnitSystem) -> str`** — line 295; function; public/exported. No docstring.
- **`convert_units(df: pd.DataFrame, unit_system: UnitSystem) -> pd.DataFrame`** — line 310; function; public/exported. No docstring.
- **`parse_sensor_column(col: str, unit_system: UnitSystem) -> dict[str, str]`** — line 328; function; public/exported. No docstring.
- **`_get_irrigation_workbook() -> Optional[pd.ExcelFile]`** — line 358; function; internal. Legacy workbook helper retained for diagnostics only.
- **`_clean_irrigation_column(name: str) -> str`** — line 385; function; internal. No docstring.
- **`_find_irrigation_sheet_name(xls: pd.ExcelFile, year: int) -> Optional[str]`** — line 398; function; internal. No docstring.
- **`_load_irrigation_sheet(year: int) -> pd.DataFrame`** — line 406; function; internal. Legacy workbook loader retained for diagnostics only.
- **`_safe_datestr(val: Any) -> str`** — line 459; function; internal. No docstring.
- **`_reconcile_irrigation_volume_with_acre_ft(df: pd.DataFrame, year: int, group: str, volume_col: str='gallons_group', acreft_col: str='acre_ft', rel_tol: float=0.15) -> None`** — line 475; function; internal. Diagnostic helper for checking a group-level gallons column against acre-ft.
- **`load_irrigation_events(strip: str, year: int) -> pd.DataFrame`** — line 561; function; public/exported. Return strip-level irrigation events for one strip and year.

### `biochar_app/scripts/pull_leaves.py`

- **`floor_to_15min(ts: datetime) -> datetime`** — line 49; function; public/exported. No docstring.
- **`to_iso(ts: datetime) -> str`** — line 53; function; public/exported. No docstring.
- **`parse_iso(ts: str) -> datetime`** — line 56; function; public/exported. No docstring.
- **`compute_window(now: datetime, last_seen: Optional[datetime], hours: float) -> tuple[datetime, datetime]`** — line 60; function; public/exported. No docstring.
- **`acquire_lock(lock_path: Path) -> int`** — line 72; function; public/exported. No docstring.
- **`release_lock(fd: int, lock_path: Path) -> None`** — line 81; function; public/exported. No docstring.
- **`load_state(path: Path) -> dict[str, dict[str, str]]`** — line 92; function; public/exported. No docstring.
- **`save_state(path: Path, state: dict[str, dict[str, str]]) -> None`** — line 97; function; public/exported. No docstring.
- **`pick_timestamp_column(df: pd.DataFrame) -> Optional[str]`** — line 103; function; public/exported. fetch_leaf_records.py output may vary; we try common possibilities.
- **`coerce_timestamp_series(s: pd.Series) -> pd.Series`** — line 112; function; public/exported. No docstring.
- **`append_new_rows(*, leaf_id: int, table: str, pulled_csv: Path, ingest_csv: Path, last_seen: Optional[datetime], start: datetime, end: datetime) -> IngestResult`** — line 123; function; public/exported. No docstring.
- **`run_fetch_leaf_records(*, host: str, port: int, router: int, src: int, leaf: int, table: str, num: int, output_csv: Path) -> None`** — line 168; function; public/exported. No docstring.
- **`parse_leaves(spec: str) -> list[int]`** — line 197; function; public/exported. No docstring.
- **`main(argv: Optional[list[str]]=None) -> int`** — line 203; function; public/exported. No docstring.

### `biochar_app/scripts/readme_builders.py`

- **`build_plot_download_readme(*, download_type: str, year: int, variable: str, strip: str, granularity: str, unit_system: str, logger_location: str, trace_option: str, depth: str) -> str`** — line 43; function; public/exported. No docstring.
- **`load_readme_fragment(name: str) -> str`** — line 157; function; public/exported. No docstring.
- **`format_resolution_label(resolution: str) -> str`** — line 163; function; public/exported. No docstring.
- **`build_nir_reference_note() -> str`** — line 169; function; public/exported. No docstring.
- **`_join_readme_lines(lines: list[str]) -> str`** — line 183; function; internal. No docstring.
- **`_normalize_unit_system(unit_system: str='us') -> str`** — line 186; function; internal. No docstring.
- **`_unit_system_label(unit_system: str='us') -> str`** — line 192; function; internal. No docstring.
- **`build_download_header(*, title: str, year: int, variable: str, strip: str, granularity: str, unit_system: str, extra_lines: list[str] \| None=None) -> str`** — line 199; function; public/exported. No docstring.
- **`_detect_year_span(df: pd.DataFrame) -> str`** — line 226; function; internal. No docstring.
- **`_dataset_summary(df: pd.DataFrame) -> str`** — line 256; function; internal. No docstring.
- **`build_project_reference_note() -> str`** — line 262; function; public/exported. No docstring.
- **`build_depth_codes_section(unit_system: str='us') -> str`** — line 275; function; public/exported. No docstring.
- **`build_logger_location_codes_section() -> str`** — line 283; function; public/exported. No docstring.
- **`build_logger_variable_codes_section() -> str`** — line 289; function; public/exported. No docstring.
- **`build_strip_codes_section() -> str`** — line 295; function; public/exported. No docstring.
- **`build_weather_variable_codes_section() -> str`** — line 305; function; public/exported. No docstring.
- **`build_logger_units_section(unit_system: str='us') -> str`** — line 317; function; public/exported. No docstring.
- **`build_weather_units_section(unit_system: str='us') -> str`** — line 349; function; public/exported. No docstring.
- **`build_management_units_section(dataset: str, unit_system: str='us') -> str`** — line 369; function; public/exported. No docstring.
- **`build_units_text(dataset: str, unit_system: str='us') -> str`** — line 395; function; public/exported. No docstring.
- **`build_logger_column_naming_section(unit_system: str='us') -> str`** — line 409; function; public/exported. No docstring.
- **`_example_columns(cols: list[str], max_columns: int=4) -> str`** — line 448; function; internal. No docstring.
- **`_variable_line(label: str, cols: list[str], units: str='') -> list[str]`** — line 461; function; internal. No docstring.
- **`_glossary_definition_for_key(key: str) -> str`** — line 471; function; internal. No docstring.
- **`_variable_line_from_glossary(label: str, cols: list[str], glossary_key: str, units: str='') -> list[str]`** — line 478; function; internal. No docstring.
- **`build_logger_variable_section(df: pd.DataFrame, unit_system: str='us') -> str`** — line 500; function; public/exported. No docstring.
- **`build_timeseries_yearly_readme(*, dataset: str, year: int, resolution: str, notes: str='', df: Optional[pd.DataFrame]=None, units_text: str='', unit_system: str='us', season_periods_text: str='') -> str`** — line 625; function; public/exported. No docstring.
- **`build_management_readme(*, dataset: str, dataset_label: str, df: pd.DataFrame, unit_system: str='us') -> str`** — line 729; function; public/exported. No docstring.
- **`build_soilchem_readme(dataset_label: str, df: pd.DataFrame) -> str`** — line 788; function; public/exported. No docstring.
- **`build_soilbio_readme(dataset_label: str, df: pd.DataFrame) -> str`** — line 824; function; public/exported. No docstring.
- **`build_hay_readme(dataset_label: str, df: pd.DataFrame) -> str`** — line 862; function; public/exported. No docstring.
- **`build_hay_variable_section(df: pd.DataFrame) -> str`** — line 897; function; public/exported. No docstring.
- **`build_hay_variable_section.present(names: list[str]) -> list[str]`** — line 900; nested function; public/exported. No docstring.
- **`build_soilchem_variable_section(df: pd.DataFrame) -> str`** — line 1006; function; public/exported. No docstring.
- **`build_soilchem_variable_section.present(names: list[str]) -> list[str]`** — line 1009; nested function; public/exported. No docstring.
- **`build_soilbio_variable_section(df: pd.DataFrame) -> str`** — line 1199; function; public/exported. No docstring.
- **`build_soilbio_variable_section.present(names: list[str]) -> list[str]`** — line 1202; nested function; public/exported. No docstring.
- **`build_experiment_lookup_section(unit_system: str='us') -> str`** — line 1333; function; public/exported. No docstring.
- **`build_generic_file_readme(dataset_label: str, df: pd.DataFrame) -> str`** — line 1346; function; public/exported. No docstring.
- **`build_file_dataset_readme(dataset_key: str, dataset_label: str, df: pd.DataFrame) -> str`** — line 1378; function; public/exported. No docstring.
- **`build_variable_section(df: pd.DataFrame, *, max_terms: int=40, max_columns_per_term: int=4) -> str`** — line 1394; function; public/exported. No docstring.
- **`_display_glossary_term(entry: dict[str, Any]) -> str`** — line 1476; function; internal. No docstring.
- **`_normalize_match_text(value: str) -> str`** — line 1484; function; internal. No docstring.
- **`load_glossary_entries() -> list[dict[str, Any]]`** — line 1489; function; public/exported. No docstring.
- **`_entry_matches_column(entry: dict[str, Any], column_name: str) -> bool`** — line 1515; function; internal. No docstring.

### `biochar_app/scripts/replay_collect.py`

- **`deframe_all(buf: bytes)`** — line 6; function; public/exported. No docstring.
- **`list_collect_requests(stream: bytes)`** — line 19; function; public/exported. No docstring.
- **`decode_table1_row(data: bytes)`** — line 39; function; public/exported. Heuristic: find 10 big-endian floats aligned within the first 16 bytes.
- **`find_pakbus_time_1990(data: bytes, search_window=64)`** — line 52; function; public/exported. Try to find (secs, nsecs) since 1990-01-01. Only accept if 2014 <= year <= (now + 1 day).
- **`last_timestamp_from_dat(dat_path: pathlib.Path)`** — line 71; function; public/exported. Return the last timestamp (UTC) found in a CRBasic .dat file, or None.
- **`align_to_interval(base_dt: datetime, interval_min: int)`** — line 112; function; public/exported. No docstring.
- **`send_and_read(ipv6_host: str, port: int, req_inner: bytes, wait_s: float=8.0)`** — line 121; function; public/exported. No docstring.
- **`main()`** — line 143; function; public/exported. No docstring.

### `biochar_app/scripts/routes.py`

- **`get_latest_ward_html(pattern: str) -> Path`** — line 137; function; public/exported. No docstring.
- **`get_latest_ward_pdf(pattern: str) -> Path`** — line 143; function; public/exported. No docstring.
- **`_spec_dicts_to_objs(specs: list[dict[str, Any]]) -> list[Any]`** — line 185; function; internal. tables_lab.py expects each variable spec to have .key .label .candidates. In table_specs.py we store dicts, so convert dict -> SimpleNamespace.
- **`_normalize_sheet_name(s: str) -> str`** — line 203; function; internal. No docstring.
- **`_clean_for_json(obj: Any) -> Any`** — line 206; function; internal. No docstring.
- **`_ensure_year_allowed(year: int) -> None`** — line 217; function; internal. No docstring.
- **`_normalize_unit_system(raw: Any) -> UnitSystem`** — line 221; function; internal. Narrow arbitrary user input to UnitSystem = Literal["us","metric"].
- **`_round_ratio_columns(df: pd.DataFrame, decimals: int=6) -> pd.DataFrame`** — line 228; function; internal. No docstring.
- **`_select_trace_columns(df: pd.DataFrame, variable: str, strip: str, depth: str, logger_location: str, trace_option: str, kind: str) -> pd.DataFrame`** — line 235; function; internal. No docstring.
- **`_select_trace_columns.is_ratio_col(col: str) -> bool`** — line 264; nested function; public/exported. No docstring.
- **`_add_unit_suffixes_for_download(df: pd.DataFrame, variable: str) -> pd.DataFrame`** — line 311; function; internal. No docstring.
- **`get_bulk_download_options()`** — line 341; async function; public/exported. No docstring.
- **`download_loggers_zip(year: int)`** — line 366; async function; public/exported. No docstring.
- **`download_weather_zip(year: int)`** — line 376; async function; public/exported. No docstring.
- **`download_irrigation_zip(year: int)`** — line 386; async function; public/exported. No docstring.
- **`download_soil_chem_zip(year: int)`** — line 396; async function; public/exported. No docstring.
- **`download_soil_bio_zip(year: int)`** — line 406; async function; public/exported. No docstring.
- **`download_biomass_zip(year: int)`** — line 416; async function; public/exported. No docstring.
- **`_find_sheet_for_year(xlsx_path: Path \| str, base_sheet: str, year: int) -> Optional[str]`** — line 432; function; internal. No docstring.
- **`_load_ancillary_df_for_year(xlsx_path: Path \| str, dataset_key: str, year: int) -> pd.DataFrame`** — line 458; function; internal. No docstring.
- **`_build_ancillary_zip_bytes(xlsx_path: Path \| str, dataset_key: str, year: int) -> bytes`** — line 483; function; internal. No docstring.
- **`_ancillary_available_for_year(xlsx_path: Path \| str, dataset_key: str, year: int) -> bool`** — line 532; function; internal. No docstring.
- **`get_markdown_files()`** — line 543; async function; public/exported. No docstring.
- **`get_defaults_and_options()`** — line 548; async function; public/exported. No docstring.
- **`api_plot_raw(req: PlotRequest)`** — line 646; async function; public/exported. No docstring.
- **`api_plot_ratio(req: PlotRequest)`** — line 743; async function; public/exported. No docstring.
- **`api_get_summary_stats(payload: dict[str, Any]=Body(...))`** — line 812; async function; public/exported. No docstring.
- **`api_get_summary_stats._clean(obj: Any) -> Any`** — line 829; nested function; internal. No docstring.
- **`api_download_summary_data(req: DownloadSummaryDataRequest)`** — line 996; async function; public/exported. No docstring.
- **`api_download_summary_data._stats_dict_to_df(stats: Any) -> pd.DataFrame`** — line 1008; nested function; internal. No docstring.
- **`api_get_soilbio_table()`** — line 1111; async function; public/exported. No docstring.
- **`api_get_soilchem_table()`** — line 1116; async function; public/exported. No docstring.
- **`api_get_nir_table()`** — line 1121; async function; public/exported. No docstring.
- **`api_get_nir_table._coerce_to_set(obj: dict[str, Any], fallback_key: str, fallback_label: str) -> dict[str, Any]`** — line 1127; nested function; internal. No docstring.
- **`serve_markdown(filename: str)`** — line 1175; async function; public/exported. No docstring.
- **`custom_gseason(request: Request)`** — line 1184; async function; public/exported. No docstring.
- **`api_bulk_download_manifest()`** — line 1197; async function; public/exported. No docstring.
- **`api_bulk_download(req: BulkDownloadRequest)`** — line 1202; async function; public/exported. No docstring.
- **`api_get_biomass_field_table()`** — line 1228; async function; public/exported. No docstring.
- **`_download_depth_lookup_text(unit_system: str='us') -> str`** — line 1232; function; internal. No docstring.
- **`api_download_plot_data(req: DownloadDataRequest)`** — line 1241; async function; public/exported. No docstring.
- **`api_get_lab_table(table_key: str)`** — line 1445; async function; public/exported. No docstring.
- **`ward_guide()`** — line 1470; async function; public/exported. No docstring.
- **`soil_health_guide()`** — line 1475; async function; public/exported. No docstring.
- **`ward_biological_report()`** — line 1480; async function; public/exported. No docstring.
- **`ward_biological_report_pdf()`** — line 1485; async function; public/exported. No docstring.
- **`biochar_lab_report(report_key: str, download: bool=False)`** — line 1495; async function; public/exported. Serve one explicitly configured biochar analysis report.
- **`ward_nirs_report()`** — line 1518; async function; public/exported. No docstring.
- **`ward_soil_sha_report()`** — line 1523; async function; public/exported. No docstring.
- **`irrigation_entry_page(request: Request)`** — line 1528; async function; public/exported. No docstring.

### `biochar_app/scripts/routes_smoke_check.py`

- **`client()`** — line 5; function; public/exported. No docstring.
- **`test_home_route(client)`** — line 10; function; public/exported. No docstring.
- **`test_plot_raw_route(client)`** — line 16; function; public/exported. No docstring.
- **`test_plot_raw_route_bad_input(client)`** — line 29; function; public/exported. No docstring.
- **`test_plot_ratio_route(client)`** — line 38; function; public/exported. No docstring.
- **`test_plot_ratio_route_bad_input(client)`** — line 50; function; public/exported. No docstring.

### `biochar_app/scripts/routes_utils.py`

- **`load_summary_df(year: int, granularity: str, variable: str, strip: str) -> pd.DataFrame`** — line 28; function; public/exported. No docstring.
- **`merge_all_loggers(year: int) -> pd.DataFrame`** — line 33; function; public/exported. No docstring.
- **`load_gseason_df(year: int, periods: Any, unit_system: str='us', use_ratios: bool=False) -> pd.DataFrame`** — line 51; function; public/exported. Load growing-season aggregated data for `year`.
- **`restrict_to_year(df: pd.DataFrame, year: int) -> pd.DataFrame`** — line 118; function; public/exported. No docstring.

### `biochar_app/scripts/tables/table_metadata_helpers.py`

- **`metadata_label(key: str, fallback: str \| None=None) -> str`** — line 8; function; public/exported. No docstring.
- **`metadata_note(key: str, fallback: str \| None=None) -> str`** — line 12; function; public/exported. No docstring.

### `biochar_app/scripts/tables/tables_common.py`

- **`_dedupe_note(group_note: str, top_note: str) -> str`** — line 24; function; internal. Return group_note unless it is empty or matches top_note (after strip).
- **`make_set(*, key: str, label: str, payload: dict[str, Any], top_note: str='', group_note: str='', display_label: Optional[str]=None) -> dict[str, Any]`** — line 36; function; public/exported. Standardize a single set object.
- **`build_grouped_tab_payload(*, title: str, top_note: str, groups: Sequence[dict[str, Any]], build_payload_for_group: Callable[[dict[str, Any]], dict[str, Any]], include_display_labels: bool=False) -> dict[str, Any]`** — line 69; function; public/exported. Build a tab payload with a shared top-level note and grouped sets.
- **`build_variable_meta(var_spec: Any) -> dict[str, Any]`** — line 116; function; public/exported. Build a standard variable metadata payload, including optional Ward reference info.

### `biochar_app/scripts/tables/tables_lab.py`

- **`normalize_strip(value: Any) -> str`** — line 15; function; public/exported. Normalize strip variants to 'STRIP 1'..'STRIP 4'.
- **`coerce_date_to_iso(series: pd.Series) -> pd.Series`** — line 48; function; public/exported. No docstring.
- **`choose_first_present(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]`** — line 52; function; public/exported. No docstring.
- **`_payload_template(label: str, note: str='') -> dict[str, Any]`** — line 63; function; internal. No docstring.
- **`build_lab_table_payload_long(df: pd.DataFrame, *, set_label: str, set_note: str, row_key: str, period_key: str, variable_specs: Sequence[Any], normalize_row_as_strip: bool=False) -> dict[str, Any]`** — line 79; function; public/exported. Convert a LONG-ish lab dataset into the wide-table payload: rows => entity (strip or location) columns => sampling event dates (ISO) values => each variable
- **`build_lab_table_payload_wide(df: pd.DataFrame, *, set_label: str, set_note: str, row_key: str, normalize_row_as_strip: bool=False, wide_variable_key: str='value') -> dict[str, Any]`** — line 156; function; public/exported. Wide dataset: first column = row_key remaining columns = event dates (often m/d/yy) single “variable” (e.g., biomass dry grams)
- **`_load_csv(path: Path) -> pd.DataFrame`** — line 224; function; internal. No docstring.
- **`build_lab_table(tab_key: str) -> dict[str, Any]`** — line 229; function; public/exported. Generic builder for ALL lab-based wide tables.

### `biochar_app/scripts/tables/tables_nir.py`

- **`_normalize_colname(s: str) -> str`** — line 363; function; internal. No docstring.
- **`_pick_first_existing(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]`** — line 366; function; internal. No docstring.
- **`_parse_date_any(x: Any) -> Optional[pd.Timestamp]`** — line 381; function; internal. No docstring.
- **`_clean_strip_id(sample_id: Any) -> Optional[str]`** — line 391; function; internal. No docstring.
- **`_canonicalize_strip_value(x: Any) -> Optional[str]`** — line 413; function; internal. No docstring.
- **`_ensure_strip_and_date_columns(df: pd.DataFrame, source_name: str) -> pd.DataFrame`** — line 435; function; internal. No docstring.
- **`safe_ratio(numer: Optional[float], denom: Optional[float]) -> Optional[float]`** — line 465; function; public/exported. No docstring.
- **`_filter_period_years(df: pd.DataFrame) -> pd.DataFrame`** — line 470; function; internal. No docstring.
- **`load_ward_master_csv(ward_master_csv: Path) -> pd.DataFrame`** — line 483; function; public/exported. No docstring.
- **`load_single_event_csv(event_path: Path) -> pd.DataFrame`** — line 532; function; public/exported. No docstring.
- **`_build_period_list(df: pd.DataFrame) -> list[dict[str, str]]`** — line 551; function; internal. No docstring.
- **`_build_nir_table_payload(df: pd.DataFrame, variables: Sequence[LabVarSpec], extra_event_csvs: Optional[list[Path]]=None) -> dict[str, Any]`** — line 570; function; internal. No docstring.
- **`build_nir_set1_table(ward_master_csv: Path, extra_event_csvs: Optional[list[Path]]=None) -> dict[str, Any]`** — line 649; function; public/exported. No docstring.
- **`build_nir_set2_table(ward_master_csv: Path, extra_event_csvs: Optional[list[Path]]=None) -> dict[str, Any]`** — line 653; function; public/exported. No docstring.
- **`build_nir_set3_table(ward_master_csv: Path, extra_event_csvs: Optional[list[Path]]=None) -> dict[str, Any]`** — line 657; function; public/exported. No docstring.
- **`build_nir_set4_table(ward_master_csv: Path, extra_event_csvs: Optional[list[Path]]=None) -> dict[str, Any]`** — line 661; function; public/exported. No docstring.
- **`build_nir_tables(ward_master_csv: Path=WARD_MASTER_NIR_CSV, extra_event_csvs: Optional[list[Path]]=None) -> dict[str, Any]`** — line 665; function; public/exported. Build the full NIR payload (title + 4 sets) from the authoritative Ward master CSV.

### `biochar_app/scripts/tables/tables_soil_bio.py`

- **`_normalize_strip(value: Any) -> str`** — line 452; function; internal. Normalize common strip variants to the dashboard-standard form: strip_1..strip_4
- **`_coerce_numeric(value: Any) -> Optional[float]`** — line 472; function; internal. Convert Ward-style numeric strings to float.
- **`_parse_date_iso(value: Any) -> str`** — line 502; function; internal. No docstring.
- **`_normalize_raw_header(header: Any) -> str`** — line 510; function; internal. Normalize raw Ward headers so small punctuation/spacing differences do not break matching.
- **`_build_raw_header_lookup(columns: Iterable[Any]) -> dict[str, str]`** — line 520; function; internal. Build normalized_header -> actual_header lookup. If duplicates normalize to the same key, the first one wins.
- **`_find_actual_raw_col(raw_lookup: dict[str, str], aliases: Sequence[str]) -> Optional[str]`** — line 532; function; internal. Find the actual raw column name for one cleaned field.
- **`_first_matching_raw_value(row: pd.Series, raw_lookup: dict[str, str], aliases: Sequence[str]) -> Any`** — line 575; function; internal. Return the first raw value whose alias exists in the raw file.
- **`_backfill_blank_strips(clean_df: pd.DataFrame) -> pd.DataFrame`** — line 588; function; internal. Fix existing cleaned rows that have blank strip values.
- **`_infer_supplemental_raw_path(clean_csv: Path) -> Optional[Path]`** — line 612; function; internal. Try to infer a matching raw Ward file from the cleaned filename.
- **`_convert_raw_bio_to_clean_shape(raw_csv: Path, clean_columns: Iterable[str]) -> pd.DataFrame`** — line 644; function; internal. Convert a raw Ward Biological_*.csv file into the cleaned machine-readable schema.
- **`_prepare_soilbio_csv(clean_csv: Path, output_csv: Path, supplemental_raw_csv: Optional[Path]=None) -> Path`** — line 695; function; internal. Create a prepared soil-bio CSV for table rendering.
- **`build_soilbio_table(clean_csv: Path, min_year: int=2023, supplemental_raw_csv: Optional[Path]=None) -> dict[str, Any]`** — line 750; function; public/exported. Build the soil biology tab payload.
- **`build_soilbio_table._builder(grp: dict[str, Any]) -> dict[str, Any]`** — line 766; nested function; internal. No docstring.

### `biochar_app/scripts/tables/tables_soil_chem.py`

- **`build_soilchem_table(clean_csv: Path, min_year: int=2023) -> dict[str, Any]`** — line 412; function; public/exported. No docstring.
- **`build_soilchem_table._builder(grp: dict[str, Any]) -> dict[str, Any]`** — line 413; nested function; internal. No docstring.

### `biochar_app/scripts/tables/tables_soil_common.py`

- **`_norm(s: Any) -> str`** — line 51; function; internal. No docstring.
- **`_keyify(s: str) -> str`** — line 54; function; internal. Make a stable key from a label.
- **`_is_missing(v: Any) -> bool`** — line 69; function; internal. No docstring.
- **`_to_float(v: Any) -> Optional[float]`** — line 78; function; internal. Convert to float if possible; otherwise None.
- **`_is_numeric_value(v: Any) -> bool`** — line 100; function; internal. No docstring.
- **`_normalize_strip(x: Any) -> str`** — line 103; function; internal. Normalize strip identifiers to STRIP 1..4 when possible. Assumes cleaned master may already use values like "strip_1", "S1", etc.
- **`require_date_rec(df: pd.DataFrame, source_name: str) -> None`** — line 127; function; public/exported. No docstring.
- **`_parse_date_rec(df: pd.DataFrame) -> pd.DataFrame`** — line 134; function; internal. Keep original date_rec string for payload keys, but also create a parsed datetime column for sorting/filtering.
- **`read_clean_master_csv(csv_path: Path, source_name: str) -> pd.DataFrame`** — line 182; function; public/exported. No docstring.
- **`_pick_first_existing_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]`** — line 216; function; internal. Prefer exact candidate matches, but allow a case-insensitive fallback.
- **`_aggregate_value(series: pd.Series) -> Any`** — line 238; function; internal. Preserve text if unique; average numeric if multiple numeric values; else None.
- **`_compute_ratio(numer: Any, denom: Any) -> Optional[float]`** — line 273; function; internal. No docstring.
- **`build_soil_table_payload(clean_csv: Path, variables: Sequence[VariableSpec], min_year: int=2023, include_ratio_rows: bool=True) -> dict[str, Any]`** — line 283; function; public/exported. Build the standard soil table payload.

### `biochar_app/scripts/type_utils.py`

- **`df_cols(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame`** — line 26; function; public/exported. Always return a DataFrame (helps df[cols] stub issues). Using .loc keeps this stable and type checkers like it.
- **`to_float_series(s: Any) -> pd.Series`** — line 33; function; public/exported. Best-effort numeric conversion to a *pandas Series[float]*.
- **`safe_tolist(x: Any) -> list[Any]`** — line 53; function; public/exported. Convert series/array/scalar to JSON-safe list without Optional issues.
- **`finite_min_max(block: pd.DataFrame) -> tuple[Optional[float], Optional[float]]`** — line 73; function; public/exported. Scalar min/max ignoring NaN/inf.
- **`safe_timestamp(value: Any) -> Optional[pd.Timestamp]`** — line 104; function; public/exported. Return scalar Timestamp or None; avoids container types and type unions.
- **`agg(self, spec: Any) -> Any`** — line 142; function; public/exported. No docstring.
- **`df_agg(obj: Any, spec: AggDict) -> pd.DataFrame`** — line 144; function; public/exported. Typed wrapper around obj.agg(spec) for DataFrame/Resampler/GroupBy.
- **`gb_agg(gb: Any, spec: AggDict) -> pd.DataFrame`** — line 150; function; public/exported. Typed wrapper around groupby.agg(spec).

### `biochar_app/scripts/utils/compare_photo_directories.py`

- **`build_index(directory: Path)`** — line 26; function; public/exported. No docstring.
- **`human_size(size)`** — line 34; function; public/exported. No docstring.

### `biochar_app/scripts/utils/compare_photo_hashes.py`

- **`sha256(path: Path, chunk_size: int=1024 * 1024) -> str`** — line 36; function; public/exported. No docstring.
- **`image_files(directory: Path) -> list[Path]`** — line 46; function; public/exported. No docstring.
- **`build_hash_index(paths: list[Path]) -> dict[str, list[Path]]`** — line 54; function; public/exported. No docstring.

### `biochar_app/scripts/utils/type_coercion.py`

- **`coerce_optional_timestamp(value: object) -> Optional[pd.Timestamp]`** — line 7; function; public/exported. Convert a scalar-like value to pd.Timestamp.
- **`coerce_optional_float(value: object) -> Optional[float]`** — line 26; function; public/exported. Convert a scalar-like value to float.
- **`coerce_optional_int(value: object) -> Optional[int]`** — line 40; function; public/exported. Convert a scalar-like value to int.

### `biochar_app/scripts/weather_runtime.py`

- **`load_weather_year(year: int) -> pd.DataFrame`** — line 9; function; public/exported. Load one year of weather, normalized, with both inch/mm + °F/°C.
- **`load_weather_range(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame`** — line 33; function; public/exported. No docstring.

### `biochar_app/tests/playwright_smoke.py`

- **`local_server_for_pytest()`** — line 18; function; public/exported. Ensure the local FastAPI server is running when tests are launched by pytest. When this file is run directly as a script, the __main__ block handles server startup/shutdown. When PyCharm or pytest runs test_* functions directly, this fixture handles it instead.
- **`is_server_running(host: str='127.0.0.1', port: int=8000) -> bool`** — line 37; function; public/exported. Return True if the local FastAPI server is already accepting connections.
- **`ensure_server_running() -> subprocess.Popen \| None`** — line 46; function; public/exported. Start the local FastAPI server if it is not already running.
- **`new_page_with_console_capture(p: Playwright)`** — line 84; function; public/exported. No docstring.
- **`assert_no_console_errors(console_errors: list[str]) -> None`** — line 100; function; public/exported. No docstring.
- **`assert_filename_contains(filename: str, expected_parts: list[str]) -> None`** — line 107; function; public/exported. No docstring.
- **`download_filename(download) -> str`** — line 119; function; public/exported. No docstring.
- **`assert_download_exists(download) -> Path`** — line 125; function; public/exported. No docstring.
- **`open_bulk_downloads_tab(page) -> None`** — line 135; function; public/exported. No docstring.
- **`test_home_page() -> None`** — line 140; function; public/exported. No docstring.
- **`test_soil_biology_bulk_download() -> None`** — line 154; function; public/exported. No docstring.
- **`test_logger_bulk_download() -> None`** — line 188; function; public/exported. No docstring.
- **`test_plot_raw_download() -> None`** — line 222; function; public/exported. No docstring.

### `biochar_app/tests/test_apply_duplicate_actions.py`

- **`sha256(content: bytes) -> str`** — line 17; function; public/exported. No docstring.
- **`inventory_row(relative_path: str, action: str, digest: str, *, group: str='duplicate_0001', exact: str='TRUE') -> dict[str, str]`** — line 21; function; public/exported. No docstring.
- **`write_inventory(tmp_path, rows: list[dict[str, str]]) -> pd.DataFrame`** — line 38; function; public/exported. No docstring.
- **`test_valid_group_verifies_keep_and_delete_files(tmp_path) -> None`** — line 44; function; public/exported. No docstring.
- **`test_rejects_group_without_exactly_one_keep(tmp_path) -> None`** — line 68; function; public/exported. No docstring.
- **`test_rejects_path_that_escapes_photo_directory(tmp_path) -> None`** — line 82; function; public/exported. No docstring.
- **`test_rejects_hash_mismatch(tmp_path) -> None`** — line 91; function; public/exported. No docstring.
- **`test_rejects_missing_retained_copy(tmp_path) -> None`** — line 101; function; public/exported. No docstring.

### `biochar_app/tests/test_biochar_lab_reports.py`

- **`test_configured_biochar_lab_reports_exist_and_are_pdfs() -> None`** — line 8; function; public/exported. No docstring.
- **`test_biochar_subtab_and_report_links_are_present() -> None`** — line 21; function; public/exported. No docstring.

### `biochar_app/tests/test_build_irrigation_from_master.py`

- **`event(*, strip_group: str, reported_gallons: float \| None, source_row: int) -> dict[str, object]`** — line 37; function; public/exported. No docstring.
- **`BuildIrrigationFromMasterTests.test_shared_meter_event_expands_to_four_strip_rows(self) -> None`** — line 76; method; public/exported. No docstring.
- **`BuildIrrigationFromMasterTests.test_single_group_event_uses_full_meter_volume(self) -> None`** — line 121; method; public/exported. No docstring.
- **`BuildIrrigationFromMasterTests.test_missing_totalizer_uses_allocated_group_fallback(self) -> None`** — line 143; method; public/exported. No docstring.
- **`BuildIrrigationFromMasterTests.test_missing_volume_event_is_excluded_for_review(self) -> None`** — line 169; method; public/exported. No docstring.
- **`BuildIrrigationFromMasterTests.test_event_id_is_deterministic(self) -> None`** — line 191; method; public/exported. No docstring.

### `biochar_app/tests/test_build_meter_review_workbook.py`

- **`MeterReviewWorkbookTests.test_load_metadata_preserves_leading_zero_reading(self) -> None`** — line 32; method; public/exported. No docstring.
- **`MeterReviewWorkbookTests.test_correction_populates_new_photo(self) -> None`** — line 50; method; public/exported. No docstring.
- **`MeterReviewWorkbookTests.test_workbook_contains_preserved_fields_and_sha256(self) -> None`** — line 82; method; public/exported. No docstring.
- **`MeterReviewWorkbookTests.test_counts_existing_six_digit_readings(self) -> None`** — line 138; method; public/exported. No docstring.

### `biochar_app/tests/test_bulk_download_current_data.py`

- **`BulkDownloadCurrentDataTests.test_biomass_hay_bundle_contains_two_distinct_datasets(self) -> None`** — line 22; method; public/exported. No docstring.
- **`BulkDownloadCurrentDataTests.test_plot_irrigation_source_includes_august_14_event(self) -> None`** — line 45; method; public/exported. No docstring.
- **`LoggerRatioBulkDownloadTests.test_logger_bundle_contains_ratio_csv_when_parquet_exists(self) -> None`** — line 52; async method; public/exported. No docstring.

### `biochar_app/tests/test_compare_meter_photos_to_irrigation.py`

- **`test_loads_only_usable_canonical_inventory_rows(tmp_path) -> None`** — line 12; function; public/exported. No docstring.
- **`test_legacy_clean_photo_schema_remains_supported(tmp_path) -> None`** — line 66; function; public/exported. No docstring.

### `biochar_app/tests/test_convert_word_to_html.py`

- **`test_table_cell_images_become_numbered_figures_with_source_links() -> None`** — line 9; function; public/exported. No docstring.

### `biochar_app/tests/test_etl_cs650_sensing_volume_water.py`

- **`test_calculates_water_from_vwc_and_documented_sensing_volume() -> None`** — line 17; function; public/exported. No docstring.
- **`test_skips_sensor_combinations_without_a_vwc_column() -> None`** — line 34; function; public/exported. No docstring.
- **`test_coerces_non_numeric_vwc_values_to_missing() -> None`** — line 45; function; public/exported. No docstring.
- **`test_copy_option_controls_whether_input_is_mutated() -> None`** — line 55; function; public/exported. No docstring.

### `biochar_app/tests/test_etl_logger_clock_corrections.py`

- **`LoggerClockCorrectionTests.test_s3m_offsets_are_absolute_states_not_cumulative(self) -> None`** — line 22; method; public/exported. No docstring.
- **`LoggerClockCorrectionTests.test_s3m_2026_clock_reset_ends_the_manual_offset(self) -> None`** — line 50; method; public/exported. No docstring.
- **`LoggerClockCorrectionTests.test_latest_s4b_state_replaces_prior_states(self) -> None`** — line 74; method; public/exported. No docstring.
- **`LoggerClockCorrectionTests.test_s2t_2026_mst_sync_ends_the_manual_offset(self) -> None`** — line 82; method; public/exported. No docstring.
- **`LoggerClockCorrectionTests.test_summer_standard_time_is_converted_to_denver_daylight_time(self) -> None`** — line 112; method; public/exported. No docstring.

### `biochar_app/tests/test_finalize_meter_photo_inventory.py`

- **`write_csv(path: Path, rows: list[dict[str, object]]) -> None`** — line 25; function; public/exported. No docstring.
- **`make_main(path: Path) -> None`** — line 32; function; public/exported. No docstring.
- **`make_followup(path: Path) -> None`** — line 43; function; public/exported. No docstring.
- **`FinalizeInventoryTests.test_precedence_and_unique_selection(self) -> None`** — line 61; method; public/exported. No docstring.
- **`FinalizeInventoryTests.test_conflicting_readable_main_family_is_not_propagated(self) -> None`** — line 120; method; public/exported. No docstring.

### `biochar_app/tests/test_irrigation_figure_cleanup_and_depth_context.py`

- **`FlowRateComparisonTests.test_large_boundary_difference_is_flagged(self) -> None`** — line 32; method; public/exported. No docstring.
- **`FlowRateComparisonTests.test_normal_boundary_variation_is_not_flagged(self) -> None`** — line 48; method; public/exported. No docstring.
- **`UnretainedWaterTests.test_complete_profile_reports_short_unretained_fields(self) -> None`** — line 65; method; public/exported. No docstring.
- **`UnretainedWaterTests.test_incomplete_profile_does_not_inflate_unretained_water(self) -> None`** — line 85; method; public/exported. No docstring.
- **`UnretainedWaterTests.test_unretained_water_does_not_require_bottom_arrival(self) -> None`** — line 101; method; public/exported. No docstring.
- **`IrrigationFigureCleanupTests.test_failed_event_plot_compares_both_strips_and_three_loggers(self) -> None`** — line 135; method; public/exported. No docstring.
- **`IrrigationFigureCleanupTests.test_successful_build_removes_only_stale_year_figures(self) -> None`** — line 161; method; public/exported. No docstring.
- **`IrrigationFigureCleanupTests.test_failed_build_does_not_remove_stale_figures(self) -> None`** — line 188; method; public/exported. No docstring.
- **`ElevatedDepthContextTests.test_elevated_18in_baseline_is_context_not_order_override(self) -> None`** — line 211; method; public/exported. No docstring.
- **`ElevatedDepthContextTests.test_logger_position_order_classes_are_canonical(self) -> None`** — line 239; method; public/exported. No docstring.
- **`CombinedOutputTests.test_concat_drops_per_frame_all_na_columns_but_keeps_real_values(self) -> None`** — line 284; method; public/exported. No docstring.
- **`CombinedOutputTests.test_logger_order_summary_has_one_row_per_event(self) -> None`** — line 301; method; public/exported. No docstring.

### `biochar_app/tests/test_lab_etl_builders.py`

- **`_add_sheet(workbook: Workbook, name: str, year: int, old_layout: bool=False) -> None`** — line 26; function; internal. No docstring.
- **`LabEtlBuilderTests.test_biomass_preserves_history_and_adds_only_new_dates(self) -> None`** — line 41; method; public/exported. No docstring.
- **`LabEtlBuilderTests.test_2026_nir_file_uses_filename_sampling_date(self) -> None`** — line 73; method; public/exported. No docstring.
- **`LabEtlBuilderTests.test_nir_table_includes_latest_year_from_clean_master(self) -> None`** — line 83; method; public/exported. No docstring.

### `biochar_app/tests/test_plot_builder_precipitation_bars.py`

- **`PrecipitationBarWidthTests.setUp(self) -> None`** — line 16; method; public/exported. No docstring.
- **`PrecipitationBarWidthTests.precipitation_width(self, granularity: str) -> int`** — line 26; method; public/exported. No docstring.
- **`PrecipitationBarWidthTests.test_units_are_in_legend_instead_of_right_axis_title(self) -> None`** — line 37; method; public/exported. No docstring.
- **`PrecipitationBarWidthTests.test_15min_bar_uses_visible_30_minute_display_width(self) -> None`** — line 50; method; public/exported. No docstring.
- **`PrecipitationBarWidthTests.test_15minute_alias_uses_15min_width(self) -> None`** — line 56; method; public/exported. No docstring.
- **`PrecipitationBarWidthTests.test_hourly_bar_uses_one_hour_width(self) -> None`** — line 62; method; public/exported. No docstring.
- **`PrecipitationBarWidthTests.test_daily_bar_uses_half_day_width(self) -> None`** — line 68; method; public/exported. No docstring.

### `biochar_app/tests/test_project_inventories.py`

- **`test_deployment_requirement_keys_are_unique() -> None`** — line 21; function; public/exported. No docstring.
- **`test_git_deployment_requirements_pass_preflight() -> None`** — line 27; function; public/exported. No docstring.
- **`test_documentation_catalog_contains_operational_guides() -> None`** — line 45; function; public/exported. No docstring.
- **`test_function_catalog_is_searchable_and_parseable() -> None`** — line 52; function; public/exported. No docstring.

### `biochar_app/tests/test_update_master_workbook_snapshot.py`

- **`create_workbook(path: Path, sheet_names: tuple[str, ...]) -> None`** — line 55; function; public/exported. Create a small workbook containing exactly ``sheet_names``.
- **`MasterWorkbookSnapshotTests.test_onedrive_process_check_accepts_running_app(self, run_mock: Mock) -> None`** — line 80; method; public/exported. No docstring.
- **`MasterWorkbookSnapshotTests.test_onedrive_process_check_rejects_stopped_app(self, run_mock: Mock) -> None`** — line 104; method; public/exported. No docstring.
- **`MasterWorkbookSnapshotTests.test_onedrive_process_check_rejects_non_macos(self) -> None`** — line 121; method; public/exported. No docstring.
- **`MasterWorkbookSnapshotTests.test_validation_accepts_harmless_sheet_name_whitespace(self) -> None`** — line 125; method; public/exported. No docstring.
- **`MasterWorkbookSnapshotTests.test_validation_rejects_a_missing_required_sheet(self) -> None`** — line 150; method; public/exported. No docstring.
- **`MasterWorkbookSnapshotTests.test_validate_only_does_not_replace_destination(self) -> None`** — line 164; method; public/exported. No docstring.
- **`MasterWorkbookSnapshotTests.test_install_replaces_destination_with_identical_copy(self) -> None`** — line 192; method; public/exported. No docstring.

## JavaScript

### `biochar_app/diagnostics/debug_plotly_layout.js`

- **`rect(sel)`** — line 18; function; internal. No JSDoc summary.

### `biochar_app/markdown/chat_summaries/archive_md/pakbus_master.html`

- **`A(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`At(n,e,r,i)`** — line 30; function; internal. No JSDoc summary.
- **`B()`** — line 30; function; internal. No JSDoc summary.
- **`Ce(d,h,g,v,y,e)`** — line 30; function; internal. No JSDoc summary.
- **`De(e)`** — line 30; function; internal. No JSDoc summary.
- **`Ee(e)`** — line 30; function; internal. No JSDoc summary.
- **`Fe(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`Ft(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`G()`** — line 30; function; internal. No JSDoc summary.
- **`He(n,r,i,o)`** — line 30; function; internal. No JSDoc summary.
- **`I(e,t,n,r)`** — line 30; function; internal. No JSDoc summary.
- **`It(o)`** — line 30; function; internal. No JSDoc summary.
- **`Je(e,t,n)`** — line 30; function; internal. No JSDoc summary.
- **`Ke(e,t,n,r,i)`** — line 30; function; internal. No JSDoc summary.
- **`Le(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`M(e)`** — line 30; function; internal. No JSDoc summary.
- **`O(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`Oe(e,t,n)`** — line 30; function; internal. No JSDoc summary.
- **`Qe(e,t,n,r,i,o)`** — line 30; function; internal. No JSDoc summary.
- **`R(e)`** — line 30; function; internal. No JSDoc summary.
- **`Se(e,i,o)`** — line 30; function; internal. No JSDoc summary.
- **`Te(e,t,n,r,i)`** — line 30; function; internal. No JSDoc summary.
- **`U(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`We(e,t,n)`** — line 30; function; internal. No JSDoc summary.
- **`Wt(t,i,o,a)`** — line 30; function; internal. No JSDoc summary.
- **`X(e)`** — line 30; function; internal. No JSDoc summary.
- **`Ye(e,t,n)`** — line 30; function; internal. No JSDoc summary.
- **`Z(e,t,n)`** — line 30; function; internal. No JSDoc summary.
- **`at()`** — line 30; function; internal. No JSDoc summary.
- **`b(e,t,n)`** — line 30; function; internal. No JSDoc summary.
- **`be(s,e,t)`** — line 30; function; internal. No JSDoc summary.
- **`ce(e)`** — line 30; function; internal. No JSDoc summary.
- **`de(t)`** — line 30; function; internal. No JSDoc summary.
- **`e(t,n)`** — line 30; function; internal. No JSDoc summary.
- **`fe(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`ge(t)`** — line 30; function; internal. No JSDoc summary.
- **`gt(e)`** — line 30; function; internal. No JSDoc summary.
- **`he(n)`** — line 30; function; internal. No JSDoc summary.
- **`ht(e)`** — line 30; function; internal. No JSDoc summary.
- **`j(e,n,r)`** — line 30; function; internal. No JSDoc summary.
- **`je(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`l(i,o,a,s)`** — line 30; function; internal. No JSDoc summary.
- **`le(e)`** — line 30; function; internal. No JSDoc summary.
- **`lt(o,e,t)`** — line 30; function; internal. No JSDoc summary.
- **`me()`** — line 30; function; internal. No JSDoc summary.
- **`n(e)`** — line 30; function; internal. No JSDoc summary.
- **`ot()`** — line 30; function; internal. No JSDoc summary.
- **`p(e)`** — line 30; function; internal. No JSDoc summary.
- **`pe(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`qe(e)`** — line 30; function; internal. No JSDoc summary.
- **`se(t,e,n,r)`** — line 30; function; internal. No JSDoc summary.
- **`st(e,t)`** — line 30; function; internal. No JSDoc summary.
- **`t(e)`** — line 30; function; internal. No JSDoc summary.
- **`ue()`** — line 30; function; internal. No JSDoc summary.
- **`ut(e,t,n)`** — line 30; function; internal. No JSDoc summary.
- **`ve(a)`** — line 30; function; internal. No JSDoc summary.
- **`vt(e)`** — line 30; function; internal. No JSDoc summary.
- **`w(e)`** — line 30; function; internal. No JSDoc summary.
- **`we(i)`** — line 30; function; internal. No JSDoc summary.
- **`xe(e)`** — line 30; function; internal. No JSDoc summary.
- **`ye(e)`** — line 30; function; internal. No JSDoc summary.
- **`ze(e)`** — line 30; function; internal. No JSDoc summary.
- **`b()`** — line 40; function; internal. No JSDoc summary.
- **`c()`** — line 40; function; internal. No JSDoc summary.
- **`d(b)`** — line 40; function; internal. No JSDoc summary.
- **`b(b)`** — line 41; function; internal. No JSDoc summary.
- **`f()`** — line 41; function; internal. No JSDoc summary.
- **`c(a,b)`** — line 47; function; internal. No JSDoc summary.
- **`d()`** — line 47; function; internal. No JSDoc summary.
- **`e(a,b)`** — line 47; function; internal. No JSDoc summary.
- **`f(a)`** — line 47; function; internal. No JSDoc summary.
- **`g(a,c,d)`** — line 47; function; internal. No JSDoc summary.
- **`h(a,c)`** — line 47; function; internal. No JSDoc summary.
- **`i(a,b)`** — line 47; function; internal. No JSDoc summary.
- **`j(a)`** — line 47; function; internal. No JSDoc summary.
- **`b()`** — line 56; function; internal. No JSDoc summary.
- **`buildTabset(tabset)`** — line 120; function; internal. No JSDoc summary.
- **`bootstrapStylePandocTables()`** — line 564; function; internal. No JSDoc summary.

### `biochar_app/static/js/api_requests.js`

- **`capitalizeFirst(str)`** — line 39; function; internal. Simple helper to capitalize the first letter of a string.
- **`resolveUnitLabelStrict(labelEntry, unitSystem, fallback = "")`** — line 61; function; public/exported. Strict unit-aware label resolver. Accepts: - string - object like { us: "...", metric: "..." } Rules: - if object form is used, BOTH keys must exist - NO fallback to the other unit system - throws on invalid shape so we catch bugs early
- **`formatGseasonLabel(code, spec, prettyVarWithContext)`** — line 114; function; public/exported. Helper: format the accordion header title e.g. "Winter (Nov–Apr) — Volumetric Water Content (%)"
- **`fmtMonth(md)`** — line 126; function; internal. No JSDoc summary.
- **`fetchJson(url, init = {})`** — line 151; async function; public/exported. Fetch JSON with sensible defaults and clearer errors.
- **`generateSummaryTable(stats, variable, opts = {})`** — line 186; function; public/exported. Build a summary table from one of several supported summary-stat shapes.
- **`finish()`** — line 199; function; internal. No JSDoc summary.
- **`isPlainObject(v)`** — line 216; function; internal. No JSDoc summary.
- **`displayValue(v)`** — line 223; function; internal. No JSDoc summary.

### `biochar_app/static/js/config.js`

- **`fetchMarkdownFiles()`** — line 9; async function; public/exported. Fetch Markdown container-id → URL mapping from the backend. Source of truth is markdown_config.build_markdown_mapping() in Python.

### `biochar_app/static/js/control_panel.js`

- **`updateMainTraceControlState(traceOption)`** — line 34; function; internal. Keep both top-plot controls available. Current UI behavior: - "Top Plot grouped by" affects only how the TOP plot is grouped - Depth and Logger Location are both still meaningful selections overall because the ratio plot uses both selections So we no longer disable either control here.
- **`clearMainPlots()`** — line 59; function; internal. Clear stale plot output when an invalid date/filter cancels an update.
- **`initializeTraceOptionControls()`** — line 90; function; public/exported. Wire the main trace-option dropdown. We still react to changes so helper text / future logic can stay synced, but we no longer disable Depth or Logger Location.
- **`initializeUpdateButtons()`** — line 114; function; public/exported. Wire the "Update Plots" and "Update Summary" buttons.
- **`setupUnitToggleHandlers(initialUnitSystem)`** — line 157; function; public/exported. Configure the US/Metric toggle on both Main + Summary. Keeps them in sync and updates: - window.unitSystem - depth dropdown labels - plots - summary tables
- **`mirrorToggles(isMetric)`** — line 171; function; internal. No JSDoc summary.
- **`onToggleChange(event)`** — line 184; async function; internal. No JSDoc summary.
- **`triggerUpdates()`** — line 239; function; public/exported. Convenience helper used elsewhere to trigger both updates.
- **`getAllDropdownIds()`** — line 249; function; public/exported. The IDs we wait for before doing certain operations (like in waitForAllDropdowns).

### `biochar_app/static/js/custom_gseason.js`

- **`initCustomGseason(cfg)`** — line 14; function; public/exported. Initialize the “Custom Season” editor.
- **`formatDateLabel(dateString)`** — line 52; function; internal. No JSDoc summary.
- **`updateSeasonalPeriodSummary()`** — line 64; function; internal. No JSDoc summary.
- **`initPeriodsData()`** — line 94; function; internal. No JSDoc summary.
- **`renderPeriods()`** — line 115; function; internal. No JSDoc summary.

### `biochar_app/static/js/debug_utils.js`

- **`debugLog(...args)`** — line 7; function; public/exported. No JSDoc summary.
- **`debugGroup(title, callback)`** — line 16; function; public/exported. No JSDoc summary.

### `biochar_app/static/js/downloads.js`

- **`buildFilename(parts)`** — line 21; function; internal. No JSDoc summary.
- **`getFilenameFromContentDisposition(cd)`** — line 33; function; internal. No JSDoc summary.
- **`extFromContentType(ct)`** — line 55; function; internal. No JSDoc summary.
- **`postAndDownload(url, payload, fallbackFilename)`** — line 77; async function; internal. No JSDoc summary.
- **`downloadTraceData(kind = "all")`** — line 124; async function; public/exported. No JSDoc summary.
- **`downloadSummaryData(mode = "all")`** — line 185; async function; public/exported. No JSDoc summary.
- **`initSummaryDownloadMenu()`** — line 245; function; public/exported. No JSDoc summary.
- **`buildPlotFilename({ plotType, format })`** — line 291; function; internal. No JSDoc summary.
- **`getVal(id, fallback = "")`** — line 297; function; internal. No JSDoc summary.
- **`downloadPlot(target = "raw", format = "png", sizeMode = "screen")`** — line 344; async function; public/exported. No JSDoc summary.
- **`normalizeBulkDataset(uiDatasetRaw)`** — line 473; function; internal. No JSDoc summary.
- **`isAllYearsDataset(normalizedDataset)`** — line 485; function; internal. No JSDoc summary.
- **`allYearsManifestKey(normalizedDataset)`** — line 493; function; internal. No JSDoc summary.
- **`normalizeResolution(raw)`** — line 501; function; internal. No JSDoc summary.
- **`getManifestEntries(manifest)`** — line 538; function; internal. No JSDoc summary.
- **`findEntryByKey(manifest, key)`** — line 550; function; internal. No JSDoc summary.
- **`findManifestKey(manifest, { dataset, year, granularity })`** — line 561; function; internal. No JSDoc summary.
- **`selectHasRealOptions(el)`** — line 594; function; internal. No JSDoc summary.
- **`hasDatasetFamily(manifest, dataset)`** — line 605; function; internal. No JSDoc summary.
- **`selectedBulkUnitSystem()`** — line 622; function; internal. No JSDoc summary.
- **`initBulkDownloadTab()`** — line 636; async function; public/exported. No JSDoc summary.
- **`selectedYear()`** — line 660; function; internal. No JSDoc summary.
- **`selectedGranularity()`** — line 667; function; internal. No JSDoc summary.
- **`setButtonState(btn, { visualEnabled, hardDisable = false })`** — line 672; function; internal. No JSDoc summary.
- **`refreshEnabledState()`** — line 737; function; internal. No JSDoc summary.

### `biochar_app/static/js/glossary.js`

- **`glossaryDisplayTerm(entry)`** — line 59; function; internal. No JSDoc summary.
- **`normalizeSearchText(value)`** — line 69; function; internal. No JSDoc summary.
- **`searchableEntryText(entry)`** — line 77; function; internal. No JSDoc summary.
- **`escapeRegex(value)`** — line 94; function; internal. No JSDoc summary.
- **`clearHighlights(root)`** — line 98; function; internal. No JSDoc summary.
- **`highlightMatches(root, query)`** — line 105; function; internal. No JSDoc summary.
- **`relatedTermsText(allEntries, relatedKeys)`** — line 133; function; internal. No JSDoc summary.
- **`setEntryVisible(container, show)`** — line 148; function; internal. No JSDoc summary.
- **`loadGlossaryData()`** — line 155; async function; public/exported. No JSDoc summary.
- **`renderGlossary()`** — line 173; async function; public/exported. No JSDoc summary.
- **`buildGlossaryLookup()`** — line 379; async function; public/exported. No JSDoc summary.
- **`applyGlossaryTooltips(root = document)`** — line 407; async function; public/exported. No JSDoc summary.

### `biochar_app/static/js/irrigation_entry.js`

- **`mustGet(id)`** — line 32; function; internal. No JSDoc summary.
- **`input(id)`** — line 42; function; internal. No JSDoc summary.
- **`textarea(id)`** — line 50; function; internal. No JSDoc summary.
- **`select(id)`** — line 58; function; internal. No JSDoc summary.
- **`pad2(n)`** — line 66; function; internal. No JSDoc summary.
- **`setInputValue(id, value)`** — line 75; function; internal. No JSDoc summary.
- **`getInputValue(id)`** — line 85; function; internal. No JSDoc summary.
- **`fillPhoneTime(prefix)`** — line 94; function; internal. No JSDoc summary.
- **`normalizeDateValue(raw)`** — line 117; function; internal. No JSDoc summary.
- **`getTimestampFromParts(prefix)`** — line 154; function; internal. No JSDoc summary.
- **`setStatus(msg, kind = "info")`** — line 181; function; internal. No JSDoc summary.
- **`selectedGroups()`** — line 190; function; internal. No JSDoc summary.
- **`selectedFlowAllocationFraction()`** — line 206; function; internal. No JSDoc summary.
- **`nullableNumber(value)`** — line 216; function; internal. No JSDoc summary.
- **`loadActiveEvents()`** — line 226; function; internal. No JSDoc summary.
- **`saveActiveEvents(events)`** — line 237; function; internal. No JSDoc summary.
- **`saveFormState()`** — line 244; function; internal. No JSDoc summary.
- **`restoreFormState()`** — line 272; function; internal. No JSDoc summary.
- **`postJson(endpoint, payload)`** — line 311; async function; internal. No JSDoc summary.
- **`refreshRecentEvents()`** — line 326; async function; internal. No JSDoc summary.
- **`updateFlowAllocationUI()`** — line 377; function; internal. No JSDoc summary.
- **`uploadPhotoIfPresent(eventId, photoType, fileInput)`** — line 408; async function; internal. No JSDoc summary.
- **`startIrrigation()`** — line 429; async function; internal. No JSDoc summary.
- **`finishIrrigation()`** — line 503; async function; internal. No JSDoc summary.
- **`exportIrrigationCleanCsv()`** — line 566; async function; internal. No JSDoc summary.
- **`attachAutosave()`** — line 598; function; internal. No JSDoc summary.
- **`init()`** — line 634; function; internal. No JSDoc summary.

### `biochar_app/static/js/jquery.min.js`

- **`A(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`At(n,e,r,i)`** — line 2; function; internal. No JSDoc summary.
- **`B()`** — line 2; function; internal. No JSDoc summary.
- **`Ce(d,h,g,v,y,e)`** — line 2; function; internal. No JSDoc summary.
- **`De(e)`** — line 2; function; internal. No JSDoc summary.
- **`Ee(e)`** — line 2; function; internal. No JSDoc summary.
- **`Fe(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`Ft(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`G()`** — line 2; function; internal. No JSDoc summary.
- **`He(n,r,i,o)`** — line 2; function; internal. No JSDoc summary.
- **`I(e,t,n,r)`** — line 2; function; internal. No JSDoc summary.
- **`It(o)`** — line 2; function; internal. No JSDoc summary.
- **`Je(e,t,n)`** — line 2; function; internal. No JSDoc summary.
- **`Ke(e,t,n,r,i)`** — line 2; function; internal. No JSDoc summary.
- **`Le(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`M(e)`** — line 2; function; internal. No JSDoc summary.
- **`O(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`Oe(e,t,n)`** — line 2; function; internal. No JSDoc summary.
- **`Qe(e,t,n,r,i,o)`** — line 2; function; internal. No JSDoc summary.
- **`R(e)`** — line 2; function; internal. No JSDoc summary.
- **`Se(e,i,o)`** — line 2; function; internal. No JSDoc summary.
- **`Te(e,t,n,r,i)`** — line 2; function; internal. No JSDoc summary.
- **`U(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`We(e,t,n)`** — line 2; function; internal. No JSDoc summary.
- **`Wt(t,i,o,a)`** — line 2; function; internal. No JSDoc summary.
- **`X(e)`** — line 2; function; internal. No JSDoc summary.
- **`Ye(e,t,n)`** — line 2; function; internal. No JSDoc summary.
- **`Z(e,t,n)`** — line 2; function; internal. No JSDoc summary.
- **`at()`** — line 2; function; internal. No JSDoc summary.
- **`b(e,t,n)`** — line 2; function; internal. No JSDoc summary.
- **`be(s,e,t)`** — line 2; function; internal. No JSDoc summary.
- **`ce(e)`** — line 2; function; internal. No JSDoc summary.
- **`de(t)`** — line 2; function; internal. No JSDoc summary.
- **`e(t,n)`** — line 2; function; internal. No JSDoc summary.
- **`fe(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`ge(t)`** — line 2; function; internal. No JSDoc summary.
- **`gt(e)`** — line 2; function; internal. No JSDoc summary.
- **`he(n)`** — line 2; function; internal. No JSDoc summary.
- **`ht(e)`** — line 2; function; internal. No JSDoc summary.
- **`j(e,n,r)`** — line 2; function; internal. No JSDoc summary.
- **`je(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`l(i,o,a,s)`** — line 2; function; internal. No JSDoc summary.
- **`le(e)`** — line 2; function; internal. No JSDoc summary.
- **`lt(o,e,t)`** — line 2; function; internal. No JSDoc summary.
- **`me()`** — line 2; function; internal. No JSDoc summary.
- **`n(e)`** — line 2; function; internal. No JSDoc summary.
- **`ot()`** — line 2; function; internal. No JSDoc summary.
- **`p(e)`** — line 2; function; internal. No JSDoc summary.
- **`pe(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`qe(e)`** — line 2; function; internal. No JSDoc summary.
- **`se(t,e,n,r)`** — line 2; function; internal. No JSDoc summary.
- **`st(e,t)`** — line 2; function; internal. No JSDoc summary.
- **`t(e)`** — line 2; function; internal. No JSDoc summary.
- **`ue()`** — line 2; function; internal. No JSDoc summary.
- **`ut(e,t,n)`** — line 2; function; internal. No JSDoc summary.
- **`ve(a)`** — line 2; function; internal. No JSDoc summary.
- **`vt(e)`** — line 2; function; internal. No JSDoc summary.
- **`w(e)`** — line 2; function; internal. No JSDoc summary.
- **`we(i)`** — line 2; function; internal. No JSDoc summary.
- **`xe(e)`** — line 2; function; internal. No JSDoc summary.
- **`ye(e)`** — line 2; function; internal. No JSDoc summary.
- **`ze(e)`** — line 2; function; internal. No JSDoc summary.

### `biochar_app/static/js/main.js`

- **`hideBootLoading()`** — line 75; function; internal. Hide the initial boot/loading overlay if present. Kept local so main.js does not depend on a missing export.
- **`wireTabRender({ href, tabId, paneId, renderFn, label })`** — line 103; function; internal. No JSDoc summary.

### `biochar_app/static/js/markdown.js`

- **`getMarkdownRenderer()`** — line 35; function; internal. No JSDoc summary.
- **`prettyNameFromContainerId(containerId)`** — line 54; function; internal. No JSDoc summary.
- **`loadMarkdownContent(containerId, markdownPath)`** — line 71; async function; public/exported. Fetch a Markdown file, render it to HTML, and inject into the container. Then, if MathJax is present, re-typeset that container so TeX equations render.

### `biochar_app/static/js/plot_utils.js`

- **`getPlotly()`** — line 26; function; internal. Return Plotly from the global window object. Kept as a helper so TypeScript stops complaining about the bare global.
- **`waitForAllDropdowns(ids)`** — line 69; async function; public/exported. Pause until each of the given dropdown IDs exists in the DOM and has been populated with at least one <option>.
- **`delay(ms)`** — line 74; function; internal. No JSDoc summary.
- **`syncZoom(sourceDiv, targetDiv, eventData)`** — line 106; function; internal. Apply the x-axis range from one plot to another.
- **`maybeAttachZoomSyncHandlers()`** — line 147; function; internal. Once both plots exist, attach relayout handlers in both directions.
- **`makeHandler(source, target, label)`** — line 158; function; internal. No JSDoc summary.
- **`measurePlotWidth(el)`** — line 183; function; internal. No JSDoc summary.
- **`getSharedPlotWidth(targetId, container)`** — line 200; function; internal. No JSDoc summary.
- **`computeRightGutterPx(containerOrGd, plotType, plotLayout = null, plotData = null)`** — line 233; function; public/exported. No JSDoc summary.
- **`chooseSharedLegendMode(targetId, rightGutterPx)`** — line 272; function; internal. Choose a single legend mode for the pair. plot-1 decides, plot-2 follows.
- **`applyResponsiveLegend(layout, rightGutterPx, targetId)`** — line 300; function; internal. Update legend placement based on gutter choice and shared pair mode.
- **`computeResponsivePlotGeometry(rightGutterPx,   legendResponse,   baseHeight = FALLBACK_PLOT_HEIGHT,   baseBottomMargin = DEFAULT_BOTTOM_MARGIN)`** — line 338; function; internal. Calculate the responsive dimensions used by every plot layout path. Keeping this in one helper prevents initial render and resize behavior from drifting apart.
- **`syncPairGeometryFromRaw()`** — line 364; async function; internal. No JSDoc summary.
- **`fetchAndRenderPlot(plotType, plotDivId)`** — line 416; async function; public/exported. No JSDoc summary.
- **`refineGutter()`** — line 551; async function; internal. No JSDoc summary.
- **`relayoutOne(el, kind, targetIdForLegend, forceGutter = null, forceWidth = null)`** — line 621; async function; internal. No JSDoc summary.
- **`renderMainPlots()`** — line 709; async function; public/exported. No JSDoc summary.
- **`wireMainPlotZoomSync()`** — line 745; function; public/exported. Optional explicit hook for existing callers. The handlers are also attached automatically after both plots render.

### `biochar_app/static/js/plots.js`

- **`updatePlot(plotType, plotDivId)`** — line 25; async function; public/exported. No JSDoc summary.
- **`capitalize(s)`** — line 33; function; public/exported. No JSDoc summary.

### `biochar_app/static/js/tab_biomass_field.js`

- **`renderBiomassFieldTables()`** — line 9; async function; public/exported. No JSDoc summary.

### `biochar_app/static/js/tab_nir.js`

- **`renderNirTables()`** — line 16; async function; public/exported. No JSDoc summary.

### `biochar_app/static/js/tab_soil.js`

- **`renderSoilTab({   containerId,   endpoint,   fallbackLabel,   subtitleText = "", })`** — line 20; async function; internal. No JSDoc summary.
- **`renderSoilChemTable()`** — line 85; async function; public/exported. No JSDoc summary.
- **`renderSoilBioTable()`** — line 93; async function; public/exported. No JSDoc summary.

### `biochar_app/static/js/tab_summary.js`

- **`capitalizeFirst(str)`** — line 29; function; internal. No JSDoc summary.
- **`isTemperatureVariable(variableKey)`** — line 38; function; internal. No JSDoc summary.
- **`resolveUnitLabelStrict(labelEntry, unitSystem, fallback)`** — line 48; function; internal. No JSDoc summary.
- **`getUnitSystemForSummary()`** — line 89; function; internal. No JSDoc summary.
- **`isPlainObject(v)`** — line 100; function; internal. No JSDoc summary.
- **`getDepthDisplayLabel(unitSystem)`** — line 112; function; internal. Convert depth dropdown value to a display label: - Assumes dropdown value is inches as a string like "6", "12", "18" - Uses the dropdown selected text if parsing fails
- **`buildSummaryTitle({ year, variable, strip, granularity, unitSystem })`** — line 147; function; internal. No JSDoc summary.
- **`prettifyStatsKeys(stats, variable, unitSystem)`** — line 171; function; internal. No JSDoc summary.
- **`buildGseasonAccordionHTML(gseasonStats, variable, unitSystem)`** — line 236; function; internal. No JSDoc summary.
- **`normalizeFlatGseasonStats(stats)`** — line 249; function; internal. No JSDoc summary.
- **`showSummaryStatus(text = "")`** — line 417; function; internal. No JSDoc summary.
- **`hideSummaryStatus()`** — line 427; function; internal. No JSDoc summary.
- **`updateSummaryStatistics()`** — line 437; async function; public/exported. No JSDoc summary.
- **`initSummaryTab()`** — line 603; function; public/exported. No JSDoc summary.

### `biochar_app/static/js/tab_ui.js`

- **`makeSetSectionTitle(titleText, subtitleText = "", variant = "nir")`** — line 16; function; public/exported. No JSDoc summary.

### `biochar_app/static/js/tables.js`

- **`isObject(x)`** — line 59; function; public/exported. No JSDoc summary.
- **`safeStr(v, fallback = "")`** — line 68; function; public/exported. No JSDoc summary.
- **`appendTextWithLinks(parentEl, text)`** — line 88; function; internal. Append a note string to a container, converting plain http(s) URLs into clickable <a> links WITHOUT using innerHTML (avoids XSS). Example: "Source: https://example.com/foo" => Source: [link]
- **`normalizeOneSet(s, idx = 0, totalSets = null)`** — line 131; function; internal. No JSDoc summary.
- **`normalizeKeyLabelItem(x)`** — line 159; function; internal. No JSDoc summary.
- **`normalizeKeyLabelList(arr)`** — line 190; function; internal. No JSDoc summary.
- **`normalizePayload(raw)`** — line 233; function; public/exported. normalizePayload supports BOTH: A) Standard multi-set: { title, sets: [ {key,label,periods,variables,rows,rowLabels,data,note/notes?}, ... ] } B) Legacy single-set: { title, periods, variables, rows, rowLabels, data, note/notes? } C) Legacy wrapper: { title, set: { ...single set... } }
- **`renderOneSetFromPayload(parentEl, setPayload)`** — line 301; function; public/exported. No JSDoc summary.
- **`escapeHtml(s)`** — line 366; function; internal. No JSDoc summary.
- **`formatBandRange(band)`** — line 379; function; internal. No JSDoc summary.
- **`openReferenceModal(variable)`** — line 399; function; internal. No JSDoc summary.
- **`buildTableForVariable(setPayload, variableKey, variableLabel, variableNote = "")`** — line 505; function; public/exported. No JSDoc summary.
- **`norm(s)`** — line 536; function; internal. No JSDoc summary.
- **`isRatioRowKey(rowKey)`** — line 542; function; internal. No JSDoc summary.
- **`looksNumeric(s)`** — line 558; function; internal. No JSDoc summary.
- **`formatValue(v, rowKey)`** — line 567; function; internal. No JSDoc summary.

### `biochar_app/static/js/ui_controls.js`

- **`applyDateRangeFromDefaults(year, granularity, dateRanges)`** — line 52; function; public/exported. Apply DATE_RANGES[year][granularity] to the main start/end date inputs. Falls back to year-wide dates only if no mapping exists.
- **`wireMainDateRangeListeners()`** — line 79; function; public/exported. Wire up listeners so changing year or granularity updates the main date inputs using DATE_RANGES.
- **`markUserEdited()`** — line 98; function; internal. No JSDoc summary.
- **`applyDefaults()`** — line 105; function; internal. No JSDoc summary.
- **`fetchDefaultsAndOptions()`** — line 133; async function; public/exported. 1) Fetch the JSON of defaults & options from your API.
- **`populateAllDropdowns(options)`** — line 186; function; public/exported. 2) Populate every <select> across both tabs using your mapping. We no longer sort — we respect the server’s order.
- **`populateDropdown(elementId,   values,   defaultValue,   labelMapping = {})`** — line 234; function; public/exported. Helper to fill a single <select> with <option>s.
- **`getSelectedFilters(tab)`** — line 273; function; public/exported. Collects all of the controls for the given tab, and if on the Main tab with granularity="gseason", also pulls in your custom-season rows.
- **`parseStrictDate(value)`** — line 315; function; internal. No JSDoc summary.
- **`updateDepthLabels(unitSystem)`** — line 423; function; public/exported. Update the depth dropdown labels on both tabs based on window.depthMapping and the current unit system.
- **`initializeMainDatepickers()`** — line 471; function; public/exported. Wire up the two main-tab date inputs. Uses DATE_RANGES if available; falls back to defaults.
- **`toIsoDate(value)`** — line 493; function; internal. No JSDoc summary.
- **`handleTraceOptionChange(event)`** — line 536; function; public/exported. Placeholder for traceOption logic (kept so imports don’t break).
- **`attachNativeDateInputGuards(startEl, endEl)`** — line 544; function; internal. No JSDoc summary.
- **`clearIfInvalid(el)`** — line 548; function; internal. No JSDoc summary.

### `biochar_app/static/js/ui_loading.js`

- **`ensurePositioned(el)`** — line 11; function; internal. Ensure the container has non-static positioning so an absolute overlay can be placed inside it safely.
- **`ensureMinHeight(el, px = 160)`** — line 29; function; internal. Ensure the container has a minimum height while loading so the overlay has visible space even before Plotly finishes rendering.
- **`showLoadingOverlay(container, label = "Loading")`** — line 48; function; public/exported. Show a loading overlay inside the given container.
- **`hideLoadingOverlay(container)`** — line 125; function; public/exported. Hide the loading overlay inside the given container and restore any temporary sizing/positioning adjustments.
- **`startLoadingDots(elId, baseText = "Loading")`** — line 154; function; public/exported. Start animated loading dots in a text element.
- **`stopLoadingDots(elId, finalText = "")`** — line 176; function; public/exported. Stop animated loading dots and optionally set final text.

### `biochar_app/static/js/ui_utils.js`

- **`getDropdownValue(id, parseAsInt = false)`** — line 9; function; public/exported. Return a select dropdown value by element id.
- **`showAlert(message)`** — line 24; function; public/exported. Show an alert to the user.
- **`getInputValue(id)`** — line 34; function; public/exported. Get the value of an input element by id.
- **`setInputValue(id, value)`** — line 44; function; public/exported. Set the value of an input element by id.
- **`getElementByIdSafe(id)`** — line 54; function; public/exported. Safely get any element by id.
- **`formatValue(value)`** — line 67; function; public/exported. Format a numeric value for display.
- **`isMobileDevice()`** — line 78; function; public/exported. Detect whether the current device is likely mobile.

### `biochar_app/templates/index.html`

- **`toggleDateControls()`** — line 909; function; internal. No JSDoc summary.
- **`clearActive()`** — line 929; function; internal. No JSDoc summary.
- **`getMainUnitSystem()`** — line 955; function; internal. No JSDoc summary.
- **`downloadTraceBundleZip()`** — line 960; async function; internal. No JSDoc summary.
- **`updateTraceHelp()`** — line 1055; function; internal. No JSDoc summary.
- **`updateFlowAllocationUI()`** — line 1083; function; internal. No JSDoc summary.

## Python parse errors

- `biochar_app/pakbus/examples/show_clock.py: Missing parentheses in call to 'print'. Did you mean print(...)? (show_clock.py, line 45)`
- `biochar_app/pakbus/examples/show_files.py: Missing parentheses in call to 'print'. Did you mean print(...)? (show_files.py, line 45)`
- `biochar_app/pakbus/examples/show_progstat.py: Missing parentheses in call to 'print'. Did you mean print(...)? (show_progstat.py, line 45)`
- `biochar_app/pakbus/scripts/probe_bd_field_offsets.py: (unicode error) 'unicodeescape' codec can't decode bytes in position 746-748: truncated \xXX escape (probe_bd_field_offsets.py, line 2)`

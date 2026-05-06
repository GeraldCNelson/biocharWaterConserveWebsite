import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from biochar_app.config.core import (
    SENSOR_DEPTH_LABELS,
    COAGMET_VARIABLE_MAP,
    logger_location_mapping,
    strip_name_mapping,
    variable_name_mapping,
)

from biochar_app.config.descriptions import (
    PROJECT_README_TITLE,
    PROJECT_METHOD_NOTE,
    PROJECT_REFERENCE_NOTE,
    PROJECT_REFERENCE_SOURCES,
    PROJECT_SAMPLE_CONTEXT_NOTE,
    LOGGER_DESCRIPTION,
    WEATHER_DESCRIPTION,
    WEATHER_DATA_SOURCE,
    IRRIGATION_DESCRIPTION,
    FERTILIZER_DESCRIPTION,
    SOIL_CHEMISTRY_DESCRIPTION,
    SOIL_CHEMISTRY_SCOPE_NOTE,
    SOIL_BIOLOGY_DESCRIPTION,
    SOIL_BIOLOGY_SCOPE_NOTE,
    HAY_DESCRIPTION,
    HAY_SAMPLE_CONTEXT_NOTE,
    GENERIC_FILE_DESCRIPTION,
    PROJECT_NIR_REFERENCE_NOTE,
    PROJECT_NIR_REFERENCE_SOURCES,
)

GLOSSARY_JSON_PATH = Path(__file__).resolve().parents[1] / "static" / "data" / "glossary_terms.json"


def build_nir_reference_note() -> str:
    lines = [
        "Reference note",
        "--------------",
        PROJECT_NIR_REFERENCE_NOTE,
        "",
        "Primary sources:",
    ]

    for label, source in PROJECT_NIR_REFERENCE_SOURCES:
        lines.append(f"- {label}: {source}")

    return _join_readme_lines(lines).rstrip()

def _join_readme_lines(lines: list[str]) -> str:
    return "\n".join(lines).rstrip() + "\n"


def _normalize_unit_system(unit_system: str = "us") -> str:
    unit_system = str(unit_system or "us").strip().lower()
    if unit_system in {"metric", "m", "si"}:
        return "metric"
    return "us"


def _unit_system_label(unit_system: str = "us") -> str:
    return "metric" if _normalize_unit_system(unit_system) == "metric" else "US"


def _detect_year_span(df: pd.DataFrame) -> str:
    candidate_date_cols = [
        "date_rec",
        "date",
        "sample_date",
        "date_rept",
        "start_timestamp",
        "end_timestamp",
        "application_date",
        "timestamp",
        "nir_date",
    ]
    candidate_year_cols = ["year", "Year"]

    for col in candidate_date_cols:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce")
            years = sorted(y for y in dt.dt.year.dropna().astype(int).unique())
            if years:
                return f"{years[0]}–{years[-1]}" if len(years) > 1 else str(years[0])

    for col in candidate_year_cols:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna().astype(int)
            years = sorted(vals.unique())
            if years:
                return f"{years[0]}–{years[-1]}" if len(years) > 1 else str(years[0])

    return "unknown"


def _dataset_summary(df: pd.DataFrame) -> str:
    return _join_readme_lines([
        f"Rows: {len(df)}",
        f"Variables: {len(df.columns)}",
    ]).rstrip()

def build_project_reference_note() -> str:
    lines = [
        "Reference note",
        "--------------",
        PROJECT_REFERENCE_NOTE,
        "",
        "Primary sources:",
    ]

    for label, url in PROJECT_REFERENCE_SOURCES:
        lines.append(f"- {label}: {url}")
    return _join_readme_lines(lines).rstrip()

def build_depth_codes_section(unit_system: str = "us") -> str:
    unit_system = _normalize_unit_system(unit_system)
    lines = ["Depth codes:"]
    for depth_code in sorted(SENSOR_DEPTH_LABELS.keys(), key=lambda x: int(x)):
        label = SENSOR_DEPTH_LABELS[depth_code].get(unit_system, "")
        lines.append(f"- {depth_code} = {label}")
    return "\n".join(lines)


def build_logger_location_codes_section() -> str:
    lines = ["Logger location codes:"]
    for code, label in logger_location_mapping.items():
        lines.append(f"- {code} = {label} logger position within the strip")
    return "\n".join(lines)


def build_logger_variable_codes_section() -> str:
    lines = ["Logger variable codes:"]
    for code, label in variable_name_mapping.items():
        lines.append(f"- {code} = {label}")
    return "\n".join(lines)


def build_strip_codes_section() -> str:
    lines = ["Strip codes:"]
    for code, label in strip_name_mapping.items():
        lines.append(f"- {code} = {label}")
    return "\n".join(lines)


def build_weather_variable_codes_section() -> str:
    lines = [
        "Weather data source:",
        WEATHER_DATA_SOURCE,
        "",
        "Weather variable names:",
        "These are the standardized exported weather column names used by the dashboard.",
    ]
    for source_code, exported_name in COAGMET_VARIABLE_MAP.items():
        lines.append(f"- {source_code} → `{exported_name}`")
    return "\n".join(lines)


def build_logger_units_section(unit_system: str = "us") -> str:
    unit_system = _normalize_unit_system(unit_system)

    if unit_system == "metric":
        rows = [
            ("Volumetric Water Content (VWC)", "percent volumetric water content (%)"),
            ("Electrical Conductivity (EC)", "standardized logger EC units"),
            ("Soil Temperature (T)", "degrees Celsius (°C)"),
            ("Soil Water Content (SWC)", "liters (L) where present"),
            ("SWC difference", "liters (L) where shown in the column name"),
            ("Temperature difference (ΔT)", "degrees Celsius (°C)"),
            ("Ratio columns", "unitless"),
            ("Irrigation volume", "liters (L), for irrigation datasets or overlays"),
            ("Depth labels", "centimeters (cm)"),
        ]
    else:
        rows = [
            ("Volumetric Water Content (VWC)", "percent volumetric water content (%)"),
            ("Electrical Conductivity (EC)", "standardized logger EC units"),
            ("Soil Temperature (T)", "degrees Fahrenheit (°F)"),
            ("Soil Water Content (SWC)", "gallons (gal) where present"),
            ("SWC difference", "gallons (gal) where shown in the column name"),
            ("Temperature difference (ΔT)", "degrees Fahrenheit (°F)"),
            ("Ratio columns", "unitless"),
            ("Irrigation volume", "gallons (gal), for irrigation datasets or overlays"),
            ("Depth labels", "inches (in)"),
        ]

    lines = ["Variable units", "--------------"]
    lines.extend(f"- {name}: {unit}" for name, unit in rows)
    return _join_readme_lines(lines).rstrip()


def build_weather_units_section(unit_system: str = "us") -> str:
    unit_system = _normalize_unit_system(unit_system)

    if unit_system == "metric":
        rows = [
            ("Air temperature", "degrees Celsius (°C)"),
            ("Precipitation", "millimeters (mm)"),
            ("Other weather variables", "standardized units shown in the exported column names"),
        ]
    else:
        rows = [
            ("Air temperature", "degrees Fahrenheit (°F)"),
            ("Precipitation", "inches (in)"),
            ("Other weather variables", "standardized units shown in the exported column names"),
        ]

    lines = ["Variable units", "--------------"]
    lines.extend(f"- {name}: {unit}" for name, unit in rows)
    return _join_readme_lines(lines).rstrip()


def build_management_units_section(dataset: str, unit_system: str = "us") -> str:
    unit_system = _normalize_unit_system(unit_system)

    if dataset == "irrigation":
        volume_unit = "liters (L)" if unit_system == "metric" else "gallons (gal)"
        rows = [
            ("Irrigation volume", volume_unit),
            ("Duration", "hours or minutes, depending on the column"),
            ("Start and end time", "local timestamp fields as stored in the standardized CSV"),
        ]
    elif dataset == "fertilizer":
        amount_unit = "metric units where available" if unit_system == "metric" else "US units where available"
        rows = [
            ("Fertilizer amount or rate", amount_unit),
            ("Application date", "date or timestamp field as stored in the standardized CSV"),
            ("Material or product", "text field, where available"),
        ]
    else:
        rows = [
            ("Dataset variables", "units are reported as stored in the standardized source dataset"),
        ]

    lines = ["Variable units", "--------------"]
    lines.extend(f"- {name}: {unit}" for name, unit in rows)
    return _join_readme_lines(lines).rstrip()


def build_units_text(dataset: str, unit_system: str = "us") -> str:
    dataset = str(dataset or "").strip().lower()

    if dataset in {"logger", "loggers"}:
        return build_logger_units_section(unit_system)

    if dataset == "weather":
        return build_weather_units_section(unit_system)

    if dataset in {"irrigation", "fertilizer"}:
        return build_management_units_section(dataset, unit_system)

    return "Units are reported as stored in the standardized source dataset."


def build_logger_column_naming_section(unit_system: str = "us") -> str:
    unit_system = _normalize_unit_system(unit_system)

    if unit_system == "metric":
        swc_example = (
            "SWCdiff_L_S3_S4_M_2 = soil water content difference, in liters, "
            "comparing S3 to S4, middle logger location, depth 2"
        )
    else:
        swc_example = (
            "SWCdiff_gal_S3_S4_M_2 = soil water content difference, in gallons, "
            "comparing S3 to S4, middle logger location, depth 2"
        )

    return _join_readme_lines([
        "Column naming",
        "-------------",
        "Most logger-derived columns follow one of these patterns:",
        "",
        "  variable_depth_type_strip_location",
        "  derived_metric_depth_treated_control_location",
        "  derived_metric_units_treated_control_location_depth",
        "",
        "Examples:",
        "- VWC_1_raw_S1_T = volumetric water content, depth 1, raw value, Strip 1, top logger location",
        "- EC_2_raw_S3_M = electrical conductivity, depth 2, raw value, Strip 3, middle logger location",
        "- T_3_raw_S4_B = soil temperature, depth 3, raw value, Strip 4, bottom logger location",
        "- Tdiff_1_S1_S2_T = soil temperature difference, depth 1, comparing S1 to S2, top logger location",
        f"- {swc_example}",
        "",
        build_logger_variable_codes_section(),
        "",
        build_strip_codes_section(),
        "",
        build_depth_codes_section(unit_system),
        "",
        build_logger_location_codes_section(),
    ]).rstrip()


def _example_columns(cols: list[str], max_columns: int = 4) -> str:
    if not cols:
        return ""

    shown = cols[:max_columns]
    more = len(cols) - len(shown)

    text = ", ".join(f"`{c}`" for c in shown)
    if more > 0:
        text += f", plus {more} more"

    return text


def _variable_line(label: str, cols: list[str], units: str = "") -> list[str]:
    if not cols:
        return []

    lines = [f"- {label}"]
    if units:
        lines.append(f"  Units: {units}")
    lines.append(f"  Example columns: {_example_columns(cols)}")
    return lines


def _glossary_definition_for_key(key: str) -> str:
    entries = load_glossary_entries()
    for entry in entries:
        if str(entry.get("key", "")).strip() == key:
            return str(entry.get("definition", "")).strip()
    return ""


def _variable_line_from_glossary(
    label: str,
    cols: list[str],
    glossary_key: str,
    units: str = "",
) -> list[str]:
    if not cols:
        return []

    definition = _glossary_definition_for_key(glossary_key)

    if definition:
        lines = [f"- {label}: {definition}"]
    else:
        lines = [f"- {label}"]

    if units:
        lines.append(f"  Units: {units}")

    lines.append(f"  Example columns: {_example_columns(cols)}")
    return lines


def build_logger_variable_section(df: pd.DataFrame, unit_system: str = "us") -> str:
    unit_system = _normalize_unit_system(unit_system)
    cols = [str(c) for c in df.columns]

    raw = [c for c in cols if "_raw_" in c]
    swc = [c for c in cols if c.startswith("SWC_vol_")]
    tdiff = [c for c in cols if c.startswith("Tdiff")]
    swcdiff = [c for c in cols if c.startswith("SWCdiff")]
    ratio = [c for c in cols if "_ratio_" in c]

    weather_prefixes = (
        "temp_air",
        "rh_",
        "dewpoint",
        "precip",
        "wind_",
        "solar_",
        "vapor_pressure",
        "soil_temp",
    )
    weather = [c for c in cols if c.startswith(weather_prefixes)]

    temp_units = "degrees Celsius (°C)" if unit_system == "metric" else "degrees Fahrenheit (°F)"
    swc_units = "liters (L)" if unit_system == "metric" else "gallons (gal)"
    precip_units = "millimeters (mm)" if unit_system == "metric" else "inches (in)"

    lines: list[str] = []

    raw_lines: list[str] = []
    raw_lines.extend(_variable_line(
        "Volumetric Water Content (VWC)",
        [c for c in raw if c.startswith("VWC")],
        "percent volumetric water content (%)",
    ))
    raw_lines.extend(_variable_line(
        "Electrical Conductivity (EC)",
        [c for c in raw if c.startswith("EC")],
        "standardized logger EC units",
    ))
    raw_lines.extend(_variable_line(
        "Soil Temperature (T)",
        [c for c in raw if c.startswith("T_")],
        temp_units,
    ))

    if raw_lines:
        lines.extend(["Raw sensor variables", ""])
        lines.extend(raw_lines)
        lines.append("")

    derived_lines: list[str] = []
    derived_lines.extend(_variable_line("Soil Water Content (SWC)", swc, swc_units))
    derived_lines.extend(_variable_line("ΔT / Temperature difference", tdiff, temp_units))
    derived_lines.extend(_variable_line("ΔSWC / Soil water content difference", swcdiff, swc_units))

    if derived_lines:
        lines.extend(["Derived variables", ""])
        lines.extend(derived_lines)
        lines.append("")

    ratio_lines: list[str] = []
    ratio_lines.extend(_variable_line("Treatment/control ratios", ratio, "unitless"))

    if ratio_lines:
        lines.extend(["Ratio variables", ""])
        lines.extend(ratio_lines)
        lines.append("")

    weather_lines: list[str] = []
    weather_lines.extend(_variable_line(
        "Air temperature",
        [c for c in weather if c.startswith("temp_air")],
        temp_units,
    ))
    weather_lines.extend(_variable_line(
        "Relative humidity",
        [c for c in weather if c.startswith("rh_")],
        "percent (%)",
    ))
    weather_lines.extend(_variable_line(
        "Dewpoint",
        [c for c in weather if c.startswith("dewpoint")],
        temp_units,
    ))
    weather_lines.extend(_variable_line(
        "Vapor pressure",
        [c for c in weather if c.startswith("vapor_pressure")],
        "pressure units shown in the column name",
    ))
    weather_lines.extend(_variable_line(
        "Solar radiation",
        [c for c in weather if c.startswith("solar_")],
        "W/m²",
    ))
    weather_lines.extend(_variable_line(
        "Precipitation",
        [c for c in weather if c.startswith("precip")],
        precip_units,
    ))
    weather_lines.extend(_variable_line(
        "Wind speed",
        [c for c in weather if c.startswith("wind_speed")],
        "speed units shown in the column name",
    ))
    weather_lines.extend(_variable_line(
        "Wind direction",
        [c for c in weather if c.startswith("wind_dir")],
        "degrees from north",
    ))
    weather_lines.extend(_variable_line(
        "Weather station soil temperature",
        [c for c in weather if c.startswith("soil_temp")],
        temp_units,
    ))

    if weather_lines:
        lines.extend(["Weather variables included with logger data", ""])
        lines.extend(weather_lines)
        lines.append("")

    if not lines:
        return "No logger variable groups were detected."

    return "\n".join(lines).rstrip()


def build_timeseries_yearly_readme(
    *,
    dataset: str,
    year: int,
    resolution: str,
    notes: str = "",
    df: Optional[pd.DataFrame] = None,
    units_text: str = "",
    unit_system: str = "us",
) -> str:
    dataset_key = str(dataset or "").strip().lower()
    unit_system = _normalize_unit_system(unit_system)

    dataset_label = {
        "logger": "Logger data",
        "loggers": "Logger data",
        "weather": "Weather data",
    }.get(dataset_key, dataset)

    if not units_text:
        units_text = build_units_text(dataset_key, unit_system)

    if dataset_key in {"logger", "loggers"}:
        description = f"{LOGGER_DESCRIPTION} This download covers {year} at {resolution} resolution."
    elif dataset_key == "weather":
        description = f"{WEATHER_DESCRIPTION} This download covers {year} at {resolution} resolution."
    else:
        description = f"This file contains standardized {dataset_label} for {year} at {resolution} resolution."

    lines = [
        PROJECT_README_TITLE,
        "",
        f"Dataset: {dataset_label}",
        f"Coverage: {year}",
        f"Units: {_unit_system_label(unit_system)}",
        f"File type: standardized {resolution} dataset (CSV packaged as ZIP)",
        "",
    ]

    if df is not None:
        lines.extend([
            "Dataset summary",
            "---------------",
            _dataset_summary(df),
            "",
        ])

    lines.extend([
        "Description",
        "-----------",
        description,
        "",
    ])

    if dataset_key in {"logger", "loggers"}:
        lines.extend([
            build_logger_column_naming_section(unit_system),
            "",
        ])

    if df is not None:
        lines.extend([
            "Variables",
            "---------",
            build_logger_variable_section(df, unit_system) if dataset_key in {"logger", "loggers"} else build_variable_section(df),
            "",
        ])

    if dataset_key == "weather":
        lines.extend([
            build_weather_variable_codes_section(),
            "",
        ])

    lines.extend([
        "Units",
        "-----",
        units_text,
        "",
    ])

    if notes:
        lines.extend([
            "Notes",
            "-----",
            notes,
            "",
        ])

    return _join_readme_lines(lines)


def build_management_readme(
    *,
    dataset: str,
    dataset_label: str,
    df: pd.DataFrame,
    unit_system: str = "us",
) -> str:
    dataset_key = str(dataset or "").strip().lower()
    unit_system = _normalize_unit_system(unit_system)
    years_text = _detect_year_span(df)

    if dataset_key == "irrigation":
        description = "\n".join([
            f"{IRRIGATION_DESCRIPTION} This download covers {years_text}.",
            "",
            "During the study period, S1/S2 are irrigated roughly every four weeks while S3/S4 are irrigated every two weeks. "
            "Records include irrigation start and end times, duration, and applied water volume "
            "where available.",
        ])
    elif dataset_key == "fertilizer":
        description = "\n".join([
            f"{FERTILIZER_DESCRIPTION} This download covers {years_text}.",
            "",
            "Records should be interpreted alongside crop management, irrigation practices, "
            "and harvest timing.",
        ])
    else:
        description = f"This file contains standardized management records for {dataset_label} for {years_text}."

    return _join_readme_lines([
        PROJECT_README_TITLE,
        "",
        f"Dataset: {dataset_label}",
        f"Coverage: {years_text}",
        f"Units: {_unit_system_label(unit_system)}",
        "File type: standardized all-years dataset (CSV packaged as ZIP)",
        "",
        "Dataset summary",
        "---------------",
        _dataset_summary(df),
        "",
        "Description",
        "-----------",
        description,
        "",
        "Variables",
        "---------",
        build_variable_section(df),
        "",
        "Units",
        "-----",
        build_units_text(dataset_key, unit_system),
        "",
    ])


def build_soilchem_readme(dataset_label: str, df: pd.DataFrame) -> str:
    coverage = _detect_year_span(df)

    return _join_readme_lines([
        PROJECT_README_TITLE,
        "",
        f"Dataset: {dataset_label}",
        f"Coverage: {coverage}",
        "File type: standardized all-years dataset (CSV packaged as ZIP)",
        "",
        "Dataset summary",
        "---------------",
        _dataset_summary(df),
        "",
        "Description",
        "-----------",
        SOIL_CHEMISTRY_DESCRIPTION,
        "",
        PROJECT_SAMPLE_CONTEXT_NOTE,
        "",
        SOIL_CHEMISTRY_SCOPE_NOTE,
        "",
        "Variables",
        "---------",
        build_soilchem_variable_section(df),
        "",
        "Units",
        "-----",
        "Units vary by variable and follow standard soil testing conventions such as ppm, %, or meq/100g.",
        "",
        build_project_reference_note(),
        "",
    ])


def build_soilbio_readme(dataset_label: str, df: pd.DataFrame) -> str:
    coverage = _detect_year_span(df)

    return _join_readme_lines([
        PROJECT_README_TITLE,
        "",
        f"Dataset: {dataset_label}",
        f"Coverage: {coverage}",
        "File type: standardized all-years dataset (CSV packaged as ZIP)",
        "",
        "Dataset summary",
        "---------------",
        _dataset_summary(df),
        "",
        "Description",
        "-----------",
        SOIL_BIOLOGY_DESCRIPTION,
        "",
        SOIL_BIOLOGY_SCOPE_NOTE,
        "",
        PROJECT_SAMPLE_CONTEXT_NOTE,
        "",
        "Variables",
        "---------",
        build_soilbio_variable_section(df),
        "",
        "Units",
        "-----",
        "Units vary by variable and include biomass measures, percentages, ratios, and index values depending on the variable.",
        "",
        build_project_reference_note(),
        "",
    ])


def build_hay_variable_section(df: pd.DataFrame) -> str:
    cols = [str(c) for c in df.columns]

    def present(names: list[str]) -> list[str]:
        return [c for c in names if c in cols]

    lines: list[str] = [
        "All forage quality metrics are reported on a dry matter basis unless otherwise noted.",
        "",
        "Core identifiers",
        "",
    ]

    lines.extend(_variable_line(
        "Strip / sample identifier",
        present(["strip", "sample_id"]),
        "",
    ))
    lines.extend(_variable_line(
        "Sample date",
        present(["nir_date"]),
        "",
    ))

    lines.extend(["", "Forage quality metrics (dry basis)", ""])

    lines.extend(_variable_line_from_glossary(
        "Crude Protein (CP)",
        present(["crude_protein_pct_db"]),
        "crude_protein",
        "percent of dry matter (%)",
    ))
    lines.extend(_variable_line_from_glossary(
        "Acid Detergent Fiber (ADF)",
        present(["adf_pct_db"]),
        "adf",
        "percent of dry matter (%)",
    ))
    lines.extend(_variable_line_from_glossary(
        "Neutral Detergent Fiber (NDF)",
        present(["ndf_pct_db"]),
        "ndf",
        "percent of dry matter (%)",
    ))
    lines.extend(_variable_line(
        "Total Digestible Nutrients (TDN)",
        present(["tdn_pct_db"]),
        "percent of dry matter (%)",
    ))
    lines.extend(_variable_line(
        "Net Energy",
        present(["nel_pct_db", "nem_pct_db", "neg_pct_db"]),
        "energy values as provided in the NIR export",
    ))

    lines.extend(["", "Carbohydrates and energy fractions (dry basis)", ""])
    lines.extend(_variable_line(
        "Non-fiber carbohydrates, starch, sugars, and fructans",
        present(["nfc_pct_db", "starch_pct_db", "esc_pct_db", "wsc_pct_db", "fructan_pct_db"]),
        "percent of dry matter (%)",
    ))

    lines.extend(["", "Mineral content (dry basis)", ""])
    lines.extend(_variable_line(
        "Calcium, phosphorus, potassium, and magnesium",
        present(["Ca_pct_db", "P_pct_db", "K_pct_db", "Mg_pct_db"]),
        "percent of dry matter (%)",
    ))
    lines.extend(_variable_line(
        "Ash",
        present(["ash_pct_db"]),
        "percent of dry matter (%)",
    ))

    lines.extend(["", "Digestibility and structural metrics (dry basis)", ""])
    lines.extend(_variable_line(
        "NDF digestibility and in vitro true digestibility",
        present(["ndfd48_pctndf_db", "ivtdmd48_pctndf_db"]),
        "percent digestibility, as provided in the NIR export",
    ))
    lines.extend(_variable_line(
        "Fat and lignin",
        present(["fat_pct_db", "lignin_pct_db"]),
        "percent of dry matter (%)",
    ))

    lines.extend(["", "Forage quality indices", ""])
    lines.extend(_variable_line_from_glossary(
        "Relative Feed Value (RFV)",
        present(["RFV"]),
        "rfv",
        "unitless index value",
    ))
    lines.extend(_variable_line_from_glossary(
        "Relative Forage Quality (RFQ)",
        present(["RFQ"]),
        "rfq",
        "unitless index value",
    ))

    lines.extend(["", "Moisture characteristics (as-received basis)", ""])
    lines.extend(_variable_line(
        "Moisture and dry matter",
        present(["moisture_pct", "dry_matter_pct"]),
        "percent of as-received sample (%)",
    ))

    return "\n".join(line for line in lines if line is not None).rstrip()


def build_soilchem_variable_section(df: pd.DataFrame) -> str:
    cols = [str(c) for c in df.columns]

    def present(names: list[str]) -> list[str]:
        return [c for c in names if c in cols]

    lines: list[str] = ["Core identifiers", ""]

    lines.extend(_variable_line(
        "Strip / sample identifier",
        present(["strip", "sample_id"]),
        "",
    ))
    lines.extend(_variable_line(
        "Sample date",
        present(["date_rec", "date_rept", "sample_date"]),
        "",
    ))
    lines.extend(_variable_line(
        "Sample depth",
        present(["begin_depth_in", "end_depth_in"]),
        "inches",
    ))

    lines.extend(["", "Soil properties", ""])
    lines.extend(_variable_line_from_glossary(
        "Soil pH",
        present(["soil_ph", "1_1_soil_ph", "wdrf_buffer_ph"]),
        "ph",
        "pH units",
    ))
    lines.extend(_variable_line_from_glossary(
        "Salinity / soluble salts",
        present(["1_1_s_salts_mmho_cm"]),
        "salinity",
        "mmho/cm",
    ))
    lines.extend(_variable_line_from_glossary(
        "Organic matter / soil organic carbon",
        present(["organic_matter_loi_pct", "organic_c_percent", "organic_c_h2o_ppm"]),
        "som",
        "percent (%) or ppm, depending on column",
    ))
    lines.extend(_variable_line(
        "Excess lime",
        present(["excess_lime"]),
        PROJECT_METHOD_NOTE,
    ))

    lines.extend(["", "Macronutrients", ""])
    lines.extend(_variable_line_from_glossary(
        "Nitrogen",
        present([
            "nitrate_n_ppm",
            "h2o_no3_n",
            "h2o_nh4_n",
            "organic_n_h2o_ppm",
            "total_n_h2o_ppm_n",
            "nitrogen_rec",
        ]),
        "nitrogen",
        "ppm or mg/kg; recommendations as application rate",
    ))
    lines.extend(_variable_line_from_glossary(
        "Phosphorus (P)",
        present(["olsen_p_ppm_p", "p2o5_rec"]),
        "phosphorus",
        "ppm or mg/kg; recommendations as application rate",
    ))
    lines.extend(_variable_line_from_glossary(
        "Potassium (K)",
        present(["potassium_ppm_k", "k2o_rec"]),
        "potassium",
        "ppm or mg/kg; recommendations as application rate",
    ))
    lines.extend(_variable_line_from_glossary(
        "Sulfur",
        present(["sulfate_s_ppm_s", "sulfur_rec"]),
        "sulfur",
        "ppm or mg/kg; recommendations as application rate",
    ))

    lines.extend(["", "Secondary nutrients and micronutrients", ""])
    lines.extend(_variable_line_from_glossary(
        "Calcium",
        present(["calcium_ppm_ca"]),
        "calcium",
        "ppm or mg/kg",
    ))
    lines.extend(_variable_line_from_glossary(
        "Magnesium",
        present(["magnesium_ppm_mg", "magnesium_rec"]),
        "magnesium",
        "ppm or mg/kg; recommendations as application rate",
    ))
    lines.extend(_variable_line_from_glossary(
        "Sodium",
        present(["sodium_ppm_na"]),
        "sodium",
        "ppm or mg/kg",
    ))
    lines.extend(_variable_line_from_glossary(
        "Zinc",
        present(["zinc_ppm_zn", "zinc_rec"]),
        "zinc",
        "ppm or mg/kg; recommendations as application rate",
    ))
    lines.extend(_variable_line_from_glossary(
        "Iron",
        present(["iron_ppm_fe", "iron_rec"]),
        "iron",
        "ppm or mg/kg; recommendations as application rate",
    ))
    lines.extend(_variable_line_from_glossary(
        "Manganese",
        present(["manganese_ppm_mn", "manganese_rec"]),
        "manganese",
        "ppm or mg/kg; recommendations as application rate",
    ))
    lines.extend(_variable_line_from_glossary(
        "Copper",
        present(["copper_ppm_cu", "copper_rec"]),
        "copper",
        "ppm or mg/kg; recommendations as application rate",
    ))

    lines.extend(["", "Cation exchange and base chemistry", ""])
    lines.extend(_variable_line_from_glossary(
        "Cation Exchange Capacity (CEC)",
        present(["cec_meq_100g", "cec_sum_of_cations_me_100g"]),
        "cec",
        "meq/100g",
    ))
    lines.extend(_variable_line_from_glossary(
        "Base saturation",
        present(["pcth_sat", "pctk_sat", "pctca_sat", "pctmg_sat", "pctna_sat"]),
        "base_saturation",
        "percent saturation (%)",
    ))

    lines.extend(["", "Soil health indicators", ""])
    lines.extend(_variable_line_from_glossary(
        "Soil respiration",
        present(["co2_soil_respiration"]),
        "soil_respiration",
        PROJECT_METHOD_NOTE,
    ))
    lines.extend(_variable_line_from_glossary(
        "Water stable aggregates",
        present(["water_stable_aggregates_mod"]),
        "water_stable_aggregates",
        PROJECT_METHOD_NOTE,
    ))
    lines.extend(_variable_line_from_glossary(
        "Soil health score",
        present(["soil_health_score"]),
        "soil_health_score",
        "index value",
    ))
    lines.extend(_variable_line_from_glossary(
        "Microbially active carbon",
        present(["microbially_active_carbon_pctma"]),
        "active_carbon",
        "percent microbially active carbon (%)",
    ))
    lines.extend(_variable_line_from_glossary(
        "Organic nitrogen release",
        present(["organic_nitrogen_release_ppm_n"]),
        "organic_n_release",
        "ppm N",
    ))
    lines.extend(_variable_line_from_glossary(
        "Organic nitrogen reserve",
        present(["organic_nitrogen_reserve_ppm_n"]),
        "organic_n_reserve",
        "ppm N",
    ))
    lines.extend(_variable_line_from_glossary(
        "Organic C:N ratio",
        present(["organic_c_n_h2o"]),
        "organic_c_n_ratio",
        "unitless ratio",
    ))

    lines.extend(["", "Crop and recommendation context", ""])
    lines.extend(_variable_line(
        "Crop information",
        present(["past_crop", "crop_1", "yg_1"]),
        "",
    ))

    return "\n".join(line for line in lines if line is not None).rstrip()


def build_soilbio_variable_section(df: pd.DataFrame) -> str:
    cols = [str(c) for c in df.columns]

    def present(names: list[str]) -> list[str]:
        return [c for c in names if c in cols]

    lines: list[str] = ["Core identifiers", ""]

    lines.extend(_variable_line(
        "Strip / sample identifier",
        present(["strip", "sample_id"]),
        "",
    ))
    lines.extend(_variable_line(
        "Sample date",
        present(["date_rec", "sample_date"]),
        "",
    ))

    lines.extend(["", "Microbial biomass", ""])
    lines.extend(_variable_line_from_glossary(
        "Total microbial biomass",
        present(["total_biomass"]),
        "total_microbial_biomass",
        "biomass per mass of soil",
    ))
    lines.extend(_variable_line_from_glossary(
        "Bacterial biomass",
        present(["total_bacteria_biomass", "bacteria_pct"]),
        "bacterial_biomass",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Fungal biomass",
        present(["total_fungi_biomass", "total_fungi_pct"]),
        "fungal_biomass",
        "biomass per mass of soil or percent, depending on column",
    ))

    lines.extend(["", "Functional groups", ""])
    lines.extend(_variable_line_from_glossary(
        "Actinomycetes",
        present(["actinomycetes_biomass", "actinomycetes_pct"]),
        "actinomycetes",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Gram-positive bacteria",
        present(["gram_pos_biomass", "gram_pos_pct"]),
        "gram_positive_biomass",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Gram-negative bacteria",
        present(["gram_neg_biomass", "gram_neg_pct"]),
        "gram_negative_biomass",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Arbuscular mycorrhizal fungi",
        present(["arbuscular_mycorrhizal_biomass", "arbuscular_mycorrhizal_pct"]),
        "am_fungi",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Saprophytic fungi",
        present(["saprophytes_biomass", "saprophytic_pct"]),
        "saprophytes",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Protozoa",
        present(["protozoa_biomass", "protozoan_pct"]),
        "protozoa",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Rhizobia",
        present(["rhizobia_biomass", "rhizobia_pct"]),
        "rhizobia",
        "biomass per mass of soil or percent, depending on column",
    ))
    lines.extend(_variable_line_from_glossary(
        "Undifferentiated microbial biomass",
        present(["undifferentiated_biomass", "undifferentiated_pct"]),
        "undifferentiated_biomass",
        "biomass per mass of soil or percent, depending on column",
    ))

    lines.extend(["", "Ratios and indices", ""])
    lines.extend(_variable_line_from_glossary(
        "Fungal:Bacterial ratio",
        present(["fungi_bacteria"]),
        "fungal_bacterial_ratio",
        "unitless ratio",
    ))
    lines.extend(_variable_line_from_glossary(
        "Predator:Prey ratio",
        present(["predator_prey"]),
        "predator_prey",
        "unitless ratio",
    ))
    lines.extend(_variable_line_from_glossary(
        "Gram(+):Gram(−) ratio",
        present(["gram_pos_gram_neg_ratio"]),
        "gram_ratio",
        "unitless ratio",
    ))
    lines.extend(_variable_line_from_glossary(
        "Microbial diversity",
        present(["diversity_index"]),
        "microbial_diversity",
        "unitless index",
    ))
    lines.extend(_variable_line_from_glossary(
        "Saturated:Unsaturated fatty acid ratio",
        present(["saturated_unsaturated_ratio"]),
        "saturated_unsaturated_ratio",
        "unitless ratio",
    ))
    lines.extend(_variable_line_from_glossary(
        "Monounsaturated:Polyunsaturated ratio",
        present(["monounsaturated_polyunsaturated_ratio"]),
        "monounsaturated_polyunsaturated_ratio",
        "unitless ratio",
    ))
    lines.extend(_variable_line(
        "Cyclopropyl fatty acid precursor ratios",
        present(["pre_16_1w7c_cy17_0", "pre_18_1w7c_cy19_0"]),
        "unitless ratio",
    ))

    return "\n".join(line for line in lines if line is not None).rstrip()


def build_hay_readme(dataset_label: str, df: pd.DataFrame) -> str:
    coverage = _detect_year_span(df)

    return _join_readme_lines([
        PROJECT_README_TITLE,
        "",
        f"Dataset: {dataset_label}",
        f"Coverage: {coverage}",
        "File type: standardized all-years dataset (CSV packaged as ZIP)",
        "",
        "Dataset summary",
        "---------------",
        _dataset_summary(df),
        "",
        "Description",
        "-----------",
        HAY_DESCRIPTION,
        "",
        HAY_SAMPLE_CONTEXT_NOTE,
        "",
        "Variables",
        "---------",
        build_hay_variable_section(df),
        "",
        "Units",
        "-----",
        "Most forage quality, mineral, carbohydrate, digestibility, fat, lignin, and ash variables are reported on a dry matter basis. Moisture and dry matter columns are retained as as-received sample characteristics. RFV and RFQ are unitless index values.",
        "",
        build_nir_reference_note(),
    ])


def build_generic_file_readme(dataset_label: str, df: pd.DataFrame) -> str:
    coverage = _detect_year_span(df)

    return _join_readme_lines([
        PROJECT_README_TITLE,
        "",
        f"Dataset: {dataset_label}",
        f"Coverage: {coverage}",
        "File type: standardized all-years dataset (CSV packaged as ZIP)",
        "",
        "Dataset summary",
        "---------------",
        _dataset_summary(df),
        "",
        "Description",
        "-----------",
        GENERIC_FILE_DESCRIPTION,
        "",
        "Variables",
        "---------",
        build_variable_section(df),
        "",
        "Units",
        "-----",
        "Units vary by variable and follow the standardized units stored in the exported CSV.",
        "",
    ])


def build_file_dataset_readme(
    dataset_key: str,
    dataset_label: str,
    df: pd.DataFrame,
) -> str:
    if dataset_key == "soil_chem_all":
        return build_soilchem_readme(dataset_label, df)

    if dataset_key == "soil_bio_all":
        return build_soilbio_readme(dataset_label, df)

    if dataset_key == "hay_all":
        return build_hay_readme(dataset_label, df)

    return build_generic_file_readme(dataset_label, df)


def build_variable_section(
    df: pd.DataFrame,
    *,
    max_terms: int = 40,
    max_columns_per_term: int = 4,
) -> str:
    entries = load_glossary_entries()

    if not entries:
        lines = ["Example columns included in this dataset:"]
        lines.extend(f"- `{col}`" for col in df.columns[:max_terms])
        return "\n".join(lines).rstrip()

    excluded_keys = {"strip", "depth", "logger_location"}
    matched: dict[str, dict[str, Any]] = {}

    for col in df.columns:
        col_str = str(col)

        for entry in entries:
            key = str(entry.get("key") or entry.get("term") or _display_glossary_term(entry))
            if key in excluded_keys:
                continue

            if _entry_matches_column(entry, col_str):
                if key not in matched:
                    matched[key] = {
                        **entry,
                        "_columns": [],
                    }

                matched[key]["_columns"].append(col_str)

    if not matched:
        lines = ["No glossary terms were matched to these columns."]
        lines.extend(f"- `{col}`" for col in df.columns[:max_terms])
        return "\n".join(lines).rstrip()

    lines: list[str] = []

    for entry in list(matched.values())[:max_terms]:
        term = _display_glossary_term(entry)
        definition = str(entry.get("definition", "")).strip()
        units = entry.get("units")
        columns = entry.get("_columns", [])

        shown = columns[:max_columns_per_term]
        remaining = max(0, len(columns) - len(shown))

        col_text = ", ".join(f"`{c}`" for c in shown)
        if remaining:
            col_text += f", plus {remaining} more"

        if definition:
            lines.append(f"- {term}: {definition}")
        else:
            lines.append(f"- {term}")

        if units:
            lines.append(f"  Units: {units}")

        if col_text:
            lines.append(f"  Example columns: {col_text}")

        related_keys = entry.get("related_to", []) or []
        if related_keys:
            related_terms = []
            for rk in related_keys:
                match = next((e for e in entries if e.get("key") == rk), None)
                if match:
                    related_terms.append(_display_glossary_term(match))

            if related_terms:
                lines.append(f"  Related terms: {', '.join(related_terms)}")

        lines.append("")

    if len(matched) > max_terms:
        lines.append(f"- ... {len(matched) - max_terms} additional glossary terms not shown")

    return "\n".join(lines).rstrip()


def _display_glossary_term(entry: dict[str, Any]) -> str:
    term = str(entry.get("term", "")).strip()
    abbreviation = str(entry.get("abbreviation", "")).strip()

    if term and abbreviation:
        return f"{term} ({abbreviation})"
    return term or abbreviation


def _normalize_match_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


@lru_cache(maxsize=1)
def load_glossary_entries() -> list[dict[str, Any]]:
    if not GLOSSARY_JSON_PATH.exists():
        return []

    try:
        data = json.loads(GLOSSARY_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    entries: list[dict[str, Any]] = []

    for section in data.get("sections", []):
        section_key = section.get("key", "")
        section_label = section.get("label", "")

        for item in section.get("items", []):
            if not isinstance(item, dict):
                continue

            entry = dict(item)
            entry["_section_key"] = section_key
            entry["_section_label"] = section_label
            entries.append(entry)

    return entries


def _entry_matches_column(entry: dict[str, Any], column_name: str) -> bool:
    col_norm = _normalize_match_text(column_name)

    match_values = list(entry.get("matches", []) or [])

    abbreviation = entry.get("abbreviation")
    if abbreviation:
        match_values.append(str(abbreviation))

    key = entry.get("key")
    if key:
        match_values.append(str(key))

    for match in match_values:
        match_norm = _normalize_match_text(str(match))
        if not match_norm:
            continue

        if len(match_norm) <= 2:
            if f"_{match_norm}_" in f"_{col_norm}_":
                return True
            if col_norm.startswith(f"{match_norm}_"):
                return True
            continue

        if f"_{match_norm}_" in f"_{col_norm}_":
            return True

        if col_norm.startswith(f"{match_norm}_"):
            return True

    return False
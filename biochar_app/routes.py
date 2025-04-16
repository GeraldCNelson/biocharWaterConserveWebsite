# Routes.py - Flask application for Biochar Project

from flask import Blueprint, jsonify, request, send_from_directory, render_template
import os
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from biochar_app.utils import load_logger_data, compute_summary_statistics, parse_filenames, assign_gseason_periods, load_or_generate_gseason_summary
import numpy as np
import logging
import json
import glob
import re
from io import BytesIO
import zipfile
from flask import Response, send_file
from collections import namedtuple
from biochar_app.config import (
    DEFAULT_YEAR,
    DEFAULT_START_DATE,
    DEFAULT_VARIABLE,
    DEFAULT_DEPTH,
    DEFAULT_STRIP,
    DEFAULT_LOGGER_LOCATION,
    DEFAULT_GRANULARITY,
    DATA_PROCESSED_DIR,
    YEARS,
    GSEASON_PERIODS,
    sensor_depth_mapping,
    logger_location_mapping,
    variable_name_mapping,
    granularity_name_mapping,
    strip_name_mapping,
    label_name_mapping
)


# Blueprint
main = Blueprint("main", __name__)

###############################################
# Summary Table
###############################################
# Helper Functions
# 2. get_default_end_date: Determine default end date from filenames.
# 3. load_loggerdata: Load loggerdata from a ZIP file containing a CSV.
# 4. log_and_translate_depth_info: Log debug information for routes.
# 5. ensure_serializable: Recursively convert non-serializable objects.
# 6. axis_label: Get appropriate axis label for a variable.
# 7. sanitize_json: Replace NaN/ndarray objects in JSON.
# 8. filter_data_logger: Load and filter loggerdata for main data display.
# 8. filter_summary_statistics: Load and filter loggerdata for summary statistics.

# Routes
# 1. /get_defaults_and_options: Provide default options for controls.
# 2. /favicon.ico: Serve the favicon.
# 3. /markdown/<path:filename>: Serve markdown files.
# 4. /: Serve the home page.
# 5. /plot_raw: Generate and serve raw data plots.
# 6. /plot_ratio: Generate and serve ratio data plots.

###############################################
# Configuration Data
###############################################

common_xaxis_config = {
    "title": "Date",
    "type": "date",
    "tickformat": "%b\n%Y",  # Default format
    "tickformatstops": [
        # Full year view (months only)
        {"dtickrange": [86400000 * 30, None], "value": "%b\n%Y"},  # > ~30 days
        # Zoomed into weeks
        {"dtickrange": [86400000 * 7, 86400000 * 30], "value": "%b %d"},  # 1–4 weeks
        # Zoomed into days
        {"dtickrange": [None, 86400000 * 7], "value": "%b %d"},  # < 1 week
    ],
    "showline": True,
    "linewidth": 1,
    "linecolor": "gray",
    "showgrid": True,
    "zeroline": False
}


def common_yaxis_config(variable=None):
    title = label_name_mapping.get(variable, variable if variable else "Y Axis Label")
    return dict(
        title=title,
        showticklabels=True,
        showline=True,
        linewidth=1,
        linecolor="gray",
        showgrid=True,
        zeroline=False,
        automargin=True
    )


def common_legend_config(title="<b>Legend</b>"):
    return dict(
        title=dict(text=title, font=dict(size=12)),
        orientation="v",
        yanchor="top",
        y=1.15,  # Adjust as needed for spacing
        xanchor="right",
        x=1
    )

def build_plot_title_and_legend_label(granularity, variable, strip, year, trace_option, logger_location, depth):
    if trace_option == "depths":
        suffix = f"({logger_location_mapping.get(logger_location, logger_location)} Logger)"
    else:
        suffix = f"({sensor_depth_mapping.get(int(depth), f'{depth} inch')} Depth)"

    plot_title = (
        f"{granularity_name_mapping[granularity]} Data Plot for {variable_name_mapping[variable]} "
        f"in Strip {strip}, {year}<br>{suffix}"
    )

    legend_title = "<b>Legend</b>"  # Or set to None if you'd prefer no title
    return plot_title, legend_title

###############################################
# Helper Functions
###############################################

# In-memory cache to store loaded datasets
loaded_datasets = {}


LoggerFileInfo = namedtuple("LoggerFileInfo", ["start_date", "end_date", "granularity", "filename"])


def get_available_years():
    """Extracts available years from filenames in the data directory."""
    data_dir = os.path.join(os.getcwd(), "biochar_app", "data-processed")
    parsed_files = parse_filenames(data_dir)

    # ✅ Use a different variable name to prevent shadowing
    available_years = sorted({start[:4] for start, _, _, _ in parsed_files})

    logging.info(f"📆 Available Years: {available_years}")
    return available_years


def find_matching_file(year, granularity):
    """
    Searches for the correct loggerdata file based on year and granularity.

    :param year: The year to look for.
    :param granularity: The data granularity (e.g., "daily", "monthly", "growingseason").
    :return: The file path if found, otherwise None.
    """
    try:
        logging.info(f"🔍 Searching for loggerdata: Year={year}, Granularity={granularity}")

        # ✅ Construct the expected filename pattern
        file_pattern = f"dataloggerData_{year}-*_*-*_{granularity}.zip"
        matching_files = glob.glob(os.path.join(DATA_PROCESSED_DIR, file_pattern))

        # ✅ If we find multiple matches, pick the most recent one
        if matching_files:
            selected_file = sorted(matching_files)[-1]  # Pick the latest
            logging.info(f"📂 Using loggerdata: {selected_file}")
            return selected_file

        logging.warning(f"⚠️ No matching loggerdata found for {year}, {granularity}")
        return None

    except Exception as e:
        logging.error(f"❌ Error finding matching file: {e}")
        return None


def get_default_end_date(year=None):
    parsed_files = parse_filenames(DATA_PROCESSED_DIR)
    if year:
        filtered_files = [file for file in parsed_files if file.start_date.startswith(str(year))]
    else:
        filtered_files = parsed_files

    if not filtered_files:
        return f"{year}-12-31" if year else "2024-12-31"

    return max(filtered_files, key=lambda x: x.end_date).end_date


def axis_label(variable):
    """Returns appropriate labels for different sensor variables."""
    return label_name_mapping.get(variable, variable)  # Default to variable name if not found


def sanitize_json(data):
    """
    Recursively convert NumPy objects to native Python objects,
    ensuring JSON serializability while handling NaN values.
    """

    def convert(obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()  # Convert NumPy arrays to lists
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)  # Convert NumPy integer to Python int
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)  # Convert NumPy float to Python float
        elif isinstance(obj, np.bool_):
            return bool(obj)  # Convert NumPy bool to Python bool
        elif isinstance(obj, list):
            return [convert(item) for item in obj]  # Recursively handle lists
        elif isinstance(obj, dict):
            return {key: convert(value) for key, value in obj.items()}  # Recursively handle dictionaries
        elif isinstance(obj, float) and np.isnan(obj):
            return None  # Convert NaN to None for JSON safety
        return obj  # Return everything else unchanged

    return convert(data)  # Directly return sanitized data without JSON dumps


def filter_data_logger(df, start_date, end_date):
    """
    Filters logger data for Main Data Display based on the provided date range.
    """
    try:
        # Ensure timestamp is in datetime format
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # ✅ Apply date filtering
        filtered_df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]

        return filtered_df  # No type conversion, NaNs preserved

    except Exception as e:
        logging.error(f"❌ Error in filter_data_logger: {e}")
        return pd.DataFrame()  # Return empty DataFrame if an error occurs


def filter_summary_statistics(df, year, variable, strip, granularity):
    """
    Filters logger data for Summary Statistics tab based on year, variable, strip, and granularity.
    """
    try:
        # ✅ Ensure year column exists
        if "year" not in df.columns:
            logging.warning("⚠️ 'year' column missing from df.")
            return pd.DataFrame()

        # ✅ Apply filtering
        filtered_df = df[
            (df["year"] == year) &
            (df["variable"] == variable) &
            (df["strip"] == strip) &
            (df["granularity"] == granularity)
            ]

        return filtered_df  # NaNs preserved

    except Exception as e:
        logging.error(f"❌ Error in filter_summary_statistics: {e}")
        return pd.DataFrame()  # Return empty DataFrame if an error occurs


###############################################
# Routes
###############################################

@main.route("/get_defaults_and_options", methods=["GET"])
def get_defaults_and_options():
    """Returns dropdown default values and options for the web interface."""

    options = {
        "defaults": {
            "year": DEFAULT_YEAR,
            "variable": DEFAULT_VARIABLE,
            "strip": DEFAULT_STRIP,
            "granularity": DEFAULT_GRANULARITY,
            "depth": DEFAULT_DEPTH,
            "loggerLocation": DEFAULT_LOGGER_LOCATION,
            "startDate": DEFAULT_START_DATE,
            "endDate": get_default_end_date(DEFAULT_YEAR)
        },

        "years": YEARS,  # ✅ Use predefined YEARS variable
        "variables": list(variable_name_mapping.keys()),  # ✅ Extract from mapping
        "granularities": list(granularity_name_mapping.keys()),  # ✅ Extract from mapping
        "strips": list(strip_name_mapping.keys()),  # ✅ Extract from mapping
        "loggerLocations": list(logger_location_mapping.keys()),  # ✅ Extract from mapping
        "depths": list(sensor_depth_mapping.keys()),  # ✅ Extract from mapping

        # ✅ Ensure mappings are correctly assigned
        "variableNameMapping": variable_name_mapping,
        "labelNameMapping": label_name_mapping,
        "granularityNameMapping": granularity_name_mapping,
        "stripNameMapping": strip_name_mapping,
        "loggerLocationMapping": logger_location_mapping,
        "depthMapping": sensor_depth_mapping,
        "gseasonPeriods": GSEASON_PERIODS  # ✅ Add seasonal periods with labels and month ranges
    }

    # logging.info(f"📤 Sending options: {json.dumps(options, indent=2)}")  # ✅ Debugging output
    return jsonify(options)

@main.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(main.root_path, 'static', 'images'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )


# routes.py

@main.route("/get_summary_stats", methods=["POST"])
def get_summary_stats():
    try:
        data = request.get_json()
        year = int(data.get("year"))
        variable = data.get("variable")
        strip = data.get("strip")
        granularity = data.get("granularity")
        depth = str(data.get("depth"))
        start_date = data.get("startDate")
        end_date = data.get("endDate")

        logging.info(f"📊 Summary request: {year}, {variable}, {strip}, {granularity}, {depth}")

        is_temp_variable = variable in ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]

        if granularity == "gseason":
            gseason_stats = load_or_generate_gseason_summary(year)
            key = f"{strip}_D{depth}"
            safe_stats = {}

            for season_code, season_data in gseason_stats.items():
                season_variable_block = season_data.get(variable)
                if not isinstance(season_variable_block, dict):
                    logging.warning(f"⚠️ No data for variable '{variable}' in {season_code}")
                    safe_stats[season_code] = {}
                    continue

                if key not in season_variable_block:
                    logging.warning(f"⚠️ No data for key '{key}' in {season_code}")
                    safe_stats[season_code] = {}
                    continue

                safe_stats[season_code] = season_variable_block[key]

            granularity_label = "Seasonal"
            variable_label = label_name_mapping.get(variable, variable)
            strip_label = strip_name_mapping.get(strip, strip)
            depth_label = sensor_depth_mapping.get(depth, f"{depth} in")
            title = f"{granularity_label} Summary for {variable_label}, {strip_label}, {depth_label}, {year}"

            return jsonify({
                "year": year,
                "variable": variable,
                "strip": strip,
                "granularity": granularity,
                "depth": depth,
                "title": title,
                "gseason_stats": safe_stats
            })

        # ✅ For non-gseason granularities
        df_15min = load_logger_data(year, "15min")
        if df_15min is None or df_15min.empty:
            return jsonify({"error": "No data found for the selected filters."})

        if "timestamp" in df_15min.columns and start_date and end_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            df_15min = filter_data_logger(df_15min, start_dt, end_dt)
            if df_15min.empty:
                return jsonify({"error": "No data available in the specified date range."})

        stats_raw, stats_ratio = compute_summary_statistics(df_15min, variable, strip, depth)
        if is_temp_variable:
            stats_ratio = {}

        granularity_label = granularity_name_mapping.get(granularity, granularity.capitalize())
        variable_label = label_name_mapping.get(variable, variable)
        strip_label = strip_name_mapping.get(strip, strip)
        depth_label = sensor_depth_mapping.get(depth, f"{depth} in")
        title = f"{granularity_label} Summary for {variable_label}, {strip_label}, {depth_label}, {year}"

        return jsonify({
            "year": year,
            "variable": variable,
            "strip": strip,
            "granularity": granularity,
            "depth": depth,
            "title": title,
            "raw_statistics": stats_raw,
            "ratio_statistics": stats_ratio
        })

    except Exception as e:
        logging.exception("❌ Error in /get_summary_stats")
        return jsonify({"error": str(e)}), 500

@main.route("/markdown/<path:filename>")
def serve_markdown(filename):
    """Serves markdown files from the markdown directory."""
    markdown_path = os.path.join(os.path.dirname(__file__), "markdown")
    file_path = os.path.join(markdown_path, filename)

    if not os.path.exists(file_path):
        logging.warning(f"Markdown file not found: {file_path}")
        return jsonify({"error": f"Markdown file '{filename}' not found."}), 404

    return send_from_directory(markdown_path, filename)


@main.route('/get_end_date', methods=['GET'])
def get_end_date():
    year = request.args.get('year', type=str)
    if not year:
        return jsonify({"error": "Year parameter is required"}), 400

    data_dir = DATA_PROCESSED_DIR
    parsed_files = parse_filenames(data_dir)

    try:
        # Extract valid end dates while ensuring proper unpacking
        parsed_end_dates = [
            end_date for parts in parsed_files
            if len(parts) >= 3 and parts[0].startswith(year)
            for start_date, end_date, *_ in [parts]  # Unpack safely
        ]

        if parsed_end_dates:
            max_end_date = max(parsed_end_dates)  # Get latest end date
            return jsonify({"endDate": max_end_date})
        else:
            logging.warning(f"⚠️ No valid end date found for year {year}")
            return jsonify({"endDate": f"{year}-12-31"})  # Default to Dec 31 if none found

    except Exception as e:
        logging.error(f"❌ Error extracting end date: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@main.route("/")
def home():
    return render_template("index.html",
                           strips=[{"value": key, "label": label} for key, label in strip_name_mapping.items()],
                           variables=[{"value": key, "label": label} for key, label in variable_name_mapping.items()],
                           depths=[{"value": key, "label": label} for key, label in sensor_depth_mapping.items()],
                           loggers=[{"value": key, "label": label} for key, label in logger_location_mapping.items()],
                           DEFAULT_YEAR=DEFAULT_YEAR, DEFAULT_START_DATE=DEFAULT_START_DATE,
                           DEFAULT_END_DATE=get_default_end_date(DEFAULT_YEAR), DEFAULT_VARIABLE=DEFAULT_VARIABLE,
                           DEFAULT_DEPTH=DEFAULT_DEPTH,
                           DEFAULT_STRIP=DEFAULT_STRIP, DEFAULT_LOGGER=DEFAULT_LOGGER_LOCATION)

@main.route("/plot_raw", methods=["POST"])
def plot_raw():
    try:
        data = request.get_json()
        logging.info(f"🧪 Incoming request to /plot_raw with data: {data}")

        # ✅ Extract and validate parameters
        required_keys = ["year", "variable", "strip", "granularity", "loggerLocation", "depth", "traceOption"]
        missing_keys = [key for key in required_keys if not data.get(key)]
        if missing_keys:
            return jsonify({"error": f"Missing required parameters: {missing_keys}"}), 400

        year = data["year"]
        variable = data["variable"]
        strip = data["strip"]
        granularity = data["granularity"]
        logger_location = data["loggerLocation"]
        depth = str(data["depth"])
        trace_option = data["traceOption"]
        start_date = str(data["startDate"])
        end_date = str(data["endDate"])

        df = load_logger_data(year, granularity)
        filtered_df = filter_data_logger(df, start_date, end_date)

        traces = []
        if trace_option == "depths":
            for val in ["1", "2", "3"]:
                col = f"{variable}_{val}_raw_{strip}_{logger_location}"
                if col in filtered_df:
                    traces.append(go.Scatter(
                        x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
                        y=filtered_df[col],
                        mode="lines",
                        name=f"{variable} ({sensor_depth_mapping[int(val)]})"
                    ))
        else:
            for val in ["T", "M", "B"]:
                col = f"{variable}_{depth}_raw_{strip}_{val}"
                if col in filtered_df:
                    traces.append(go.Scatter(
                        x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
                        y=filtered_df[col],
                        mode="lines",
                        name=f"{variable} ({val})"
                    ))

        # Optional overlays for VWC
        if variable == "VWC" and "precip_mm" in filtered_df:
            traces.append(go.Bar(
                x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
                y=filtered_df["precip_mm"],
                name="Precipitation (mm)",
                yaxis="y2",
                marker=dict(color="lightgray"),
                opacity=0.7,
                width=1000 * 60 * 60 * 24
            ))

        if variable == "T" and "temp_air_degC" in filtered_df:
            traces.append(go.Scatter(
                x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
                y=filtered_df["temp_air_degC"],
                name="Air Temp (°C)",
                mode="lines",
                line=dict(color="gray", dash="dot"),
                yaxis="y2"
            ))

        # Auto y-axis ranges
        y1_vals, y2_vals = [], []
        for t in traces:
            if getattr(t, "yaxis", "y") == "y":
                y1_vals += list(t.y)
            elif t.yaxis == "y2":
                y2_vals += list(t.y)

        y1_max = max(y1_vals) * 1.1 if y1_vals else None
        y2_max = max(y2_vals) * 1.1 if y2_vals else None

        # Irrigation overlay
        layout_shapes = []
        layout_annotations = []
        if variable == "VWC":
            irrigation_file = f"biochar_app/data-processed/Harmonized_Irrigation_Data_{year}.csv"
            irrigation_df = pd.read_csv(irrigation_file)
            irrigation_df["start_time"] = pd.to_datetime(irrigation_df["start_time"], format="%m/%d/%y %H:%M")
            irrigation_df["end_time"] = pd.to_datetime(irrigation_df["end_time"], format="%m/%d/%y %H:%M")
            irrigation_df["gallons"] = pd.to_numeric(irrigation_df["gallons"], errors="coerce")

            group = "west" if strip in ["S1", "S2"] else "east"
            for _, row in irrigation_df[irrigation_df["location"] == group].iterrows():
                start = pd.to_datetime(row["start_time"])
                gallons = int(float(row["gallons"])) if pd.notna(row["gallons"]) else 0
                midpoint = start + (pd.to_datetime(row["end_time"]) - start) / 2
                layout_shapes.append(dict(
                    type="line",
                    xref="x", yref="paper",
                    x0=start.strftime("%Y-%m-%dT%H:%M:%S"),
                    x1=start.strftime("%Y-%m-%dT%H:%M:%S"),
                    y0=0, y1=1,
                    line=dict(color="rgba(181, 101, 29, 0.3)", width=2.5, dash="dot"),
                    layer="below"
                ))
                layout_annotations.append(dict(
                    x=midpoint.strftime("%Y-%m-%dT%H:%M:%S"),
                    y=1, yref="paper",
                    text=f"{round(gallons / 1000)}k",
                    textangle=0,
                    showarrow=False,
                    font=dict(size=10, color="black"),
                    yanchor="top"
                ))
            traces.append(go.Scatter(
                x=[None], y=[None], mode="lines",
                name="Irrig. Vol. (000 gal)",
                line=dict(color="rgba(181, 101, 29, 0.6)", width=2.5, dash="dot"),
                showlegend=True
            ))

        # Final layout + return
        plot_title, legend_title = build_plot_title_and_legend_label(
            granularity, variable, strip, year, trace_option, logger_location, depth
        )

        layout = go.Layout(
            title=plot_title,
            xaxis=dict(**common_xaxis_config, range=[start_date, end_date]),
            yaxis=dict(**common_yaxis_config(variable), range=[0, y1_max] if y1_max else None),
            yaxis2=dict(
                title="Precipitation (mm)" if variable == "VWC" else "Air Temp (°C)",
                overlaying="y", side="right",
                showgrid=False, showline=True, linecolor="gray",
                range=[0, y2_max] if y2_max else None
            ),
            legend=common_legend_config(title=legend_title),
            template="plotly_white",
            margin=dict(l=50, r=50, t=40, b=40),
            barmode="overlay",
            autosize=True,
            shapes=layout_shapes,
            annotations=layout_annotations
        )

        return jsonify(sanitize_json({
            "data": [trace.to_plotly_json() for trace in traces],
            "layout": layout.to_plotly_json()
        }))

    except Exception as e:
        logging.error(f"❌ Unexpected error in plot_raw: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@main.route("/download_summary_data", methods=["POST"])
def download_summary_data():
    try:
        data = request.get_json()
        year = data.get("year", "unknown")
        variable = data.get("variable", "unknown")
        strip = data.get("strip", "unknown")
        granularity = data.get("granularity", "unknown")
        stats = data.get("summaryStats", {})

        logging.info(f"📥 Download request: {year}, {variable}, {strip}, {granularity}")

        # 🌱 Growing Season Logic
        if granularity == "gseason":
            gseason_stats = load_or_generate_gseason_summary(year)
            raw_rows, ratio_rows = [], []

            for season_code, season_data in gseason_stats.items():
                for var_key, strip_blocks in season_data.items():
                    for strip_key, stat_obj in strip_blocks.items():
                        raw_stats = stat_obj.get("raw_statistics", {})
                        ratio_stats = stat_obj.get("ratio_statistics", {})

                        for trace, values in raw_stats.items():
                            logger = trace.split("_")[-1]
                            raw_rows.append({
                                "Season": season_code,
                                "Variable": var_key,
                                "Strip": strip_key,
                                "Logger Location": logger,
                                "Min": round(values.get("min", 0), 4),
                                "Mean": round(values.get("mean", 0), 4),
                                "Max": round(values.get("max", 0), 4),
                                "Std": round(values.get("std", 0), 4)
                            })

                        for trace, values in ratio_stats.items():
                            parts = trace.split("_")
                            strips = parts[3] if len(parts) >= 5 else "unknown"
                            logger = parts[-1]
                            ratio_rows.append({
                                "Season": season_code,
                                "Variable": var_key,
                                "Strips": strips,
                                "Logger Location": logger,
                                "Min": round(values.get("min", 0), 4),
                                "Mean": round(values.get("mean", 0), 4),
                                "Max": round(values.get("max", 0), 4),
                                "Std": round(values.get("std", 0), 4)
                            })

            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, "w") as zf:
                if raw_rows:
                    df_raw = pd.DataFrame(raw_rows)
                    zf.writestr("gseason_raw_summary.csv", df_raw.to_csv(index=False))
                if ratio_rows:
                    df_ratio = pd.DataFrame(ratio_rows)
                    zf.writestr("gseason_ratio_summary.csv", df_ratio.to_csv(index=False))

            zip_buffer.seek(0)
            filename = f"summary_gseason_{year}_{variable}_{strip}.zip"
            logging.info(f"📦 Sending ZIP: {filename}")
            return send_file(zip_buffer, mimetype="application/zip", download_name=filename, as_attachment=True)

        # 📊 Standard Granularities
        if not stats:
            return jsonify({"error": "No summary statistics provided"}), 400

        rows = []
        for trace, values in stats.items():
            parts = trace.split("_")
            var = parts[0] if parts else "unknown"
            type_ = "raw" if "_raw_" in trace else "ratio" if "_ratio_" in trace else "unknown"
            strip_match = re.search(r"_(S\d(?:_S\d)*)_", trace)
            strips = strip_match.group(1) if strip_match else "unknown"
            logger_location = parts[-1] if parts else "unknown"

            rows.append({
                "Variable": var,
                "Type": type_,
                "Strips": strips,
                "Logger Location": logger_location,
                "Min": round(values.get("min", 0), 4),
                "Mean": round(values.get("mean", 0), 4),
                "Max": round(values.get("max", 0), 4),
                "Std": round(values.get("std", 0), 4)
            })

        df = pd.DataFrame(rows)
        csv_data = df.to_csv(index=False)
        filename = f"summary_data_{year}_{variable}_{strip}_{granularity}.csv"
        logging.info(f"📤 Sending CSV: {filename}")
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )

    except Exception as e:
        logging.exception("❌ Error in /download_summary_data")
        return jsonify({"error": str(e)}), 500


@main.route("/plot_ratio", methods=["POST"])
def plot_ratio():
    try:
        data = request.get_json()
        logging.info(f"🧪 Incoming request to /plot_ratio with data: {data}")

        # ✅ Extract user selections
        year = data["year"]
        start_date = str(data["startDate"])
        end_date = str(data["endDate"])
        granularity = data["granularity"]
        selected_depth = str(data["depth"])
        selected_logger = data["loggerLocation"]
        variable = data["variable"]
        trace_option = data.get("traceOption", "depths")  # Optional fallback

        # ✅ Validate required fields
        if selected_depth == "undefined" or selected_logger == "undefined":
            return jsonify({"error": "Depth or Logger Location is undefined."}), 400
        if granularity not in granularity_name_mapping:
            return jsonify({"error": f"Invalid granularity: {granularity}"}), 400

        # ✅ Load data
        try:
            df = load_logger_data(year, granularity)
        except FileNotFoundError as e:
            return jsonify({"error": str(e)}), 400

        filtered_df = filter_data_logger(df, start_date, end_date)
        if filtered_df.empty:
            return jsonify({"error": "No data found for the selected parameters."}), 404

        # ✅ Determine columns
        expected_columns = [
            f"{variable}_{selected_depth}_ratio_S1_S2_{selected_logger}",
            f"{variable}_{selected_depth}_ratio_S3_S4_{selected_logger}"
        ]
        available_columns = [col for col in expected_columns if col in filtered_df.columns]
        if not available_columns:
            return jsonify({"error": "No matching columns found in dataset"}), 400

        # ✅ Generate plot
        fig = go.Figure()
        for col in available_columns:
            group_label = "S1/S2" if "S1_S2" in col else "S3/S4"
            y_values = filtered_df[col].replace({pd.NA: None}).tolist()
            fig.add_trace(go.Scatter(
                x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                y=y_values,
                mode="lines",
                name=group_label,
                hovertemplate="%{x|%b %d, %Y}: %{y:.2f}<extra></extra>",
                connectgaps=False
            ))

        # ✅ Shared title and legend format
        plot_title, legend_title = build_plot_title_and_legend_label(
            granularity, variable, "ALL", year, trace_option, selected_logger, selected_depth
        )

        fig.update_layout(
            title=plot_title.replace("Data Plot", "Ratio Data Plot").replace("in Strip ALL, ", ""),
            xaxis=dict(
                **common_xaxis_config,
                range=[start_date, end_date]
            ),
            yaxis=dict(
                title=f"{label_name_mapping.get(variable, variable)} Ratio",
                tickformat=".2f",
                showline=True,
                linecolor="black"
            ),
            margin=dict(l=50, r=50, t=40, b=40),
            legend=common_legend_config(title=legend_title),
            template="plotly_white",
            autosize=True
        )

        return jsonify(sanitize_json(fig.to_plotly_json()))

    except Exception as e:
        logging.error(f"❌ Unexpected error in plot_ratio: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@main.route("/plot_ratio_gseason", methods=["POST"])
def plot_ratio_gseason():
    logging.info("✅ plot_ratio_gseason was called")
    try:
        data = request.get_json()
        logging.info(f"🧪 Incoming request to /plot_ratio_gseason with data: %s", data)

        required_keys = ["year", "variable", "depth", "loggerLocation"]
        missing_keys = [key for key in required_keys if key not in data or not data[key]]
        if missing_keys:
            return jsonify({"error": f"Missing required parameters: {missing_keys}"}), 400

        year = data["year"]
        variable = data["variable"]
        selected_depth = str(data["depth"])
        selected_logger = data["loggerLocation"]

        df = load_logger_data(year, "gseason")
        logging.info(f"🧪 plot_ratio_gseason request:", data)
        logging.info(f"📁 gseason DataFrame columns:", df.columns.tolist())

        expected_columns = [
            f"{variable}_{selected_depth}_ratio_S1_S2_{selected_logger}",
            f"{variable}_{selected_depth}_ratio_S3_S4_{selected_logger}"
        ]

        logging.info(f"🔎 Gseason ratio expected columns: {expected_columns}")
        logging.info(f"📋 Available columns in df: {df.columns.tolist()}")

        available_columns = [col for col in expected_columns if col in df.columns]
        if not available_columns:
            return jsonify({"error": "No matching ratio columns found for growing season"}), 404

        fig = go.Figure()
        for col in available_columns:
            label = "S1/S2" if "S1_S2" in col else "S3/S4"
            fig.add_trace(go.Bar(
                x=df["gseason_periods"],
                y=df[col].replace({pd.NA: None}).tolist(),
                name=label,
                width=0.2,
                hovertemplate="%{x}: %{y:.2f}<extra></extra>"
            ))

        legend_title = f"{variable}, {selected_logger}, {sensor_depth_mapping.get(int(selected_depth), 'Unknown Depth')}"
        ratio_y_label = f"{label_name_mapping.get(variable, variable)} Ratio"

        season_keys = list(GSEASON_PERIODS.keys())
        season_labels = [
            f"{key.split('_', 1)[1]} ({start} to {end})"
            for key, (start, end) in GSEASON_PERIODS.items()
        ]

        fig.update_layout(
            title=f"Growing Season Ratio Plot for {variable_name_mapping.get(variable, variable)} at {sensor_depth_mapping.get(int(selected_depth), 'Unknown Depth')}",
            xaxis=dict(
                title="Growing Season Period",
                type="category",
                categoryorder="array",
                categoryarray=season_keys,
                tickmode="array",
                tickvals=season_keys,
                ticktext=season_labels
            ),
            yaxis=dict(
                title=ratio_y_label,
                tickformat=".2f",
                showline=True,
                linecolor="black"
            ),
            legend=common_legend_config(title=legend_title),
            template="plotly_white",
            margin=dict(l=50, r=50, t=40, b=40),
            autosize=True
        )

        logging.info("✅ plot_ratio_gseason completed successfully.")
        return jsonify(sanitize_json(fig.to_plotly_json()))

    except Exception as e:
        logging.error(f"❌ Error in plot_ratio_gseason: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@main.route("/download_data", methods=["POST"])
def download_data():
    try:
        params = request.get_json()
        year = int(params["year"])
        variable = params["variable"]
        strip = params["strip"]
        granularity = params["granularity"]
        data_type = params["dataType"]  # "raw", "ratio", or "all"

        # Load data
        df = load_logger_data(year, granularity)

        # Filter by user inputs
        filtered_df = filter_data_logger(df, params["startDate"], params["endDate"])

        # Get columns for the selected variable and strip
        variable_cols = [col for col in filtered_df.columns if variable in col and strip in col]

        # Select raw or ratio columns
        if data_type == "raw":
            cols = [col for col in variable_cols if "_ratio_" not in col]
        elif data_type == "ratio":
            cols = [col for col in variable_cols if "_ratio_" in col]
        elif data_type == "all":
            cols = variable_cols
        else:
            return jsonify({"error": f"Invalid dataType: {data_type}"}), 400

        output_df = filtered_df[["timestamp"] + cols].copy()
        output_df["timestamp"] = output_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Stream CSV
        csv_data = output_df.to_csv(index=False)
        filename = f"{data_type}_data_{year}_{variable}_{strip}_{granularity}.csv"
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )
    except Exception as e:
        logging.error(f"❌ Error in download_data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

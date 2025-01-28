# Routes.py - Flask application for Biochar Project

# Libraries
from flask import Blueprint, jsonify, request, send_from_directory, render_template
import os
import pandas as pd
import zipfile
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import logging
import json

# Blueprint
main = Blueprint("main", __name__)

###############################################
# Summary Table
###############################################
# Helper Functions
# 1. parse_filenames: Parse filenames to extract date ranges.
# 2. get_default_end_date: Determine default end date from filenames.
# 3. load_dataset: Load dataset from a ZIP file containing a CSV.
# 4. log_and_translate_depth_info: Log debug information for routes.
# 5. ensure_serializable: Recursively convert non-serializable objects.
# 6. y_axis_label: Get appropriate Y-axis label for a variable.
# 7. sanitize_json: Replace NaN/ndarray objects in JSON.
# 8. filter_loaded_dataset: Load and filter datasets by date range.

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

DEFAULT_YEAR = 2024
DEFAULT_START_DATE = f"{DEFAULT_YEAR}-01-01"
DEFAULT_VARIABLE = "VWC"
DEFAULT_DEPTH = "1"
DEFAULT_STRIP = "S1"
DEFAULT_LOGGER_LOCATION = "T"
DEFAULT_GRANULARITY = "daily"

strips = ["S1", "S2", "S3", "S4"]
variables = ["VWC", "T", "EC", "SWC"]
sensor_depths = [1, 2, 3]
loggers = ["T", "M", "B"]
plot_colors = ["red", "blue", "green", "purple", "orange"]  # Add more colors as needed
sensor_depth_mapping = {
    1: "6 inches",
    2: "12 inches",
    3: "18 inches"
}

logger_location_mapping = {
    "T": "Top",
    "M": "Middle",
    "B": "Bottom"
}

###############################################
# Helper Functions
###############################################

def parse_filenames(data_dir, prefix="dataloggerData_", suffix=".zip"):
    filenames = [f for f in os.listdir(data_dir) if f.startswith(prefix) and f.endswith(suffix)]
    parsed_files = []

    for filename in filenames:
        try:
            parts = filename.split("_")
            start_date = parts[1]
            end_date = parts[2].split(".")[0]
            parsed_files.append((start_date, end_date, filename))
        except (IndexError, ValueError):
            continue

    return parsed_files

def get_default_end_date():
    data_dir = os.path.join(os.getcwd(), "flask", "data-raw")
    parsed_files = parse_filenames(data_dir)
    if not parsed_files:
        return f"{DEFAULT_YEAR}-12-31"
    return max(parsed_files, key=lambda x: x[1])[1]

DEFAULT_END_DATE = get_default_end_date()

def load_dataset(file_path):
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f, parse_dates=['datetime'])
                df = df.where(pd.notnull(df), None)
                return df
    except Exception as e:
        logging.error(f"Error loading dataset from {file_path}: {e}")
        return None

def log_and_translate_depth_info(data, route_name):
    depth = data.get("depth", DEFAULT_DEPTH)
    try:
        depth_display = sensor_depth_mapping[int(depth)]
    except (ValueError, KeyError):
        depth_display = f"{depth} inches"
    print(f"--- Debug Info for {route_name} ---")
    print(f"Incoming request data: {data}")
    print(f"Translated depth: {depth_display}")
    print("----------------------------")

def ensure_serializable(obj):
    if isinstance(obj, dict):
        return {key: ensure_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [ensure_serializable(item) for item in obj]
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

def y_axis_label(variable):
    if variable == "VWC":
        return "Volumetric Water Content (%)"
    elif variable == "T":
        return "Temperature (°C)"
    elif variable == "EC":
        return "Electrical Conductivity (dS/m)"
    elif variable == "SWC":
        return "Soil Water Content (Volume)"
    else:
        return variable

def sanitize_json(data):
    return json.loads(json.dumps(data, default=ensure_serializable))

def filter_loaded_dataset(year, granularity, start_date, end_date):
    data_dir = os.path.join(os.getcwd(), "flask", "data-processed")
    parsed_files = parse_filenames(data_dir, suffix=f"_{granularity}.zip")
    matching_files = [f for start, end, f in parsed_files if start.startswith(f"{year}-01-01")]

    if not matching_files:
        raise FileNotFoundError(f"No dataset found for year {year} with granularity {granularity}")

    file_path = os.path.join(data_dir, matching_files[0])
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f, parse_dates=['datetime'])
    except Exception as e:
        raise FileNotFoundError(f"Dataset could not be loaded from {file_path}: {e}")

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    filtered_df = df[(df["datetime"] >= start_date) & (df["datetime"] <= end_date)]
    filtered_df = filtered_df.replace({np.nan: None})
    print(f"Filtered dataset for {granularity}:")
    print(filtered_df.head())
    print(f"Available columns in dataset: {filtered_df.columns}")
    return filtered_df, df["datetime"], start_date, end_date

###############################################
# Routes
###############################################

@main.route("/get_defaults_and_options")
def get_defaults_and_options():
    response_data = {
        "defaults": {
            "year": DEFAULT_YEAR,
            "startDate": DEFAULT_START_DATE,
            "endDate": DEFAULT_END_DATE,
            "variable": DEFAULT_VARIABLE,
            "depth": str(DEFAULT_DEPTH),
            "strip": DEFAULT_STRIP,
            "loggerLocation": DEFAULT_LOGGER_LOCATION,
        },
        "years": [2024, 2023],
        "strips": strips,
        "variables": variables,
        "depths": [{"value": str(depth), "label": sensor_depth_mapping[depth]} for depth in sensor_depths],
        "loggerLocations": [{"value": key, "label": value} for key, value in logger_location_mapping.items()],
    }
    print("Returning defaults and options:", response_data)
    return jsonify(response_data)

@main.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(main.root_path, 'static', 'images'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )

@main.route("/markdown/<path:filename>")
def serve_markdown(filename):
    markdown_path = os.path.join(os.getcwd(), "flask", "markdown")
    print(f"Requested file: {filename}")
    print(f"Looking for file at: {markdown_path}")
    try:
        return send_from_directory(markdown_path, filename)
    except Exception as e:
        print(f"Error: {e}")
        return f"Error serving markdown file: {e}", 404

@main.route("/")
def home():
    return render_template(
        "index.html",
        strips=strips,
        variables=variables,
        depths=[{"value": key, "label": label} for key, label in sensor_depth_mapping.items()],
        loggers=loggers,
        DEFAULT_YEAR=DEFAULT_YEAR,
        DEFAULT_START_DATE=DEFAULT_START_DATE,
        DEFAULT_END_DATE=DEFAULT_END_DATE,
        DEFAULT_VARIABLE=DEFAULT_VARIABLE,
        DEFAULT_DEPTH=DEFAULT_DEPTH,
        DEFAULT_STRIP=DEFAULT_STRIP,
        DEFAULT_LOGGER=DEFAULT_LOGGER_LOCATION,
    )


@main.route("/plot_raw", methods=["POST"])
def plot_raw():
    data = request.json
    print("Received Raw Plot Params:", data)
    log_and_translate_depth_info(data, "plot_raw")

    try:
        granularity = "15min" if (datetime.strptime(data["endDate"], "%Y-%m-%d") - datetime.strptime(data["startDate"], "%Y-%m-%d")).days <= 30 else "daily"
        filtered_df, _, _, _ = filter_loaded_dataset(data["year"], granularity, data["startDate"], data["endDate"])

        variable_columns = [
            f"{data['variable']}_{data['depth']}_raw_{data['strip']}_{logger}_{granularity}"
            for logger in loggers
        ]
        available_columns = [col for col in variable_columns if col in filtered_df.columns]

        if not available_columns:
            return jsonify({"error": f"No matching columns found in dataset: {variable_columns}"}), 400

        fig = go.Figure()
        added_columns = set()
        strip_number = int(data['strip'][1:])  # Extract numeric part of the strip
        legend_title = f"{data['variable']} Strip {strip_number}, {granularity} data"

        for col in available_columns:
            logger_label = logger_location_mapping.get(col.split("_")[-2], "Unknown")

            if col not in added_columns:
                added_columns.add(col)
                fig.add_trace(go.Scatter(
                    x=filtered_df["datetime"],
                    y=filtered_df[col],
                    mode="lines",
                    name=logger_label,  # Legend simplified to Top, Middle, Bottom
                    line=dict(color=plot_colors[len(added_columns) % len(plot_colors)]),
                    hovertemplate="%{x|%m/%d}: %{y:.2f}<extra></extra>",
                    connectgaps=False
                ))

        selected_depth = data.get("depth", DEFAULT_DEPTH)
        fig.update_layout(
            title=f"Raw Data Plot for {data['variable']} at {sensor_depth_mapping[int(selected_depth)]} in {data['strip']}",
            xaxis_title="Date",
            yaxis_title=y_axis_label(data['variable']),
            template="plotly_white",
            legend=dict(
                title=dict(
                    text=f"<b>{legend_title}</b>",
                    font=dict(size=12)
                ),
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99
            )
        )

        sanitized_json = sanitize_json(fig.to_plotly_json())
        return jsonify(sanitized_json)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Error generating raw plot."}), 500


@main.route("/plot_ratio", methods=["POST"])
def plot_ratio():
    data = request.json
    print("Received Ratio Plot Params:", data)
    log_and_translate_depth_info(data, "plot_ratio")

    try:
        granularity = "15min" if (datetime.strptime(data["endDate"], "%Y-%m-%d") - datetime.strptime(data["startDate"], "%Y-%m-%d")).days <= 30 else "daily"
        filtered_df, _, _, _ = filter_loaded_dataset(data["year"], DEFAULT_GRANULARITY, data["startDate"], data["endDate"])

        selected_depth = data.get("depth", DEFAULT_DEPTH)
        selected_variable = data.get("variable", DEFAULT_VARIABLE)
        selected_logger = data.get("loggerLocation", DEFAULT_LOGGER_LOCATION)

        # Filter for the two necessary ratio columns
        ratio_columns = [
            col for col in filtered_df.columns
            if selected_variable.lower() in col.lower()
               and "ratio" in col.lower()
               and f"_{selected_depth}_" in col
               and f"_{selected_logger}_" in col
               and ("S1_S2" in col or "S3_S4" in col)  # Filter for S1/S2 and S3/S4
        ]

        if len(ratio_columns) != 2:
            return jsonify({"error": "Expected exactly two ratio columns for S1/S2 and S3/S4."}), 400

        fig = go.Figure()
        legend_title = f"{selected_variable}, {logger_location_mapping[selected_logger]}, {sensor_depth_mapping[int(selected_depth)]}"

        for col in ratio_columns:
            if "S1_S2" in col:
                trace_label = "S1/S2"
            elif "S3_S4" in col:
                trace_label = "S3/S4"
            else:
                continue

            fig.add_trace(go.Scatter(
                x=filtered_df["datetime"],
                y=filtered_df[col],
                mode="lines",
                name=trace_label,
                line=dict(color=plot_colors[len(fig.data) % len(plot_colors)]),
                hovertemplate="%{x|%m/%d}: %{y:.2f}<extra></extra>",
                connectgaps=False
            ))

        fig.update_layout(
            title=f"Ratio Data Plot for {selected_variable} at {sensor_depth_mapping[int(selected_depth)]}, Biochar-Injected Strips (S1 & S3) to <br>no Biochar Strips (S2 & S4)",
            xaxis_title="Date",
            yaxis_title="Ratio",
            template="plotly_white",
            legend=dict(
                title=dict(
                    text=f"<b>{legend_title}</b>",
                    font=dict(size=12)
                ),
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99
            )
        )

        sanitized_json = sanitize_json(fig.to_plotly_json())
        return jsonify(sanitized_json)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Error generating ratio plot."}), 500



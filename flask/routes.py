# Routes.py - Flask application for Biochar Project

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
plot_colors = ["red", "blue", "green", "purple", "orange"]

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
    """
    Parses filenames in the specified directory to extract start and end dates.

    Args:
        data_dir (str): The directory containing the files.
        prefix (str): The prefix of the files to parse.
        suffix (str): The suffix of the files to parse.

    Returns:
        list: A list of tuples in the format (start_date, end_date, filename).
    """
    filenames = [f for f in os.listdir(data_dir) if f.startswith(prefix) and f.endswith(suffix)]
    parsed_files = []

    for filename in filenames:
        try:
            # Ensure the filename matches the expected format
            if not filename.startswith(prefix) or not filename.endswith(suffix):
                continue

            parts = filename[len(prefix):-len(suffix)].split("_")  # Extract the date parts
            if len(parts) != 2:
                print(f"Skipping invalid filename format: {filename}")
                continue

            start_date, end_date = parts
            parsed_files.append((start_date, end_date, filename))

        except (IndexError, ValueError) as e:
            print(f"Error parsing filename {filename}: {e}")
            continue

    return parsed_files


def get_default_end_date(year=None):
    data_dir = os.path.join(os.getcwd(), "flask", "data-raw")
    parsed_files = parse_filenames(data_dir)
    if not parsed_files:
        return f"{DEFAULT_YEAR}-12-31"

    if year:
        # Filter parsed files by the specified year
        filtered_files = [file for file in parsed_files if file[1].startswith(str(year))]
        if filtered_files:
            return max(filtered_files, key=lambda x: x[1])[1]

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

def y_axis_label(variable):
    """Returns appropriate Y-axis labels for different sensor variables."""
    labels = {
        "VWC": "Volumetric Water Content (%)",
        "T": "Temperature (°C)",
        "EC": "Electrical Conductivity (dS/m)",
        "SWC": "Soil Water Content (Volume)"
    }
    return labels.get(variable, variable)  # Default to the variable name if not found


def log_and_translate_depth_info(data, route_name):
    """Logs and translates depth info for debugging."""
    depth = data.get("depth", DEFAULT_DEPTH)
    try:
        depth_display = sensor_depth_mapping[int(depth)]
    except (ValueError, KeyError):
        depth_display = f"{depth} inches"

    print(f"--- Debug Info for {route_name} ---")
    print(f"Incoming request data: {data}")
    print(f"Translated depth: {depth_display}")
    print("----------------------------")


def sanitize_json(data):
    def convert(obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()  # Convert NumPy arrays to lists
        elif isinstance(obj, list):
            return [convert(item) for item in obj]  # Recursively handle lists
        elif isinstance(obj, dict):
            return {key: convert(value) for key, value in obj.items()}  # Recursively handle dictionaries
        elif isinstance(obj, float) and np.isnan(obj):
            return None  # Convert NaN to None
        return obj  # Return everything else unchanged

    return json.loads(json.dumps(data, default=convert))


def filter_loaded_dataset(year, granularity, start_date, end_date):
    """
    Load and filter the dataset based on year, granularity, and date range.

    Args:
        year (str): The year to load data for.
        granularity (str): The time granularity ('15min', '1hour', 'daily').
        start_date (str): The requested start date in 'YYYY-MM-DD' format.
        end_date (str): The requested end date in 'YYYY-MM-DD' format.

    Returns:
        tuple: (filtered DataFrame, full datetime series, start_date, end_date)
    """
    data_dir = os.path.join(os.getcwd(), "flask", "data-processed")
    parsed_files = parse_filenames(data_dir, suffix=f"_{granularity}.zip")

    # ✅ Find the latest dataset for the requested year
    matching_files = [f for start, end, g, f in parsed_files if start.startswith(f"{year}-01-01") and g == granularity]

    if not matching_files:
        raise FileNotFoundError(f"No dataset found for year {year} with granularity {granularity}")

    # ✅ Select the dataset with the latest end date
    selected_file = max(matching_files, key=lambda x: x[1])

    file_path = os.path.join(data_dir, selected_file)
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f, parse_dates=['datetime'])
    except Exception as e:
        raise FileNotFoundError(f"Dataset could not be loaded from {file_path}: {e}")

    # ✅ Convert datetime format
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")

    # ✅ Filter based on selected date range
    filtered_df = df[(df["datetime"] >= start_date) & (df["datetime"] <= end_date)]
    filtered_df = filtered_df.replace({np.nan: None})

    # ✅ Round numeric values for cleaner JSON
    for col in filtered_df.columns:
        if col != "datetime":
            filtered_df[col] = filtered_df[col].apply(lambda x: round(x, 4) if pd.notna(x) else None)

    return filtered_df, df["datetime"], start_date, end_date


###############################################
# Routes
###############################################

@main.route("/get_defaults_and_options")
def get_defaults_and_options():
    response_data = {
        "defaults": {
            "year": DEFAULT_YEAR,
            "startDate": f"{DEFAULT_YEAR}-01-01",
            "endDate": f"{DEFAULT_YEAR}-12-31",
            "variable": DEFAULT_VARIABLE,
            "depth": str(DEFAULT_DEPTH),
            "strip": DEFAULT_STRIP,
            "loggerLocation": DEFAULT_LOGGER_LOCATION,
        },
        "years": [2023, 2024, 2025],
        "strips": strips,
        "variables": variables,
        "depths": [{"value": str(depth), "label": sensor_depth_mapping[depth]} for depth in sensor_depths],
        "loggerLocations": [{"value": key, "label": value} for key, value in logger_location_mapping.items()],
    }
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

    data_dir = os.path.join(os.getcwd(), "flask", "data-raw")
    parsed_files = parse_filenames(data_dir)

    # Filter parsed files for the given year
    filtered_files = [
        end_date for start_date, end_date, _ in parsed_files if start_date.startswith(year)
    ]
    if not filtered_files:
        return jsonify({"endDate": f"{year}-12-31"})  # Default to December 31 if no files

    max_end_date = max(filtered_files)  # Get the latest end date
    return jsonify({"endDate": max_end_date})

@main.route("/")
def home():
    return render_template("index.html", strips=strips, variables=variables,
                           depths=[{"value": key, "label": label} for key, label in sensor_depth_mapping.items()],
                           loggers=loggers,
                           DEFAULT_YEAR=DEFAULT_YEAR, DEFAULT_START_DATE=DEFAULT_START_DATE,
                           DEFAULT_END_DATE=DEFAULT_END_DATE, DEFAULT_VARIABLE=DEFAULT_VARIABLE,
                           DEFAULT_DEPTH=DEFAULT_DEPTH,
                           DEFAULT_STRIP=DEFAULT_STRIP, DEFAULT_LOGGER=DEFAULT_LOGGER_LOCATION)


@main.route("/plot_raw", methods=["POST"])
def plot_raw():
    data = request.json
    if not data:
        logging.error("No data provided in the plot raw request.")
        return jsonify({"error": "No data provided in the plot raw request."}), 400

    try:
        logging.info(f"📩 Received request data: {data}")
        granularity = "15min" if (datetime.strptime(data["endDate"], "%Y-%m-%d") - datetime.strptime(data["startDate"], "%Y-%m-%d")).days <= 30 else "daily"
        filtered_df, _, _, _ = filter_loaded_dataset(data["year"], granularity, data["startDate"], data["endDate"])

        if filtered_df.empty:
            logging.warning("No data found for the selected parameters.")
            return jsonify({"error": "No data found for the selected parameters."}), 404

        selected_depth = data.get("depth", DEFAULT_DEPTH)
        variable_columns = [
            f"{data['variable']}_{selected_depth}_raw_{data['strip']}_{logger}_{granularity}"
            for logger in loggers
        ]
        available_columns = [col for col in variable_columns if col in filtered_df.columns]

        if not available_columns:
            logging.warning(f"No matching columns found in dataset: {variable_columns}")
            return jsonify({"error": f"No matching columns found in dataset: {variable_columns}"}), 400

        fig = go.Figure()
        legend_title = "Logger Location"

        for col in available_columns:
            logger_label = logger_location_mapping.get(col.split("_")[-2], "Unknown")

            # 🛠️ **Ensure proper NaN conversion before adding traces**
            y_values = [None if pd.isna(val) else val for val in filtered_df[col].tolist()]

            fig.add_trace(go.Scatter(
                x=filtered_df["datetime"],
                y=y_values,
                mode="lines",
                name=logger_label,
                hovertemplate="%{x|%m/%d}: %{y:.2f}<extra></extra>",
                connectgaps=False
            ))

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

        # 🛠️ **Debug: Before Sanitization**
        raw_json = fig.to_plotly_json()
        logging.info(f"📝 JSON before sanitize_json (first 20 y-values if available):")
        if "data" in raw_json and len(raw_json["data"]) > 0:
            first_20_y_values = raw_json["data"][0]["y"]
            if isinstance(first_20_y_values, list):
                logging.info(f"First 20 y-values: {first_20_y_values[:20]}")
            else:
                logging.error(f"❌ Unexpected y-values format: {type(first_20_y_values)} - {first_20_y_values}")

        # ✅ Apply sanitize_json
        sanitized_json = sanitize_json(raw_json)

        # 🛠️ **Debug: After Sanitization**
        logging.info(f"✅ JSON after sanitize_json (first 20 y-values if available):")
        if "data" in sanitized_json and len(sanitized_json["data"]) > 0:
            first_20_y_values_sanitized = sanitized_json["data"][0]["y"]
            if isinstance(first_20_y_values_sanitized, list):
                logging.info(f"First 20 y-values after sanitization: {first_20_y_values_sanitized[:20]}")
            else:
                logging.error(f"❌ Unexpected y-values format after sanitization: {type(first_20_y_values_sanitized)} - {first_20_y_values_sanitized}")

        return jsonify(sanitized_json)

    except Exception as e:
        logging.error(f"❌ Unexpected error in plot_raw: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@main.route("/plot_ratio", methods=["POST"])
def plot_ratio():
    data = request.json
    if not data:
        logging.error("No data provided in the ratio plot request.")
        return jsonify({"error": "No data provided in the request."}), 400

    try:
        logging.info(f"📩 Received request data: {data}")
        granularity = "15min" if (datetime.strptime(data["endDate"], "%Y-%m-%d") - datetime.strptime(data["startDate"], "%Y-%m-%d")).days <= 30 else "daily"
        filtered_df, _, _, _ = filter_loaded_dataset(data["year"], granularity, data["startDate"], data["endDate"])

        if filtered_df.empty:
            logging.warning("No data found for the selected parameters.")
            return jsonify({"error": "No data found for the selected parameters."}), 404

        selected_depth = data.get("depth", DEFAULT_DEPTH)
        selected_variable = data.get("variable", DEFAULT_VARIABLE)
        selected_logger = data.get("locationType", DEFAULT_LOGGER_LOCATION)

        # Identify relevant ratio columns
        ratio_columns = [
            col for col in filtered_df.columns
            if selected_variable.lower() in col.lower()
            and "ratio" in col.lower()
            and f"_{selected_depth}_" in col
            and f"_{selected_logger}_" in col
            and ("S1_S2" in col or "S3_S4" in col)
        ]

        if len(ratio_columns) != 2:
            logging.error(f"Expected exactly two ratio columns, found {len(ratio_columns)}: {ratio_columns}")
            return jsonify({"error": "Expected exactly two ratio columns for S1/S2 and S3/S4."}), 400

        # Create Plotly figure
        fig = go.Figure()
        legend_title = f"{selected_variable}, {logger_location_mapping[selected_logger]}, {sensor_depth_mapping[int(selected_depth)]}"

        for col in ratio_columns:
            trace_label = "S1/S2" if "S1_S2" in col else "S3/S4"

            # 🛠️ **Ensure proper NaN conversion before adding traces**
            y_values = [None if pd.isna(val) else val for val in filtered_df[col].tolist()]

            fig.add_trace(go.Scatter(
                x=filtered_df["datetime"],
                y=y_values,
                mode="lines",
                name=trace_label,
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

        # 🛠️ **Debug: Before Sanitization**
        ratio_json = fig.to_plotly_json()
        logging.info(f"📝 JSON before sanitize_json (first 20 y-values if available):")
        if "data" in ratio_json and len(ratio_json["data"]) > 0:
            first_20_y_values = ratio_json["data"][0]["y"]
            if isinstance(first_20_y_values, list):
                logging.info(f"First 20 y-values: {first_20_y_values[:20]}")
            else:
                logging.error(f"❌ Unexpected y-values format: {type(first_20_y_values)} - {first_20_y_values}")

        # ✅ Apply sanitize_json
        sanitized_json = sanitize_json(ratio_json)

        # 🛠️ **Debug: After Sanitization**
        logging.info(f"✅ JSON after sanitize_json (first 20 y-values if available):")
        if "data" in sanitized_json and len(sanitized_json["data"]) > 0:
            first_20_y_values_sanitized = sanitized_json["data"][0]["y"]
            if isinstance(first_20_y_values_sanitized, list):
                logging.info(f"First 20 y-values after sanitization: {first_20_y_values_sanitized[:20]}")
            else:
                logging.error(f"❌ Unexpected y-values format after sanitization: {type(first_20_y_values_sanitized)} - {first_20_y_values_sanitized}")

        return jsonify(sanitized_json)

    except Exception as e:
        logging.error(f"❌ Unexpected error in plot_ratio: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
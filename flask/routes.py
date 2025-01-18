# revised routes.py Jan 1
from flask import Blueprint, jsonify, request, send_from_directory, render_template
import os
import pandas as pd
import markdown
import zipfile
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import logging

main = Blueprint("main", __name__)

def parse_filenames(data_dir, prefix="dataloggerData_", suffix=".zip"):
    """
    Parses filenames in the specified directory to extract date ranges.

    Parameters:
    - data_dir: Directory to search for files.
    - prefix: Filename prefix to filter (default: "dataloggerData_").
    - suffix: Filename suffix to filter (default: ".zip").

    Returns:
    - List of tuples (start_date, end_date, filename).
    """
    filenames = [f for f in os.listdir(data_dir) if f.startswith(prefix) and f.endswith(suffix)]
    parsed_files = []

    for filename in filenames:
        try:
            parts = filename.split("_")
            start_date = parts[1]
            end_date = parts[2].split(".")[0]
            parsed_files.append((start_date, end_date, filename))
        except (IndexError, ValueError):
            continue  # Skip files with unexpected naming patterns

    return parsed_files

# Default configurations

def get_default_end_date():
    """
    Determines the DEFAULT_END_DATE by parsing filenames in the data-raw directory.

    Returns:
    - A string representing the default end date in 'YYYY-MM-DD' format.
    """
    data_dir = os.path.join(os.getcwd(), "flask", "data-raw")
    parsed_files = parse_filenames(data_dir)

    if not parsed_files:
        return DEFAULT_END_DATE  # Fallback to hardcoded date if no valid files found

    # Find the latest end date
    latest_end_date = max(parsed_files, key=lambda x: x[1])[1]
    return latest_end_date

DEFAULT_YEAR = 2024
DEFAULT_START_DATE = f"{DEFAULT_YEAR}-01-01"
DEFAULT_END_DATE = get_default_end_date()
DEFAULT_VARIABLE = "VWC"
DEFAULT_DEPTH = "1"
DEFAULT_STRIP = "S1"
DEFAULT_LOGGER = "T"
DEFAULT_GRANULARITY = "daily"  # Can be 'daily' or '15min'

# Additional configurations
strips = ["S1", "S2", "S3", "S4"]
variables = ["VWC", "T", "EC", "SWC"]
sensor_depths = [1, 2, 3]
loggers = ["T", "M", "B"]

# Depth mapping
sensor_depth_mapping = {
    1: "6 inches",
    2: "12 inches",
    3: "18 inches"
}

def load_dataset(file_path):
    """
    Loads a dataset from a ZIP file containing a CSV.

    Parameters:
    - file_path: The path to the ZIP file containing the dataset.

    Returns:
    - DataFrame: The loaded dataset as a Pandas DataFrame, or None if loading fails.
    """
    try:
        # Open the ZIP file and read the CSV inside
        with zipfile.ZipFile(file_path, 'r') as z:
            # Assumes the ZIP contains exactly one file
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                # Read the CSV into a DataFrame
                df = pd.read_csv(f, parse_dates=['datetime'])
                return df
    except Exception as e:
        # Log and return None if an error occurs
        logging.error(f"Error loading dataset from {file_path}: {e}")
        return None


# Data cache to store loaded datasets for 15min and daily granularities
data_cache = {"15min": {}, "daily": {}}  # A dictionary to store cached datasets

def get_data(year, granularity):
    """
    Retrieve a DataFrame from cache or load it if not already cached.

    Parameters:
    - year: The year of the dataset (e.g., 2024).
    - granularity: The granularity of the dataset ('15min' or 'daily').

    Returns:
    - DataFrame: The loaded or cached dataset.
    """
    # Check if the dataset is already cached
    if year in data_cache[granularity]:
        return data_cache[granularity][year]

    # Construct the file path
    data_dir = os.path.join(os.getcwd(), "flask", "data-processed")
    parsed_files = parse_filenames(data_dir, suffix=f"_{granularity}.zip")
    matching_files = [f for start, end, f in parsed_files if start.startswith(f"{year}-01-01")]

    if not matching_files:
        raise FileNotFoundError(f"No dataset found for year {year} with granularity {granularity}")

    file_path = os.path.join(data_dir, matching_files[0])

    # Load the dataset
    df = load_dataset(file_path)
    if df is None:
        raise FileNotFoundError(f"Dataset could not be loaded from {file_path}")

    # Cache the dataset for future use
    data_cache[granularity][year] = df
    return df

# Helper Function to Log Debug Info
def log_debug_info(data, route_name):
    depth = data.get("depth", DEFAULT_DEPTH)
    depth_display = sensor_depth_mapping.get(depth, f"{depth} inches")
    print(f"--- Debug Info for {route_name} ---")
    print(f"Incoming request data: {data}")
    print(f"Translated depth: {depth_display}")
    print("----------------------------")


def convert_ndarray(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def ensure_serializable(obj):
    """Recursively convert all ndarray objects in a dictionary to lists."""
    if isinstance(obj, dict):
        return {key: ensure_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [ensure_serializable(item) for item in obj]
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def y_axis_label(variable):
    """Returns appropriate y-axis label based on the selected variable."""
    if variable == "VWC":
        return "Volumetric Water Content (%)"
    elif variable == "T":
        return "Temperature (°C)"
    elif variable == "EC":
        return "Electrical Conductivity (dS/m)"
    elif variable == "SWC":
        return "Soil Water Content (Volume)"
    else:
        return variable  # Default to the variable name if no match is found



@main.route("/get_defaults_and_options")
def get_defaults_and_options():
    response_data = {
        "defaults": {
            "year": DEFAULT_YEAR,
            "startDate": DEFAULT_START_DATE,
            "endDate": DEFAULT_END_DATE,
            "variable": DEFAULT_VARIABLE,
            "depth": str(DEFAULT_DEPTH),  # Ensure depth is string if dropdown uses strings
            "strip": DEFAULT_STRIP,
        },
        "years": [2024, 2023],  # Example; adjust as needed
        "strips": strips,
        "variables": variables,
        "depths": [{"value": str(depth), "label": sensor_depth_mapping[depth]} for depth in sensor_depths],  # Use sensor_depths
    }
    print("Returning defaults and options:", response_data)
    return jsonify(response_data)

@main.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(main.root_path, 'static', 'images'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )


@main.route("/markdown/<path:filename>")
def serve_markdown(filename):
    """Serve markdown files from the markdown directory."""
    markdown_path = os.path.join(os.getcwd(), "flask", "markdown")
    print(f"Requested file: {filename}")
    print(f"Looking for file at: {markdown_path}")
    try:
        return send_from_directory(markdown_path, filename)
    except Exception as e:
        print(f"Error: {e}")
        return f"Error serving markdown file: {e}", 404


# Home route
@main.route("/")
def home():
    """Serve the home page."""
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
        DEFAULT_LOGGER=DEFAULT_LOGGER,
    )


# Route to get available years
@main.route("/available-years")
def available_years():
    """Return a list of available years based on data files."""
    data_dir = os.path.join(os.getcwd(), "flask", "data-raw")
    years = []
    for filename in os.listdir(data_dir):
        if filename.startswith("dataloggerData_") and filename.endswith(".zip"):
            year_part = filename.split("_")[1]
            year = year_part.split("-")[0]
            if year.isdigit():
                years.append(int(year))
    return jsonify(sorted(set(years)))


# Helper function to filter dataset by date
def filter_loaded_dataset(year, granularity, start_date, end_date):
    """
    Loads and filters a dataset by date range and ensures datetime formatting.

    Parameters:
    - year: The year of the dataset (e.g., 2024).
    - granularity: The granularity of the dataset ('15min' or 'daily').
    - start_date: The start date for filtering (string in "YYYY-MM-DD" format).
    - end_date: The end date for filtering (string in "YYYY-MM-DD" format).

    Returns:
    - filtered_df: The filtered DataFrame.
    - datetime_column: The formatted datetime column from the original DataFrame.
    - start_date: The start date used for filtering (unchanged, for reference).
    - end_date: The end date used for filtering (unchanged, for reference).
    """
    # Load dataset using get_data
    df = get_data(year, granularity)
    if df is None:
        raise FileNotFoundError(f"Dataset could not be loaded for year {year} and granularity {granularity}")

    # Format datetime and filter by date range
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    filtered_df = df[(df["datetime"] >= start_date) & (df["datetime"] <= end_date)]
    filtered_df = filtered_df.where(pd.notnull(filtered_df), None)  # Replace NaN with None for JSON compliance

    # Return filtered dataset, datetime column, and date range
    return filtered_df, df["datetime"], start_date, end_date


# Route to fetch default dates
@main.route("/default_dates")
def default_dates():
    """Return default start and end dates."""
    return jsonify({
        "start_date": DEFAULT_START_DATE,
        "end_date": DEFAULT_END_DATE,
        "depths": [{"value": key, "label": label} for key, label in sensor_depth_mapping.items()]
    })


def sanitize_json(data):
    """Recursively replace NaN with null and handle ndarray in a JSON object."""
    if isinstance(data, dict):
        return {k: sanitize_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_json(i) for i in data]
    elif isinstance(data, (float, int)) and np.isnan(data):
        return None  # JSON libraries automatically convert None to null
    elif isinstance(data, np.ndarray):
        return [sanitize_json(item) for item in data.tolist()]  # Recursively sanitize ndarray elements
    return data

# Plot Raw Data
@main.route("/plot_raw", methods=["POST"])
def plot_raw():
    data = request.json
    print("Raw Plot Params:", data)
    log_debug_info(data, "plot_raw")

    year = data.get("year", DEFAULT_YEAR)
    start_date = data.get("startDate", DEFAULT_START_DATE)
    end_date = data.get("endDate", DEFAULT_END_DATE)
    strip = data.get("strip", DEFAULT_STRIP)
    variable = data.get("variable", DEFAULT_VARIABLE)
    depth = data.get("depth", DEFAULT_DEPTH)
    depth_display = sensor_depth_mapping.get(depth, f"{depth} inches")

    try:
        granularity = "15min" if (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days <= 30 else "daily"
        print(f"Determined granularity: {granularity}")

        # Get the dataset from the global cache
        df = get_data(year, granularity)
        if df is None:
            raise FileNotFoundError(f"No cached data found for year {year} and granularity {granularity}")

        # Convert NaN to None for JSON compliance
        df = df.where(pd.notnull(df), None)

        # Filter the dataset
        filtered_df, _, _, _ = filter_loaded_dataset(year, granularity, start_date, end_date)
        filtered_df = filtered_df.where(pd.notnull(filtered_df), None)

        # Extract raw data columns
        variable_columns = [
            f"{variable}_{depth}_raw_{strip}_{logger}_{granularity}"
            for logger in loggers
        ]
        available_columns = [col for col in variable_columns if col in filtered_df.columns]

        if not available_columns:
            return jsonify({"error": f"No matching columns found in dataset: {variable_columns}"}), 400

        # Generate Plotly figure
        fig = go.Figure()
        for col in available_columns:
            fig.add_trace(go.Scatter(
                x=filtered_df["datetime"],
                y=filtered_df[col],
                mode="lines",
                name=col.replace(f"{variable}_{depth}_raw_{strip}", "").strip("_")
            ))

        fig.update_layout(
            title=f"Raw Data Plot for {variable} at {depth_display} in {strip}",
            xaxis_title="Date",
            yaxis_title="Value",
            template="plotly_white"
        )

        # Sanitize the figure JSON to replace NaN with None
        print("Filtered DataFrame for raw plot:", filtered_df)
        sanitized_json = sanitize_json(fig.to_plotly_json())
        print("Sanitized JSON for raw plot:", sanitized_json)

        # Return the sanitized JSON
        return jsonify(sanitized_json)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Error generating raw plot."}), 500


# Plot Ratio Data
@main.route("/plot_ratio", methods=["POST"])
def plot_ratio():
    data = request.json
    print("Ratio Plot Params:", data)
    log_debug_info(data, "plot_ratio")

    year = data.get("year", DEFAULT_YEAR)
    start_date = data.get("startDate", DEFAULT_START_DATE)
    end_date = data.get("endDate", DEFAULT_END_DATE)
    strip = data.get("strip", DEFAULT_STRIP)
    variable = data.get("variable", DEFAULT_VARIABLE)
    depth = data.get("depth", DEFAULT_DEPTH)
    depth_display = sensor_depth_mapping.get(depth, f"{depth} inches")

    try:
        granularity = "15min" if (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days <= 30 else "daily"

        # Get the dataset from the global cache
        df = get_data(year, granularity)
        if df is None:
            raise FileNotFoundError(f"No cached data found for year {year} and granularity {granularity}")

        # Convert NaN to None for JSON compliance
        df = df.where(pd.notnull(df), None)

        # Filter the dataset
        filtered_df, _, _, _ = filter_loaded_dataset(year, granularity, start_date, end_date)
        filtered_df = filtered_df.where(pd.notnull(filtered_df), None)

        # Extract ratio data columns
        ratio_columns = [
            col for col in filtered_df.columns
            if "ratio" in col.lower() and variable.lower() in col.lower()
        ]

        if not ratio_columns:
            return jsonify({"error": "No matching ratio columns found in dataset."}), 400

        # Generate Plotly figure
        fig = go.Figure()
        for col in ratio_columns:
            fig.add_trace(go.Scatter(
                x=filtered_df["datetime"],
                y=filtered_df[col],
                mode="lines",
                name=col.replace(f"{variable}_{depth}_", "").strip("_")
            ))

        fig.update_layout(
            title=f"Ratio Data Plot for {variable} at {depth_display} in {strip}",
            xaxis_title="Date",
            yaxis_title="Value",
            template="plotly_white"
        )

        # Sanitize the figure JSON to replace NaN with None
        print("Filtered DataFrame for ratio plot:", filtered_df)
        sanitized_json = sanitize_json(fig.to_plotly_json())

        # Return the sanitized JSON
        return jsonify(sanitized_json)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Error generating ratio plot."}), 500


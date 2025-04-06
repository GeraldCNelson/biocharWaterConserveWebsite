# Routes.py - Flask application for Biochar Project

from flask import Blueprint, jsonify, request, send_from_directory, render_template
import os
import pandas as pd
import zipfile
import plotly.graph_objects as go
# from datetime import datetime
import numpy as np
import logging
import json
import glob
from collections import namedtuple
from biochar_app.config import (
    DEFAULT_YEAR,
    DEFAULT_START_DATE,
    DEFAULT_VARIABLE,
    VALUE_COLS_STANDARD, VALUE_COLS_2024_PLUS,
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
# 1. parse_filenames: Parse filenames to extract date ranges.
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

common_xaxis_config = dict(
    title="Date",
    tickformat="%b\n%Y",  # ✅ Ensures "Jan 2024", "Feb 2024" format
    type="date",
    showline=True,
    linewidth=1,
    linecolor='gray',
    showgrid=True,
    zeroline=False
)


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


###############################################
# Helper Functions
###############################################

# In-memory cache to store loaded datasets
loaded_datasets = {}


def load_logger_data(year: int, granularity: str):
    key = f"{year}-{granularity}"  # ✅ Include granularity in cache key
    if key in loaded_datasets:
        return loaded_datasets[key]

    # Parse all available files
    parsed_files = parse_filenames(DATA_PROCESSED_DIR)

    # Find the matching file
    matching_file = next(
        (f for f in parsed_files if f.granularity == granularity and f.start_date.startswith(str(year))),
        None
    )
    if matching_file is None:
        raise FileNotFoundError(f"No file found for {year} - {granularity}")

    file_path = os.path.join(DATA_PROCESSED_DIR, matching_file.filename)
    logging.info(f"file_path:", file_path)

    with zipfile.ZipFile(file_path, 'r') as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            df = pd.read_csv(f)

    # Only convert timestamp column if it exists and granularity isn't gseason
    if granularity != "gseason" and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    loaded_datasets[key] = df  # ✅ Cache it with granularity-specific key
    return df


LoggerFileInfo = namedtuple("LoggerFileInfo", ["start_date", "end_date", "granularity", "filename"])


def parse_filenames(data_dir, prefix="dataloggerData_", suffix=".zip"):
    filenames = [f for f in os.listdir(data_dir) if f.startswith(prefix) and f.endswith(suffix)]
    parsed_files = []

    for filename in filenames:
        try:
            if not filename.startswith(prefix) or not filename.endswith(suffix):
                continue

            parts = filename[len(prefix):-len(suffix)].split("_")
            if len(parts) != 3:
                logging.warning(f"Skipping invalid filename format: {filename}")
                continue

            start_date, end_date, granularity = parts
            parsed_files.append(LoggerFileInfo(start_date, end_date, granularity, filename))

        except (IndexError, ValueError) as e:
            logging.error(f"Error parsing filename {filename}: {e}")
            continue

    logging.info(f"🔍 Parsed Files:", parsed_files)
    return parsed_files


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


def get_default_end_date(year=DEFAULT_YEAR):
    data_dir = DATA_PROCESSED_DIR
    parsed_files = parse_filenames(data_dir)

    if not parsed_files:
        raise FileNotFoundError(f"❌ No files found in {data_dir}. Cannot determine default end date.")

    # Filter parsed files by the specified year
    filtered_files = [file for file in parsed_files if file[1].startswith(str(year))]
    if filtered_files:
        return max(filtered_files, key=lambda x: x[1])[1]

    raise ValueError(f"❌ No data files found for the year {year} in {data_dir}.")
DEFAULT_END_DATE = get_default_end_date()


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


def compute_summary_statistics(df):
    """Computes summary statistics for the filtered dataset."""
    if df.empty:
        return {}

    stats = {
        "mean": df.select_dtypes(include=["number"]).mean().to_dict(),
        "median": df.select_dtypes(include=["number"]).median().to_dict(),
        "min": df.select_dtypes(include=["number"]).min().to_dict(),
        "max": df.select_dtypes(include=["number"]).max().to_dict()
    }
    return stats


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
            "endDate": DEFAULT_END_DATE
        },

        "years": YEARS,  # ✅ Use predefined YEARS variable
        "variables": list(variable_name_mapping.keys()),  # ✅ Extract from mapping
        "granularities": list(granularity_name_mapping.keys()),  # ✅ Extract from mapping
        "strips": list(strip_name_mapping.keys()),  # ✅ Extract from mapping
        "loggerLocations": list(logger_location_mapping.keys()),  # ✅ Extract from mapping
        "depths": list(sensor_depth_mapping.keys()),  # ✅ Extract from mapping

        # ✅ Ensure mappings are correctly assigned
        "variableNameMapping": variable_name_mapping,
        "granularityNameMapping": granularity_name_mapping,
        "stripNameMapping": strip_name_mapping,
        "loggerLocationMapping": logger_location_mapping,  # ✅ Fix this
        "depthMapping": sensor_depth_mapping  # ✅ Fix this
    }

    logging.info(f"📤 Sending options: {json.dumps(options, indent=2)}")  # ✅ Debugging output
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
        year = int(data["year"])
        variable = data["variable"]
        strip = data["strip"]
        granularity = data["granularity"]

        df = load_logger_data(year, granularity)

        # Filter relevant columns for RAW
        raw_cols = [col for col in df.columns if col.startswith(f"{variable}_") and "_raw_" in col and f"_{strip}_" in col]
        ratio_cols = [col for col in df.columns if col.startswith(f"{variable}_") and "_ratio_" in col and f"_{strip}_" in col]

        def compute_stats(columns):
            return {
                col.split("_")[-1]: {
                    "min": round(df[col].min(), 4),
                    "mean": round(df[col].mean(), 4),
                    "max": round(df[col].max(), 4),
                    "std": round(df[col].std(), 4),
                }
                for col in columns if col in df.columns
            }

        return jsonify({
            "raw_statistics": compute_stats(raw_cols),
            "ratio_statistics": compute_stats(ratio_cols),
            "year": year,
            "variable": variable,
            "strip": strip,
            "granularity": granularity
        })
    except Exception as e:
        logging.error(f"❌ Error in /get_summary_stats: {e}", exc_info=True)
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
                           DEFAULT_END_DATE=DEFAULT_END_DATE, DEFAULT_VARIABLE=DEFAULT_VARIABLE,
                           DEFAULT_DEPTH=DEFAULT_DEPTH,
                           DEFAULT_STRIP=DEFAULT_STRIP, DEFAULT_LOGGER=DEFAULT_LOGGER_LOCATION)

@main.route("/plot_raw", methods=["POST"])
def plot_raw():
    try:
        data = request.get_json()
        logging.info(f"🧪 Incoming request to /plot_raw with data:", data)

        # ✅ Required keys validation
        required_keys = ["year", "variable", "strip", "granularity", "loggerLocation", "depth", "traceOption"]
        missing_keys = [key for key in required_keys if key not in data or not data[key]]

        if missing_keys:
            logging.error(f"❌ Missing required parameters: {missing_keys}")
            return jsonify({"error": f"Missing required parameters: {missing_keys}"}), 400

        # ✅ Extract user selections
        year = data["year"]
        variable = data["variable"]
        strip = data["strip"]
        granularity = data["granularity"]
        logger_location = data["loggerLocation"]
        depth = str(data["depth"])
        trace_option = data["traceOption"]
        start_date = str(data["startDate"])
        end_date = str(data["endDate"])

        logging.info(f"🎯 Params: Year={year}, Variable={variable}, Strip={strip}, Granularity={granularity}, Logger={logger_location}, Depth={depth}, TraceOption={trace_option}")

        # ✅ Get correct dataset file path
        try:
            df = load_logger_data(year, granularity)
        except FileNotFoundError as e:
            logging.error(str(e))
            return jsonify({"error": str(e)}), 400

        filtered_df = filter_data_logger(df, start_date, end_date)

        traces = []

        # ✅ Handle trace grouping (depth-based or logger location-based)
        if trace_option == "depths":
            logging.info(f"📊 Trace mode: DEPTHS")
            logging.info(f"🔎 DataFrame columns: {filtered_df.columns.tolist()}")
            grouping_values = ["1", "2", "3"]  # Corresponds to depths: 6, 12, 18 inches
            for val in grouping_values:
                col_name = f"{variable}_{val}_raw_{strip}_{logger_location}"
                if col_name in filtered_df.columns:
                    traces.append(go.Scatter(
                        x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                        y=filtered_df[col_name].tolist(),
                        mode="lines",
                        name=f"{variable} ({sensor_depth_mapping[int(val)]})"
                    ))
        else:  # traceOption == "location"
            logging.info(f"📊 Trace mode: LOCATION")
            logging.info(f"🔎 DataFrame columns: {filtered_df.columns.tolist()}")
            grouping_values = ["T", "M", "B"]  # Top, Middle, Bottom locations
            for val in grouping_values:
                col_name = f"{variable}_{depth}_raw_{strip}_{val}"
                if col_name in filtered_df.columns:
                    traces.append(go.Scatter(
                        x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                        y=filtered_df[col_name].tolist(),
                        mode="lines",
                        name=f"{variable} ({val})"
                    ))

        # ✅ Add precipitation as bars for VWC
        precip_col = f"precip_mm"
        if variable == "VWC" and precip_col in filtered_df.columns:
            traces.append(go.Bar(
                x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                y=filtered_df[precip_col].tolist(),
                name="Precipitation (mm)",
                yaxis="y2",
                marker=dict(color="lightgray"),
                opacity=0.7,
                width=1000 * 60 * 60 * 24  # day in ms
            ))

        # ✅ Add air temperature trace for T if available
        temp_col = f"temp_air_degC"
        if variable == "T" and temp_col in filtered_df.columns:
            traces.append(go.Scatter(
                x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                y=filtered_df[temp_col].tolist(),
                name="Air Temp (°C)",
                mode="lines",
                line=dict(color="gray", dash="dot"),
                yaxis="y2"
            ))

        # ✅ Build Plotly layout
        layout = go.Layout(
            title=f"{granularity_name_mapping[granularity]} Data Plot for {variable_name_mapping[variable]} in Strip {strip}, {year}",
            xaxis=dict(
                **common_xaxis_config,
                range=[start_date, end_date]
            ),
            yaxis=common_yaxis_config(variable),
            yaxis2=dict(
                title="Precipitation (mm)",
                overlaying="y",
                side="right",
                showgrid=False,
                showline=True,
                linecolor="gray"
            ),
            legend=common_legend_config(),
            template="plotly_white",
            margin=dict(l=50, r=50, t=40, b=40),
            barmode="overlay",  # ✅ Corrected
            autosize=True
        )

        logging.info("✅ plot_raw generated successfully.")
        return jsonify(sanitize_json({
            "data": [trace.to_plotly_json() for trace in traces],
            "layout": layout.to_plotly_json()
        }))

    except Exception as e:
        logging.error(f"❌ Unexpected error in plot_raw: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@main.route("/plot_raw_gseason", methods=["POST"])
def plot_raw_gseason():
    try:

        request_data = request.get_json()
        logging.info(f"🧪 Incoming request to /plot_raw_gseason with data: %s", request_data)

        required_keys = ["year", "variable", "strip", "loggerLocation", "traceOption"]
        missing_keys = [key for key in required_keys if key not in request_data or not request_data[key]]

        if missing_keys:
            return jsonify({"error": f"Missing required parameters: {missing_keys}"}), 400

        year = request_data["year"]
        variable = request_data["variable"]
        strip = request_data["strip"]
        logger_location = request_data["loggerLocation"]
        trace_option = request_data["traceOption"]
        df = load_logger_data(year, "gseason")

        traces = []

        if trace_option == "depths":
            logging.info(f"📊 Gseason trace mode: DEPTHS")
            for depth in ["1", "2", "3"]:
                col_name = f"{variable}_{depth}_raw_{strip}_{logger_location}"
                if col_name in df.columns:
                    logging.info(f"✅ Found column {col_name}, sample values:", df[col_name].dropna().head())
                    traces.append(go.Bar(
                        x=df["gseason_periods"],
                        y=df[col_name].tolist(),
                        name=sensor_depth_mapping.get(int(depth), f"Depth {depth}"),
                        width=0.2,
                        hovertemplate="%{x}: %{y:.2f}<extra></extra>"
                    ))
                else:
                    print(f"❌ Missing column: {col_name}")
        else:
            logging.info(f"📊 Gseason trace mode: LOGGER")
            for loc in ["T", "M", "B"]:
                col_name = f"{variable}_{request_data['depth']}_raw_{strip}_{loc}"
                if col_name in df.columns:
                    traces.append(go.Bar(
                        x=df["gseason_periods"],
                        y=df[col_name].tolist(),
                        name=f"Logger {loc}",
                        width=0.2,
                        hovertemplate="%{x}: %{y:.2f}<extra></extra>"
                    ))

        season_keys = list(GSEASON_PERIODS.keys())
        season_labels = [
            f"{key.split('_', 1)[1]} ({start} to {end})"
            for key, (start, end) in GSEASON_PERIODS.items()
        ]

        layout = go.Layout(
            title=f"Growing Season Plot for {variable_name_mapping.get(variable, variable)} in Strip {strip}, Logger {logger_location}",
            xaxis=dict(
                title="Growing Season Period",
                type="category",
                categoryorder="array",
                categoryarray=season_keys,
                tickmode="array",
                tickvals=season_keys,
                ticktext=season_labels
            ),
            yaxis=common_yaxis_config(variable),
            legend=common_legend_config(),
            template="plotly_white",
            margin=dict(l=50, r=30, t=40, b=40)
        )

        logging.info(f"✅ plot_raw_gseason completed successfully.")
        fig = go.Figure(data=traces, layout=layout)
        return jsonify(sanitize_json(fig.to_plotly_json()))

    except Exception as e:
        logging.error(f"❌ Error in plot_raw_gseason: {e}", exc_info=True)
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500



@main.route("/plot_ratio", methods=["POST"])
def plot_ratio():
    try:
        data = request.get_json()
        logging.info(f"🧪 Incoming request to /plot_ratio with data:", data)
        if not isinstance(data, dict):
            logging.error(f"❌ Expected JSON dictionary but got: {type(data)}")
            return jsonify({"error": "Invalid data format received"}), 400

        # ✅ Extract user selections
        year = (data["year"])
        start_date = str(data["startDate"])
        end_date = str(data["endDate"])
        granularity = data["granularity"]
        selected_depth = str(data["depth"])
        selected_logger = data["loggerLocation"]
        variable = data["variable"]

        # ✅ Validate depth, logger location, and granularity
        if selected_depth == "undefined" or selected_logger == "undefined":
            logging.error("❌ Depth or Logger Location is undefined.")
            return jsonify({"error": "Depth or Logger Location is undefined."}), 400
        if granularity not in granularity_name_mapping.keys():
            logging.error(f"❌ Invalid granularity: {granularity}")
            return jsonify({"error": f"Invalid granularity: {granularity}"}), 400

        # ✅ Determine expected data columns
        expected_columns = [
            f"{variable}_{selected_depth}_ratio_S1_S2_{selected_logger}",
            f"{variable}_{selected_depth}_ratio_S3_S4_{selected_logger}"
        ]
        logging.info(f"🔎 Expected columns: {expected_columns}")

        # ✅ Load dataset with granularity included
        try:
            df = load_logger_data(year, granularity)
        except FileNotFoundError as e:
            logging.error(str(e))
            return jsonify({"error": str(e)}), 400

        filtered_df = filter_data_logger(df, start_date, end_date)
        logging.info(f"📊 Ratio column keys requested: {expected_columns}")
        logging.info(f"📊 Columns actually found in filtered_df: {filtered_df.columns.tolist()}")

        if filtered_df.empty:
            logging.warning("⚠️ No data found for the selected parameters.")
            return jsonify({"error": "No data found for the selected parameters."}), 404

        # ✅ Filter available columns
        available_columns = [col for col in expected_columns if col in filtered_df.columns]

        if len(available_columns) == 0:
            logging.error(f"❌ No matching columns found in data. Available: {filtered_df.columns}")
            return jsonify({"error": "No matching columns found in dataset"}), 400

        # ✅ Generate Plotly figure
        fig = go.Figure()

        for col in available_columns:
            trace_label = "S1/S2" if "S1_S2" in col else "S3/S4"
            y_values = filtered_df[col].replace({pd.NA: None}).tolist()

            fig.add_trace(go.Scatter(
                x=filtered_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                y=y_values,
                mode="lines",
                name=trace_label,
                hovertemplate="%{x|%b %d, %Y}: %{y:.2f}<extra></extra>",
                connectgaps=False
            ))

        # ✅ Clean up layout
        legend_title = f"{variable}, {selected_logger}, {sensor_depth_mapping.get(int(selected_depth), 'Unknown Depth')}"
        ratio_y_label = f"{label_name_mapping.get(variable, variable)} Ratio"
        fig.update_layout(
            title=(
                f"{granularity_name_mapping[granularity]} Ratio Data Plot for {variable_name_mapping[variable]} at {sensor_depth_mapping.get(int(selected_depth), 'Unknown Depth')}; "
                f"<br>Biochar-Injected Strips (S1 & S3) to no Biochar Strips (S2 & S4)"),
            xaxis=dict(
                **common_xaxis_config,
                range=[start_date, end_date]
            ),
            yaxis=dict(
                title=ratio_y_label,
                tickformat=".2f",
                showline=True,
                linecolor="black"
            ),
            margin=dict(l=50, r=50, t=40, b=40),
            legend=common_legend_config(title=legend_title),
            template="plotly_white",
            autosize=True
        )

        logging.info(f"✅ Ratio plot generated with {len(fig.data)} traces.")
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


@main.route('/download_ratio_data')
def download_ratio_data():
    """ Route to handle ratio data downloads. """
    return download_csv_file("ratio")

def download_csv_file(data_type):
    """ Helper function to generate the correct filename and serve the CSV file. """
    try:
        start_date = request.args.get("startDate")
        end_date = request.args.get("endDate")
        variable = request.args.get("variable")
        strip = request.args.get("strip")
        depth = request.args.get("depth")
        logger_location = request.args.get("loggerLocation")

        # ✅ Construct filename with all relevant metadata
        filename = f"{data_type}_{variable}_{strip}_{depth}_{logger_location}_{start_date}_to_{end_date}.csv"
        file_path = os.path.join("data-processed", filename)

        # ✅ Check if the file exists before sending
        if not os.path.exists(file_path):
            return jsonify({"error": f"In download_csv, File not found: {filename}"}), 404

        return send_from_directory("biochar_app/data-processed", filename, as_attachment=True)

    except Exception as e:
        return jsonify({"error": f"Failed to download {data_type} data: {str(e)}"}), 500
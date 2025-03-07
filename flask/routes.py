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
import glob

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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get script directory
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data-processed")

STRIPS = ["S1", "S2", "S3", "S4"]
VARIABLES = ["VWC", "T", "EC", "SWC"]
SENSOR_DEPTHS = [1, 2, 3]
LOGGER_LOCATIONS = ["T", "M", "B"]
YEARS = ["2023", "2024", "2025"]
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

variable_name_mapping = {
    "VWC": "Vol. Water Content",
    "T": "Temp",  # Keep it concise but clear
    "EC": "Electrical Conductivity",
    "SWC": "Soil Water Content"
}

granularity_name_mapping = {
    "gseason": "Growing Season",
    "monthly": "Monthly",
    "daily": "Daily",
    "15min": "15 Minute",
    "1hour": "Hourly"  # ✅ Added hourly granularity
}


###############################################
# Helper Functions
###############################################

def parse_filenames(data_dir, prefix="dataloggerData_", suffix=".zip"):
    filenames = [f for f in os.listdir(data_dir) if f.startswith(prefix) and f.endswith(suffix)]
    parsed_files = []

    for filename in filenames:
        try:
            if not filename.startswith(prefix) or not filename.endswith(suffix):
                continue

            # ✅ Extract start_date, end_date, granularity
            parts = filename[len(prefix):-len(suffix)].split("_")
            if len(parts) != 3:
                logging.warning(f"Skipping invalid filename format: {filename}")
                continue

            start_date, end_date, granularity = parts
            parsed_files.append((start_date, end_date, granularity, filename))

        except (IndexError, ValueError) as e:
            logging.error(f"Error parsing filename {filename}: {e}")
            continue

    # ✅ Debugging: Print parsed files
    print("🔍 Parsed Files:", parsed_files)
    return parsed_files

def get_available_years():
    """Extracts available years from filenames in the data directory."""
    data_dir = os.path.join(os.getcwd(), "flask", "data-processed")
    parsed_files = parse_filenames(data_dir)

    # ✅ Use a different variable name to prevent shadowing
    available_years = sorted({start[:4] for start, _, _, _ in parsed_files})

    logging.info(f"📆 Available Years: {available_years}")
    return available_years


def find_matching_file(year, granularity):
    """
    Searches for the correct dataset file based on year and granularity.

    :param year: The year to look for.
    :param granularity: The data granularity (e.g., "daily", "monthly", "growingseason").
    :return: The file path if found, otherwise None.
    """
    try:
        logging.info(f"🔍 Searching for dataset: Year={year}, Granularity={granularity}")

        # ✅ Construct the expected filename pattern
        file_pattern = f"dataloggerData_{year}-*_*-*_{granularity}.zip"
        matching_files = glob.glob(os.path.join(DATA_PROCESSED_DIR, file_pattern))

        # ✅ If we find multiple matches, pick the most recent one
        if matching_files:
            selected_file = sorted(matching_files)[-1]  # Pick the latest
            logging.info(f"📂 Using dataset: {selected_file}")
            return selected_file

        logging.warning(f"⚠️ No matching dataset found for {year}, {granularity}")
        return None

    except Exception as e:
        logging.error(f"❌ Error finding matching file: {e}")
        return None


def get_default_end_date(year=None):
    data_dir = os.path.join(os.getcwd(), "flask", "data-processed")
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
                # Try parsing timestamp in different formats
                df = pd.read_csv(f)

                # Normalize timestamp column handling
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                else:
                    logging.warning("⚠️ No timestamp column found in dataset.")

                # ✅ Replace empty strings, "nan" strings, and np.nan with None
                df.replace(["", "nan", np.nan], None, inplace=True)

                # ✅ Convert all non-timestamp columns to numeric
                for col in df.columns:
                    if col != "timestamp":
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                logging.info(f"🔢 Data types after conversion:\n{df.dtypes}")
                return df

    except Exception as e:
        logging.error(f"❌ Error loading dataset from {file_path}: {e}")
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
    data_dir = os.path.join(os.getcwd(), "flask", "data-processed")
    parsed_files = parse_filenames(data_dir)

    logging.info(f"🔍 Looking for files with Year: {year}, Granularity: {granularity}")

    matching_files = [
        f for start, end, g, f in parsed_files if start.startswith(f"{year}-") and g == granularity
    ]

    if not matching_files:
        raise FileNotFoundError(f"⚠️ No dataset found for year {year} with granularity {granularity}")

    file_path = os.path.join(data_dir, matching_files[0])
    logging.info(f"📂 Using dataset: {file_path}")

    # ✅ Load dataset and ensure correct timestamp parsing
    df = load_dataset(file_path)

    if df is None:
        raise FileNotFoundError(f"⚠️ Failed to load dataset from {file_path}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # ✅ Fix: Adjust parsing for "1hour" timestamps
    if granularity == "1hour":
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # ✅ Apply date filtering
    filtered_df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]
    filtered_df = filtered_df.replace({np.nan: None})

    return filtered_df, df["timestamp"], start_date, end_date


def compute_summary_stats(df):
    """
    Computes summary statistics (mean, min, max, std) for each numeric column in the DataFrame.
    Excludes non-numeric columns (like timestamps) and filters out variables that start with 'BattV'.
    """
    if df.empty:
        return pd.DataFrame()  # Return an empty DataFrame if there's no data

    # ✅ Select only numeric columns
    numeric_columns = df.select_dtypes(include=["number"])

    if numeric_columns.empty:
        logging.warning("⚠️ No numeric columns available for summary statistics!")
        return pd.DataFrame()

    # ✅ Exclude columns that start with "BattV"
    filtered_columns = numeric_columns.loc[:, ~numeric_columns.columns.str.startswith("BattV")]

    if filtered_columns.empty:
        logging.warning("⚠️ All numeric columns were filtered out. No data remains for statistics.")
        return pd.DataFrame()

    # ✅ Compute summary statistics (mean, min, max, std)
    summary_df = filtered_columns.agg(['mean', 'min', 'max', 'std']).transpose()

    # ✅ Round 'mean' and 'std' to 4 decimal places
    summary_df['mean'] = summary_df['mean'].round(4)
    summary_df['std'] = summary_df['std'].round(4)

    # ✅ Reset index for better formatting
    summary_df.reset_index(inplace=True)
    summary_df.rename(columns={"index": "Variable"}, inplace=True)

    return summary_df

###############################################
# Routes
###############################################

@main.route("/get_defaults_and_options", methods=["GET"])
def get_defaults_and_options():
    logging.info("📡 Fetching default values and dropdown options...")

    try:
        # ✅ Ensure DEFAULT_YEAR has a valid end date fallback
        default_end_date = get_default_end_date(DEFAULT_YEAR) or f"{DEFAULT_YEAR}-12-31"

        # ✅ Correctly structure default values
        defaults = {
            "year": DEFAULT_YEAR,
            "startDate": f"{DEFAULT_YEAR}-01-01",
            "endDate": default_end_date,  # Ensure correct end date is set
            "strip": DEFAULT_STRIP,
            "variable": DEFAULT_VARIABLE,
            "depth": DEFAULT_DEPTH,
            "loggerLocation": DEFAULT_LOGGER_LOCATION,
        }

        # ✅ Include defaults in response_data
        response_data = {
            "years": YEARS,  # Defined global list of years
            "strips": STRIPS,
            "variables": VARIABLES,
            "depths": SENSOR_DEPTHS,
            "loggerLocations": LOGGER_LOCATIONS,  # ✅ Fixed inconsistent casing
            "sensorDepthMapping": sensor_depth_mapping,  # ✅ Send to frontend
            "defaults": defaults  # ✅ Now included in response
        }

        logging.info(f"✅ Defaults and options sent: {response_data}")
        return jsonify(response_data)

    except Exception as e:
        logging.error(f"❌ Error fetching defaults and options: {e}", exc_info=True)
        return jsonify({"error": "Failed to fetch defaults and options."}), 500


@main.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(main.root_path, 'static', 'images'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )


valid_prefixes = ["raw", "gseason", "monthly"]

@main.route("/get_granularity_names", methods=["GET"])
def get_granularity_names():
    return jsonify(granularity_name_mapping)

# ✅ Ensure "gseason" is used when filtering growingseason granularity
def get_filtered_columns(df, granularity):
    """Filters valid columns based on granularity."""
    column_prefixes = {
        "gseason": "gseason",
        "monthly": "monthly",
        "daily": "raw",
        "15min": "raw",
        "1hour": "hour"  # ✅ Added support for hourly data

    }

    # ✅ Ensure granularity is mapped correctly
    prefix = column_prefixes.get(granularity, "raw")

    # ✅ Select columns that match the correct prefix
    filtered_columns = [col for col in df.columns if prefix in col]

    return filtered_columns

# routes.py

@main.route("/get_summary_stats", methods=["POST"])
def get_summary_stats():
    """Handles requests for summary statistics."""
    data = request.get_json()
    required_keys = ["year", "granularity", "startDate", "endDate"]

    if not all(key in data for key in required_keys):
        return jsonify({"error": "Missing required parameters"}), 400

    year = str(data["year"])
    granularity = data["granularity"].lower()  # Normalize granularity
    start_date = data["startDate"]
    end_date = data["endDate"]

    # ✅ Ensure filtering logic is correct
    try:
        df, _, _, _ = filter_loaded_dataset(year, granularity, start_date, end_date)
    except FileNotFoundError:
        return jsonify({"error": f"No data found for {granularity_name_mapping.get(granularity, granularity)}"}), 404

    if df is None or df.empty:
        return jsonify({"error": f"No data found for {granularity_name_mapping.get(granularity, granularity)}"}), 404

    stats = compute_summary_stats(df)
    return jsonify({
        "granularity": granularity_name_mapping.get(granularity, granularity),
        "statistics": stats.to_dict(orient="records")
    })


@main.route("/markdown/<path:filename>")
def serve_markdown(filename):
    """Serves markdown files from the markdown directory."""
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

    data_dir = os.path.join(os.getcwd(), "flask", "data-processed")
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
    return render_template("index.html", strips=STRIPS, variables=VARIABLES,
                           depths=[{"value": key, "label": label} for key, label in sensor_depth_mapping.items()],
                           loggers=LOGGER_LOCATIONS,
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

        # ✅ Assign granularity from request data (fallback to DEFAULT_GRANULARITY)
        granularity = data.get("granularity", DEFAULT_GRANULARITY)

        # ✅ Determine correct date format based on granularity
        if granularity == "daily":
            date_format = "%Y-%m-%d"
        elif granularity == "monthly":
            date_format = "%Y-%m"
        elif granularity == "15min":
            date_format = "%Y-%m-%d %H:%M:%S"
        else:
            date_format = "%Y-%m-%d"  # Default fallback

        # ✅ Validate dates before passing to `filter_loaded_dataset()`
        try:
            datetime.strptime(data["startDate"], date_format)
            datetime.strptime(data["endDate"], date_format)
        except ValueError as e:
            logging.error(f"❌ Date format error: {e}")
            return jsonify({"error": f"Invalid date format for granularity {granularity}"}), 400

        # ✅ Use the parsed granularity
        filtered_df, _, _, _ = filter_loaded_dataset(data["year"], granularity, data["startDate"], data["endDate"])

        if filtered_df.empty:
            logging.warning("No data found for the selected parameters.")
            return jsonify({"error": "No data found for the selected parameters."}), 404

        # ✅ Construct variable column names
        variable_columns = [
            f"{data['variable']}_{i}_raw_{data['strip']}_{data['loggerLocation']}"
            for i in SENSOR_DEPTHS  # Using sensor_depths = [1, 2, 3]
        ]
        available_columns = [col for col in variable_columns if col in filtered_df.columns]

        if not available_columns:
            logging.warning(f"No matching columns found in dataset: {variable_columns}")
            return jsonify({"error": f"No matching columns found in dataset: {variable_columns}"}), 400

        # ✅ Create Plotly figure
        fig = go.Figure()
        legend_title = "Sensor depth"

        for col in available_columns:
            depth_index = int(col.split("_")[1])  # Extract depth index (1, 2, 3)
            sensor_depth = sensor_depth_mapping.get(depth_index, "Unknown Depth")
            logger_label = f"Sensor {sensor_depth} - {data['loggerLocation']}"

            y_values = [None if pd.isna(val) else val for val in filtered_df[col].tolist()]

            fig.add_trace(go.Scatter(
                x=filtered_df["timestamp"],
                y=y_values,
                mode="lines",
                name=logger_label,
                hovertemplate="%{x|%m/%d}: %{y:.2f}<extra></extra>",
                connectgaps=False
            ))

        fig.update_layout(
            title=f"Raw Data Plot for {variable_name_mapping.get(data['variable'], data['variable'])} in strip {data['strip']}, {logger_location_mapping.get(data['loggerLocation'], 'Unknown Location')} Logger",
            xaxis=dict(
                title="Date",
                showline=True,  # ✅ Ensure axis line is drawn
                linewidth=1,  # ✅ Make it visible
                linecolor='black',  # ✅ Force it to show in black
                showgrid=True,  # ✅ Keep grid visible
                zeroline=False  # ✅ Remove unwanted baseline
            ),
            yaxis=dict(
                title=y_axis_label(data['variable']),
                showticklabels=True,  # ✅ Keep tick labels
                showline=True,  # ✅ Ensure axis line is drawn
                linewidth=1,  # ✅ Make it visible
                linecolor='black',  # ✅ Force it to show in black
                showgrid=True,  # ✅ Keep grid visible
                zeroline=False  # ✅ Remove unwanted baseline
            ),
            margin=dict(l=40, r=10, t=50, b=50),  # Reduce left and right margins
            autosize=True,  # Allow auto-resizing
            template="plotly_white",
            legend=dict(
                title=dict(text=f"<b>{legend_title}</b>", font=dict(size=12)),
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99
            )
        )

        sanitized_json = sanitize_json(fig.to_plotly_json())
        logging.info(f"✅ Raw plot data successfully processed!")

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

        # ✅ Ensure valid values for depth and loggerLocation
        selected_depth = data.get("depth", DEFAULT_DEPTH)
        selected_logger = data.get("loggerLocation", DEFAULT_LOGGER_LOCATION)

        if selected_depth == "undefined" or selected_logger == "undefined":
            logging.error("❌ Depth or Logger Location is undefined. Cannot construct column names.")
            return jsonify({"error": "Depth or Logger Location is undefined."}), 400

        # 🔍 **Construct correct ratio column names**
        expected_columns = [
            f"{data['variable']}_{selected_depth}_ratio_S1_S2_{selected_logger}",
            f"{data['variable']}_{selected_depth}_ratio_S3_S4_{selected_logger}"
        ]

        logging.info(f"🔎 Expected columns: {expected_columns}")

        # ✅ Filter dataset
        filtered_df, _, _, _ = filter_loaded_dataset(data["year"], data.get("granularity", DEFAULT_GRANULARITY), data["startDate"], data["endDate"])

        available_columns = [col for col in expected_columns if col in filtered_df.columns]

        if len(available_columns) != 2:
            logging.error(f"❌ Expected exactly two ratio columns but found {len(available_columns)}")
            logging.error(f"🔍 Expected: {expected_columns}")
            logging.error(f"📂 Available: {filtered_df.columns.tolist()}")
            return jsonify({"error": f"Expected exactly two ratio columns, found {len(available_columns)}."}), 400

        # ✅ Proceed with plotting
        fig = go.Figure()
        legend_title = f"{data['variable']}, {selected_logger}, {sensor_depth_mapping[int(selected_depth)]}"
        for col in available_columns:
            trace_label = "S1/S2" if "S1_S2" in col else "S3/S4"

            y_values = [None if pd.isna(val) else val for val in filtered_df[col].tolist()]

            fig.add_trace(go.Scatter(
                x=filtered_df["timestamp"].tolist(),
                y=y_values,
                mode="lines",
                name=trace_label,
                hovertemplate="%{x|%m/%d}: %{y:.2f}<extra></extra>",
                connectgaps=False
            ))

        # ✅ Remove subplot annotations before updating layout
        fig.for_each_annotation(lambda a: a.update(text=""))
        fig.update_layout(
            title=f"Ratio Data Plot for {data['variable']} at {sensor_depth_mapping[int(selected_depth)]}, Biochar-Injected Strips (S1 & S3) to <br>no Biochar Strips (S2 & S4)",
            xaxis=dict(
                title="Date",
                showline=True,  # ✅ Ensure axis line is drawn
                linewidth=1,  # ✅ Make it visible
                linecolor='gray',  # ✅ Force it to show in black
                showgrid=True,  # ✅ Keep grid visible
                zeroline=False,  # ✅ Remove unwanted baseline
                automargin=True  # ✅ Dynamically adjust margins
            ),
            yaxis=dict(
                title="Ratio",
                showticklabels=True,  # ✅ Keep tick labels
                showline=True,  # ✅ Ensure axis line is drawn
                linewidth=1,  # ✅ Make it visible
                linecolor='gray',  # ✅ Force it to show in black
                showgrid=True,  # ✅ Keep grid visible
                zeroline=False,  # ✅ Remove unwanted baseline
                automargin=True  # ✅ Dynamically adjust margins
            ),
            annotations=[],  # ✅ Explicitly remove auto-generated subplot annotations
            margin=dict(l=40, r=10, t=50, b=50),  # 🔥 Reduce left margin (was reserving space)
            showlegend=True,  # ✅ Ensure legend is visible
            template="plotly_white",
            legend=dict(
                title=dict(text=f"<b>{legend_title}</b>", font=dict(size=12)),
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99
            )
        )

        # ✅ Overwrite annotations instead of removing from a tuple
        fig.layout.annotations = [
            annotation for annotation in fig.layout.annotations
            if "Ratio Data" not in annotation.text
        ]

        print(fig.layout)
        return jsonify(fig.to_plotly_json())

    except Exception as e:
        logging.error(f"❌ Unexpected error in plot_ratio: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@main.route('/download_raw_data')
def download_raw_data():
    """ Route to handle raw data downloads. """
    return download_csv_file("raw")

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
        file_path = os.path.join("flask", "data-processed", filename)

        # ✅ Check if the file exists before sending
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {filename}"}), 404

        return send_from_directory("flask/data-processed", filename, as_attachment=True)

    except Exception as e:
        return jsonify({"error": f"Failed to download {data_type} data: {str(e)}"}), 500
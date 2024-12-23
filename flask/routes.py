from flask import Blueprint, render_template, request, jsonify, make_response, send_from_directory
from flask import current_app as app
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

import os
import markdown
import zipfile

# Create a blueprint
main = Blueprint("main", __name__)

# Define available selections
strips = ['S1', 'S2', 'S3', 'S4']
variables = ['VWC', 'T', 'EC']
sensor_depths = [1, 2, 3]
logger_locations = {
    "T": "Top Logger",
    "M": "Mid Logger",
    "B": "Bottom Logger"
}
depth_labels = {1: '6"', 2: '12"', 3: '18"'}
years = {'2023', '2024'}

# default variables
DEFAULT_YEAR = 2024
DEFAULT_VARIABLE = 'VWC'
DEFAULT_DEPTH = '1'
DEFAULT_LOGGER = 'T'
DEFAULT_START_DATE = f'{DEFAULT_YEAR}-01-01'
DEFAULT_END_DATE = f'{DEFAULT_YEAR}-12-31'
print(f"Defaults: Year={DEFAULT_YEAR}, Start={DEFAULT_START_DATE}, End={DEFAULT_END_DATE}")

# Load dataset function
def load_dataset(year, granularity="daily"):
    """Load precomputed dataset for the given year and granularity."""
    file_type = "daily" if granularity == "daily" else "15min"
    zip_path = os.path.join("data", "processed", f"dataloggerData_{year}_{file_type}.zip")
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_file = [f for f in z.namelist() if f.endswith(".csv")][0]
        with z.open(csv_file) as file:
            df = pd.read_csv(file)
    return df


# Function to determine y-axis label based on variable
def y_axis_label(var_name):
    if var_name == 'VWC':
        return 'Soil Moisture (%)'
    elif var_name == 'T':
        return 'Soil Temperature (°C)'
    elif var_name == 'EC':
        return 'Electrical Conductivity (dS/m)'
    return var_name


@main.route('/default_dates', methods=['GET'])
def default_dates():
    selected_year = request.form.get("year", DEFAULT_YEAR)  # Retrieve year from request or use default
    df = load_dataset(selected_year)
    # Ensure the 'timestamp' column is present and valid
    if 'timestamp' in df.columns:
        start_date = df['timestamp'].dropna().min().strftime('%Y-%m-%d')
        end_date = df['timestamp'].dropna().max().strftime('%Y-%m-%d')
        return jsonify(start_date=start_date, end_date=end_date)
    else:
        return jsonify(start_date='2024-01-01', end_date='2024-12-17')


@main.route("/")
def home():
    # Load markdown for the introduction
    md_file_path = os.path.join(os.getcwd(), 'flask', 'markdown', 'intro.md')
    if not os.path.exists(md_file_path):
        return f"Markdown file not found at {md_file_path}", 404

    with open(md_file_path, 'r') as file:
        md_content = file.read()

    # Convert Markdown to HTML
    intro_html = markdown.markdown(md_content)

    return render_template(
        "index.html",
        intro_content=intro_html,
        strips=strips,
        variables=variables,
        depths=sensor_depths,
        logger_locations=logger_locations,
        DEFAULT_YEAR=DEFAULT_YEAR,
        DEFAULT_START_DATE=DEFAULT_START_DATE,
        DEFAULT_END_DATE=DEFAULT_END_DATE
    )


@main.route('/intro')
def intro():
    # Construct the path to the markdown file relative to app.py
    md_file_path = os.path.join(os.getcwd(), 'flask', 'markdown', 'intro.md')
    print(f"md_file_path: {md_file_path}")
    if not os.path.exists(md_file_path):
        return f"Markdown file not found at {md_file_path}", 404

    with open(md_file_path, 'r') as file:
        md_content = file.read()

    # Convert Markdown to HTML
    html_content = markdown.markdown(md_content)

    return render_template('index.html', intro_content=html_content)


@main.route("/download_raw_data", methods=["GET"])
def download_raw_data():
    filename = request.args.get("filename", "raw_data.csv")
    selected_year = request.args.get("year", DEFAULT_YEAR)
    granularity = request.args.get("granularity", "daily")  # Default to daily data

    # Load dataset
    df = load_dataset(selected_year, granularity)

    # Include ratios in the download
    data_to_download = df[["timestamp"] + [col for col in df.columns if "Ratio" in col]]

    # Send CSV file
    response = make_response(data_to_download.to_csv(index=False))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "text/csv"
    return response


@main.route("/download_ratio_data", methods=["GET"])
def download_ratio_data():
    filename = request.args.get("filename", "ratio_data.csv")

    # Default values for user selections
    selected_variable = request.args.get("variable", "VWC")
    selected_depth = request.args.get("depth", "1")
    selected_logger = request.args.get("logger", "T")
    selected_year = request.args.get("year", None)  # Retrieve the year

    # Construct the columns for ratio calculation
    col_s1 = f"{selected_variable}_{selected_depth}_Avg_S1{selected_logger}"
    col_s2 = f"{selected_variable}_{selected_depth}_Avg_S2{selected_logger}"
    col_s3 = f"{selected_variable}_{selected_depth}_Avg_S3{selected_logger}"
    col_s4 = f"{selected_variable}_{selected_depth}_Avg_S4{selected_logger}"

    # Load dataset
    selected_year = request.form.get("year", DEFAULT_YEAR)  # Retrieve year from request or use default
    df = load_dataset(selected_year)

    # Filter data by year if specified
    if selected_year:
        df = df[df["timestamp"].dt.year == int(selected_year)]

    # Check if the columns exist
    missing_columns = [col for col in [col_s1, col_s2, col_s3, col_s4] if col not in df.columns]
    if missing_columns:
        return f"Error: Missing columns {missing_columns} in the data.", 400

    # Calculate ratios
    df["S2_S1_Ratio"] = df[col_s2] / df[col_s1]
    df["S4_S3_Ratio"] = df[col_s4] / df[col_s3]

    # Select relevant columns
    data_to_download = df[["timestamp", "S2_S1_Ratio", "S4_S3_Ratio"]]

    # Send CSV file
    response = make_response(data_to_download.to_csv(index=False))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "text/csv"
    return response


def load_and_filter_data():
    # Get parameters from the request
    selected_strip = request.form.get("strip", "S1")
    selected_variable = request.form.get("variable", "VWC")
    selected_depth = request.form.get("depth", "1")
    selected_logger = request.form.get("logger", "T")
    selected_start_date = request.form.get("start_date")
    selected_end_date = request.form.get("end_date")
    # selected_year = request.form.get("year")
    print("selected_variable in load and filter:", selected_variable)
    print("selected_depth in load and filter", selected_depth)

    print("selected_start_date in load and filter:", selected_start_date)
    print("selected_end_date in load and filter", selected_end_date)

    # Construct dynamic column names
    col_s1 = f"{selected_variable}_{selected_depth}_Avg_S1{selected_logger}"
    col_s2 = f"{selected_variable}_{selected_depth}_Avg_S2{selected_logger}"
    col_s3 = f"{selected_variable}_{selected_depth}_Avg_S3{selected_logger}"
    col_s4 = f"{selected_variable}_{selected_depth}_Avg_S4{selected_logger}"
    print("col_s1 in depth and filter:", col_s1)

    column_name = f"{selected_variable}_{selected_depth}_Avg_{selected_strip}{selected_logger}"

    # Load dataset
    data_dir = os.path.join(os.getcwd(), "data", "processed")
    csv_files = [f for f in os.listdir(data_dir) if f.startswith("dataloggerData_daily") and f.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError("No daily averaged CSV files found in the processed data directory.")

    latest_csv = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(data_dir, f)))
    csv_path = os.path.join(data_dir, latest_csv)
    df = pd.read_csv(csv_path)
    print('df.columns:', df.columns)
    # Ensure 'timestamp' is in datetime format
    df["timestamp"] = pd.to_datetime(df["datetime"], errors='coerce')
    print("Original Timestamps:", df["timestamp"])
    print(f"Min Timestamp: {df['timestamp'].min()}, Max Timestamp: {df['timestamp'].max()}")
    # Filter by date range
    # Temporarily comment out the date filtering
    # if selected_start_date:
    #     df = df[df["timestamp"] >= datetime.strptime(selected_start_date, "%Y-%m-%d")]
    # if selected_end_date:
    #     df = df[df["timestamp"] <= datetime.strptime(selected_end_date, "%Y-%m-%d")]
    print(f"Data Without Filtering: {df[['timestamp']].head()}")
    return df, column_name, col_s1, col_s2, col_s3, col_s4


# Route for raw data plot
@main.route("/plot_raw", methods=["POST"])
def plot_raw():
    df, column_name, _, _, _, _ = load_and_filter_data()
    selected_year = request.form.get("year", DEFAULT_YEAR)
    selected_variable = request.form.get("variable", DEFAULT_VARIABLE)
    granularity = request.form.get("granularity", "daily")  # Default to daily data
    df = load_dataset(selected_year, granularity)
    print(f"Selected variable in plot_raw: {selected_variable}")  # Debugging
    # Create Plotly figure
    fig = go.Figure()
    fig.update_layout(
        title="Raw Data Plot",
        xaxis_title="Date",
        yaxis_title=y_axis_label(selected_variable),
        template="plotly_white",
        legend = dict(
            x=1,  # Right edge of the plot
            y=1,  # Top edge of the plot
            xanchor="right",
            yanchor="top",
            bgcolor="rgba(255, 255, 255, 0.8)",  # Semi-transparent background
            bordercolor="black",
            borderwidth=1
         ),
        margin = dict(l=60, r=20, t=50, b=50)  # Consistent margins

    )
    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df[column_name],
        mode="lines+markers",
        name=f"Raw Data: {column_name}",
        showlegend=True  # Force legend display
    ))

    return fig.to_json()

@main.route("/plot_ratio", methods=["POST"])
def plot_ratio():
    # Retrieve user selections
    selected_year = request.form.get("year", DEFAULT_YEAR)
    selected_variable = request.form.get("variable", DEFAULT_VARIABLE)
    selected_depth = request.form.get("depth", DEFAULT_DEPTH)

    # Load precomputed dataset
    df = load_dataset(selected_year, "daily")

    # Construct column names for precomputed ratios, including depth
    ratio_col_s2_s1 = f"S2_S1_Ratio_{selected_variable}_{selected_depth}"
    ratio_col_s4_s3 = f"S4_S3_Ratio_{selected_variable}_{selected_depth}"

    # Check if the ratio columns exist
    missing_columns = [col for col in [ratio_col_s2_s1, ratio_col_s4_s3] if col not in df.columns]
    if missing_columns:
        return jsonify(error=f"Missing columns: {missing_columns}"), 400

    # Create Plotly figure using precomputed ratios
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df[ratio_col_s2_s1],
        mode="lines+markers",
        name=f"S2/S1 Ratio: {selected_variable} (Depth: {selected_depth})"
    ))
    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df[ratio_col_s4_s3],
        mode="lines+markers",
        name=f"S4/S3 Ratio: {selected_variable} (Depth: {selected_depth})"
    ))

    # Set axis labels
    fig.update_layout(
        title="Ratio Plot",
        xaxis_title="Date",
        yaxis_title=f"{selected_variable} Ratio (Depth: {selected_depth})",
        template="plotly_white",
        legend = dict(
            x=1,  # Right edge of the plot
            y=1,  # Top edge of the plot
            xanchor="right",
            yanchor="top",
            bgcolor="rgba(255, 255, 255, 0.8)",  # Semi-transparent background
            bordercolor="black",
            borderwidth=1
        ),
        margin = dict(l=60, r=20, t=50, b=50)  # Consistent margins
    )


    return fig.to_json()
from flask import Flask, render_template, request
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as pyo
import os
import zipfile

app = Flask(__name__)

# Set up file paths
base_path = '/Users/gcn/Documents/workspace/biocharWaterConserveWebsite/data-raw'
zip_file = 'dataloggerData_2024-01-01_2024-11-12.zip'
zip_path = os.path.join(base_path, zip_file)

# Extract and read CSV data
try:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            # Extract the file with its base name (remove the directory structure)
            target_path = os.path.join(base_path, os.path.basename(member))
            with zip_ref.open(member) as source, open(target_path, 'wb') as target:
                target.write(source.read())
        print(f"Extracted files: {zip_ref.namelist()}")  # List extracted files for debugging
except FileNotFoundError:
    print(f"Error: Zip file '{zip_path}' not found.")
    raise
except zipfile.BadZipFile:
    print(f"Error: '{zip_path}' is not a valid zip file.")
    raise

# Check the extracted file name
data_file = 'dataloggerData_2024-01-01_2024-11-12.csv'
data_path = os.path.join(base_path, data_file)

# Verify if the extracted file exists
if not os.path.exists(data_path):
    print(f"Error: Extracted file '{data_path}' not found.")
    raise FileNotFoundError(f"Extracted file '{data_path}' not found.")

# Load CSV data and parse datetime
try:
    data = pd.read_csv(data_path, parse_dates=['datetime'])
    print("CSV data loaded successfully.")
except Exception as e:
    print(f"Error loading CSV: {e}")
    raise

# Debug: Display the first few rows of the data
print(data.head())

# Ensure datetime is properly formatted and convert invalid values to NaT
data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce')

# Debugging: Check for rows with NaT in the datetime column
invalid_dates = data['datetime'].isna().sum()
print(f"Number of invalid datetime entries: {invalid_dates}")

# Preprocess data: calculate daily averages
data['date'] = data['datetime'].dt.floor('d')

# Convert VWC columns to percentages
vwc_columns = [col for col in data.columns if 'VWC' in col]
data[vwc_columns] = data[vwc_columns] * 100
daily_avg_data = data.groupby(['date']).mean().reset_index()

# Rename columns to more descriptive names
def rename_columns(df):
    col_mapping = {}
    for col in df.columns:
        # Only replace the suffix if it matches exactly
        if col.endswith('T'):
            col_mapping[col] = col[:-1] + 'Top'  # Replace '_T' with '_Top'
        elif col.endswith('M'):
            col_mapping[col] = col[:-1] + 'Mid'  # Replace '_M' with '_Mid'
        elif col.endswith('B'):
            col_mapping[col] = col[:-1] + 'Bot'  # Replace '_B' with '_Bot'
    df.rename(columns=col_mapping, inplace=True)

rename_columns(daily_avg_data)

# Define available strips, variables, depths, and data loggers
strips = ['S1', 'S2', 'S3', 'S4']
variables = ['VWC', 'T', 'EC']
sensor_depths = [1, 2, 3]
comparison_types = ['location', 'depths']
data_logger_levels = ['Top', 'Mid', 'Bot']
depth_labels = {1: '6"', 2: '12"', 3: '18"'}
logger_locations = {'Top': 'Top', 'Mid': 'Mid', 'Bot': 'Bot'}

def y_axis_label(var_name):
    if var_name == 'VWC':
        return 'Soil Moisture (%)'
    elif var_name == 'T':
        return 'Soil temperature (°C)'
    elif var_name == 'EC':
        return 'Electrical conductivity (dS/m)'
    return var_name

def build_page_context():
    return {
        'strips': strips,
        'variables': variables,
        'sensor_depths': sensor_depths,
        'comparison_types': comparison_types,
        'logger_locations': logger_locations,
        'depth_labels': depth_labels,
        'selected_strip': strips[0],
        'selected_variable': variables[0],
        'selected_depth': sensor_depths[0],
        'selected_comparison_type': comparison_types[0],
        'selected_logger_location': 'Top',
    }

@app.route('/')
def index():
    page_context = build_page_context()
    return render_template('index.html', **page_context)

@app.route('/plot_raw', methods=['POST'])
def plot_raw_route():
    page_context = build_page_context()

    # Get selected parameters from the form
    selected_strip = request.form.get('strip', page_context['selected_strip'])
    selected_depth = int(request.form.get('depth', page_context['selected_depth']))
    selected_variable = request.form.get('variable', page_context['selected_variable'])
    selected_comparison_type = request.form.get('comparison_type', page_context['selected_comparison_type'])

    # Debugging: Print selected parameters
    print(f"Selected parameters - Strip: {selected_strip}, Depth: {selected_depth}, Variable: {selected_variable}, Comparison Type: {selected_comparison_type}")

    # Set default for cols_to_plot to avoid potential errors
    cols_to_plot = []

    if selected_comparison_type == 'location':
        cols_to_plot = [
            'date',
            f'{selected_variable}_{selected_depth}_Avg_{selected_strip}_Top',
            f'{selected_variable}_{selected_depth}_Avg_{selected_strip}_Mid',
            f'{selected_variable}_{selected_depth}_Avg_{selected_strip}_Bot'
        ]
    elif selected_comparison_type == 'depths':
        cols_to_plot = [
            'date',
            f'{selected_variable}_1_Avg_{selected_strip}_Top',
            f'{selected_variable}_2_Avg_{selected_strip}_Mid',
            f'{selected_variable}_3_Avg_{selected_strip}_Bot'
        ]

    # Check if columns exist in the DataFrame
    available_cols = daily_avg_data.columns.tolist()
    missing_cols = [col for col in cols_to_plot if col not in available_cols]
    if missing_cols:
        error_message = f"Error: Selected columns not found in the data. Please check your selection. Missing columns: {missing_cols}"
        print(error_message)  # Debugging: Print error message
        page_context['error_message'] = error_message
        return render_template('index.html', **page_context)

    # Filter data by selected columns
    filtered_raw_data = daily_avg_data[cols_to_plot]

    # Debugging: Print filtered data
    print("Filtered raw data for plotting:")
    print(filtered_raw_data.head())

    # Create the plot
    fig = go.Figure()
    for col in filtered_raw_data.columns[1:]:
        fig.add_trace(go.Scatter(x=filtered_raw_data['date'],
                                 y=filtered_raw_data[col],
                                 mode='lines',
                                 name=col,
                                 connectgaps=True))

    # Update layout for raw data plot
    fig.update_layout(title=f'Raw Data Plot: {selected_variable} at Depth {depth_labels[selected_depth]} in {selected_strip}',
                      xaxis_title='Date',
                      yaxis_title=y_axis_label(selected_variable),
                      template='plotly_white',
                      annotations=[
                          dict(
                              text="Use the mouse to zoom in on a shorter period.",
                              xref="paper",
                              yref="paper",
                              x=0.5,
                              y=1.1,
                              showarrow=False,
                              font=dict(size=12)
                          )
                      ])

    # Convert the figure to HTML
    graph_html = pyo.plot(fig, output_type='div')

    # Update page context with graph HTML
    page_context['graph_raw_html'] = graph_html
    return render_template('index.html', **page_context)

@app.route('/plot_ratio', methods=['POST'])
def plot_ratio_route():
    page_context = build_page_context()

    # Get selected parameters from the form
    selected_depth = int(request.form.get('depth', page_context['selected_depth']))
    selected_variable = request.form.get('variable', page_context['selected_variable'])
    selected_logger_location = request.form.get('logger_location', page_context['selected_logger_location'])

    # Debugging: Print selected parameters for ratio plot
    print(f"Selected parameters for ratio plot - Depth: {selected_depth}, Variable: {selected_variable}, Logger Location: {selected_logger_location}")

    # Construct the columns needed for ratio calculation
    cols_to_plot = [
        'date',
        f'{selected_variable}_{selected_depth}_Avg_S1_{selected_logger_location}',
        f'{selected_variable}_{selected_depth}_Avg_S2_{selected_logger_location}',
        f'{selected_variable}_{selected_depth}_Avg_S3_{selected_logger_location}',
        f'{selected_variable}_{selected_depth}_Avg_S4_{selected_logger_location}'
    ]

    # Check if columns exist in the DataFrame
    available_cols = daily_avg_data.columns.tolist()
    missing_cols = [col for col in cols_to_plot if col not in available_cols]
    if missing_cols:
        error_message = f"Error: Selected columns not found in the data. Please check your selection. Missing columns: {missing_cols}"
        print(error_message)  # Debugging: Print error message
        page_context['error_message'] = error_message
        return render_template('index.html', **page_context)

    ratio_data = daily_avg_data[cols_to_plot].copy()

    # Calculate ratios
    try:
        ratio_data['VWC_S1_S2_Ratio'] = ratio_data[f'{selected_variable}_{selected_depth}_Avg_S1_{selected_logger_location}'] / \
                                        ratio_data[f'{selected_variable}_{selected_depth}_Avg_S2_{selected_logger_location}']
        ratio_data['VWC_S3_S4_Ratio'] = ratio_data[f'{selected_variable}_{selected_depth}_Avg_S3_{selected_logger_location}'] / \
                                        ratio_data[f'{selected_variable}_{selected_depth}_Avg_S4_{selected_logger_location}']
    except KeyError as e:
        print(f"KeyError during ratio calculation: {e}")  # Debugging: Print exception details
        page_context['error_message'] = "Error: Required columns for ratio calculation are missing."
        return render_template('index.html', **page_context)

    filtered_ratio_data = ratio_data[['date', 'VWC_S1_S2_Ratio', 'VWC_S3_S4_Ratio']].dropna(subset=['VWC_S1_S2_Ratio', 'VWC_S3_S4_Ratio'], how='all')

    # Debugging: Print filtered ratio data
    print("Filtered ratio data for plotting:")
    print(filtered_ratio_data.head())

    # Create the plot
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=filtered_ratio_data['date'],
                             y=filtered_ratio_data['VWC_S1_S2_Ratio'],
                             mode='lines',
                             name='VWC Ratio (S1/S2)',
                             connectgaps=True))
    fig.add_trace(go.Scatter(x=filtered_ratio_data['date'],
                             y=filtered_ratio_data['VWC_S3_S4_Ratio'],
                             mode='lines',
                             name='VWC Ratio (S3/S4)',
                             connectgaps=True))

    # Update layout for biochar ratio plot
    fig.update_layout(title=f'Biochar Ratios: {selected_variable} at Depth {depth_labels[selected_depth]}',
                      xaxis_title='Date',
                      yaxis_title=y_axis_label(selected_variable),
                      template='plotly_white')

    # Convert the figure to HTML
    graph_html = pyo.plot(fig, output_type='div')

    # Update page context with graph HTML
    page_context['graph_ratio_html'] = graph_html
    return render_template('index.html', **page_context)

if __name__ == '__main__':
    app.run(debug=True)

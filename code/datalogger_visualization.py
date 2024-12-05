# Metadata / Overview
# Full Structure Overview:
# The code starts by importing required modules (Flask, Pandas, Plotly, etc.).
# It then initializes a Flask app and reads CSV data from a specified path.
# Data preprocessing involves parsing the datetime column, calculating daily averages, and adjusting columns like VWC to percentages.
# Key variables and constants (e.g., strips, variables, sensor_depths) are defined.
# The main route (/) returns the template with initial parameters.
# The /plot route takes user input, processes the selected data, and plots using Plotly.
# Finally, the app is run in debug mode (app.run(debug=True)).

from flask import Flask, render_template, request
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as pyo
import os
import zipfile

# Initialize Flask app
app = Flask(__name__)

# Define available strips, variables, depths, and data loggers
strips = ['S1', 'S2', 'S3', 'S4']
variables = ['VWC', 'T', 'EC']
sensor_depths = [1, 2, 3]
comparison_types = ['location', 'depths']
biochar_comparisons = ['biochar_ratio']
logger_locations = {
    "Top": "Top Logger",
    "Mid": "Mid Logger",
    "Bot": "Bottom Logger"
}
depth_labels = {1: '6"', 2: '12"', 3: '18"'}

# Define default starting values
default_strip = 'S1'
default_variable = 'VWC'
default_comparison_type = 'location'
default_biochar_comparison = 'biochar_ratio'
default_depth = '1'
default_logger_location = 'Top'

# Load CSV data from zip file
base_path = '/Users/gcn/Documents/workspace/biocharWaterConserveWebsite/data-raw'
data_file = 'dataloggerData_2024-01-01_2024-11-12.zip'
data_path = os.path.join(base_path, data_file)
# Load the data and parse datetime
#data = pd.read_csv(data_path, parse_dates=['datetime'])
data = ZipFile.open(data_path, parse_dates=['datetime'])
# Ensure datetime is properly formatted
data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce')

# Preprocess data by calculating daily averages
data['date'] = data['datetime'].dt.floor('d')

# Convert VWC columns to percentages and calculate daily averages
vwc_columns = [col for col in data.columns if 'VWC' in col]
data[vwc_columns] = data[vwc_columns] * 100
daily_avg_data = data.groupby(['date']).mean().reset_index()

# Replace NaN values with None for JSON compatibility
daily_avg_data = daily_avg_data.where(pd.notnull(daily_avg_data), None)

def build_page_context():
    return {
        'strips': strips,
        'variables': variables,
        'comparison_types': comparison_types,
        'biochar_comparisons': biochar_comparisons,
        'sensor_depths': sensor_depths,
        'depth_labels': depth_labels,
        'logger_locations': logger_locations,
        'selected_strip': request.args.get('strip', default_strip),
        'selected_variable': request.args.get('variable', default_variable),
        'selected_comparison_type': request.args.get('comparison_type', default_comparison_type),
        'selected_biochar_comparison': request.args.get('biochar_comparison', default_biochar_comparison),
        'selected_depth': request.args.get('depth', default_depth),
        'selected_logger_location': request.args.get('logger', default_logger_location)
    }
# Rename columns to more descriptive names
def rename_columns(df):
    col_mapping = {}
    for col in df.columns:
        # Only replace the suffix if it matches exactly
        if col.endswith('T'):
            col_mapping[col] = col[:-1] + '_Top'  # Replace '_T' with '_Top'
        elif col.endswith('M'):
            col_mapping[col] = col[:-1] + '_Mid'  # Replace '_M' with '_Mid'
        elif col.endswith('B'):
            col_mapping[col] = col[:-1] + '_Bot'  # Replace '_B' with '_Bot'
    df.rename(columns=col_mapping, inplace=True)

rename_columns(daily_avg_data)

# Debugging: print available columns for verification
print("Updated columns in daily_avg_data:", daily_avg_data.columns.tolist())

# Function to determine y-axis label based on variable
def y_axis_label(var_name):
    if var_name == 'VWC':
        return 'Soil Moisture (%)'
    elif var_name == 'T':
        return 'Soil temperature (°C)'
    elif var_name == 'EC':
        return 'Electrical conductivity (dS/m)'
    return var_name

# def prepare_plot_data(data_in, y_columns, title):
#     """ Prepares Plotly plot data from DataFrame columns """
#     plot_data_out = []
#     for col in y_columns:
#         plot_data_out.append({
#             'x': data_in['date'].tolist(),
#             'y': data_in[col].tolist(),
#             'mode': 'lines',
#             'name': col
#         })
#     layout = {
#         'title': title,
#         'xaxis': {'title': 'Date'},
#         'yaxis': {'title': y_axis_label(y_columns[0][:3])}
#     }
#     return {'data': plot_data_out, 'layout': layout}
#
# @app.route('/plot_data', methods=['POST'])
# def plot_data():
#     # Use all VWC columns for raw data plot
#     y_columns_raw = [col for col in data.columns if 'VWC' in col]
#
#     # For ratio plot, use the same columns or define calculated ratios
#     y_columns_ratio = [col for col in daily_avg_data.columns if 'VWC' in col]
#
#     raw_data_plot = prepare_plot_data(data, y_columns_raw, 'Raw Data Plot')
#     ratio_data_plot = prepare_plot_data(daily_avg_data, y_columns_ratio, 'Ratio Data Plot')
#
#     return jsonify({'raw_data': raw_data_plot, 'ratio_data': ratio_data_plot})
#
# @app.route('/')
# def index():
#     return render_template('index.html')

# Define routes - a route defines which function should be executed when a user accesses a particular URL.
@app.route('/')
def index():
    page_context = build_page_context()
    return render_template('index.html', **page_context)

@app.route('/plot', methods=['POST'])
def plot_data():
    # Get selected parameters from the form
    biochar_comparison = request.form.get('data_plot_type', 'selected_biochar_comparison')
    print("Received form data:", request.form)

    if biochar_comparison == 'biochar_ratio':
        return plot_ratio_route()
    else:
        return plot_raw_route()

@app.route('/plot_raw', methods=['POST'])
def plot_raw_route():
    # Get selected parameters from the form
    selected_strip = request.form.get('strip')
    selected_depth = int(request.form.get('depth'))
    selected_variable = request.form.get('variable')
    selected_comparison_type = request.form.get('comparison_type')

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

 #   if selected_variable == 'T':
  #      cols_to_plot.insert(f'{temp_air_mean')


    # Check if columns exist in the DataFrame
    available_cols = daily_avg_data.columns.tolist()
    missing_cols = [col for col in cols_to_plot if col not in available_cols]
    if missing_cols:
        return f"Error: Selected columns not found in the data. Missing columns: {missing_cols}", 400

    # Filter data by selected columns
    filtered_raw_data = daily_avg_data[cols_to_plot]

    # Create the plot
    fig = go.Figure()
    for col in filtered_raw_data.columns[1:]:
        fig.add_trace(go.Scatter(x=filtered_raw_data['date'],
                                 y=filtered_raw_data[col],
                                 mode='lines',
                                 name=col,
                                 connectgaps=True))

    # Update layout for raw data plot
    fig.update_layout(
        title=dict(
            text=f'Raw Data Plot: {selected_variable} at Depth {depth_labels[selected_depth]} in {selected_strip}',
            x=0.5,
            y=1.05,
            xanchor='center',
            yanchor='bottom',
            font=dict(size=16)  # Reduce size to fit both title and annotation
        ),
        xaxis_title='Date',
        yaxis_title=y_axis_label(selected_variable),
        template='plotly_white',
        annotations=[
            dict(
                text="Use the mouse to zoom in on a shorter period.",
                xref="paper",
                yref="paper",
                x=0.5,
                y=1.02,
                showarrow=False,
                font=dict(size=12)
            )
        ]
    )

    # Convert the figure to HTML
    graph_html = pyo.plot(fig, output_type='div')

    # Only return the graph HTML
    return graph_html


@app.route('/plot_ratio', methods=['POST'])
def plot_ratio_route():
    # Get selected parameters from the form
    selected_depth = int(request.form.get('depth'))
    selected_variable = request.form.get('variable')
    selected_logger_location = request.form.get('logger_location')

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
        return f"Error: Selected columns not found in the data. Missing columns: {missing_cols}", 400

    ratio_data = daily_avg_data[cols_to_plot].copy()

    # Calculate ratios
    try:
        ratio_data['VWC_S1_S2_Ratio'] = ratio_data[f'{selected_variable}_{selected_depth}_Avg_S1_{selected_logger_location}'] / \
                                        ratio_data[f'{selected_variable}_{selected_depth}_Avg_S2_{selected_logger_location}']
        ratio_data['VWC_S3_S4_Ratio'] = ratio_data[f'{selected_variable}_{selected_depth}_Avg_S3_{selected_logger_location}'] / \
                                        ratio_data[f'{selected_variable}_{selected_depth}_Avg_S4_{selected_logger_location}']
    except KeyError as e:
        return "Error: Required columns for ratio calculation are missing.", 400

    filtered_ratio_data = ratio_data[['date', 'VWC_S1_S2_Ratio', 'VWC_S3_S4_Ratio']].dropna(subset=['VWC_S1_S2_Ratio', 'VWC_S3_S4_Ratio'], how='all')

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
    fig.update_layout(title=f'Biochar Ratios: {selected_variable} at depth {depth_labels[selected_depth]} for {logger_locations[selected_logger_location]}s',
                      xaxis_title='Date',
                      yaxis_title=y_axis_label(selected_variable),
                      template='plotly_white',
                      annotations=[
                          dict(
                              text="Use the mouse to zoom in on a shorter period.",
                              x=0.1,
                              y=1.1,
                              xref="paper",
                              yref="paper",
                              showarrow=False,
                              font=dict(size=12)
                          )
                      ]
    )

    # Convert the figure to HTML
    graph_html = pyo.plot(fig, output_type='div')

    # Only return the graph HTML
    return graph_html

# Run Flask app in debug mode
if __name__ == '__main__':
    app.run(debug=True)


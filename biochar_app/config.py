import os

# Core default values
DEFAULT_YEAR = "2024"
DEFAULT_START_DATE = f"{DEFAULT_YEAR}-01-01"
DEFAULT_VARIABLE = "VWC"
DEFAULT_DEPTH = "1"
DEFAULT_STRIP = "S1"
DEFAULT_LOGGER_LOCATION = "T"
DEFAULT_GRANULARITY = "daily"
DATALOGGER_NAMES = ["S1T", "S1M", "S1B", "S2T", "S2M", "S2B", "S3T", "S3B", "S3M", "S4T", "S4M", "S4B"]

# Base paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data-raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data-processed")

# Available years
YEARS_ALL = [2023, 2024, 2025]
YEARS = [2024, 2025]

# Weather data constants
COAG_STATION = "frt03"
COLLECT_PERIOD = "5min"
METRICS_COAGDATA = ["t", "rh", "dewpt", "vp", "solarRad", "precip", "windSpeed", "windDir", "st5cm", "st15cm"]
METRICS_LABELS = ["temp_air_degC", "rh", "dewpoint_degC", "vaporpressure_kpa", "solarrad_wm-2",
                  "precip_mm", "wind_m_s", "winddir_degN", "temp_soil_5cm_degC", "temp_soil_15cm_degC"]
UNITS = "m"

sensor_depth_mapping = {
    1: "6 inches",
    2: "12 inches",
    3: "18 inches"
}


# SWC-specific weighting by depth
SWC_DEPTH_WEIGHTS = {
    "1": 0.25,  # 6 inches
    "2": 0.25,  # 12 inches
    "3": 0.5    # 18 inches
}

GSEASON_PERIODS = {
    "Q1_Winter": ("11-01", "02-28"),
    "Q2_Early_Growing": ("03-01", "05-31"),
    "Q3_Peak_Harvest": ("06-01", "10-31")
}

STRIPS = ["S1", "S2", "S3", "S4"]

VALUE_COLS_STANDARD = [
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"
]

VALUE_COLS_2024_PLUS = ["BattV_Min"] + VALUE_COLS_STANDARD

logger_location_mapping = {
    "T": "Top",
    "M": "Middle",
    "B": "Bottom"
}

variable_name_mapping = {
    "VWC": "Vol. Water Content",
    "T": "Temperature",  # Keep it concise but clear
    "EC": "Electrical Conductivity",
    "SWC": "Soil Water Content"
}

granularity_name_mapping = {
    "gseason": "Growing Season",
    "monthly": "Monthly",
    "daily": "Daily",
    "15min": "15 Minute",
    "1hour": "Hourly"
}

strip_name_mapping = {
    "S1": "Strip 1",
    "S2": "Strip 2",
    "S3": "Strip 3",
    "S4": "Strip 4"
}

label_name_mapping = {
    "VWC": "Volumetric Water Content (%)",
    "T": "Temperature (°C)",
    "EC": "Electrical Conductivity (dS/m)",
    "SWC": "Soil Water Content (Volume)"
}

# Extracted depths as strings from the sensor_depth_mapping keys
DEPTHS = [str(k) for k in sensor_depth_mapping.keys()]
LOGGER_LOCATIONS = [str(k) for k in logger_location_mapping.keys()]

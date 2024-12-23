import os
import re
import zipfile

import pandas as pd

# Define constants
DATA_RAW_DIR = os.path.join(os.getcwd(), "data-raw")
PROCESSED_DIR = os.path.join(os.getcwd(), "data", "processed")
SENSOR_DEPTHS = [1, 2, 3]
os.makedirs(PROCESSED_DIR, exist_ok=True)

VARIABLES = ["VWC", "T", "EC"]  # Variables for ratios


def get_years_from_zip():
    """Identify available years from zip filenames in data-raw."""
    zip_files = [f for f in os.listdir(DATA_RAW_DIR) if f.endswith(".zip")]
    years = set()

    for zip_file in zip_files:
        match = re.search(r"\d{4}", zip_file)
        if match:
            years.add(int(match.group()))

    return sorted(years)


def extract_csv_from_zip(zip_path):
    """Extract CSV files from a zip archive."""
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        dataframes = []
        for csv_file in csv_files:
            with z.open(csv_file) as file:
                df = pd.read_csv(file)
                dataframes.append(df)
        return pd.concat(dataframes, ignore_index=True)


def precompute_daily_averages_and_ratios(df):
    """Precompute daily averages and ratio columns for all variables."""
    # Calculate daily averages
    date_column = pd.DataFrame({"date": df["timestamp"].dt.date})
    df = pd.concat([df, date_column], axis=1)
    daily_averages = df.groupby("date").mean()

    # Compute ratios using daily averages
    ratio_columns = {}
    for var in VARIABLES:
        for depth in SENSOR_DEPTHS:  # Assume sensor_depths = [1, 2, 3]
            try:
                ratio_columns[f"S2_S1_Ratio_{var}_{depth}"] = (
                        daily_averages[f"{var}_{depth}_Avg_S2T"].fillna(0) /
                        daily_averages[f"{var}_{depth}_Avg_S1T"].fillna(1)
                )
                ratio_columns[f"S4_S3_Ratio_{var}_{depth}"] = (
                        daily_averages[f"{var}_{depth}_Avg_S4T"].fillna(0) /
                        daily_averages[f"{var}_{depth}_Avg_S3T"].fillna(1)
                )
            except KeyError as e:
                print(f"Warning: Missing columns for {var} at depth {depth}: {e}")

    # Combine computed ratio columns into the daily_averages DataFrame
    daily_averages = pd.concat([daily_averages, pd.DataFrame(ratio_columns)], axis=1)
    return daily_averages


def generate_yearly_data(year):
    """Generate data for the specified year."""
    zip_files = [f for f in os.listdir(DATA_RAW_DIR) if f.endswith(".zip")]
    combined_df = pd.DataFrame()

    for zip_file in zip_files:
        zip_path = os.path.join(DATA_RAW_DIR, zip_file)
        df = extract_csv_from_zip(zip_path)

        # Ensure the 'datetime' column is in datetime format
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

        # Create 'timestamp' column for consistency
        df["timestamp"] = df["datetime"]

        # Filter for the year
        df = df[df["datetime"].dt.year == year]
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    return combined_df


def precompute_data():
    """Precompute and save datasets for all years."""
    years_to_compute = get_years_from_zip()

    for year in years_to_compute:
        print(f"Processing data for {year}...")

        # Generate data
        df = generate_yearly_data(year)

        # Save 15-minute data
        zip_path = os.path.join(PROCESSED_DIR, f"dataloggerData_{year}_15min.zip")
        with zipfile.ZipFile(zip_path, 'w') as z:
            csv_path = f"dataloggerData_15min_{year}.csv"
            df.to_csv(csv_path, index=False)
            z.write(csv_path, arcname=os.path.basename(csv_path))
            os.remove(csv_path)

        # Calculate and save daily averages with ratios
        daily_data = precompute_daily_averages_and_ratios(df)
        daily_zip_path = os.path.join(PROCESSED_DIR, f"dataloggerData_{year}_daily.zip")
        with zipfile.ZipFile(daily_zip_path, 'w') as z:
            csv_path = f"dataloggerData_daily_{year}.csv"
            daily_data.to_csv(csv_path, index=False)
            z.write(csv_path, arcname=os.path.basename(csv_path))
            os.remove(csv_path)

        print(f"15-Minute Data saved to: {zip_path}")
        print(f"Daily Data with Ratios saved to: {daily_zip_path}")


if __name__ == "__main__":
    precompute_data()
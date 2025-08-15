import os
import pandas as pd
from datetime import datetime, timedelta

def find_dst_shifts(datadir: str):
    """
    Scan all S?_Table1.dat files in datadir, detect ~1-hour jumps in timestamps,
    and return a dict mapping each file to a list of shift points.
    """
    shifts = {}
    for fname in os.listdir(datadir):
        if not fname.endswith('_Table1.dat'):
            continue

        path = os.path.join(datadir, fname)
        df = pd.read_csv(path, parse_dates=[0], header=None)
        df.columns = ['timestamp'] + [f'col{i}' for i in range(1, len(df.columns))]

        # compute time deltas
        df['delta'] = df['timestamp'].diff().dt.total_seconds().abs()
        # look for approximately 3600-second jumps
        mask = df['delta'].between(3550, 3650)
        shift_times = df.loc[mask, 'timestamp'].tolist()
        if shift_times:
            shifts[fname] = shift_times
    return shifts

if __name__ == '__main__':
    DATADIR = 'biochar_app/data-raw/datfiles_2025'
    results = find_dst_shifts(DATADIR)
    print("Detected DST shifts:")
    for file, times in results.items():
        print(f"{file}:")
        for t in times:
            print(f"  - {t.isoformat()}")

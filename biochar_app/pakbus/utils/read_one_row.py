# read_one_row.py
# A simple script to fetch a single record from a CR800 web server

import sys
import requests
from urllib.parse import urlencode

# Replace with your logger's IPv6 address (or hostname)
HOST = "2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd"
BASE_URL = f"http://[{HOST}]/"
TABLE = "Table1"  # adjust if your table has a different name

# Choose fetch mode: either by timestamp (date-range) or by record number
MODE = "date-range"  # or "record"
DEFAULT_TIMESTAMP = "2025-07-08T06:00:00"
DEFAULT_RECORD = "61215"


def fetch_by_timestamp(ts: str):
    params = {
        "command": "dataquery",
        "uri": f"dl:{TABLE}",
        "format": "json",
        "mode": "date-range",
        "p1": ts,
        "p2": ts,
    }
    url = BASE_URL + "?" + urlencode(params)
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def fetch_by_record(rec: str):
    params = {
        "command": "dataquery",
        "uri": f"dl:{TABLE}",
        "format": "json",
        "mode": "record",
        "p1": rec,
    }
    url = BASE_URL + "?" + urlencode(params)
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    if MODE == "date-range":
        # timestamp argument or default
        ts = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TIMESTAMP
        print(f"Fetching timestamp {ts}")
        try:
            data = fetch_by_timestamp(ts)
            print(data)
        except requests.HTTPError as e:
            print("Error fetching data:", e)
            sys.exit(1)
    else:
        rec = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RECORD
        print(f"Fetching record #{rec}")
        try:
            data = fetch_by_record(rec)
            print(data)
        except requests.HTTPError as e:
            print("Error fetching data:", e)
            sys.exit(1)

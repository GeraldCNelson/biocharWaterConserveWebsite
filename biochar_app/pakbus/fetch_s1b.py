#!/usr/bin/env python3
"""
fetch_s1b.py

Fetch the last N records from a CR800 data logger via its HTTP interface.
"""

import argparse
import logging
from io import StringIO
from urllib.parse import quote

import pandas as pd
import requests

# ——— Configuration ———
BASE_URL = "http://[2605:59C0:30F3:2500:2D0:2CFF:FE02:1DDD]"  # CR800 root URL
DEFAULT_NUM_RECORDS = 5

# set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def check_root_page():
    """Verify we can reach the CR800 home page."""
    r = requests.get(BASE_URL)
    r.raise_for_status()
    server_header = r.headers.get("Server", "<unknown>")
    print(f"1) Verify root page…\n   HTTP {r.status_code} — {server_header}\n")


def list_tables():
    """
    Try to discover available tables by polling known command variants.
    Returns True if any variant returned 200; otherwise False.
    """
    candidates = ("Tables", "tables", "TableNames", "tablenames")
    print("2) What tables are available?")
    for cmd in candidates:
        url = f"{BASE_URL}/?command={cmd}"
        print(f"→ Listing tables: {url}")
        r = requests.get(url)
        if r.status_code == 200:
            print("   → Found table list (but skipping parse).")
            return True
        else:
            print(f"   → {r.status_code} {r.reason}")
    print("  ⚠️ Unable to list tables (404). Proceeding with Default=Table1.\n")
    return False


def fetch_last_records(table: str = "Table1", num: int = DEFAULT_NUM_RECORDS) -> pd.DataFrame:
    """
    Fetch the last `num` records from `table`.
    Falls back to NewestRecord if record_count mode fails.
    """
    # build the record‐count query
    uri = f"dl:{table}"
    params = {
        "command": "DataQuery",
        "uri": uri,
        "format": "csv",
        "mode": "record_count",   # MUST use underscore
        "n": num,
    }
    qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    url = f"{BASE_URL}/?{qs}"
    print(f"3) Requesting last {num} records:\n   {url}")
    r = requests.get(url)

    if r.status_code == 404:
        # fallback to NewestRecord one‐by‐one
        print("   → record_count unsupported; falling back to NewestRecord")
        rows = []
        for i in range(num):
            nr_url = f"{BASE_URL}/?command=NewestRecord&table={quote(table)}"
            print(f"   → NewestRecord #{i+1}: {nr_url}")
            nr = requests.get(nr_url)
            nr.raise_for_status()
            df = pd.read_html(nr.text, header=0, index_col=None)[0]
            rows.append(df)
        # concat in correct order
        return pd.concat(rows[::-1], ignore_index=True)

    r.raise_for_status()
    buf = StringIO(r.text)
    return pd.read_csv(buf)


def main():
    parser = argparse.ArgumentParser(description="Fetch last records from CR800 data logger")
    parser.add_argument(
        "--num", "-n",
        type=int,
        default=DEFAULT_NUM_RECORDS,
        help=f"number of records to fetch (default: {DEFAULT_NUM_RECORDS})",
    )
    parser.add_argument(
        "--table", "-t",
        default="Table1",
        help="table name to query (default: Table1)",
    )
    args = parser.parse_args()

    try:
        check_root_page()
    except Exception as e:
        logging.error("Failed to reach root page: %s", e)
        return

    list_tables()  # we don’t strictly need the result, just informative

    try:
        df = fetch_last_records(table=args.table, num=args.num)
        print("\nFetched DataFrame:")
        print(df)
    except Exception as e:
        logging.error("❌ Error fetching last %d records: %s", args.num, e)


if __name__ == "__main__":
    main()
# biochar_app/etf.py

import struct
import datetime

import struct
import datetime

def decode_row(raw_bytes: bytes) -> dict:
    """
    Decode one ETF record from Table1.

    Table1 layout (per your TDF):
      • 4-byte big-endian unsigned int  → UNIX timestamp (seconds)
      • 1×BattV (Minimum)
      • 3×(VWC, EC, T) for sensor 1
      • 3×(VWC, EC, T) for sensor 2
      • 3×(VWC, EC, T) for sensor 3

    All floats are 4-byte big-endian.
    """
    # total bytes = 4 + 10 * 4 = 44
    fmt = ">I10f"
    expected_len = struct.calcsize(fmt)  # 44

    if len(raw_bytes) < expected_len:
        raise ValueError(
            f"Expected at least {expected_len} bytes, got {len(raw_bytes)}"
        )

    ts, *vals = struct.unpack(fmt, raw_bytes[:expected_len])

    return {
        "timestamp": datetime.datetime.fromtimestamp(ts),
        "BattV":     vals[0],
        "VWC_1":     vals[1],
        "EC_1":      vals[2],
        "T_1":       vals[3],
        "VWC_2":     vals[4],
        "EC_2":      vals[5],
        "T_2":       vals[6],
        "VWC_3":     vals[7],
        "EC_3":      vals[8],
        "T_3":       vals[9],
    }
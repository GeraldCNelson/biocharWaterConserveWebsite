# biochar_app/etf.py

import struct
import datetime as dt
from typing import Dict, Any

# Campbell "seconds since 1990-01-01 00:00:00"
CAMPBELL_EPOCH = dt.datetime(1990, 1, 1)


def campbell_seconds_to_datetime(seconds: int) -> dt.datetime:
    """
    Convert Campbell 'seconds since 1990-01-01' to a naive datetime.

    Example:
      0                -> 1990-01-01 00:00:00
      550000000        -> ~2007-06-06 17:46:40
    """
    return CAMPBELL_EPOCH + dt.timedelta(seconds=int(seconds))


def decode_row(raw_bytes: bytes) -> Dict[str, Any]:
    """
    Decode one ETF record from Table1.

    Table1 layout (per TDF / CR206 program):

      • 4-byte big-endian unsigned int:
          Campbell timestamp = seconds since 1990-01-01 00:00:00
      • 1×BattV (Minimum)
      • 3×(VWC, EC, T) for sensor 1
      • 3×(VWC, EC, T) for sensor 2
      • 3×(VWC, EC, T) for sensor 3

    All floats are 4-byte big-endian.
    """
    fmt = ">I10f"
    expected_len = struct.calcsize(fmt)  # 44 bytes

    if len(raw_bytes) < expected_len:
        raise ValueError(
            f"Expected at least {expected_len} bytes, got {len(raw_bytes)}"
        )

    ts_campbell, *vals = struct.unpack(fmt, raw_bytes[:expected_len])
    timestamp = campbell_seconds_to_datetime(ts_campbell)

    return {
        "timestamp": timestamp,   # Campbell → datetime
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
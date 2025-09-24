# biochar_app/pakbus/serial_collect_tail.py
# Tail most-recent Table1 rows over a direct serial cable to a CR200/CR200X.

from __future__ import annotations

import argparse
import datetime as dt
import sys
from typing import Optional, Any, cast

import serial
from serial.tools import list_ports

from biochar_app.pakbus.cr200_client_utils import (
    ping_node,
    ensure_tabledefs,
    collect_most_recent,
    flatten_records,
    nsec_to_time,
)

# ---- serial→socket shim ------------------------------------------------------

class SerialSock:
    """Minimal shim so cr200_client_utils.send/recv can use a pyserial port."""
    def __init__(self, ser: serial.Serial):
        self._ser = ser

    # cr200_client_utils expects these 3 methods:
    def sendall(self, b: bytes) -> None:
        self._ser.write(b)

    def recv(self, n: int) -> bytes:
        return self._ser.read(n)

    def settimeout(self, t: float) -> None:
        self._ser.timeout = t

    def close(self) -> None:
        try:
            self._ser.close()
        except Exception:
            pass


# ---- helpers -----------------------------------------------------------------

def fmt_iso_pairs(sec_nsec: tuple[int, int]) -> tuple[str, str]:
    # Use fromtimestamp(..., tz=UTC) to avoid utcfromtimestamp deprecation warnings
    ts = dt.datetime.fromtimestamp(nsec_to_time(sec_nsec), tz=dt.timezone.utc)
    local = ts.astimezone()
    return ts.isoformat(), local.isoformat()

def default_serial_port() -> Optional[str]:
    """Try to guess a USB-serial device on macOS/Linux/Windows."""
    for p in list_ports.comports():
        name = (p.device or "").lower()
        if name.startswith("/dev/tty.usb") or name.startswith("/dev/cu.usb"):
            return p.device
        if name.startswith("com"):
            return p.device
    return None


# ---- main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Tail most-recent rows from Table1 over serial PakBus.")
    ap.add_argument("--port", default=None, help="Serial device (e.g. /dev/tty.usbserial-XXXX or COM7).")
    ap.add_argument("--baud", type=int, default=9600, help="Baud rate (CR200X max 9600).")
    ap.add_argument("--dst", type=int, default=13, help="Logger PakBus address (leaf).")
    ap.add_argument("--src", type=int, default=4093, help="Our PakBus address (PC).")
    ap.add_argument("--table", default="Table1", help="Table name to collect.")
    ap.add_argument("--count", type=int, default=12, help="How many most-recent records to fetch.")
    args = ap.parse_args()

    port = args.port or default_serial_port()
    if not port:
        print("Could not auto-detect a serial device. Pass --port explicitly.", file=sys.stderr)
        all_ports = [p.device for p in list_ports.comports()]
        print(f"Detected serial ports: {all_ports}", file=sys.stderr)
        sys.exit(2)

    try:
        ser = serial.Serial(port, args.baud, bytesize=8, parity="N", stopbits=1, timeout=2.0)
    except (serial.SerialException, OSError) as e:
        print(f"Failed to open serial port {port!r} at {args.baud} bps: {e}", file=sys.stderr)
        sys.exit(1)

    sock = SerialSock(ser)
    # The utils are type-hinted for socket.socket; cast to Any to quiet type checkers.
    s_any = cast(Any, sock)

    try:
        # Quick hello: helps open a route and confirms addresses
        _hello = ping_node(s_any, DstNodeId=args.dst, SrcNodeId=args.src, RouterPhyAddr=None, timeout=5.0)

        # Pull table definitions (needed to decode CollectData exactly)
        tbl = ensure_tabledefs(s_any, DstNodeId=args.dst, SrcNodeId=args.src, RouterPhyAddr=None, timeout=10.0)

        # Grab most recent rows
        rec_frags, _more = collect_most_recent(
            s_any,
            DstNodeId=args.dst,
            SrcNodeId=args.src,
            TableDef=tbl,
            TableName=args.table,
            Count=args.count,
            RouterPhyAddr=None,
            timeout=12.0,
        )
        rows = flatten_records(rec_frags)

        if not rows:
            print("[warn] No rows returned.")
            return

        # Oldest → newest
        rows.sort(key=lambda r: r.get("RecNbr", 0))

        # Print CSV-ish header compatible with your .dat file
        header = [
            "TS", "RecNbr",
            "BattV_Min",
            "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
            "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
            "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
        ]
        print(",".join(header))

        def g(d: dict, k: str, default: str | float = "NaN") -> str:
            v = d.get(k, default)
            if isinstance(v, float):
                return f"{v:.5f}"
            return str(v)

        for row in rows:
            _iso_utc, iso_local = fmt_iso_pairs(row["TimeOfRec"])
            line = [
                f'"{iso_local.replace("T", " ")[:19]}"',
                str(row.get("RecNbr", "")),
                g(row, "BattV_Min"),
                g(row, "VWC_1_Avg"), g(row, "EC_1_Avg"), g(row, "T_1_Avg"),
                g(row, "VWC_2_Avg"), g(row, "EC_2_Avg"), g(row, "T_2_Avg"),
                g(row, "VWC_3_Avg"), g(row, "EC_3_Avg"), g(row, "T_3_Avg"),
            ]
            print(",".join(line))

    finally:
        try:
            sock.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
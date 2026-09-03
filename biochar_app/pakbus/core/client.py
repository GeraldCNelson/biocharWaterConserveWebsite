#!/usr/bin/env python3
"""
client.py — Fetch recent records from Campbell loggers via PakBus/TCP over IPv6.

Key points:
- DEBUG logging enabled.
- Uses a fresh IPv6 TCP socket for each logger attempt so a delayed response
  from one station cannot corrupt the next station's transaction.
- Pulls host/port/base_id/logger_ids from config.PAKBUS.
- Handles NoDeviceException when instantiating CR1000 per logger ID (won’t crash).
- ICMPv6 ping is advisory; TCP preflight is the hard gate.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator
from zoneinfo import ZoneInfo

from biochar_app.config.pakbus import (
    PAKBUS,
    DEFAULT_HOURS,
    DEFAULT_TABLE,
    DEFAULT_LAG_MINUTES,
    DEFAULT_RETRY_DELAY_SECONDS,
    DEFAULT_STATION_ATTEMPTS,
    DEFAULT_STATION_PAUSE_SECONDS,
    DEFAULT_TIMEZONE,
    ID_BY_STATION,
    STATION_BY_ID,
)

if TYPE_CHECKING:
    import pandas as pd
# Use the shared IPv6 link + URL override implementation
CR1000 = None
open_pakbus_link = None
_legacy_retryable_exceptions: tuple[type[Exception], ...] = ()


def _load_legacy_transport() -> None:
    """Load the optional PyCampbell/PyLink transport only for real downloads."""
    global CR1000, open_pakbus_link, _legacy_retryable_exceptions
    if CR1000 is not None and open_pakbus_link is not None:
        return
    try:
        from pycampbellcr1000 import CR1000 as cr1000_class
        from pycampbellcr1000.exceptions import (
            DeliveryFailureException,
            NoDeviceException,
        )
        from biochar_app.pakbus.core.link import (
            install_url_override,
            open_pakbus_link as link_opener,
        )
    except (ImportError, AttributeError) as exc:
        raise RuntimeError(
            "PakBus downloads require the optional legacy PyCampbellCR1000/PyLink "
            "transport. TCP preflight remains available without it."
        ) from exc

    install_url_override()
    CR1000 = cr1000_class
    open_pakbus_link = link_opener
    _legacy_retryable_exceptions = (
        DeliveryFailureException,
        NoDeviceException,
    )

# CR800 (router) PakBus ID; default to 1 if not present in config
ROUTER_ID = PAKBUS.router_id

# ----------------------------------------------------------------------------
# Logging (DEBUG as requested)
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("pycampbellcr1000").setLevel(logging.DEBUG)

# ----------------------------------------------------------------------------
# Reachability helpers
# ----------------------------------------------------------------------------
def ping6(host: str) -> bool:
    """
    ICMPv6 probe (macOS: ping6 or ping -6). Returns True on any reply.
    Advisory only; many networks rate-limit ICMPv6.
    """
    logging.info(f"Pinging IPv6 host [{host}]...")
    if shutil.which("ping6"):
        cmd = ["ping6", "-c", "3", "-W", "2000", host]
    elif shutil.which("ping"):
        cmd = ["ping", "-6", "-c", "3", "-W", "2", host]
    else:
        logging.warning("No ping utility available; skipping ICMPv6 check.")
        return True

    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info("ICMPv6 reachable.")
        return True
    except subprocess.CalledProcessError:
        logging.error("ICMPv6 ping failed (no reply).")
        return False

def quick_port_check_ipv6(host: str, port: int, timeout: float = 3.0) -> tuple[bool, str]:
    """
    Small TCP handshake to verify the PakBus/TCP port is listening.
    """
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port, 0, 0))
        return True, "ok"
    except ConnectionRefusedError as e:
        return False, f"refused: {e}"
    except TimeoutError as e:
        return False, f"timeout: {e}"
    except OSError as e:
        return False, f"oserror: {e}"

# ----------------------------------------------------------------------------
# Data helpers
# ----------------------------------------------------------------------------
TABLE1_NUMBER = 2
TABLE1_SIGNATURE = 0x2C79
TABLE1_INTERVAL_MINUTES = 15
TABLE1_FIELDS = (
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
)
TABLE1_RECORD = struct.Struct(">I10f")
CAMPBELL_EPOCH = datetime(1990, 1, 1)
LOGGER_STANDARD_TIME = ZoneInfo("Etc/GMT+7")  # MST year-round (UTC-07:00)


def _compute_window(hours: int, tz_name: str) -> tuple[datetime, datetime]:
    tz = tz_name if isinstance(tz_name, ZoneInfo) else ZoneInfo(tz_name)
    now = datetime.now(tz)
    stop = now - timedelta(minutes=DEFAULT_LAG_MINUTES)
    start = stop - timedelta(hours=hours)
    if start >= stop:
        raise ValueError(f"Bad time window: start {start} >= stop {stop}")
    return start, stop

def decode_table1_response(raw: bytes) -> pd.DataFrame:
    """Decode CR206 Table1 fragments without downloading the live TDF."""
    import pandas as pd

    if len(raw) < 9:
        raise ValueError("Table1 response is too short")

    table_number, first_record, record_count = struct.unpack_from(">HIH", raw)
    if table_number != TABLE1_NUMBER:
        raise ValueError(
            f"Expected Table1 number {TABLE1_NUMBER}, received {table_number}"
        )

    expected_size = 8 + record_count * TABLE1_RECORD.size + 1
    if len(raw) < expected_size:
        raise ValueError(
            f"Incomplete Table1 response: expected {expected_size} bytes, "
            f"received {len(raw)}"
        )

    rows = []
    offset = 8
    for index in range(record_count):
        seconds, *values = TABLE1_RECORD.unpack_from(raw, offset)
        offset += TABLE1_RECORD.size
        logger_time = (CAMPBELL_EPOCH + timedelta(seconds=seconds)).replace(
            tzinfo=LOGGER_STANDARD_TIME
        )
        rows.append(
            {
                "Datetime": logger_time,
                "RecNbr": first_record + index,
                **dict(zip(TABLE1_FIELDS, values, strict=True)),
            }
        )
    frame = pd.DataFrame(rows)
    frame.attrs["first_record"] = first_record
    frame.attrs["record_count"] = record_count
    frame.attrs["more"] = bool(raw[expected_size - 1])
    return frame


def _fetch_window(
    dev: CR1000, table: str, start: datetime, stop: datetime
) -> Iterator[pd.DataFrame]:
    import pandas as pd

    if table != "Table1":
        raise ValueError("The fixed CR206 downloader currently supports Table1 only")

    requested_minutes = max(1, int((stop - start).total_seconds() // 60))
    # Include a small boundary cushion, but do not cap multi-day requests at
    # 96 records (24 hours at the normal 15-minute interval).
    record_count = requested_minutes // TABLE1_INTERVAL_MINUTES + 8
    frames: list[pd.DataFrame] = []
    # CR200/CR206 firmware uses 0x05 for the most-recent-N request. Mode 0x04
    # starts at a record number and therefore returns the oldest retained rows
    # when given a small count such as 96.
    mode = 0x05
    p1 = record_count
    p2 = 0
    next_record: int | None = None
    final_record: int | None = None
    for _fragment_number in range(100):
        command = dev.pakbus.get_collectdata_cmd(
            TABLE1_NUMBER,
            TABLE1_SIGNATURE,
            mode=mode,
            p1=p1,
            p2=p2,
        )
        try:
            result = dev.send_wait(command)
        except TypeError as exc:
            # pycampbellcr1000 currently subscripts a missing response and
            # exposes it as a cryptic ``NoneType`` error. Translate that into
            # the communication failure that actually occurred.
            if "NoneType" not in str(exc):
                raise
            raise TimeoutError("logger did not return a Table1 response") from exc
        if result is None:
            raise TimeoutError("logger did not return a Table1 response")
        _header, message, _send_time = result
        response_code = int(message.get("RespCode", 0))
        if response_code:
            raise RuntimeError(
                f"Table1 collection failed with response code {response_code}"
            )

        try:
            record_data = message["RecData"]
        except KeyError as exc:
            raise TimeoutError(
                "logger response did not contain Table1 record data"
            ) from exc
        fragment = decode_table1_response(record_data)
        if fragment.empty:
            break
        frames.append(fragment)

        if final_record is None:
            # The end record used by mode 0x06 is exclusive.
            final_record = int(fragment["RecNbr"].min()) + record_count

        fragment_next_record = int(fragment["RecNbr"].max()) + 1
        if not fragment.attrs["more"] or fragment_next_record >= final_record:
            break
        if fragment_next_record == next_record:
            raise RuntimeError("Table1 continuation did not advance the record number")
        next_record = fragment_next_record
        # CR206 mode 0x05 does not reliably continue after a MostRecent
        # response. Use the explicit inclusive record-number range instead.
        mode = 0x06
        p1 = fragment_next_record
        p2 = final_record
    else:
        raise RuntimeError("Table1 collection exceeded 100 response fragments")

    if not frames:
        return
    frame = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset="RecNbr", keep="last")
        .sort_values("RecNbr")
        .tail(record_count)
        .reset_index(drop=True)
    )
    timestamps = frame["Datetime"].map(lambda value: value.astimezone(start.tzinfo))
    selected = frame.loc[(timestamps >= start) & (timestamps <= stop)].copy()
    if selected.empty and not frame.empty:
        latest = frame["Datetime"].max()
        logging.warning(
            "Logger returned Table1 records, but none were in the requested "
            "window; latest stored record is %s", latest.isoformat()
        )
        return
    if not selected.empty:
        yield selected.sort_values("Datetime").reset_index(drop=True)

# ----------------------------------------------------------------------------
# Batch fetch using one socket
# ----------------------------------------------------------------------------
def resolve_logger_ids(stations: Iterable[str] | None) -> list[int]:
    """Resolve station names such as ``S1T`` to configured PakBus IDs."""
    if stations is None:
        return list(PAKBUS.logger_ids)

    logger_ids: list[int] = []
    for station in stations:
        name = station.strip().upper()
        if not name:
            continue
        try:
            logger_id = ID_BY_STATION[name]
        except KeyError as exc:
            choices = ", ".join(ID_BY_STATION)
            raise ValueError(f"Unknown station {station!r}; choose from {choices}") from exc
        if logger_id == ROUTER_ID:
            raise ValueError("CR800 is the router, not a leaf logger")
        if logger_id not in logger_ids:
            logger_ids.append(logger_id)
    if not logger_ids:
        raise ValueError("At least one leaf logger station is required")
    return logger_ids


def fetch_batch(
    table: str,
    hours: int,
    tz_name: str | ZoneInfo,
    logger_ids: Iterable[int] | None = None,
    station_attempts: int = DEFAULT_STATION_ATTEMPTS,
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
) -> Iterator[tuple[int, pd.DataFrame]]:
    """
    Walk the logger IDs using an isolated IPv6/TCP connection per attempt.
    Yields (logger_id, DataFrame) pages.
    """
    _load_legacy_transport()
    host = PAKBUS.host
    port = PAKBUS.port
    base_id = PAKBUS.base_id

    # Hard gate: TCP preflight
    ok, why = quick_port_check_ipv6(host, port)
    if not ok:
        logging.error(f"TCP check failed for [{host}]:{port} → {why}")
        raise SystemExit(1)

    # Advisory: ping6
    if not ping6(host):
        logging.warning("ICMPv6 ping had no reply; proceeding since TCP is reachable.")

    start, stop = _compute_window(hours, tz_name)
    logging.info(f"Fetching window {start.isoformat()} → {stop.isoformat()} (table={table})")

    if station_attempts < 1:
        raise ValueError("station_attempts must be at least 1")

    for dest_id in logger_ids or PAKBUS.logger_ids:
        station = STATION_BY_ID.get(dest_id, f"PakBus {dest_id}")
        for attempt in range(1, station_attempts + 1):
            try:
                # A new socket on every attempt discards late packets from a
                # previous transaction before another logger is contacted.
                with open_pakbus_link(host, port) as link:
                    # Register this fresh client connection with the physical
                    # CR800 router before addressing a logical leaf logger.
                    # Without this handshake, the router answers the leaf
                    # hello request with a 0x09 route/session failure.
                    # Keep the router session alive while the leaf session is
                    # active. PyCampbell sends Bye when a CR1000 instance is
                    # destroyed, so constructing it without retaining a
                    # reference closes the route before the leaf hello.
                    _router_session = CR1000(
                        link,
                        dest_addr=ROUTER_ID,
                        dest=ROUTER_ID,
                        src_addr=base_id,
                        src=base_id,
                    )
                    # Construct inside try so NoDeviceException does not crash
                    # the run. The TCP endpoint is the CR800 router (physical
                    # address 1); the CR2xx leaf is the logical destination.
                    dev = CR1000(
                        link,
                        dest_addr=ROUTER_ID,
                        dest=dest_id,
                        src_addr=base_id,
                        src=base_id,
                    )

                    # Optional: best-effort clock read.
                    try:
                        clk = dev.gettime()
                        logging.debug(f"Logger {dest_id} clock: {clk}")
                    except Exception:
                        pass

                    pages = list(_fetch_window(dev, table, start, stop))
                    for page in pages:
                        yield dest_id, page
                    if attempt > 1:
                        logging.info(
                            "%s (logger %s) succeeded on attempt %s/%s",
                            station, dest_id, attempt, station_attempts,
                        )
                break
            except (
                TimeoutError,
                TypeError,
                ConnectionError,
                OSError,
                RuntimeError,
            ) as exc:
                if isinstance(exc, TypeError) and "NoneType" not in str(exc):
                    logging.exception(
                        "%s (logger %s) failed with a non-retryable error: %s",
                        station, dest_id, exc,
                    )
                    break
                reason = (
                    "logger did not return a PakBus response"
                    if isinstance(exc, TypeError)
                    else str(exc)
                )
                if attempt == station_attempts:
                    logging.error(
                        "%s (logger %s) did not respond after %s attempts: %s. Skipping.",
                        station, dest_id, station_attempts, reason,
                    )
                    break
                logging.warning(
                    "%s (logger %s) attempt %s/%s failed: %s; retrying in %.1f seconds.",
                    station, dest_id, attempt, station_attempts, reason,
                    retry_delay_seconds,
                )
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
            except Exception as exc:
                if isinstance(exc, _legacy_retryable_exceptions):
                    if attempt == station_attempts:
                        logging.error(
                            "%s (logger %s) did not respond after %s attempts: %s. Skipping.",
                            station, dest_id, station_attempts, exc,
                        )
                        break
                    logging.warning(
                        "%s (logger %s) attempt %s/%s failed: %s; retrying in %.1f seconds.",
                        station, dest_id, attempt, station_attempts, exc,
                        retry_delay_seconds,
                    )
                    if retry_delay_seconds > 0:
                        time.sleep(retry_delay_seconds)
                    continue
                logging.exception(
                    "%s (logger %s) failed with a non-retryable error: %s",
                    station, dest_id, exc,
                )
                break


def fetch_isolated_stations(
    station_names: Iterable[str],
    *,
    table: str,
    hours: int,
    timezone: str,
    attempts: int,
    station_pause_seconds: float,
) -> list[dict]:
    """Fetch each station in a new Python interpreter and combine its rows."""
    import pandas as pd

    stations = list(station_names)
    if station_pause_seconds < 0:
        raise ValueError("station_pause_seconds cannot be negative")

    combined_rows: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="pakbus-stations-") as temp_dir:
        for index, station in enumerate(stations):
            station_output = Path(temp_dir) / f"{station}.csv"
            command = [
                sys.executable,
                "-m",
                "biochar_app.pakbus.core.client",
                "--stations",
                station,
                "--table",
                table,
                "--hours",
                str(hours),
                "--timezone",
                timezone,
                "--attempts",
                str(attempts),
                "--output",
                str(station_output),
                "--direct",
            ]
            logging.info(
                "Starting isolated download for %s (%s of %s)",
                station, index + 1, len(stations),
            )
            result = subprocess.run(command, check=False)
            if result.returncode != 0:
                logging.error(
                    "Isolated download for %s exited with status %s; continuing.",
                    station, result.returncode,
                )
            elif not station_output.exists():
                logging.error(
                    "Isolated download for %s returned no records; continuing.", station
                )
            else:
                combined_rows.extend(
                    pd.read_csv(station_output).to_dict(orient="records")
                )

            if index < len(stations) - 1 and station_pause_seconds > 0:
                logging.info(
                    "Waiting %.1f seconds before the next station.",
                    station_pause_seconds,
                )
                time.sleep(station_pause_seconds)
    return combined_rows

# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch historical data from Campbell dataloggers via CR800 router"
    )
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Table to fetch")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Hours back")
    parser.add_argument(
        "--timezone",
        default=str(DEFAULT_TIMEZONE),
        help="IANA timezone (e.g., America/Denver)",
    )
    parser.add_argument(
        "--stations",
        nargs="+",
        metavar="STATION",
        help="Leaf stations to query (for example: S2M S4B). Defaults to all 12.",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Check the CR800 TCP endpoint without sending PakBus requests.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional CSV destination. Without this option, records are printed only.",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=DEFAULT_STATION_ATTEMPTS,
        help="Maximum attempts per logger (default: %(default)s).",
    )
    parser.add_argument(
        "--station-pause",
        type=float,
        default=DEFAULT_STATION_PAUSE_SECONDS,
        help=(
            "Seconds between isolated station processes when querying multiple "
            "stations (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    ok, why = quick_port_check_ipv6(PAKBUS.host, PAKBUS.port)
    if args.preflight_only:
        if ok:
            print(f"PakBus TCP endpoint reachable at [{PAKBUS.host}]:{PAKBUS.port}")
            return
        raise SystemExit(
            f"PakBus TCP endpoint unavailable at [{PAKBUS.host}]:{PAKBUS.port}: {why}"
        )

    logger_ids = resolve_logger_ids(args.stations)
    import pandas as pd

    # Do not leave a successful file from an earlier diagnostic looking like
    # the result of a later run that returned no rows.
    if args.output is not None:
        args.output.unlink(missing_ok=True)

    if len(logger_ids) > 1 and not args.direct:
        station_names = [STATION_BY_ID[logger_id] for logger_id in logger_ids]
        output_rows = fetch_isolated_stations(
            station_names,
            table=args.table,
            hours=args.hours,
            timezone=args.timezone,
            attempts=args.attempts,
            station_pause_seconds=args.station_pause,
        )
        if args.output is not None and output_rows:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(output_rows).to_csv(args.output, index=False)
            print(f"Wrote {len(output_rows)} records to {args.output}")
        elif args.output is None:
            for record in output_rows:
                print(record)
        if not output_rows:
            raise SystemExit("No data returned from any requested logger.")
        return

    output_rows: list[dict] = []

    any_output = False
    for logger_id, df in fetch_batch(
        table=args.table,
        hours=args.hours,
        tz_name=args.timezone,
        station_attempts=args.attempts,
        logger_ids=logger_ids,
    ):
        logging.info(f"Received page from logger {logger_id}: {len(df)} rows")
        for row in df.to_dict(orient="records"):
            record = {
                "station": STATION_BY_ID.get(logger_id, f"logger_{logger_id}"),
                "logger_id": logger_id,
                **row,
            }
            output_rows.append(record)
            if args.output is None:
                print(record)
            any_output = True

    if args.output is not None and output_rows:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(output_rows).to_csv(args.output, index=False)
        print(f"Wrote {len(output_rows)} records to {args.output}")

    if not any_output:
        raise SystemExit("No data returned from any requested logger.")

if __name__ == "__main__":
    main()

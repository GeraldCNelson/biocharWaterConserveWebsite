#!/usr/bin/env python3
"""
client.py — Fetch recent records from Campbell loggers via PakBus/TCP over IPv6.

Key points:
- DEBUG logging enabled.
- Reuses a single IPv6 TCP socket to the CR800 for the whole batch.
- Pulls host/port/base_id/logger_ids from config.PAKBUS.
- Handles NoDeviceException when instantiating CR1000 per logger ID (won’t crash).
- ICMPv6 ping is advisory; TCP preflight is the hard gate.
"""

from __future__ import annotations

import logging
import shutil
import socket
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterator, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from pycampbellcr1000 import CR1000
from pycampbellcr1000.exceptions import NoDeviceException

from biochar_app.scripts.config import (
    PAKBUS,
    DEFAULT_HOURS,
    DEFAULT_TABLE,
    DEFAULT_LAG_MINUTES,
)
# Use the shared IPv6 link + URL override implementation
from biochar_app.pakbus.link import install_url_override, open_pakbus_link

# Install pakbus://[IPv6]:port URL override once per process
install_url_override()

# CR800 (router) PakBus ID; default to 1 if not present in config
ROUTER_ID = getattr(PAKBUS, "router_id", 1)

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
def _compute_window(hours: int, tz_name: str) -> tuple[datetime, datetime]:
    tz = tz_name if isinstance(tz_name, ZoneInfo) else ZoneInfo(tz_name)
    now = datetime.now(tz)
    stop = now - timedelta(minutes=DEFAULT_LAG_MINUTES)
    start = stop - timedelta(hours=hours)
    if start >= stop:
        raise ValueError(f"Bad time window: start {start} >= stop {stop}")
    return start, stop

def _fetch_window(dev: CR1000, table: str, start: datetime, stop: datetime) -> Iterator[pd.DataFrame]:
    for page in dev.get_data_generator(table, start, stop):
        yield page

# ----------------------------------------------------------------------------
# Batch fetch using one socket
# ----------------------------------------------------------------------------
def fetch_batch(table: str, hours: int, tz_name: str) -> Iterator[tuple[int, pd.DataFrame]]:
    """
    Keep one IPv6/TCP socket to the CR800 open and walk all logger IDs.
    Yields (logger_id, DataFrame) pages.
    """
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

    # Single shared socket to the CR800 for the whole batch
    with open_pakbus_link(host, port) as link:
        # Wake up router once (optional)
        try:
            router = CR1000(link, dest_addr=ROUTER_ID, src_addr=base_id)
            try:
                router.pakbus.get_attention()
            except Exception:
                pass
        except Exception as exc:
            logging.warning(f"Router attention failed: {exc!r} — continuing.")

        for dest_id in PAKBUS.logger_ids:
            try:
                # Construct inside try so NoDeviceException doesn’t crash the run
                dev = CR1000(link, dest_addr=dest_id, src_addr=base_id)

                # Optional: best-effort to read clock
                try:
                    clk = dev.gettime()
                    logging.debug(f"Logger {dest_id} clock: {clk}")
                except Exception:
                    pass

                for page in _fetch_window(dev, table, start, stop):
                    yield dest_id, page

            except NoDeviceException:
                logging.warning(f"Logger {dest_id}: NoDeviceException (no route/response). Skipping.")
                continue
            except Exception as e:
                logging.exception(f"Logger {dest_id}: error while fetching: {e}")
                continue

# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch historical data from Campbell dataloggers via CR800 router"
    )
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Table to fetch")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Hours back")
    parser.add_argument(
        "--timezone",
        default=str(ZoneInfo("UTC")),
        help="IANA timezone (e.g., America/Denver)",
    )
    args = parser.parse_args()

    any_output = False
    for logger_id, df in fetch_batch(table=args.table, hours=args.hours, tz_name=args.timezone):
        logging.info(f"Received page from logger {logger_id}: {len(df)} rows")
        for row in df.to_dict(orient="records"):
            print({"logger_id": logger_id, **row})
            any_output = True

    if not any_output:
        logging.warning("No data returned from any logger.")

if __name__ == "__main__":
    main()
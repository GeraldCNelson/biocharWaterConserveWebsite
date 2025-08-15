#!/usr/bin/env python3
"""
client.py — Fetch recent records from Campbell loggers via PakBus/TCP over IPv6.

Changes vs your previous version:
- No global DNS monkey-patching (safer for the process).
- Clean IPv6 TCPLink that handles pakbus://[IPv6]:port URLs.
- Fixed regex (no redundant escapes).
- Robust ping6 helper (macOS/Linux) + explicit TCP preflight.
- Clear errors if the port is closed/refused.
"""

from __future__ import annotations

import re
import socket
import logging
import shutil
import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Iterator, Optional

import pandas as pd
import pylink
from pylink import TCPLink as _OrigTCPLink
import pycampbellcr1000.device as device_mod
from pycampbellcr1000 import CR1000

from biochar_app.scripts.config import (
    PAKBUS,
    DEFAULT_HOURS,
    DEFAULT_TABLE,
    DEFAULT_LAG_MINUTES,
)

# ----------------------------------------------------------------------------
# IPv6 TCP link that understands pakbus://[IPv6]:port (no global monkeypatch)
# ----------------------------------------------------------------------------
class IPv6TCPLink(_OrigTCPLink):
    """
    TCPLink that connects to an IPv6 literal without triggering gethostbyname().
    """
    def __init__(self, host: str, port: int, timeout: float | None = None):
        # Always set _socket early so __del__ is safe
        self._socket: Optional[socket.socket] = None
        self._is_ipv6_literal = ":" in host
        self._v6_addr: Optional[tuple[str, int, int, int]] = None

        if self._is_ipv6_literal:
            # Bypass parent's __init__, set essentials manually
            self.host = host
            self.port = port
            self.timeout = timeout or 10.0
            self._v6_addr = (host, port, 0, 0)
        else:
            # For hostnames or IPv4, do the normal init
            super().__init__(host, port, timeout)

    def open(self):
        if self._is_ipv6_literal and self._v6_addr is not None:
            logging.info(f"Opening IPv6 socket to {self._v6_addr!r}")
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect(self._v6_addr)
            self._socket = s
            return self
        # Non-IPv6: let original logic handle it
        return super().open()

    def close(self):
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None
        else:
            try:
                super().close()
            except Exception:
                pass


# ----------------------------------------------------------------------------
# URL override: pakbus://[IPv6]:port  →  IPv6TCPLink
# ----------------------------------------------------------------------------
_original_link_from_url = pylink.link_from_url

def _link_from_url_override(url: str):
    # Use plain ']' outside the class; inside [^]] it must remain unescaped.
    m = re.match(r'(?i)^\s*pakbus://\[(?P<host>[^]]+)]:(?P<port>\d+)\s*$', url)
    if m:
        host, port = m.group("host"), int(m.group("port"))
        logging.info(f"PakBus IPv6 override: host={host}, port={port}")
        return IPv6TCPLink(host, port)
    return _original_link_from_url(url)

# Apply override for both pylink and pycampbellcr1000
pylink.link_from_url = device_mod.link_from_url = _link_from_url_override

# ----------------------------------------------------------------------------
# Logging configuration
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("pylink").setLevel(logging.INFO)
logging.getLogger("pycampbellcr1000").setLevel(logging.DEBUG)

# ----------------------------------------------------------------------------
# Reachability helpers
# ----------------------------------------------------------------------------
def ping6(host: str) -> bool:
    """
    ICMPv6 probe (macOS: ping6 or ping -6). Returns True on any reply.
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
# Data fetching
# ----------------------------------------------------------------------------
def fetch_all_loggers(
    base_url: str,
    table: str,
    hours: int,
    tz_name: str,
    dest_addr: int,
    src_addr: int,
) -> Iterator[pd.DataFrame]:
    """
    Yield pages (DataFrame) from CR1000.get_data_generator(table, start, stop).
    Uses TCP preflight as the hard gate; ICMPv6 is advisory only.
    """
    host = PAKBUS.host
    port = PAKBUS.port

    # --- Hard gate: TCP preflight (what we actually need) ---
    ok, why = quick_port_check_ipv6(host, port)
    if not ok:
        logging.error(
            f"TCP check failed for [{host}]:{port} → {why}. "
            "If this used to work, verify the CR800/NL module is listening on this port, "
            "and that the IPv6/route hasn’t changed."
        )
        raise SystemExit(1)

    # Advisory only: ICMPv6 often rate-limited; never hard-fail on ping
    if not ping6(host):
        logging.warning("ICMPv6 ping had no reply; proceeding since TCP is reachable.")

    # Time window
    tz = tz_name if isinstance(tz_name, ZoneInfo) else ZoneInfo(tz_name)
    now = datetime.now(tz)
    stop = now - timedelta(minutes=DEFAULT_LAG_MINUTES)
    start = stop - timedelta(hours=hours)
    if start >= stop:
        logging.error(f"Bad time window: start {start} >= stop {stop}")
        raise SystemExit(1)
    logging.info(f"Fetching from {start.isoformat()} to {stop.isoformat()}")

    # Open device/link (our URL override handles IPv6 literal)
    device = CR1000.from_url(
        base_url,
        dest_addr=dest_addr,
        src_addr=src_addr,
    )

    device.link.open()
    try:
        try:
            now_logger = device.gettime()
            logging.info(f"Logger clock: {now_logger}")
        except Exception as exc:
            logging.warning(f"Could not read logger clock: {exc!r}")

        for idx, page in enumerate(device.get_data_generator(table, start, stop), start=1):
            logging.info(f"Received page {idx}: {len(page)} rows")
            yield page
    finally:
        # Always close the link so later runs don't inherit stale sockets
        try:
            device.link.close()
        except Exception:
            pass

# ----------------------------------------------------------------------------
# CLI entrypoint
# ----------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch historical data from Campbell CR1000 dataloggers",
    )
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Table to fetch")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Hours back")
    parser.add_argument(
        "--timezone", default=str(ZoneInfo("UTC")), help="IANA timezone (e.g., America/Denver)"
    )
    args = parser.parse_args()

    base_url = f"pakbus://[{PAKBUS.host}]:{PAKBUS.port}"

    any_output = False
    for logger_id in PAKBUS.logger_ids:
        logging.info(f"Fetching logger {logger_id}")
        for record in fetch_all_loggers(
            base_url=base_url,
            table=args.table,
            hours=args.hours,
            tz_name=args.timezone,
            dest_addr=logger_id,
            src_addr=PAKBUS.base_id,
        ):
            for row in record.to_dict(orient="records"):
                print({"logger_id": logger_id, **row})
                any_output = True

    if not any_output:
        logging.warning("No data returned from any logger.")

if __name__ == "__main__":
    main()
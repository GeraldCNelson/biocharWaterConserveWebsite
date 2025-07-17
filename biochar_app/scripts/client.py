import re
import socket
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Iterator, Optional

import pandas as pd
import pylink
from pylink import TCPLink as _OrigTCPLink
import pycampbellcr1000.device as device_mod
from pycampbellcr1000 import CR1000
from biochar_app.scripts.config import PAKBUS, DEFAULT_HOURS, DEFAULT_TABLE, DEFAULT_LAG_MINUTES

# ----------------------------------------------------------------------------
# IPv6 TCP link override to support pakbus://[IPv6]:port
# ----------------------------------------------------------------------------
class IPv6TCPLink(_OrigTCPLink):
    # declare attributes for IDE/static analysis
    _address: tuple[str, int, int, int]
    _socket: Optional[socket.socket]

    def __init__(self, host: str, port: int, timeout: float = None):
        # bypass DNS resolution for IPv6 literals
        _orig_gethost = socket.gethostbyname
        socket.gethostbyname = lambda name: name if ':' in name else _orig_gethost(name)
        socket.getaddrinfo = lambda h, p, *args, **kwargs: [
            (socket.AF_INET6, socket.SOCK_STREAM, 0, '', (h, p, 0, 0))
        ]
        try:
            super().__init__(host, port, timeout)
        finally:
            # restore
            socket.gethostbyname = _orig_gethost
            socket.getaddrinfo = socket.getaddrinfo
        self._address = (host, port, 0, 0)
        self._socket = None

    def open(self):
        logging.info(f"Opening IPv6 socket to {self._address}")
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._address)
        self._socket = sock
        return self

    def close(self):
        if self._socket:
            logging.debug("Closing IPv6 socket")
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

# apply override
_original_link_from_url = pylink.link_from_url
pylink.link_from_url = device_mod.link_from_url = lambda url: _link_from_url_override(url)

def _link_from_url_override(url: str) -> IPv6TCPLink:
    """
    Override pylink.link_from_url to match pakbus://[IPv6_literal]:port
    """
    m = re.match(r'(?i)^pakbus://\[(?P<host>[^]]+)\]:(?P<port>\d+)', url)
    if m:
        host = m.group('host')
        port = int(m.group('port'))
        logging.info(f"PakBus override: host={host}, port={port}")
        return IPv6TCPLink(host, port)
    return _original_link_from_url(url)

# ----------------------------------------------------------------------------
# Logging configuration
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logging.getLogger('pylink').setLevel(logging.INFO)
logging.getLogger('pycampbellcr1000').setLevel(logging.DEBUG)

# ----------------------------------------------------------------------------
# Data fetching
# ----------------------------------------------------------------------------
def fetch_all_loggers(
    base_url: str,
    table: str,
    hours: int,
    tz_name: str,
    dest_addr: int,
    src_addr: int
) -> Iterator[pd.DataFrame]:
    tz = tz_name if isinstance(tz_name, ZoneInfo) else ZoneInfo(tz_name)
    now = datetime.now(tz)
    stop = now - timedelta(minutes=DEFAULT_LAG_MINUTES)
    start = stop - timedelta(hours=hours)
    logging.info(f"Fetching from {start.isoformat()} to {stop.isoformat()}")

    device = CR1000.from_url(
        base_url,
        dest_addr=dest_addr,
        src_addr=src_addr
    )
    device.link.open()
    now = device.gettime()
    logging.info(f"Logger clock: {now}")

    idx = 0
    for page in device.get_data_generator(table, start, stop):
        idx += 1
        logging.info(f"Received page {idx}: {len(page)} rows")
        yield page

# ----------------------------------------------------------------------------
# CLI entrypoint
# ----------------------------------------------------------------------------
def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch historical data from Campbell CR1000 dataloggers"
    )
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Table to fetch")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Hours back to fetch")
    parser.add_argument("--timezone", default=str(ZoneInfo('UTC')), help="IANA timezone")
    args = parser.parse_args()

    base_url = f"pakbus://[{PAKBUS.host}]:{PAKBUS.port}"
    any_output = False
    for logger_id in PAKBUS.logger_ids:
        logging.info(f"Fetching logger {logger_id}")
        for page in fetch_all_loggers(
            base_url,
            args.table,
            args.hours,
            args.timezone,
            dest_addr=logger_id,
            src_addr=PAKBUS.base_id
        ):
            for record in page.to_dict(orient='records'):
                print({"logger_id": logger_id, **record})
                any_output = True
    if not any_output:
        logging.warning("No data returned from any logger.")

if __name__ == '__main__':
    main()

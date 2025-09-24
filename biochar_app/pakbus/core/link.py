# pakbus/link.py
"""
IPv6 PakBus TCP link + URL override for pylink/pycampbellcr1000.

Usage
-----
from pakbus.link import install_url_override, pakbus_url, open_pakbus_link
from pycampbellcr1000 import CR1000

install_url_override()  # once at process start (e.g., in client main)

base = pakbus_url(PAKBUS.host, PAKBUS.port)
dev = CR1000.from_url(base, dest_addr=2, src_addr=PAKBUS.base_id)

# Or if you want an explicit socket you control:
from pakbus.link import open_pakbus_link
with open_pakbus_link(PAKBUS.host, PAKBUS.port) as link:
    dev = CR1000(link, dest_addr=2, src_addr=PAKBUS.base_id)
    # ... use dev ...
"""

from __future__ import annotations

import logging
import re
import socket
from contextlib import contextmanager
from typing import Optional, Iterator

import pylink
from pylink import TCPLink as _OrigTCPLink
import pycampbellcr1000.device as device_mod

__all__ = [
    "IPv6TCPLink",
    "install_url_override",
    "pakbus_url",
    "open_pakbus_link",
]

# -----------------------------------------------------------------------------
# IPv6-capable TCP link
# -----------------------------------------------------------------------------
class IPv6TCPLink(_OrigTCPLink):
    """
    A TCPLink that can connect directly to an IPv6 literal without DNS.
    - Idempotent open(): safe to call repeatedly.
    - Optional TCP keepalive for long/latent links (e.g., satellite/NAT).
    """

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        tcp_keepalive: bool = True,
    ):
        # Parent's __init__ tries IPv4 resolution; we bypass when it's a v6 literal.
        self._socket: Optional[socket.socket] = None
        self._is_ipv6_literal = ":" in host
        self._v6_addr: Optional[tuple[str, int, int, int]] = None
        self._keepalive = tcp_keepalive
        self.timeout = timeout or 10.0

        if self._is_ipv6_literal:
            self.host = host
            self.port = port
            self._v6_addr = (host, port, 0, 0)
        else:
            super().__init__(host, port, timeout)

    def open(self):
        # Idempotent: if already open, return self
        if self._socket is not None:
            return self

        if self._is_ipv6_literal and self._v6_addr is not None:
            logging.debug(f"Opening IPv6 socket to {self._v6_addr!r}")
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            if self._keepalive:
                try:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except OSError:
                    pass
            s.connect(self._v6_addr)
            self._socket = s
            return self

        # Fall back to stock behavior for hostnames/IPv4
        logging.debug(f"Opening default pylink TCP socket to {getattr(self, 'address', (self.host, self.port))!r}")
        return super().open()

    def close(self):
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
        else:
            # Let pylink close its own socket if it created one
            try:
                super().close()
            except Exception:
                pass

# -----------------------------------------------------------------------------
# URL override: pakbus://[IPv6]:port  →  IPv6TCPLink
# -----------------------------------------------------------------------------
_original_link_from_url = pylink.link_from_url

_IPV6_PAKBUS_RE = re.compile(
    r'(?i)^\s*pakbus://\[(?P<host>[^]]+)]:(?P<port>\d+)\s*$'
)

def _link_from_url_override(url: str):
    m = _IPV6_PAKBUS_RE.match(url)
    if m:
        host, port = m.group("host"), int(m.group("port"))
        logging.info(f"PakBus IPv6 override active → host={host}, port={port}")
        return IPv6TCPLink(host, port)
    # Otherwise, defer to original (hostnames/IPv4, serial, etc.)
    return _original_link_from_url(url)

def install_url_override() -> None:
    """
    Install the IPv6 URL hook into both pylink and pycampbellcr1000.
    Call this once at startup.
    """
    pylink.link_from_url = _link_from_url_override
    device_mod.link_from_url = _link_from_url_override

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def pakbus_url(host: str, port: int) -> str:
    """Build a pakbus URL with an IPv6 literal."""
    return f"pakbus://[{host}]:{port}"

@contextmanager
def open_pakbus_link(
    host: str,
    port: int,
    connect_timeout: float = 10.0,
    tcp_keepalive: bool = True,
) -> Iterator[IPv6TCPLink]:
    """
    Context manager that opens/closes a single IPv6 PakBus TCP link.
    """
    link = IPv6TCPLink(host, port, timeout=connect_timeout, tcp_keepalive=tcp_keepalive)
    link.open()
    try:
        yield link
    finally:
        try:
            link.close()
        except Exception:
            pass
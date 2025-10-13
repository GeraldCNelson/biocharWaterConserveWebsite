# pakbus/link.py
"""
IPv6 PakBus TCP link + URL override for pylink/pycampbellcr1000.

Usage
-----
from pakbus.link import install_url_override, pakbus_url, open_pakbus_link
from pycampbellcr1000 import CR1000

install_url_override()  # once at process start
base = pakbus_url(PAKBUS.host, PAKBUS.port)
# CR1000.from_url uses our override under the hood:
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
import urllib.parse
from contextlib import contextmanager
from typing import Optional, Iterator

import pylink
from pylink.link import SerialLink, UDPLink, TCPLink

def link_from_url(url: str):
    """
    Parse a URL into the appropriate pylink.Link class:
      - tcp://host:port    → TCPLink(host, port)
      - udp://host:port    → UDPLink(host, port)
      - serial:///dev/...  → SerialLink(device)
    """
    p = urllib.parse.urlparse(url)
    scheme = p.scheme.lower()
    if scheme == "tcp":
        # Default PakBus TCP port is 6785
        return TCPLink(p.hostname, p.port or 6785)
    elif scheme == "udp":
        # Default PakBus UDP port is 6785
        return UDPLink(p.hostname, p.port or 6785)
    elif scheme in ("serial", "file"):
        return SerialLink(p.path)
    else:
        raise ValueError(f"Unsupported link scheme: {p.scheme!r}")

# Fallback to our own URL factory
_pylink_link_from_url = link_from_url

__all__ = [
    "link_from_url",
    "IPv6TCPLink",
    "install_url_override",
    "pakbus_url",
    "open_pakbus_link",
]


class IPv6TCPLink(TCPLink):
    """
    A TCPLink subclass that can connect directly to an IPv6 literal without DNS.
    """

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        tcp_keepalive: bool = True,
    ):
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

        logging.debug(f"Opening default pylink TCP to {(self.host, self.port)!r}")
        return super().open()

    def close(self):
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
        else:
            try:
                super().close()
            except Exception:
                pass


# Regex for pakbus://[IPv6]:port
_IPV6_PAKBUS_RE = re.compile(
    r"(?i)^pakbus://\[(?P<host>[^]]+)]:(?P<port>\d+)$"
)

def _link_override(url: str):
    m = _IPV6_PAKBUS_RE.match(url.strip())
    if m:
        host, port = m.group("host"), int(m.group("port"))
        logging.info(f"PakBus IPv6 override → host={host}, port={port}")
        return IPv6TCPLink(host, port)
    return _pylink_link_from_url(url)


def install_url_override() -> None:
    """
    Override pylink and pycampbellcr1000 URL factory to support IPv6.
    Call this once at startup.
    """
    # Defer importing device_mod so link.py has finished loading
    import pycampbellcr1000.device as device_mod

    pylink.link_from_url              = _link_override
    device_mod.link_from_url          = _link_override


def pakbus_url(host: str, port: int) -> str:
    """Construct a pakbus:// URL for IPv6 hosts."""
    return f"pakbus://[{host}]:{port}"


@contextmanager
def open_pakbus_link(
    host: str,
    port: int,
    connect_timeout: float = 10.0,
    tcp_keepalive: bool = True,
) -> Iterator[IPv6TCPLink]:
    link = IPv6TCPLink(host, port, timeout=connect_timeout, tcp_keepalive=tcp_keepalive)
    link.open()
    try:
        yield link
    finally:
        try:
            link.close()
        except Exception:
            pass
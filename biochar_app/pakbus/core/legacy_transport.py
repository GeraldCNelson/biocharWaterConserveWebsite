"""Repository-owned lifecycle and link-control adapter for legacy PyCampbell.

The upstream decoder assumes an eight-byte application header. Link-state
packets have only four bytes after the signature is stripped. Session objects
must never perform network I/O from their destructors.
"""
import logging
import struct
import time

from pycampbellcr1000 import CR1000
from pycampbellcr1000.exceptions import NoDeviceException
from pycampbellcr1000.pakbus import PakBus


class ManagedPakBus(PakBus):
    def __del__(self):
        # The socket context, not garbage collection, owns the shared link.
        pass

    def read(self):
        deadline = time.monotonic() + self.link.timeout
        for _ in range(32):
            data = super().read()
            if not data:
                return data
            if len(data) != 4:
                if len(data) < 10:
                    raise ConnectionError("Incomplete PakBus application packet")
                return data
            destination, source = struct.unpack(">2H", data)
            state = destination >> 12
            if (destination & 0xFFF) != self.src_addr:
                continue
            if (source & 0xFFF) != self.dest_addr:
                continue
            if state in (0x8, self.FINISHED):
                raise ConnectionError(
                    f"PakBus peer {source & 0xFFF} ended the link "
                    f"(link state 0x{state:X}); reconnect required"
                )
            if state not in (self.RING, self.READY, 0xC):
                raise ConnectionError(f"Unknown PakBus link state 0x{state:X}")
            logging.debug("Received PakBus link-control state 0x%X", state)
            if state == self.RING:
                self.write(struct.pack(">2H", (self.READY << 12) | self.dest_addr,
                                       (0x2 << 14) | self.src_addr))
            if time.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for PakBus application packet")
        raise TimeoutError("Too many PakBus link-control packets without application data")


class ManagedCR1000(CR1000):
    def __init__(self, link, dest_addr=None, dest=0x001, src_addr=None,
                 src=0x802, security_code=0):
        self.connected = False
        link.open()
        self.pakbus = ManagedPakBus(link, dest_addr, dest, src_addr, src, security_code)
        self.pakbus.wait_packet()
        # Retry at the station level, re-registering the router each time.
        # Upstream's internal reconnect loop loses that registration.
        if not self.ping_node():
            raise NoDeviceException()
        self.connected = True

    def __del__(self):
        # Includes partially initialized objects: cleanup is owned by the caller.
        pass

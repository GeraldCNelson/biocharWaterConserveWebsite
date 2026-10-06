"""Offline transport regressions using the October 6 debug-log control frame."""
import gc
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("pycampbellcr1000")
from pycampbellcr1000.pakbus import PakBus
from biochar_app.pakbus.core.legacy_transport import ManagedPakBus, ManagedCR1000
from biochar_app.pakbus.core import client


class MemoryLink:
    timeout = 1

    def __init__(self, data=b""):
        self.data = bytearray(data)
        self.writes = []
        self.closes = 0

    def open(self):
        return self

    def write(self, data):
        self.writes.append(data)

    def read(self, size):
        result = bytes(self.data[:size])
        del self.data[:size]
        return result

    def close(self):
        self.closes += 1


def test_actual_checksum_valid_finished_frame_requests_reconnection():
    # Exact complete frame from download.log, including checksum and delimiters.
    link = MemoryLink(bytes.fromhex("BD BF FD 00 01 98 CF BD"))
    bus = ManagedPakBus(link, dest_addr=1, dest=2, src_addr=4093, src=4093)
    with pytest.raises(ConnectionError, match="ended the link.*0xB"):
        bus.wait_packet(17)
    assert not link.data


def test_ready_control_is_skipped_before_application_packet(monkeypatch):
    application = b"application-data"
    packets = iter([bytes.fromhex("AF FD 00 01"), application])
    monkeypatch.setattr(PakBus, "read", lambda self: next(packets))
    bus = ManagedPakBus(MemoryLink(), dest_addr=1, dest=2, src_addr=4093, src=4093)
    assert bus.read() == application


def test_unrelated_finished_packet_is_not_our_session(monkeypatch):
    packets = iter([bytes.fromhex("B0 02 00 01"), b"application-data"])
    monkeypatch.setattr(PakBus, "read", lambda self: next(packets))
    bus = ManagedPakBus(MemoryLink(), dest_addr=1, dest=2, src_addr=4093, src=4093)
    assert bus.read() == b"application-data"


def test_genuinely_incomplete_application_packet_is_rejected(monkeypatch):
    monkeypatch.setattr(PakBus, "read", lambda self: b"123456")
    bus = ManagedPakBus(MemoryLink(), dest_addr=1, src=4093)
    with pytest.raises(ConnectionError, match="Incomplete"):
        bus.read()


def test_control_packet_flood_is_bounded(monkeypatch):
    monkeypatch.setattr(PakBus, "read", lambda self: bytes.fromhex("AF FD 00 01"))
    bus = ManagedPakBus(MemoryLink(), dest_addr=1, src=4093)
    with pytest.raises(TimeoutError, match="Too many"):
        bus.read()


def test_session_and_packet_destructors_do_not_touch_socket():
    link = MemoryLink()
    device = ManagedCR1000.__new__(ManagedCR1000)
    device.connected = True
    device.pakbus = ManagedPakBus(link, dest_addr=1, src=4093)
    writes_before = list(link.writes)
    del device
    gc.collect()
    assert link.writes == writes_before
    assert link.closes == 0


def test_failed_handshake_does_not_reconnect_without_router(monkeypatch):
    from pycampbellcr1000.exceptions import NoDeviceException

    link = MemoryLink()
    monkeypatch.setattr(ManagedPakBus, "wait_packet", lambda *args: ({}, {}))
    calls = []

    def ping(device):
        calls.append(device.pakbus.dest)
        raise NoDeviceException()

    monkeypatch.setattr(ManagedCR1000, "ping_node", ping)
    with pytest.raises(NoDeviceException):
        ManagedCR1000(link, dest_addr=1, dest=2, src=4093)
    gc.collect()
    assert calls == [2]
    assert link.closes == 0
    assert link.writes == [b"\xBD" * 6]
    # Failed constructors can leave objects without even a pakbus member.
    partial = ManagedCR1000.__new__(ManagedCR1000)
    del partial
    gc.collect()
    assert link.closes == 0


def test_retry_retires_sessions_before_new_handshake(monkeypatch):
    events = []
    sessions = []

    @contextmanager
    def open_link(*args, **kwargs):
        number = len([event for event in events if event[0] == "open"]) + 1
        events.append(("open", number))
        try:
            yield SimpleNamespace(number=number)
        finally:
            assert all(not session.connected for session in sessions)
            events.append(("close", number))

    class Session:
        def __init__(self, link, **kwargs):
            assert all(not session.connected for session in sessions
                       if session.link.number != link.number)
            self.link = link
            self.dest = kwargs["dest"]
            self.connected = True
            sessions.append(self)
            events.append(("hello", link.number, self.dest))

        def gettime(self):
            return datetime(2026, 10, 6)

        def bye(self):
            events.append(("bye", self.link.number, self.dest))
            self.connected = False

    def fetch(device, *args):
        if device.link.number == 1:
            raise ConnectionError("peer ended the link")
        return iter([pd.DataFrame({"RecNbr": [104850]})])

    monkeypatch.setattr(client, "CR1000", Session)
    monkeypatch.setattr(client, "open_pakbus_link", open_link)
    monkeypatch.setattr(client, "ping6", lambda *args: True)
    monkeypatch.setattr(client, "quick_port_check_ipv6", lambda *args: (True, "ok"))
    monkeypatch.setattr(client, "_fetch_window", fetch)
    results = list(client.fetch_batch("Table1", 26, "America/Denver",
                                    logger_ids=[2], station_attempts=2,
                                    retry_delay_seconds=0))
    assert len(results) == 1
    assert events == [("open", 1), ("hello", 1, 1), ("hello", 1, 2),
                      ("close", 1), ("open", 2), ("hello", 2, 1),
                      ("hello", 2, 2), ("bye", 2, 2), ("bye", 2, 1), ("close", 2)]

"""Offline checks for the PakBus diagnostic downloader."""

from __future__ import annotations

from datetime import datetime, timedelta
from contextlib import nullcontext
from pathlib import Path
import struct
import sys
import weakref
from zoneinfo import ZoneInfo

import pytest
import pandas as pd
from biochar_app.config.pakbus import PAKBUS
from biochar_app.pakbus.core.client import (
    CAMPBELL_EPOCH,
    _compute_window,
    _fetch_window,
    decode_table1_response,
    fetch_batch,
    fetch_isolated_stations,
    resolve_logger_ids,
)


def test_pakbus_config_has_explicit_router_and_leaf_addresses() -> None:
    assert PAKBUS.router_id == 1
    assert PAKBUS.base_id == 4093
    assert PAKBUS.logger_ids == list(range(2, 14))


def test_resolve_logger_ids_preserves_order_and_removes_duplicates() -> None:
    assert resolve_logger_ids(["s2m", "S1T", "S2M"]) == [6, 2]


def test_resolve_logger_ids_rejects_router_and_unknown_station() -> None:
    with pytest.raises(ValueError, match="router"):
        resolve_logger_ids(["CR800"])
    with pytest.raises(ValueError, match="Unknown station"):
        resolve_logger_ids(["S9Z"])


def test_compute_window_uses_requested_duration_and_timezone() -> None:
    start, stop = _compute_window(3, "America/Denver")
    assert stop - start == timedelta(hours=3)
    assert stop.tzinfo == ZoneInfo("America/Denver")


def test_preflight_does_not_load_legacy_transport(monkeypatch, capsys) -> None:
    import biochar_app.pakbus.core.client as client

    monkeypatch.setattr(
        client, "quick_port_check_ipv6", lambda *_args: (True, "ok")
    )
    monkeypatch.setattr(
        client,
        "_load_legacy_transport",
        lambda: pytest.fail("preflight loaded the legacy transport"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["client.py", "--preflight-only", "--stations", "S3B"],
    )

    client.main()

    assert "PakBus TCP endpoint reachable" in capsys.readouterr().out


def test_decode_fixed_table1_response() -> None:
    logger_time = datetime(2026, 5, 14, 14, 30)
    seconds = int((logger_time - CAMPBELL_EPOCH).total_seconds())
    values = (13.278243, 0.1798, 0.3225, 21.814945,
              0.18152, 0.28876, 19.016235,
              0.28088, 0.376907, 18.281605)
    response = (
        struct.pack(">HIH", 2, 90926, 1)
        + struct.pack(">I10f", seconds, *values)
        + b"\x01"
    )

    frame = decode_table1_response(response)

    assert len(frame) == 1
    assert frame.loc[0, "RecNbr"] == 90926
    assert frame.loc[0, "Datetime"].isoformat() == "2026-05-14T14:30:00-07:00"
    assert frame.loc[0, "BattV_Min"] == pytest.approx(13.278243)
    assert frame.loc[0, "VWC_3_Avg"] == pytest.approx(0.28088)
    assert frame.loc[0, "T_3_Avg"] == pytest.approx(18.281605)
    assert frame.attrs["more"] is True


def test_fetch_window_continues_multi_fragment_table1_response() -> None:
    def response(first_record: int, times: list[datetime], more: bool) -> bytes:
        records = b"".join(
            struct.pack(
                ">I10f",
                int((timestamp - CAMPBELL_EPOCH).total_seconds()),
                *([float(first_record)] * 10),
            )
            for timestamp in times
        )
        return (
            struct.pack(">HIH", 2, first_record, len(times))
            + records
            + bytes([more])
        )

    class FakePakbus:
        def __init__(self) -> None:
            self.calls: list[tuple[int, int, int]] = []

        def get_collectdata_cmd(self, _table, _signature, *, mode, p1, p2):
            self.calls.append((mode, p1, p2))
            return len(self.calls)

    class FakeDevice:
        def __init__(self) -> None:
            self.pakbus = FakePakbus()
            self.responses = [
                response(100, [datetime(2026, 5, 15, 3, 0)], True),
                response(101, [datetime(2026, 5, 15, 3, 15)], False),
            ]

        def send_wait(self, _command):
            return {}, {"RespCode": 0, "RecData": self.responses.pop(0)}, None

    device = FakeDevice()
    mst = ZoneInfo("Etc/GMT+7")
    frames = list(
        _fetch_window(
            device,
            "Table1",
            datetime(2026, 5, 15, 2, 0, tzinfo=mst),
            datetime(2026, 5, 15, 5, 0, tzinfo=mst),
        )
    )

    assert len(frames) == 1
    assert frames[0]["RecNbr"].tolist() == [100, 101]
    assert device.pakbus.calls == [(0x05, 20, 0), (0x06, 101, 120)]


def test_fetch_window_does_not_cap_multi_day_request_at_96_records() -> None:
    class FakePakbus:
        def __init__(self) -> None:
            self.calls = []

        def get_collectdata_cmd(self, _table, _signature, *, mode, p1, p2):
            self.calls.append((mode, p1, p2))
            return object()

    class FakeDevice:
        def __init__(self) -> None:
            self.pakbus = FakePakbus()

        @staticmethod
        def send_wait(_command):
            return {}, {"RespCode": 0, "RecData": struct.pack(">HIH", 2, 1, 0) + b"\x00"}, None

    device = FakeDevice()
    mst = ZoneInfo("Etc/GMT+7")
    list(_fetch_window(
        device,
        "Table1",
        datetime(2026, 5, 13, tzinfo=mst),
        datetime(2026, 5, 15, tzinfo=mst),
    ))

    assert device.pakbus.calls == [(0x05, 200, 0)]


def test_fetch_window_translates_missing_library_response_to_timeout() -> None:
    class FakePakbus:
        @staticmethod
        def get_collectdata_cmd(*_args, **_kwargs):
            return object()

    class FakeDevice:
        pakbus = FakePakbus()

        @staticmethod
        def send_wait(_command):
            raise TypeError("'NoneType' object is not subscriptable")

    mst = ZoneInfo("Etc/GMT+7")
    with pytest.raises(TimeoutError, match="did not return a Table1 response"):
        list(
            _fetch_window(
                FakeDevice(),
                "Table1",
                datetime(2026, 5, 15, 2, 0, tzinfo=mst),
                datetime(2026, 5, 15, 5, 0, tzinfo=mst),
            )
        )


def test_fetch_window_translates_missing_record_data_to_timeout() -> None:
    class FakePakbus:
        @staticmethod
        def get_collectdata_cmd(*_args, **_kwargs):
            return object()

    class FakeDevice:
        pakbus = FakePakbus()

        @staticmethod
        def send_wait(_command):
            return {}, {"RespCode": 0}, None

    mst = ZoneInfo("Etc/GMT+7")
    with pytest.raises(TimeoutError, match="did not contain Table1 record data"):
        list(
            _fetch_window(
                FakeDevice(),
                "Table1",
                datetime(2026, 5, 15, 2, 0, tzinfo=mst),
                datetime(2026, 5, 15, 5, 0, tzinfo=mst),
            )
        )


def test_fetch_batch_reopens_connection_and_retries_missing_response(monkeypatch) -> None:
    import biochar_app.pakbus.core.client as client

    links: list[object] = []
    leaf_constructor_calls = 0

    def fake_open_link(_host, _port):
        link = object()
        links.append(link)
        return nullcontext(link)

    def fake_cr1000(link, **_kwargs):
        nonlocal leaf_constructor_calls
        if _kwargs["dest"] == 1:
            return type("FakeRouter", (), {})()
        leaf_constructor_calls += 1
        if leaf_constructor_calls == 1:
            raise TypeError("'NoneType' object is not subscriptable")
        device = type("FakeDevice", (), {})()
        device.link = link
        device.gettime = lambda: datetime(2026, 8, 31, 10, 0)
        return device

    expected = pd.DataFrame([{"Datetime": datetime(2026, 8, 31), "RecNbr": 1}])
    monkeypatch.setattr(client, "quick_port_check_ipv6", lambda *_args: (True, "ok"))
    monkeypatch.setattr(client, "ping6", lambda *_args: True)
    monkeypatch.setattr(client, "open_pakbus_link", fake_open_link)
    monkeypatch.setattr(client, "CR1000", fake_cr1000)
    monkeypatch.setattr(client, "_fetch_window", lambda *_args: iter([expected]))

    results = list(
        fetch_batch(
            "Table1",
            1,
            "America/Denver",
            logger_ids=[3],
            station_attempts=2,
            retry_delay_seconds=0,
        )
    )

    assert len(links) == 2
    assert results == [(3, expected)]


def test_fetch_batch_keeps_router_session_alive_for_leaf_handshake(monkeypatch) -> None:
    import biochar_app.pakbus.core.client as client

    router_ref = None

    class FakeRouter:
        pass

    class FakeDevice:
        @staticmethod
        def gettime():
            return datetime(2026, 8, 31, 10, 0)

    def fake_cr1000(_link, **kwargs):
        nonlocal router_ref
        if kwargs["dest"] == PAKBUS.router_id:
            router = FakeRouter()
            router_ref = weakref.ref(router)
            return router
        assert router_ref is not None and router_ref() is not None
        return FakeDevice()

    monkeypatch.setattr(client, "quick_port_check_ipv6", lambda *_args: (True, "ok"))
    monkeypatch.setattr(client, "ping6", lambda *_args: True)
    monkeypatch.setattr(client, "open_pakbus_link", lambda *_args: nullcontext(object()))
    monkeypatch.setattr(client, "CR1000", fake_cr1000)
    monkeypatch.setattr(
        client,
        "_fetch_window",
        lambda *_args: iter([pd.DataFrame({"x": [1]})]),
    )

    assert len(list(fetch_batch("Table1", 1, "America/Denver", logger_ids=[3]))) == 1


def test_fetch_batch_retries_broken_pipe_with_fresh_connection(monkeypatch) -> None:
    import biochar_app.pakbus.core.client as client

    links: list[object] = []
    leaf_constructor_calls = 0

    def fake_open_link(_host, _port):
        link = object()
        links.append(link)
        return nullcontext(link)

    def fake_cr1000(link, **kwargs):
        nonlocal leaf_constructor_calls
        if kwargs["dest"] == 1:
            return type("FakeRouter", (), {})()
        leaf_constructor_calls += 1
        if leaf_constructor_calls == 1:
            raise BrokenPipeError(32, "Broken pipe")
        device = type("FakeDevice", (), {})()
        device.gettime = lambda: datetime(2026, 8, 31, 10, 0)
        return device

    expected = pd.DataFrame([{"Datetime": datetime(2026, 8, 31), "RecNbr": 1}])
    monkeypatch.setattr(client, "quick_port_check_ipv6", lambda *_args: (True, "ok"))
    monkeypatch.setattr(client, "ping6", lambda *_args: True)
    monkeypatch.setattr(client, "open_pakbus_link", fake_open_link)
    monkeypatch.setattr(client, "CR1000", fake_cr1000)
    monkeypatch.setattr(client, "_fetch_window", lambda *_args: iter([expected]))

    results = list(
        fetch_batch(
            "Table1",
            1,
            "America/Denver",
            logger_ids=[4],
            station_attempts=2,
            retry_delay_seconds=0,
        )
    )

    assert len(links) == 2
    assert results == [(4, expected)]


def test_fetch_batch_retries_delivery_failure(monkeypatch) -> None:
    import biochar_app.pakbus.core.client as client

    links: list[object] = []
    leaf_constructor_calls = 0

    def fake_open_link(_host, _port):
        link = object()
        links.append(link)
        return nullcontext(link)

    class FakeDeliveryFailure(Exception):
        pass

    def fake_cr1000(link, **kwargs):
        nonlocal leaf_constructor_calls
        if kwargs["dest"] == 1:
            return type("FakeRouter", (), {})()
        leaf_constructor_calls += 1
        if leaf_constructor_calls == 1:
            raise FakeDeliveryFailure("Delivery failure.")
        device = type("FakeDevice", (), {})()
        device.gettime = lambda: datetime(2026, 8, 31, 10, 0)
        return device

    expected = pd.DataFrame([{"Datetime": datetime(2026, 8, 31), "RecNbr": 1}])
    monkeypatch.setattr(client, "quick_port_check_ipv6", lambda *_args: (True, "ok"))
    monkeypatch.setattr(client, "ping6", lambda *_args: True)
    monkeypatch.setattr(client, "open_pakbus_link", fake_open_link)
    monkeypatch.setattr(client, "CR1000", fake_cr1000)
    monkeypatch.setattr(client, "_legacy_retryable_exceptions", (FakeDeliveryFailure,))
    monkeypatch.setattr(client, "_fetch_window", lambda *_args: iter([expected]))

    results = list(
        fetch_batch(
            "Table1",
            1,
            "America/Denver",
            logger_ids=[10],
            station_attempts=2,
            retry_delay_seconds=0,
        )
    )

    assert len(links) == 2
    assert results == [(10, expected)]


def test_fetch_isolated_stations_uses_new_process_and_pause(monkeypatch, tmp_path) -> None:
    import biochar_app.pakbus.core.client as client

    commands: list[list[str]] = []
    pauses: list[float] = []

    def fake_run(command, check):
        assert check is False
        commands.append(command)
        station = command[command.index("--stations") + 1]
        output = Path(command[command.index("--output") + 1])
        pd.DataFrame([{"station": station, "logger_id": 2, "RecNbr": 1}]).to_csv(
            output, index=False
        )
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(client.subprocess, "run", fake_run)
    monkeypatch.setattr(client.time, "sleep", pauses.append)

    rows = fetch_isolated_stations(
        ["S1T", "S2T", "S2M"],
        table="Table1",
        hours=24,
        timezone="America/Denver",
        attempts=3,
        station_pause_seconds=15,
    )

    assert [row["station"] for row in rows] == ["S1T", "S2T", "S2M"]
    assert len(commands) == 3
    assert all("--direct" in command for command in commands)
    assert pauses == [15, 15]

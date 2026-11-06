"""Safety tests for router-first PakBus network recovery."""

from __future__ import annotations

import json
import socket
import stat
from datetime import datetime, timezone
from types import SimpleNamespace

from biochar_app.pakbus.core.network_preflight import run_network_preflight


NOW = datetime(2026, 11, 6, 8, 0, tzinfo=timezone.utc)


def test_ping_router_resolves_ddns_and_uses_bounded_icmp(monkeypatch):
    import biochar_app.pakbus.core.network_preflight as preflight

    observed = {}
    monkeypatch.setattr(
        preflight.socket,
        "getaddrinfo",
        lambda *_args: [(socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.5", 0))],
    )
    monkeypatch.setattr(preflight.shutil, "which", lambda _name: "/sbin/ping")

    def run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(preflight.subprocess, "run", run)

    reachable, detail = preflight.ping_router("router.example", timeout=2, count=3)

    assert reachable is True
    assert detail == "ICMP reply from router.example (192.0.2.5)"
    assert observed["command"][0] == "/sbin/ping"
    assert observed["command"][-1] == "192.0.2.5"
    assert observed["command"][observed["command"].index("-c") + 1] == "3"
    assert observed["kwargs"]["timeout"] == 9


def _arguments(tmp_path, port_check, **overrides):
    values = {
        "router_url": "https://router.example:8443",
        "router_ping_host": "router.example",
        "router_ping_timeout": 3,
        "router_ping_count": 3,
        "client_mac": "00:D0:2C:02:1D:DD",
        "client_ipv6_suffix": "2d0:2cff:fe02:1ddd",
        "recovery_enabled": False,
        "reboot_authorized": False,
        "username": None,
        "password": None,
        "verify_tls": True,
        "ca_bundle": None,
        "state_path": tmp_path / "router_recovery_state.json",
        "attempts": 1,
        "timeout": 5,
        "retry_seconds": 0,
        "reboot_cooldown_seconds": 21600,
        "reboot_wait_seconds": 0,
        "post_reboot_attempts": 1,
        "post_reboot_retry_seconds": 0,
        "port_check": port_check,
        "ping_check": lambda *_args: (True, "router reply"),
        "sleeper": lambda _seconds: None,
        "clock": lambda: NOW,
    }
    values.update(overrides)
    return values


def test_router_ping_is_first_and_healthy_path_never_contacts_router(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    events = []
    monkeypatch.setattr(preflight, "login_router", lambda *_args, **_kwargs: object())

    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: events.append("endpoint") or (True, "ok"),
            ping_check=lambda *_args: events.append("router_ping") or (True, "reply"),
        ),
    )

    assert events == ["router_ping", "endpoint"]
    assert result.reachable is True
    assert result.report["status"] == "reachable"
    assert result.report["actions"] == []


def test_failed_router_ping_stops_before_endpoint_without_reboot_authorization(tmp_path):
    endpoint_checks = []
    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: endpoint_checks.append(True) or (True, "ok"),
            ping_check=lambda *_args: (False, "no ICMP reply"),
        ),
    )

    assert result.reachable is False
    assert endpoint_checks == []
    assert result.report["status"] == "router_unreachable_reboot_not_authorized"


def test_inconclusive_router_probe_never_triggers_reboot(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    monkeypatch.setattr(
        preflight,
        "login_router",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("router login")),
    )
    monkeypatch.setattr(
        preflight,
        "request_router_reboot",
        lambda *_args: (_ for _ in ()).throw(AssertionError("router reboot")),
    )
    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: (True, "ok"),
            ping_check=lambda *_args: (False, "probe error: DNS resolution failed"),
            reboot_authorized=True,
            username="router-user",
            password="router-password",
        ),
    )

    assert result.report["status"] == "router_probe_failed"
    assert "could not be determined" in result.detail


def test_router_recovery_before_final_ping_avoids_reboot(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    (tmp_path / "router_recovery_state.json").write_text(
        json.dumps({"last_reboot_attempt_at": NOW.isoformat()}),
        encoding="utf-8",
    )
    pings = iter([(False, "no ICMP reply"), (True, "reply")])
    reboots = []
    monkeypatch.setattr(preflight, "login_router", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(preflight, "request_router_reboot", lambda *_args: reboots.append(True))

    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: (True, "ok"),
            ping_check=lambda *_args: next(pings),
            reboot_authorized=True,
            username="router-user",
            password="router-password",
        ),
    )

    assert result.reachable is True
    assert result.report["actions"] == ["skipped_reboot_router_recovered"]
    assert reboots == []


def test_authorized_router_reboot_happens_before_endpoint_and_only_once(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    events = []
    pings = iter(
        [(False, "no ICMP reply initially"), (False, "no ICMP reply finally"), (True, "reply")]
    )
    waits = []
    monkeypatch.setattr(
        preflight,
        "login_router",
        lambda *_args, **_kwargs: events.append("login") or object(),
    )
    monkeypatch.setattr(
        preflight,
        "request_router_reboot",
        lambda *_args: events.append("reboot"),
    )

    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: events.append("endpoint") or (True, "ok"),
            ping_check=lambda *_args: events.append("ping") or next(pings),
            reboot_authorized=True,
            username="router-user",
            password="router-password",
            reboot_wait_seconds=180,
            sleeper=waits.append,
        ),
    )

    state_path = tmp_path / "router_recovery_state.json"
    assert events == ["ping", "login", "ping", "reboot", "ping", "endpoint"]
    assert result.reachable is True
    assert result.report["status"] == "reachable_after_router_reboot"
    assert waits == [180]
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    state = json.loads(state_path.read_text())
    assert state["last_reboot_attempt_at"] == NOW.isoformat()
    assert state["reason"] == "ASUS router failed both initial and final ICMP checks"
    serialized_report = json.dumps(result.report)
    assert "router-user" not in serialized_report
    assert "router-password" not in serialized_report


def test_router_must_recover_before_any_endpoint_attempt(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    endpoint_checks = []
    monkeypatch.setattr(preflight, "login_router", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(preflight, "request_router_reboot", lambda *_args: None)
    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: endpoint_checks.append(True) or (True, "ok"),
            ping_check=lambda *_args: (False, "no ICMP reply"),
            reboot_authorized=True,
            username="router-user",
            password="router-password",
        ),
    )

    assert result.report["status"] == "router_unreachable_after_reboot"
    assert endpoint_checks == []


def test_recent_reboot_attempt_blocks_another_reboot(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    state = tmp_path / "router_recovery_state.json"
    state.write_text(json.dumps({"last_reboot_attempt_at": NOW.isoformat()}))
    monkeypatch.setattr(preflight, "login_router", lambda *_args, **_kwargs: object())

    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: (True, "ok"),
            ping_check=lambda *_args: (False, "no ICMP reply"),
            reboot_authorized=True,
            username="router-user",
            password="router-password",
        ),
    )

    assert result.report["status"] == "reboot_cooldown"
    assert "21600 seconds remain" in result.detail


def test_invalid_cooldown_state_fails_closed(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    (tmp_path / "router_recovery_state.json").write_text("not-json", encoding="utf-8")
    monkeypatch.setattr(preflight, "login_router", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        preflight,
        "request_router_reboot",
        lambda *_args: (_ for _ in ()).throw(AssertionError("reboot requested")),
    )
    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: (True, "ok"),
            ping_check=lambda *_args: (False, "no ICMP reply"),
            reboot_authorized=True,
            username="router-user",
            password="router-password",
        ),
    )

    assert result.report["status"] == "reboot_state_invalid"
    assert "cooldown state is invalid" in result.detail


def test_reboot_refuses_unverified_router_tls(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    monkeypatch.setattr(
        preflight,
        "login_router",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("credentials sent")),
    )
    result = run_network_preflight(
        "2605::1",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: (True, "ok"),
            ping_check=lambda *_args: (False, "no ICMP reply"),
            reboot_authorized=True,
            username="router-user",
            password="router-password",
            verify_tls=False,
        ),
    )

    assert result.report["status"] == "recovery_refused"
    assert "TLS certificate verification is disabled" in result.detail


def test_endpoint_failure_can_rediscover_address_but_does_not_reboot(monkeypatch, tmp_path):
    import biochar_app.pakbus.core.network_preflight as preflight

    endpoint_checks = iter([(False, "timeout"), (True, "ok")])
    reboots = []
    monkeypatch.setattr(preflight, "login_router", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        preflight,
        "discover_address_from_session",
        lambda *_args: "2605:59ca:2::2d0:2cff:fe02:1ddd",
    )
    monkeypatch.setattr(preflight, "request_router_reboot", lambda *_args: reboots.append(True))

    result = run_network_preflight(
        "2605:59ca:1::2d0:2cff:fe02:1ddd",
        6785,
        **_arguments(
            tmp_path,
            lambda *_args: next(endpoint_checks),
            recovery_enabled=True,
            username="router-user",
            password="router-password",
        ),
    )

    assert result.reachable is True
    assert result.report["status"] == "recovered_by_rediscovery"
    assert result.host == "2605:59ca:2::2d0:2cff:fe02:1ddd"
    assert reboots == []

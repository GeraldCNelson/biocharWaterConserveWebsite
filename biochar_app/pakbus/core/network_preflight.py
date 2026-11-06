"""Bounded PakBus endpoint checks and explicitly authorized ASUS recovery."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from biochar_app.pakbus.core.gateway_discovery import (
    discover_address_from_session,
    login_router,
    request_router_reboot,
)


PortCheck = Callable[..., tuple[bool, str]]
PingCheck = Callable[..., tuple[bool, str]]


@dataclass(frozen=True)
class NetworkPreflightResult:
    reachable: bool
    host: str
    detail: str
    report: dict


def _error_detail(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def ping_router(host: str, timeout: float = 3.0, count: int = 3) -> tuple[bool, str]:
    """Resolve the router DDNS name and make a bounded ICMP reachability check."""
    try:
        addresses = socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_DGRAM)
    except OSError as exc:
        return False, f"probe error: DNS resolution failed for {host}: {exc}"

    candidates = [(family, sockaddr[0]) for family, *_rest, sockaddr in addresses]
    if not candidates:
        return False, f"probe error: DNS resolution returned no addresses for {host}"
    family, address = next(
        (item for item in candidates if item[0] == socket.AF_INET),
        candidates[0],
    )
    ping_name = "ping6" if family == socket.AF_INET6 and shutil.which("ping6") else "ping"
    ping_path = shutil.which(ping_name)
    if ping_path is None:
        return False, f"probe error: {ping_name} utility is unavailable"

    bounded_timeout = max(1, round(timeout))
    wait_value = str(bounded_timeout * 1000 if sys.platform == "darwin" else bounded_timeout)
    command = [ping_path]
    if family == socket.AF_INET6 and ping_name == "ping":
        command.append("-6")
    command.extend(["-c", str(max(1, count)), "-W", wait_value, address])
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=max(5.0, max(1, count) * (timeout + 1.0)),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"probe error: ICMP check failed for {host} ({address}): {_error_detail(exc)}"
    if result.returncode == 0:
        return True, f"ICMP reply from {host} ({address})"
    if result.returncode == 1:
        return False, f"no ICMP reply from {host} ({address})"
    return False, f"probe error: ping exited with status {result.returncode}"


def _probe_endpoint(
    host: str,
    port: int,
    *,
    attempts: int,
    timeout: float,
    retry_seconds: float,
    port_check: PortCheck,
    sleeper: Callable[[float], None],
    clock: Callable[[], datetime],
) -> tuple[bool, str, list[dict]]:
    checks: list[dict] = []
    last_detail = "no endpoint checks were made"
    for number in range(1, max(1, attempts) + 1):
        try:
            reachable, detail = port_check(host, port, timeout)
        except Exception as exc:
            reachable, detail = False, _error_detail(exc)
        last_detail = detail
        checks.append(
            {
                "attempt": number,
                "checked_at": clock().isoformat(),
                "host": host,
                "port": port,
                "reachable": bool(reachable),
                "detail": detail,
            }
        )
        if reachable:
            return True, detail, checks
        if number < max(1, attempts) and retry_seconds > 0:
            sleeper(retry_seconds)
    return False, last_detail, checks


def _last_reboot_attempt(state_path: Path) -> tuple[datetime | None, str | None]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        value = datetime.fromisoformat(str(payload["last_reboot_attempt_at"]))
    except FileNotFoundError:
        return None, None
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        return None, _error_detail(exc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc), None


def _reserve_reboot(
    state_path: Path,
    *,
    now: datetime,
    configured_host: str,
    router_host: str,
) -> None:
    """Persist the attempt before submitting it so uncertainty cannot cause a retry."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(state_path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            {
                "last_reboot_attempt_at": now.astimezone(timezone.utc).isoformat(),
                "configured_host": configured_host,
                "router_host": router_host,
                "reason": "ASUS router failed both initial and final ICMP checks",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(state_path)


def run_network_preflight(
    host: str,
    port: int,
    *,
    router_url: str,
    router_ping_host: str,
    router_ping_timeout: float,
    router_ping_count: int,
    client_mac: str,
    client_ipv6_suffix: str,
    recovery_enabled: bool,
    reboot_authorized: bool,
    username: str | None,
    password: str | None,
    verify_tls: bool,
    ca_bundle: str | Path | None,
    state_path: Path,
    attempts: int,
    timeout: float,
    retry_seconds: float,
    reboot_cooldown_seconds: float,
    reboot_wait_seconds: float,
    post_reboot_attempts: int,
    post_reboot_retry_seconds: float,
    port_check: PortCheck,
    ping_check: PingCheck = ping_router,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> NetworkPreflightResult:
    """Gate all PakBus work on router ICMP, then check the logger endpoint."""
    report: dict = {
        "configured_host": host,
        "active_host": host,
        "port": port,
        "router_ping_host": router_ping_host,
        "recovery_enabled": bool(recovery_enabled),
        "reboot_authorized": bool(reboot_authorized),
        "status": "checking",
        "actions": [],
    }
    try:
        router_reachable, router_detail = ping_check(
            router_ping_host,
            router_ping_timeout,
            router_ping_count,
        )
    except Exception as exc:
        router_reachable, router_detail = False, _error_detail(exc)
    report["router_initial_ping"] = {
        "checked_at": clock().isoformat(),
        "reachable": bool(router_reachable),
        "detail": router_detail,
    }

    router_rebooted = False
    if not router_reachable:
        if not router_detail.startswith("no ICMP reply"):
            detail = f"router reachability could not be determined; {router_detail}"
            report.update(status="router_probe_failed", detail=detail)
            return NetworkPreflightResult(False, host, detail, report)
        if not reboot_authorized:
            detail = f"{router_detail}; router reboot was not explicitly authorized"
            report.update(status="router_unreachable_reboot_not_authorized", detail=detail)
            return NetworkPreflightResult(False, host, detail, report)
        if not verify_tls:
            detail = "ASUS reboot refused because TLS certificate verification is disabled"
            report.update(status="recovery_refused", detail=detail)
            return NetworkPreflightResult(False, host, detail, report)
        if not username or not password:
            detail = "ASUS reboot authorized, but router credentials are not configured"
            report.update(status="recovery_not_configured", detail=detail)
            return NetworkPreflightResult(False, host, detail, report)

        try:
            opener = login_router(
                router_url,
                username,
                password,
                verify_tls=True,
                ca_bundle=ca_bundle,
            )
        except Exception as exc:
            detail = f"ASUS HTTPS login failed; reboot not attempted: {_error_detail(exc)}"
            report.update(status="router_login_failed", detail=detail)
            return NetworkPreflightResult(False, host, detail, report)

        # Authentication takes time and proves the management path is alive;
        # make one final ICMP check immediately before the destructive action.
        try:
            router_reachable, router_detail = ping_check(
                router_ping_host,
                router_ping_timeout,
                router_ping_count,
            )
        except Exception as exc:
            router_reachable, router_detail = False, _error_detail(exc)
        report["router_final_pre_reboot_ping"] = {
            "checked_at": clock().isoformat(),
            "reachable": bool(router_reachable),
            "detail": router_detail,
        }
        if router_reachable:
            report["actions"].append("skipped_reboot_router_recovered")
        elif not router_detail.startswith("no ICMP reply"):
            detail = f"final router reachability check was inconclusive; {router_detail}"
            report.update(status="router_probe_failed", detail=detail)
            return NetworkPreflightResult(False, host, detail, report)
        else:
            now = clock().astimezone(timezone.utc)
            last_attempt, state_error = _last_reboot_attempt(state_path)
            if state_error is not None:
                detail = f"router reboot blocked because cooldown state is invalid: {state_error}"
                report.update(status="reboot_state_invalid", detail=detail)
                return NetworkPreflightResult(False, host, detail, report)
            if last_attempt is not None:
                elapsed = (now - last_attempt).total_seconds()
                if elapsed < reboot_cooldown_seconds:
                    remaining = max(0, round(reboot_cooldown_seconds - elapsed))
                    detail = f"router reboot blocked by cooldown; {remaining} seconds remain"
                    report.update(
                        status="reboot_cooldown",
                        detail=detail,
                        last_reboot_attempt_at=last_attempt.isoformat(),
                    )
                    return NetworkPreflightResult(False, host, detail, report)
            _reserve_reboot(
                state_path,
                now=now,
                configured_host=host,
                router_host=router_ping_host,
            )
            report["actions"].append("router_reboot_requested")
            report["reboot"] = {
                "attempted_at": now.isoformat(),
                "state_path": str(state_path),
                "cooldown_seconds": reboot_cooldown_seconds,
                "submission": "pending",
            }
            try:
                request_router_reboot(opener, router_url)
                report["reboot"]["submission"] = "submitted"
            except Exception as exc:
                # The router can close the connection while beginning its reboot.
                # Never retry the request; observe ICMP recovery under the cooldown.
                report["reboot"]["submission"] = "uncertain"
                report["reboot"]["detail"] = _error_detail(exc)
            router_rebooted = True

            if reboot_wait_seconds > 0:
                sleeper(reboot_wait_seconds)
            post_checks: list[dict] = []
            for number in range(1, max(1, post_reboot_attempts) + 1):
                try:
                    router_reachable, router_detail = ping_check(
                        router_ping_host,
                        router_ping_timeout,
                        router_ping_count,
                    )
                except Exception as exc:
                    router_reachable, router_detail = False, _error_detail(exc)
                post_checks.append(
                    {
                        "attempt": number,
                        "checked_at": clock().isoformat(),
                        "reachable": bool(router_reachable),
                        "detail": router_detail,
                    }
                )
                if router_reachable:
                    break
                if number < max(1, post_reboot_attempts) and post_reboot_retry_seconds > 0:
                    sleeper(post_reboot_retry_seconds)
            report["router_post_reboot_pings"] = post_checks
            if not router_reachable:
                detail = f"ASUS router remained unreachable after the authorized reboot: {router_detail}"
                report.update(status="router_unreachable_after_reboot", detail=detail)
                return NetworkPreflightResult(False, host, detail, report)

    reachable, detail, checks = _probe_endpoint(
        host,
        port,
        attempts=attempts,
        timeout=timeout,
        retry_seconds=retry_seconds,
        port_check=port_check,
        sleeper=sleeper,
        clock=clock,
    )
    report["endpoint_checks"] = checks
    if reachable:
        report["status"] = "reachable_after_router_reboot" if router_rebooted else "reachable"
        return NetworkPreflightResult(True, host, detail, report)

    if not recovery_enabled:
        detail = f"{detail}; authenticated Campbell address rediscovery is disabled"
        report.update(status="endpoint_unreachable", detail=detail)
        return NetworkPreflightResult(False, host, detail, report)
    if not verify_tls:
        detail = "ASUS address rediscovery refused because TLS certificate verification is disabled"
        report.update(status="recovery_refused", detail=detail)
        return NetworkPreflightResult(False, host, detail, report)
    if not username or not password:
        detail = "ASUS address rediscovery enabled, but router credentials are not configured"
        report.update(status="recovery_not_configured", detail=detail)
        return NetworkPreflightResult(False, host, detail, report)

    report["actions"].append("authenticated_router_discovery")
    try:
        opener = login_router(
            router_url,
            username,
            password,
            verify_tls=True,
            ca_bundle=ca_bundle,
        )
        discovered_host = discover_address_from_session(
            opener,
            router_url,
            client_mac,
            client_ipv6_suffix,
        )
    except Exception as exc:
        detail = f"ASUS recovery could not verify the Campbell client: {_error_detail(exc)}"
        report.update(status="address_discovery_failed", detail=detail)
        return NetworkPreflightResult(False, host, detail, report)

    report["discovered_host"] = discovered_host
    report["active_host"] = discovered_host
    reachable, detail, checks = _probe_endpoint(
        discovered_host,
        port,
        attempts=attempts,
        timeout=timeout,
        retry_seconds=retry_seconds,
        port_check=port_check,
        sleeper=sleeper,
        clock=clock,
    )
    report["rediscovered_host_checks"] = checks
    if reachable:
        report["status"] = "recovered_by_rediscovery"
        report["actions"].append("selected_rediscovered_host")
        return NetworkPreflightResult(True, discovered_host, detail, report)

    detail = f"PakBus endpoint remained unreachable after address rediscovery: {detail}"
    report.update(status="endpoint_unreachable", detail=detail)
    return NetworkPreflightResult(False, discovered_host, detail, report)

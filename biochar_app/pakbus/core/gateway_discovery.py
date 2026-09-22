"""Discover the Campbell base station through the authenticated ASUS client list."""

from __future__ import annotations

import argparse
import base64
import getpass
import ipaddress
import json
import os
import re
import ssl
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from http.cookiejar import CookieJar
from pathlib import Path
from zoneinfo import ZoneInfo

from biochar_app.config.pakbus import SETTINGS, SETTINGS_PATH
from biochar_app.pakbus.core.client import quick_port_check_ipv6


CLIENT_ENDPOINTS = (
    "/appGet.cgi?hook=networkmapd_clientlist()",
    "/update_clients.asp",
)


def _opener(verify_tls: bool) -> urllib.request.OpenerDirector:
    context = ssl.create_default_context() if verify_tls else ssl._create_unverified_context()
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(CookieJar()),
        urllib.request.HTTPSHandler(context=context),
    )


def _read(opener: urllib.request.OpenerDirector, url: str, data: bytes | None = None) -> str:
    request = urllib.request.Request(url, data=data, headers={"User-Agent": "biochar-pakbus-discovery/1"})
    with opener.open(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


def discover_address(
    router_url: str, username: str, password: str, client_mac: str, suffix: str, verify_tls: bool
) -> str:
    opener = _opener(verify_tls)
    authorization = base64.b64encode(f"{username}:{password}".encode()).decode()
    payload = urllib.parse.urlencode(
        {"login_authorization": authorization, "action_mode": "login", "next_page": "index.asp"}
    ).encode()
    login = _read(opener, f"{router_url.rstrip('/')}/login.cgi", payload)
    if "Main_Login.asp" in login:
        raise RuntimeError("ASUS login failed; check the username and password")

    bodies: list[str] = []
    for endpoint in CLIENT_ENDPOINTS:
        try:
            bodies.append(_read(opener, f"{router_url.rstrip('/')}{endpoint}"))
        except Exception:
            continue
    text = "\n".join(bodies)
    compact_text = re.sub(r"[^0-9a-f]", "", text.lower())
    compact_mac = re.sub(r"[^0-9a-f]", "", client_mac.lower())
    if compact_mac not in compact_text:
        raise RuntimeError(f"Campbell client MAC {client_mac} was not found in the ASUS client list")
    candidates = set(re.findall(r"(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}", text))
    matches = []
    for value in candidates:
        try:
            address = ipaddress.IPv6Address(value)
        except ValueError:
            continue
        if address.is_global and address.compressed.lower().endswith(suffix.lower()):
            matches.append(address.compressed)
    if len(matches) != 1:
        raise RuntimeError(f"Expected one Campbell IPv6 address, found {len(matches)}")
    return matches[0]


def update_settings_host(path: Path, host: str) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["connection"]["host"] = host
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    discovery = SETTINGS["discovery"]
    parser = argparse.ArgumentParser(description="Discover and test the Campbell PakBus IPv6 address")
    parser.add_argument("--station", default="S3B")
    parser.add_argument("--router-url", default=discovery["router_url"])
    parser.add_argument("--username", default=os.getenv("ASUS_ROUTER_USERNAME"))
    parser.add_argument("--no-update", action="store_true")
    args = parser.parse_args(argv)
    username = args.username or input("ASUS router username: ").strip()
    password = os.getenv("ASUS_ROUTER_PASSWORD") or getpass.getpass("ASUS router password: ")
    host = discover_address(
        args.router_url,
        username,
        password,
        discovery["client_mac"],
        discovery["client_ipv6_suffix"],
        bool(discovery["verify_tls"]),
    )
    reachable, reason = quick_port_check_ipv6(host, int(SETTINGS["connection"]["port"]), timeout=5)
    if not reachable:
        raise SystemExit(f"Discovered {host}, but PakBus TCP validation failed: {reason}")
    print(f"Discovered Campbell base station: {host}")
    if not args.no_update:
        update_settings_host(SETTINGS_PATH, host)
        print(f"Updated configuration: {SETTINGS_PATH}")
    repo_root = Path(__file__).resolve().parents[3]
    stamp = datetime.now(ZoneInfo(str(SETTINGS["download"]["timezone"]))).strftime("%Y%m%dT%H%M%S%z")
    run_dir = repo_root / "biochar_app" / "data-raw" / "pakbus_manual" / f"{stamp}_{args.station}"
    run_dir.mkdir(parents=True, exist_ok=False)
    command = [
        sys.executable,
        "-m",
        "biochar_app.pakbus.core.client",
        "--stations",
        args.station,
        "--output",
        str(run_dir / "logger_data.csv"),
        "--timing-output",
        str(run_dir / "station_timings.json"),
    ]
    print(f"Testing station {args.station}...")
    result = subprocess.run(command, check=False)
    print(f"Test output: {run_dir}")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

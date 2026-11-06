# PakBus router reachability and recovery

This runbook covers failures of the ASUS network path used by the automated
PakBus download. It is separate from logger battery and solar-power diagnosis.

The November 6, 2026 run failed before any station attempt with
`endpoint_unreachable: timeout`. Later that morning, the ASUS mobile app could
not reach the router even though the remote HTTPS administration page at
`https://Biochar-Fruita.asuscomm.com:8443` opened. Rebooting from that page
restored ASUS-app access and PC400 access to a logger. That symptom points to a
router or routing-state failure, not to an individual logger battery.

## Automatic daily sequence

The systemd timer starts `biochar-pakbus-daily.service` at approximately 12:15
a.m. Mountain time. The daily process now runs in this order:

1. Resolve `Biochar-Fruita.asuscomm.com` and send three bounded ICMP echo
   requests to its current IP address.
2. If the router replies, continue to the existing PakBus TCP preflight and
   validated all-station download.
3. If the router does not reply, do not make a PakBus or logger connection
   attempt.
4. When reboot authorization and credentials are configured, sign in through
   the verified HTTPS administration page and make one final ICMP check.
5. If the final check also fails, enforce and reserve the six-hour cooldown,
   then submit exactly one ASUSWRT reboot request. The request is never retried.
6. Wait three minutes and make up to 12 bounded router checks at 15-second
   intervals. Continue to PakBus only after the router replies. Otherwise end
   the run as `failed_preflight` without contacting a logger.
7. Run the existing PakBus endpoint checks. Optional authenticated address
   rediscovery may select a changed Campbell IPv6 address, but a PakBus-only
   failure does not cause another router reboot.

The download, full-frame validation, archive promotion, continuity audit, ETL,
website restart, and production-publication gates are unchanged. A failed or
partial run cannot be promoted or published.

## Required deployment configuration

First verify from the acquisition server that the healthy router normally
answers ICMP. This is essential: some routers or upstream networks deliberately
block ping even while HTTPS works. Do not enable automatic reboot until this
command succeeds during normal operation:

```bash
ping -c 3 Biochar-Fruita.asuscomm.com
```

Store credentials and authorization only in `/etc/biochar/pipeline.env`:

```text
# Allows address rediscovery after a PakBus endpoint failure.
BIOCHAR_ASUS_RECOVERY_ENABLED=true

# Explicitly authorizes one cooldown-protected reboot after router ping failure.
BIOCHAR_ASUS_REBOOT_ENABLED=true
ASUS_ROUTER_USERNAME=the-router-administrator
ASUS_ROUTER_PASSWORD=the-router-password

# Set only when the router certificate uses a private CA.
# ASUS_ROUTER_CA_BUNDLE=/etc/biochar/asus-router-ca.pem
```

Protect the file; systemd reads it before starting the unprivileged service:

```bash
sudo chown root:root /etc/biochar/pipeline.env
sudo chmod 600 /etc/biochar/pipeline.env
```

Credentials are not supported as command-line arguments and are not written to
diagnostic reports. Automated login refuses to proceed without verified TLS.

## Safeguards and evidence

- Reboot is separately authorized and disabled when its environment flag is
  absent or false.
- DNS failures, a missing ping utility, permission errors, and other
  inconclusive probe failures stop the run without rebooting the router. Only a
  completed ICMP check with no reply is treated as router unreachability.
- A successful HTTPS login and two failed router ping checks are required.
- A mode-`0600` state file at
  `biochar_app/data-raw/pakbus_network/router_recovery_state.json` enforces a
  six-hour cooldown. Invalid state fails closed.
- The cooldown is written before submitting the reboot, preventing a connection
  drop during reboot from causing a duplicate request.
- Only one reboot request is possible in a run. The code observes recovery but
  never loops on reboot.
- `diagnostic_report.json` records the initial ping, final pre-reboot ping,
  reboot submission status, post-reboot pings, PakBus endpoint checks, and any
  address rediscovery. It never records credentials.

## Manual retry

After the router is reachable, repeat the full scheduled workflow—not a
station-only download—with:

```bash
sudo systemctl start biochar-pakbus-daily.service
sudo systemctl status biochar-pakbus-daily.service
sudo journalctl -u biochar-pakbus-daily.service -n 200 --no-pager
```

This produces a new dated run directory. Do not promote a prior
`failed_preflight` directory because it contains no validated station data.

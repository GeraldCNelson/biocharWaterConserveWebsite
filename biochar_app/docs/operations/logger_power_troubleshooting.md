# Logger Power and Missing-Data Troubleshooting

## Purpose

Use this procedure when a field logger stops recording, becomes unreachable, records low `BattV_Min`, or appears to operate only during daylight. It separates faults in the solar panel, charge controller, battery, wiring, and logger.

## How the power system works

The solar panel supplies the charging source during daylight. The charge controller converts that input into an appropriate, temperature-compensated charging voltage for a nominal 12 V rechargeable battery. The battery supplies the logger when solar production is insufficient.

The logger's `BattV_Min` value is measured at the logger power input. It is not necessarily the same as an isolated battery measurement. Daytime readings can reflect active charging, so the minimum shortly before sunrise is generally the most informative routine battery measurement.

Typical lead-acid charging voltages may be approximately 13.6 V during float charging and approximately 14.7 V during active cycle charging, depending on controller model and temperature. Confirm values against the installed controller and battery manuals.

## Automated warning levels

The daily download uses provisional operating thresholds:

| Condition | Classification | Action |
| --- | --- | --- |
| `BattV_Min` below 11.5 V | Warning | Inspect trend and arrange a field check if repeated |
| `BattV_Min` below 10.5 V | Critical | Reject the daily run and investigate promptly |
| No station data | Critical | Check communications and power |
| Gap longer than 30 minutes | Critical | Preserve the raw run and investigate |
| Latest record more than 90 minutes old | Critical | Treat the station as stale |

These thresholds should be refined using healthy overnight records, battery chemistry, temperature, and measured system load.

## Safety

- Cover or disconnect the solar panel as directed by the equipment manual before changing wiring.
- Switch the controller output off before connecting or disconnecting the battery when the equipment instructions require it.
- Remove rings, watches, and bracelets. A 12 V battery can deliver enough current through a short circuit to cause burns or fire.
- Observe polarity and use insulated tools.
- Do not load-test a visibly swollen, leaking, frozen, cracked, or overheated battery.

## Field troubleshooting sequence

### 1. Record the initial condition

Before changing anything, record:

- station and logger ID;
- date, local time, weather, and approximate temperature;
- logger/controller indicator lights;
- last recorded timestamp and recent `BattV_Min` values;
- whether communications work;
- photographs of all connections.

### 2. Inspect wiring and connections

Check the battery, panel, controller, fuse, and logger connections for loose conductors, corrosion, damaged insulation, strained cables, incorrect polarity, or overheating. Gently tug each conductor. A wire can appear connected while not being firmly clamped.

If records stop near sunset and resume after sunrise, first suspect a disconnected battery circuit, failed battery, open fuse, or high-resistance battery connection.

### 3. Test the solar panel during daylight

Measure at the panel side of the controller:

1. Panel voltage with the panel connected and operating.
2. Open-circuit panel voltage only if the panel/controller instructions permit it.

A plausible open-circuit voltage does not prove that the panel supplies adequate current under load. Record both the measurement point and whether the circuit was loaded.

### 4. Test the charge controller

In good sunlight, measure voltage at the controller's battery/output terminals. It should normally be higher than the battery's rested voltage while charging. Verify the controller charging indicator and compare the result with the exact controller manual.

Measure voltage again at the logger power terminals. A material difference between controller/battery voltage and logger-terminal voltage suggests a wiring, connector, fuse, or contact-resistance problem.

### 5. Test the battery independently

Following the controller manual, isolate the battery from both the charging source and logger load. Then:

1. Confirm battery chemistry, nominal voltage, capacity, age, and correct charger mode.
2. Allow the battery to rest before measuring open-circuit voltage so surface charge does not mask its condition.
3. Measure and record open-circuit voltage.
4. Perform a suitable capacity, conductance, or load test, or have a battery specialist do so.

An automatic charger can report “full” when a sulfated or high-resistance battery rapidly reaches the target voltage but stores little usable energy. Charger status and unloaded voltage are therefore not substitutes for a load or capacity test. A nominal 12 V lead-acid battery falling to approximately 9.5 V under ordinary logger load is severely discharged or defective.

### 6. Reconnect and verify the complete system

After correcting the fault:

1. Confirm correct polarity and secure every terminal.
2. Confirm daytime charging voltage.
3. Confirm operation shortly after sunset.
4. Confirm operation shortly before sunrise if possible.
5. Verify uninterrupted 15-minute records for at least two nights.
6. Review the daily minimum-voltage trend rather than relying on a single daytime reading.

## S3B incident example

S3B recorded data during solar-producing hours but stopped overnight. A battery wire had become disconnected, allowing solar power to operate the system while preventing dependable battery operation. The first replacement battery was reported as fully charged by an automatic charger but later supplied only approximately 9.5 V after dark. Replacing it with another battery restored the expected configuration. Final confirmation requires continuous overnight records and acceptable pre-dawn `BattV_Min` values over multiple nights.

The incident demonstrates that two faults can coexist: a wiring fault and a battery with inadequate usable capacity.

## Field record template

| Item | Result |
| --- | --- |
| Station / logger ID | |
| Date and local time | |
| Weather / temperature | |
| Last data timestamp | |
| Recent minimum `BattV_Min` | |
| Panel voltage, connected | |
| Panel voltage, open circuit (if permitted) | |
| Controller battery/output voltage | |
| Voltage at logger terminals | |
| Isolated rested battery voltage | |
| Battery load/capacity test | |
| Wiring/fuse observations | |
| Corrective action | |
| Verified after sunset | |
| Verified before sunrise | |
| Two-night data review completed | |

## Daily diagnostic/download command

Run from the repository with its Python environment active:

```bash
python -m biochar_app.pakbus.core.daily_download
```

Normal runs suppress packet-by-packet PyLink and PyCampbell messages while retaining concise station progress, retries, warnings, and errors. For a communications investigation, run the underlying client with `--log-level DEBUG` to restore the complete protocol trace.

The command first checks the PakBus TCP endpoint and downloads 24 hours from all 12 stations using isolated station processes and retries. If the first pass misses any stations, it waits two minutes and performs a second, more persistent pass for only those stations. It merges recovered rows without duplicates and then validates the complete result. Each run is retained under `biochar_app/data-raw/pakbus_daily/YYYY/MM/DD/`. The JSON report states `accepted`, `accepted_with_warnings`, or `rejected`. A rejected run exits non-zero and must not be passed to ETL.

When SES SMTP credentials are present, a rejected run sends one consolidated email to the addresses in `biochar_app/config/pipeline_alerts.json`. Recipient addresses and the sender are ordinary configuration; SMTP credentials must remain outside Git in `/etc/biochar/pipeline.env`:

```text
PAKBUS_HOST=the-current-ipv6-address
BIOCHAR_SMTP_HOST=email-smtp.us-east-2.amazonaws.com
BIOCHAR_SMTP_PORT=587
BIOCHAR_SMTP_USERNAME=the-ses-smtp-username
BIOCHAR_SMTP_PASSWORD=the-ses-smtp-password
```

Restrict that file to root and the service account. Do not paste its contents into an issue, log, email, or Git commit.

### Installing the midnight schedule on the test server

The repository includes `deployment/systemd/biochar-pakbus-daily.service` and `deployment/systemd/biochar-pakbus-daily.timer`. The timer runs at approximately 12:15 a.m. Mountain time and catches up after a server outage. Install it only after the command above succeeds interactively and SES SMTP credentials have been tested.

```bash
sudo cp deployment/systemd/biochar-pakbus-daily.service /etc/systemd/system/
sudo cp deployment/systemd/biochar-pakbus-daily.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now biochar-pakbus-daily.timer
systemctl list-timers biochar-pakbus-daily.timer
```

Test one scheduled-style execution before relying on the timer:

```bash
sudo systemctl start biochar-pakbus-daily.service
sudo systemctl status biochar-pakbus-daily.service
sudo journalctl -u biochar-pakbus-daily.service -n 200 --no-pager
```

## Slow or intermittent radio communication

A station can remain powered and reachable while taking substantially longer to download than comparable stations. A weak or noisy radio link can require repeated packet transmissions; this may appear as slow PC400 collection, incomplete PakBus responses, broken pipes, or success only after several attempts.

Compare the affected station with a nearby healthy logger while requesting the same table and time period. Record connection time, collection time, retries, and failures. If supported by the installed radio, record received signal strength (RSSI), the address associated with that RSSI reading, packet retries, and retry failures. For RSSI in dBm, a value closer to zero is stronger; compare stations under similar conditions rather than relying on a universal cutoff.

Inspect the affected station for:

- a loose antenna or connector;
- corrosion or water intrusion at exterior connectors;
- damaged, crushed, sharply bent, or excessively coiled antenna cable;
- an antenna that is tilted or oriented differently from the others;
- vegetation, equipment, soil piles, or metal objects obstructing the path;
- a radio powered outside its specified voltage range;
- mismatched radio network address, hopping sequence, protocol, baud rate, retry, or power settings;
- new nearby radio interference.

Do not change radio settings during the initial inspection. First photograph and record the settings and compare them with a working station. If the antenna and cable are compatible, a temporary swap with a known-good unit is a strong diagnostic test: improvement that follows the antenna/cable implicates that component.

PC400 live monitoring proves that a route can be established, but it does not prove that a complete Table1 transfer is reliable. Confirm by collecting the same Table1 interval and reviewing the transaction/communications diagnostics.

## References

- Campbell Scientific, *CR800/CR850 Measurement and Control System Operator's Manual*.
- Campbell Scientific manual for the installed charging regulator or power supply.
- Battery manufacturer's charging, capacity-testing, and safety instructions.

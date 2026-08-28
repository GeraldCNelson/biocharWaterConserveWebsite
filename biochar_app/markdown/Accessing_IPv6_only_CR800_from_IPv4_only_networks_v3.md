# Accessing the IPv6 CR800 Network from Any Internet Connection

_Updated: August 7, 2026_

## Purpose

This document describes how to use Campbell Scientific PC400 to reach the Fruita CR800/PakBus network from home, hotels, IPv4-only networks, and a Windows VM running under Parallels.

It also documents the August 2026 outage in which the ASUS router stopped providing the CR800 with a global IPv6 address, plus the Windows one-click connection selector created afterward.

## Current network values

| Item | Current value |
|---|---|
| CR800 global IPv6 | `2605:59ca:227f:e00:2d0:2cff:fe02:1ddd` |
| CR800 link-local IPv6 | `fe80::2d0:2cff:fe02:1ddd` |
| CR800 configured IPv4 | `192.164.50.93` — incorrect public-range address; correct onsite |
| Intended CR800 local IPv4 | `192.168.50.93` |
| ASUS LAN IPv4 | `192.168.50.1` |
| CR800 PakBus/TCP port | `6785` |
| Lightsail public IPv4 | `18.119.131.201` |
| Lightsail SSH user | `ubuntu` |
| Windows SSH key | `%USERPROFILE%\.ssh\lightsail_win_ed25519` |
| ASUS remote management | `https://Biochar-Fruita.asuscomm.com:8443` |

The global IPv6 address is generated from the ASUS LAN prefix and the Campbell MAC address `00:D0:2C:02:1D:DD`. Confirm it in the ASUS client list if the ISP prefix ever changes.

## PC400 rule

Keep every PC400 station configured as:

```text
127.0.0.1:6785
```

Do not configure PC400 with the CR800 IPv6 address directly. The Windows connection selector decides whether port `127.0.0.1:6785` uses direct IPv6 or the Lightsail SSH relay.

## The two connection paths

### Direct IPv6 path

```text
PC400
  -> 127.0.0.1:6785
  -> Windows IP Helper v4-to-v6 port proxy
  -> [CR800 IPv6]:6785
```

The persistent Windows rule is:

```text
127.0.0.1:6785 -> [2605:59ca:227f:e00:2d0:2cff:fe02:1ddd]:6785
```

This path works only when:

1. Windows itself has a usable IPv6 route. IPv6 on the Mac host does not prove that the Parallels Windows VM has IPv6.
2. The local network allows outbound IPv6 TCP port `6785`.
3. The CR800 has its global `2605:...` address.

A 10/10 result at test-ipv6.com verifies browser connectivity to selected test servers. It does not prove that the Windows VM can open arbitrary IPv6 TCP port `6785` connections.

### Lightsail SSH fallback

```text
PC400
  -> 127.0.0.1:6785
  -> SSH over IPv4 to Lightsail
  -> [CR800 IPv6]:6785
```

This path avoids dependence on local IPv6 and arbitrary outbound port `6785`. It requires outbound SSH to Lightsail.

## Normal one-click workflow

The PowerShell selector is stored in Windows at:

```text
C:\CampbellSci\Scripts\Start-PC400Connection.ps1
```

The Windows shortcut named **Start PC400 Connection** runs:

```text
C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\CampbellSci\Scripts\Start-PC400Connection.ps1"
```

The shortcut is configured under **Properties > Shortcut > Advanced** to **Run as administrator**.

### To collect data

1. Double-click **Start PC400 Connection**.
2. Approve the Windows User Account Control prompt.
3. Wait for the connection test.
4. If direct IPv6 works, the script restores the IP Helper rule and reports that PC400 is ready at `127.0.0.1:6785`. No tunnel window needs to remain open.
5. If direct IPv6 fails, the script removes the conflicting Helper rule and starts the Lightsail SSH tunnel on the same local port. It reports:

   ```text
   PC400 remains configured as 127.0.0.1:6785.
   Leave this window open while using PC400.
   ```

6. Open PC400 and connect normally.
7. Try **Check Clock** before collecting data.
8. Collect the desired logger data.

### When finished in SSH fallback mode

1. Disconnect and close PC400.
2. Return to the PowerShell tunnel window.
3. If the title begins with **Select**, press `Esc` first. In selection mode, `Ctrl+C` copies text instead of stopping SSH.
4. Press `Ctrl+C` once.
5. Wait for:

   ```text
   Restoring the direct Windows IP Helper rule...
   The direct rule for 127.0.0.1:6785 has been restored.
   ```

Avoid closing the tunnel window with the `X`. An abrupt close may prevent restoration. If that happens, run the selector again; it repairs the appropriate `6785` configuration.

## Manual SSH fallback

If the selector script is unavailable, first remove the IP Helper rule from an Administrator PowerShell session so SSH can bind local port `6785`:

```powershell
netsh interface portproxy delete v4tov6 `
  listenaddress=127.0.0.1 `
  listenport=6785
```

Start the tunnel and leave the window open:

```powershell
ssh -4 -N `
  -o ExitOnForwardFailure=yes `
  -o ServerAliveInterval=20 `
  -o ServerAliveCountMax=3 `
  -o TCPKeepAlive=yes `
  -o IPQoS=none `
  -L "127.0.0.1:6785:[2605:59ca:227f:e00:2d0:2cff:fe02:1ddd]:6785" `
  -i "$env:USERPROFILE\.ssh\lightsail_win_ed25519" `
  ubuntu@18.119.131.201
```

When finished, restore the direct rule from Administrator PowerShell:

```powershell
netsh interface portproxy add v4tov6 `
  listenaddress=127.0.0.1 `
  listenport=6785 `
  connectaddress=2605:59ca:227f:e00:2d0:2cff:fe02:1ddd `
  connectport=6785
```

## Diagnostics

### 1. Check the ASUS client list

Open the ASUS remote-management page and choose **Network Map > View List**.

The Campbell client should show:

```text
MAC: 00:D0:2C:02:1D:DD
Global IPv6: 2605:59ca:227f:e00:2d0:2cff:fe02:1ddd
Link-local IPv6: fe80::2d0:2cff:fe02:1ddd
```

If it shows only `fe80::...`, the CR800 does not currently have a globally routable IPv6 address. Neither direct IPv6 nor the Lightsail relay can reach it.

### 2. Test from Lightsail

Log in:

```bash
ssh biochar-test-fetch
```

Test IPv6:

```bash
ping -6 -c 4 2605:59ca:227f:e00:2d0:2cff:fe02:1ddd
```

Test the actual PakBus/TCP service:

```bash
nc -6 -vz -w 10 2605:59ca:227f:e00:2d0:2cff:fe02:1ddd 6785
```

Expected:

```text
Connection to ... 6785 port [tcp/*] succeeded!
```

Ping alone is not enough. Port `6785` must succeed.

### 3. Test direct IPv6 from Windows

Run this inside the Windows VM, not on the Mac or Lightsail:

```powershell
Test-NetConnection `
  -ComputerName 2605:59ca:227f:e00:2d0:2cff:fe02:1ddd `
  -Port 6785 `
  -InformationLevel Detailed
```

Interpretation:

- Ping and TCP succeed: direct IP Helper mode should work.
- Ping succeeds but TCP fails: the local network may block outbound port `6785`.
- Both fail: Windows/Parallels likely lacks a usable direct IPv6 route.

### 4. Identify the local port owner

```powershell
netstat -ano | findstr :6785
```

Then:

```powershell
Get-Process -Id <PID>
```

- `svchost` with service `iphlpsvc` means the direct Windows port-proxy rule owns `6785`.
- `ssh` means the Lightsail tunnel owns `6785`.

To identify the service behind `svchost`:

```powershell
tasklist /svc /FI "PID eq <PID>"
```

View persistent proxy rules:

```powershell
netsh interface portproxy show all
```

### 5. Local listener caveat

```powershell
Test-NetConnection 127.0.0.1 -Port 6785
```

`TcpTestSucceeded : True` proves only that something is listening locally. It does not prove that the listener can reach the CR800. Always identify the owning process and, when troubleshooting, test from Lightsail or run SSH verbosely.

## August 2026 outage and recovery

### Symptoms

- ASUS still showed the Campbell MAC and a wired Ethernet link.
- The CR800 client entry showed only its link-local `fe80::` address.
- Its former `2605:...` address disappeared.
- Lightsail ping reached the ASUS router, which returned:

  ```text
  Destination unreachable: Address unreachable
  ```

- Power-cycling the Campbell equipment did not restore global IPv6.
- Reapplying unchanged ASUS IPv6 settings did not restore it.

### ASUS settings

The settings were correct and matched the previously working March configuration:

```text
Connection type: Native
DHCP-PD: Enable
Auto Configuration: Stateless
Router Advertisement: Enable
```

### Resolution

A normal ASUS router reboot restored the CR800 global IPv6 address immediately. Lightsail ping and TCP port `6785` then succeeded.

The likely failure was a stalled ASUS IPv6 router-advertisement or neighbor-discovery service. The ASUS firmware had changed from `3.0.0.6.102_33421` in March to `3.0.0.6.102_37436` in August. This does not prove a firmware defect, but the version change is relevant if the problem recurs.

### Recovery sequence if global IPv6 disappears again

1. Confirm that the ASUS client list shows only `fe80::...` for the Campbell client.
2. Reboot the ASUS router normally. Do not factory-reset it.
3. Wait 5–10 minutes for Internet and remote management to return.
4. Check the ASUS client list for the `2605:...` address.
5. If necessary, power-cycle the complete Campbell assembly after the router is fully operational.
6. Retest ping and port `6785` from Lightsail.

## Incorrect CR800 IPv4 configuration

The Campbell interface reports a static IPv4 address of:

```text
192.164.50.93
```

This is public IPv4 space and is not on the ASUS `192.168.50.0/24` LAN. The correct onsite configuration should be:

```text
Address: 192.168.50.93
Mask: 255.255.255.0
Gateway: 192.168.50.1
```

Correct this onsite with Campbell Device Configuration Utility. Do not change the ASUS LAN to `192.164.50.x`, and do not factory-reset the CR800 or router.

## Longer-term direction

PC400 manual collection is now usable from varied networks, but it still requires a Windows computer and operator intervention.

The preferred long-term design is:

```text
Lightsail scheduled collector
  -> CR800/PakBus network over IPv6
  -> data-raw files on Lightsail
  -> ETL for newly collected logger data
  -> relevant weather-data refresh
```

Keeping collection and ETL on Lightsail removes dependence on PC400, Parallels, the operator's location, and hotel-network policies.

# Operator Tools — Biochar Fruita CSU

This directory contains **operator-side scripts** required to access
field equipment for the Biochar Fruita CSU project.

These scripts are **not part of the web application** and should be run
locally by operators.

---

## Windows

### connect_cr800.ps1
Creates an SSH tunnel so PC400 / PC100 (Windows-only) can access the
IPv6-only CR800 datalogger via `127.0.0.1:6785`.

**Usage**
1. Open PowerShell
2. Run:
   ```powershell
   .\connect_cr800.ps1
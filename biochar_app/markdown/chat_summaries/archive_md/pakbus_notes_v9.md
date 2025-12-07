# PakBus Error and Fix Log

## Version v9 (2025-10-17)

- **Logging errors: “ValueError: incomplete format key”**  
  Switched the root logger to `{}`-style formatting (`logging.basicConfig(..., style='{' )`) to avoid stray `%` characters in packet-dump log messages breaking the formatter.

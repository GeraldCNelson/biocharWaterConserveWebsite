# PakBus packet-forensics history

**Recorded:** 2026-10-16

**Status:** Historical engineering note. The packet-capture and replay tools
described here are not supported operating procedures.

## Why this work existed

Before the current PakBus downloader was completed, PC400 Serial Port Monitor
(`.spm`) sessions and Wireshark captures were inspected to understand the
traffic between the Campbell Scientific base station and the field loggers.
Experimental scripts tried HTTP retrieval, direct serial collection, captured
packet decoding, and replay of requests observed in those sessions.

Those experiments are no longer needed for routine acquisition. The current
downloader reaches the stations through PakBus/TCP, validates the response,
writes auditable run output, and maintains accepted archives. The supported
operator interface is [`tools/pakbus-download`](../../../tools/pakbus-download),
implemented by [`pakbus/core`](../../pakbus/core/).

## Durable findings

The experiments helped confirm several details now represented in maintained
code and tests:

- remote PakBus traffic uses TCP port 6785;
- PakBus frames observed on the wire use `0xBD` framing, with escaping handled
  by the maintained transport implementation;
- Campbell timestamps use the 1990-01-01 epoch; and
- this project's Table1 records have a fixed known layout, so decoding should
  use that schema rather than search arbitrary packet bytes for plausible
  floating-point values.

The retired scripts also attempted heuristic magnitude matching. That was
useful while reconstructing the protocol, but it is not an authoritative data
recovery method. Current recovery should use the bounded archive and comparison
functions exposed through `tools/pakbus-download`.

## Retention boundary

The historical source programs were removed from the active tree after the
maintainer confirmed that PC400 downloading, Wireshark/PC400 capture analysis,
HTTP retrieval, and SPM-session inspection are not current workflows. Their
exact contents remain recoverable through Git history.

Local ignored `.pcapng`, `.raw`, `.hex`, and `.spm` evidence is outside
this source cleanup. It was not deleted or modified. Such files should be
retained or removed under a separate data-retention decision, not as part of a
code cleanup.

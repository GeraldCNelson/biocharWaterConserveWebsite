# Historical PakBus code review

**Review date:** 2026-10-16

**Branch:** `etl-refactor`

**Status:** Evidence review complete; no second-round deletions have been made.

## Decision summary

The supported PakBus system has a clear boundary:

- `biochar_app/pakbus/core/` contains the current client, archive, recovery,
  gateway-discovery, and daily-download implementation;
- `tools/pakbus-download` is the supported operator interface;
- `deployment/systemd/biochar-pakbus-daily.service` runs the scheduled download;
  and
- five focused PakBus test modules exercise the current client, transport,
  archive, ETL integration, and daily process.

The older code outside that boundary is not one coherent fallback system. It is
a sequence of packet-replay, capture-decoding, HTTP, serial, and third-party
experiments accumulated while the present downloader was being developed. Some
files cannot run because their dependencies are empty or missing. Others encode
useful historical observations but duplicate capabilities that are now tested
in `pakbus/core`.

The safest next cleanup is therefore two-stage:

1. remove the mechanically broken packages and commands after confirming that
   no maintainer still invokes them from an external notebook or shell alias;
2. preserve a short packet-forensics note before retiring the remaining capture
   and replay experiments.

## Inventory and evidence

| Area | Tracked files | Evidence | Disposition |
|---|---:|---|---|
| `pakbus/core` | 9 | Used by the supported wrapper, scheduled service, ETL, and tests. | Keep active. |
| `pakbus/scripts` | 28 | Mostly one-off 2025 packet probes, replay variants, decoders, and partially reconstructed downloaders. No scheduled service or supported command imports them. | Retire in reviewed batches. |
| `pakbus/examples` | 6 | Four Python files are 2009 GPL example code written for Python 2 and an external `pakbus` module; `pakbus.conf` belongs to those examples. The sixth file imports a module that moved and is not runnable as written. | Delete after recording provenance in this review. |
| `pakbus/scripts_zsh` | 4 | The three commands call missing `spm_*` modules or a missing `spm_pyhelper.py`; one is tied to a named 2025 monitoring session. | Delete as a mechanically broken batch. |
| `pakbus/utils` | 4 | `crc.py`, `frame.py`, and `hex.py` are empty, but four historical scripts import functions from them. | Delete together with those dependents. |
| Root `tools/` | 16 | Two wrappers are supported. Thirteen PakBus-era tools form an older fetch/decode/resample/trust pipeline. `extract_management_data.py` belongs to the separate management-builder review. | Keep the two supported wrappers; review or retire the older chain. |

Function-catalog references are generated inventory entries, not evidence that a
command is supported. Archived chat summaries are also historical evidence, not
an operational interface.

## Mechanically broken batch

The following files cannot work in the current checkout without reconstruction.
They are stronger retirement candidates than merely unreferenced scripts.

### Empty utility package and direct dependents

The implementations in `pakbus/utils/crc.py`, `frame.py`, and `hex.py` are empty.
These commands import missing names from those files:

- `pakbus/scripts/fetch_table1_live.py`
- `pakbus/scripts/pakbus_get_fields.py`
- `pakbus/scripts/replay_after_hello.py`
- `pakbus/scripts/replay_bd_frame.py`

The current framing, signature, transport, and collection implementations live
under `pakbus/core`; restoring the empty package would create a second protocol
implementation.

### Missing modules or invalid repository paths

- `pakbus/scripts/ingest_runner.py` calls the nonexistent
  `pakbus/send_catalog_request.py` and calculates its repository root from the
  scripts directory, producing invalid nested paths.
- `pakbus/scripts/serial_collect_tail.py` imports
  `biochar_app.pakbus.cr200_client_utils`, although the maintained module is
  `biochar_app.pakbus.core.cr200_client_utils`.
- `pakbus/scripts_zsh/collect_run.zsh` and `spm_quickcheck.zsh` invoke three
  nonexistent `biochar_app.pakbus.spm_*` modules.
- `pakbus/scripts_zsh/spm_analyze_all.zsh` calls the nonexistent
  `biochar_app/pakbus/spm_pyhelper.py`.
- `tools/make_tsvs.py` calls the nonexistent
  `biochar_app/pakbus/send_catalog_request.py`.

These are not emergency fallbacks in their current form. If direct-serial or
SPM-session recovery is still required, it should become a tested command under
the supported wrapper rather than keeping commands that fail at import or at
their first subprocess call.

### External Python 2 examples

`pakbus/examples/bintools.py`, `show_clock.py`, `show_files.py`, and
`show_progstat.py` are GPL example code attributed to Dietrich Feist (2009).
They use Python 2 syntax and modules such as `ConfigParser`, `StringIO`, and an
external top-level `pakbus` package. `pakbus.conf` belongs to those examples.
`show_tabledef.py` is newer but imports the old module location. None is part of
the current application or operator workflow.

## Superseded live-download experiments

The following scripts contain hard-coded frame shapes, addresses, output paths,
or experimental assumptions developed before the supported client stabilized:

- `bd_getdata_focus_downloader.py`
- `bd_getdata_probe.py`
- `bd_minimal_getdata_v8.py`
- `bd_read_table1_downloader.py`
- `bd_replay_raw_v2.py`
- `fetch_http_last.py`
- `fetch_leaf_records.py`
- `fetch_leaf_records_bd.py`
- `fetch_s1b.py`
- `forge_and_fetch_table1.py`
- `pakbus_collect00_latest.py`
- `probe_and_pull.py`
- `probe_bd_field_offsets.py`
- `replay_bd_frames_capture_reads.py`
- `replay_bd_frames_no_crc.py`
- `replay_bd_frames_v1.py`

The current client now provides bounded Table1 retrieval, explicit record-range
recovery, fixed-layout decoding, retries, station timing, continuity checks, and
accepted-run promotion. Keeping the experimental live commands beside it makes
it difficult for a maintainer to identify the safe interface.

`fetch_leaf_records.py` deserves particular caution: its own documentation says
that it parses a leaf identifier but ignores it for routing. It should not be
presented as a recovery command for the leaf loggers.

## Capture and decoding tools

This group may contain the only knowledge not fully represented by the supported
operator interface:

- `pakbus/scripts/pcap_pakbus_miner.py`
- `pakbus/scripts/decode_hex_candidates.py`
- `pakbus/scripts/decode_bd_data_frames.py`
- `pakbus/scripts/decode_table1_from_bd.py`
- `pakbus/scripts/compare_table1_magnitudes.py`
- `tools/export_frames.py`
- `tools/extract_bd_frames.py`
- `tools/inspect_payloads.py`
- `tools/decode_bd_frames.py`

They document how PC400/Wireshark captures were mined for `0xBD`-delimited
frames and how candidate records were recognized using the Campbell 1990 epoch
and ten big-endian float fields. The maintained client now implements the fixed
Table1 decoder directly, and `tools/pakbus-download compare` performs the
supported comparison with Campbell Scientific `.dat` files.

Before deleting this group, preserve one concise engineering note containing:

- the provenance and purpose of the packet capture;
- the frame delimiter and record-layout observations that were confirmed;
- the command needed to inspect a future capture, if packet capture remains a
  supported diagnostic; and
- a warning that heuristic magnitude matching is diagnostic evidence, not an
  authoritative data-recovery method.

There are local ignored `.pcapng`, `.raw`, and `.hex` files under
`pakbus/pakbus_data/bdFiles`. They were not modified by this review. Raw capture
evidence needs an explicit external archive and retention decision before local
copies are removed.

## Older root-tool pipeline

The following tools form a pre-core workflow for fetching, decoding,
resampling, and comparing logger records:

- `fetch_cr206x_via_gateway.py`
- `fetch_cr800_decoded.py`
- `fetch_all_leaves.zsh`
- `list_sites.py`
- `pipeline.zsh`
- `resample_and_trust.zsh`
- `resample_decoded.py`
- `trust_check.py`

They write under the historical `pakbus/bdFiles/out_fetch` layout and use
`config/sites.yaml`, whereas the supported command writes auditable run
directories and accepted archives. The current archive comparison supersedes
the old trust-check role. `fetch_all_leaves.zsh` also calls its Python fetcher
through an incorrect relative path when run from the repository root.

The HTTP fetcher is the one possible exception: it represents a transport
fallback different from PakBus/TCP. Retain it only if the Campbell base station
still exposes the documented HTTP data-query endpoint and maintainers want that
fallback supported. If so, it should be moved behind `tools/pakbus-download`,
given a normalized output contract, and tested. Otherwise the entire older
root-tool chain can be retired after the packet-forensics note is written.

## Configuration and operational notes

`config/sites.yaml` is referenced only by the older root-tool chain. It contains
an outdated gateway URL and a nested logger block under a single leaf, so it
should not be mistaken for current authoritative station configuration. Retire
or rename it with that toolchain; do not merge it into current configuration
without a field-by-field comparison.

`pakbus/asus_ipv6_firewall_rule.txt` is different: it records the reason the
gateway address changes and the network checks used by current gateway
discovery. Its durable content should move into the maintained operations
documentation, after which the standalone text file can be retired.

## Recommended next decisions

1. Confirm whether direct-serial collection, HTTP retrieval, or SPM-session
   inspection is still performed manually in the field.
2. Confirm whether future Wireshark/PC400 packet capture should remain a
   supported diagnostic. If yes, retain one tested offline inspection command;
   if no, preserve only the engineering note.
3. Approve deletion of the mechanically broken package, examples, shell
   scripts, and missing-module callers as one reviewable commit.
4. Approve retirement of the superseded live-download experiments as a second
   commit after the packet-forensics note exists.
5. Review the older root-tool pipeline separately from
   `tools/extract_management_data.py`, which belongs to the management-data
   builder comparison.

No directory refactor is needed to make these decisions. Removing or retaining
capabilities first will leave a much smaller, clearer PakBus structure to move.

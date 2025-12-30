#!/usr/bin/env python3
"""
decode_hex_candidates.py

Scan a hex dump for candidate ETF-like records that look like:

    [4-byte timestamp] + [N × 4-byte floats]

The timestamp can be:
  - Unsigned 32-bit int (seconds since epoch)
  - 32-bit float (seconds since epoch)

Epochs supported:
  - "unix"     → seconds since 1970-01-01
  - "campbell" → seconds since 1990-01-01 (Campbell Scientific-style)

We brute-force:
  - Endianness: big (">") and little ("<")
  - Timestamp type: uint32 or float32
  - Number of floats: between --min-floats and --max-floats

For each plausible combination, we output a CSV of candidates.

Usage example:

    python -m biochar_app.pakbus.scripts.decode_hex_candidates \
      --hex-file rx_dump_bd.hex \
      --epoch campbell \
      --min-year 2024 \
      --max-year 2026 \
      --out candidates_rx_dump_bd.csv
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as _dt
import math
import struct
import textwrap
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# Robust hex loading
# ---------------------------------------------------------------------------

def load_hex_file(path: Path) -> bytes:
    """
    Load a hex dump from `path` and return the concatenated raw bytes.

    This is robust to "annotated" dumps that contain things like:

        # comment
        Frame 18B: bdaffd70010ffd00010abe00be200420...

    Strategy:
      1. Read the file as text.
      2. Split into whitespace-separated tokens.
      3. For each token:
           - Strip "0x" prefix if present.
           - Trim non-hex characters from both ends.
           - Discard if any non-hex remains.
           - If length is odd, drop the last nibble.
      4. Concatenate the cleaned tokens and feed to bytes.fromhex().

    If nothing looks hex-like, this will raise a ValueError from bytes.fromhex,
    which is actually useful feedback.
    """
    text = path.read_text()
    # Normalize whitespace and split into tokens
    tokens = text.replace("\r", " ").replace("\n", " ").split()

    hex_chunks: List[str] = []
    hexdigits = "0123456789abcdefABCDEF"

    for tok in tokens:
        original = tok

        # Strip leading "0x" if present (common in some dumps)
        if tok.lower().startswith("0x"):
            tok = tok[2:]

        # Trim non-hex chars from left & right (e.g., "Frame", "18B:", "#---")
        start = 0
        while start < len(tok) and tok[start] not in hexdigits:
            start += 1
        end = len(tok)
        while end > start and tok[end - 1] not in hexdigits:
            end -= 1

        tok = tok[start:end]
        if not tok:
            continue

        # If token still has non-hex chars in the middle, discard it
        if any(c not in hexdigits for c in tok):
            continue

        # bytes.fromhex requires an even number of hex digits
        if len(tok) % 2 == 1:
            tok = tok[:-1]
        if not tok:
            continue

        hex_chunks.append(tok)

    hex_str = "".join(hex_chunks)
    if not hex_str:
        raise ValueError(
            f"No hex-like tokens found in {path}. "
            "Is this actually a hex dump?"
        )

    raw = bytes.fromhex(hex_str)

    print(
        f"[INFO] Parsed hex file {path} → "
        f"{len(tokens)} tokens → {len(hex_chunks)} hex chunks → {len(raw)} bytes"
    )
    return raw


# ---------------------------------------------------------------------------
# Candidate search
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class CandidateHit:
    offset: int
    epoch: str
    endian: str           # ">" or "<"
    ts_type: str          # "uint32" or "float32"
    ts_raw: float
    timestamp: _dt.datetime
    n_floats: int
    floats: Tuple[float, ...]


def get_epoch_base(name: str) -> _dt.datetime:
    name = name.lower()
    if name == "unix":
        return _dt.datetime(1970, 1, 1)
    if name == "campbell":
        # Campbell "seconds since 1990-01-01"
        return _dt.datetime(1990, 1, 1)
    raise ValueError(f"Unknown epoch: {name!r}")


def decode_timestamp(
    raw: bytes,
    offset: int,
    endian: str,
    ts_type: str,
    epoch_base: _dt.datetime,
) -> Optional[Tuple[float, _dt.datetime]]:
    """
    Decode a 4-byte timestamp at `raw[offset:offset+4]` according to:
      - endian: ">" or "<"
      - ts_type: "uint32" or "float32"

    Returns (ts_raw, timestamp) or None if invalid.
    """
    if offset + 4 > len(raw):
        return None

    chunk = raw[offset:offset + 4]

    try:
        if ts_type == "uint32":
            val = struct.unpack(f"{endian}I", chunk)[0]
            # Interpret as seconds since epoch_base
            # Require positive and not absurdly huge
            if val <= 0 or val > 2_000_000_000:
                return None
            ts_raw = float(val)
        elif ts_type == "float32":
            val = struct.unpack(f"{endian}f", chunk)[0]
            if not math.isfinite(val) or val <= 0 or val > 2_000_000_000:
                return None
            ts_raw = float(val)
        else:
            raise ValueError(f"Unknown ts_type {ts_type!r}")
    except Exception:
        return None

    try:
        ts = epoch_base + _dt.timedelta(seconds=ts_raw)
    except OverflowError:
        return None

    return ts_raw, ts


def decode_float_run(
    raw: bytes,
    offset: int,
    endian: str,
    n_floats: int,
    value_min: float = -1_000.0,
    value_max: float = 1_000.0,
) -> Optional[Tuple[float, ...]]:
    """
    Decode `n_floats` from raw[offset:offset+4*n_floats] using `endian`.
    Reject if any are non-finite or outside [value_min, value_max].
    """
    start = offset
    end = offset + 4 * n_floats
    if end > len(raw):
        return None

    floats: List[float] = []
    for i in range(n_floats):
        chunk = raw[start + 4 * i: start + 4 * (i + 1)]
        try:
            val = struct.unpack(f"{endian}f", chunk)[0]
        except Exception:
            return None

        if not math.isfinite(val):
            return None
        if not (value_min <= val <= value_max):
            return None

        floats.append(float(val))

    return tuple(floats)


def find_candidates(
    raw: bytes,
    epoch: str,
    min_year: int,
    max_year: int,
    min_floats: int,
    max_floats: int,
) -> Iterable[CandidateHit]:
    """
    Scan the raw byte stream for candidate ETF-like records.

    For each offset we try:
      - endian ∈ {">", "<"}
      - ts_type ∈ {"uint32", "float32"}
      - n_floats ∈ [min_floats, max_floats]

    Records are:

      [4-byte timestamp] + [n_floats × 4-byte float]

    We apply:
      - Year filter on decoded timestamp
      - Sanity filter on float values [-1000, 1000]
    """
    base = get_epoch_base(epoch)
    start_year = min(min_year, max_year)
    end_year = max(min_year, max_year)

    ts_types = ("uint32", "float32")
    endians = (">", "<")

    total_bytes = len(raw)
    checked_offsets = 0
    hits = 0

    for offset in range(0, total_bytes - 4):  # need at least 4 bytes for ts
        checked_offsets += 1

        for endian in endians:
            for ts_type in ts_types:
                ts_info = decode_timestamp(raw, offset, endian, ts_type, base)
                if ts_info is None:
                    continue

                ts_raw, ts_dt = ts_info
                if ts_dt.year < start_year or ts_dt.year > end_year:
                    continue

                # Timestamp looks plausible. Now try float runs after it.
                floats_offset = offset + 4
                for n_floats in range(min_floats, max_floats + 1):
                    vals = decode_float_run(raw, floats_offset, endian, n_floats)
                    if vals is None:
                        continue

                    hits += 1
                    yield CandidateHit(
                        offset=offset,
                        epoch=epoch,
                        endian=endian,
                        ts_type=ts_type,
                        ts_raw=ts_raw,
                        timestamp=ts_dt,
                        n_floats=n_floats,
                        floats=vals,
                    )

        # Optional: light progress indicator for large files
        if checked_offsets % 100_000 == 0:
            print(
                f"[INFO] Scanned offsets up to {checked_offsets}/{total_bytes} "
                f"({checked_offsets / total_bytes:.1%}), hits so far: {hits}"
            )


# ---------------------------------------------------------------------------
# CSV writer and CLI
# ---------------------------------------------------------------------------

def write_candidates_csv(path: Path, hits: Sequence[CandidateHit]) -> None:
    """
    Write candidate hits to CSV.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "offset",
        "epoch",
        "endian",
        "ts_type",
        "ts_raw",
        "timestamp_iso",
        "n_floats",
        "floats",
    ]

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for hit in hits:
            writer.writerow(
                {
                    "offset": hit.offset,
                    "epoch": hit.epoch,
                    "endian": hit.endian,
                    "ts_type": hit.ts_type,
                    "ts_raw": f"{hit.ts_raw:.1f}",
                    "timestamp_iso": hit.timestamp.isoformat(sep=" "),
                    "n_floats": hit.n_floats,
                    "floats": " ".join(f"{v:.6g}" for v in hit.floats),
                }
            )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scan a hex dump for ETF-like timestamp+float patterns.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples
            --------
            1) Campbell seconds since 1990, looking for 10-ish floats:

                python -m biochar_app.pakbus.scripts.decode_hex_candidates \\
                  --hex-file rx_dump_bd.hex \\
                  --epoch campbell \\
                  --min-year 2024 \\
                  --max-year 2026 \\
                  --out candidates_rx_dump_bd.csv

            2) UNIX seconds since 1970, 6–12 floats:

                python -m biochar_app.pakbus.scripts.decode_hex_candidates \\
                  --hex-file some_capture.hex \\
                  --epoch unix \\
                  --min-year 2020 \\
                  --max-year 2030 \\
                  --min-floats 6 \\
                  --max-floats 12 \\
                  --out candidates_some_capture.csv
            """
        ),
    )

    p.add_argument(
        "--hex-file",
        required=True,
        type=Path,
        help="Path to hex dump file (can include comments/labels).",
    )
    p.add_argument(
        "--epoch",
        choices=["unix", "campbell"],
        default="campbell",
        help="Epoch definition for timestamps (default: campbell).",
    )
    p.add_argument(
        "--min-year",
        type=int,
        default=2020,
        help="Minimum acceptable year for timestamps (inclusive).",
    )
    p.add_argument(
        "--max-year",
        type=int,
        default=2030,
        help="Maximum acceptable year for timestamps (inclusive).",
    )
    p.add_argument(
        "--min-floats",
        type=int,
        default=6,
        help="Minimum number of floats in a candidate run (default 6).",
    )
    p.add_argument(
        "--max-floats",
        type=int,
        default=12,
        help="Maximum number of floats in a candidate run (default 12).",
    )
    p.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output CSV file for candidate hits.",
    )

    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"🔍 Loading hex dump from {args.hex_file} ...")
    raw = load_hex_file(args.hex_file)

    print(
        f"🔎 Scanning {len(raw)} bytes "
        f"(epoch={args.epoch}, years={args.min_year}-{args.max_year}, "
        f"floats={args.min_floats}-{args.max_floats}) ..."
    )

    hits = list(
        find_candidates(
            raw=raw,
            epoch=args.epoch,
            min_year=args.min_year,
            max_year=args.max_year,
            min_floats=args.min_floats,
            max_floats=args.max_floats,
        )
    )

    print(f"✅ Found {len(hits)} candidate records.")
    if hits:
        print(f"💾 Writing candidates to {args.out} ...")
        write_candidates_csv(args.out, hits)
        print("✅ Done.")
    else:
        print("⚠️ No candidates found; CSV will not be very exciting, but script completed.")


if __name__ == "__main__":
    main()
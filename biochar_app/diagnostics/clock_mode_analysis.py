#!/usr/bin/env python3
"""
biochar_app/diagnostics/clock_mode_analysis.py

1) Verify seasonal misalignment hypotheses (e.g., S2M "always MDT") by measuring
   timestamp overlap improvement vs a reference logger when applying seasonal ±1h shifts.

2) Classify each logger into a controlled vocabulary:
   - seasonal_manual
   - always_mdt
   - always_mst
   - irregular_manual
   - unknown

Reads:
  biochar_app/data-processed/parquet/<year>/<year>_raw_logger.parquet
(or whatever PARQUET_DIR points to in config)

Does NOT modify raw data; emits a console report and (optional) CSV in diagnostics/reports.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from biochar_app.scripts.config import PARQUET_DIR, STRIPS, LOGGER_LOCATIONS  # type: ignore


# ----------------------------- IO ----------------------------- #

def _read_raw_logger_parquet(year: int) -> pd.DataFrame:
    path = Path(PARQUET_DIR) / str(year) / f"{year}_raw_logger.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Raw logger parquet not found for {year}: {path}")
    df = pd.read_parquet(path)

    if "timestamp" not in df.columns:
        raise ValueError(f"{path.name}: expected a 'timestamp' column.")

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].astype("datetime64[ns]")
    return df


# ----------------------------- DST boundaries ----------------------------- #

@dataclass(frozen=True)
class DstWindow:
    tz: str
    dst_start_local: pd.Timestamp  # tz-aware (America/Denver)
    dst_end_local: pd.Timestamp    # tz-aware
    dst_start_naive: pd.Timestamp  # naive wall time
    dst_end_naive: pd.Timestamp    # naive wall time


def _find_denver_dst_window(year: int, tz: str = "America/Denver") -> DstWindow:
    """
    Find the two DST transitions for a given year in America/Denver by scanning hourly.
    Returns (dst_start, dst_end) for that year as both tz-aware and naive wall-times.
    """
    start = pd.Timestamp(f"{year}-01-01 00:00:00", tz=tz)
    end = pd.Timestamp(f"{year+1}-01-15 00:00:00", tz=tz)
    rng = pd.date_range(start, end, freq="h")

    # dst() is a Timedelta: 0 in standard time, 1 hour in DST.
    dst = rng.map(lambda x: x.dst())  # type: ignore[attr-defined]
    dst_hours = pd.Series(dst).dt.total_seconds().to_numpy() / 3600.0

    change_idx = np.where(np.diff(dst_hours) != 0)[0]
    if change_idx.size < 2:
        raise ValueError(f"Could not find both DST transitions for year {year} in tz={tz}")

    i_spring = int(change_idx[0] + 1)
    i_fall = int(change_idx[1] + 1)

    dst_start = rng[i_spring]
    dst_end = rng[i_fall]

    return DstWindow(
        tz=tz,
        dst_start_local=dst_start,
        dst_end_local=dst_end,
        dst_start_naive=dst_start.tz_localize(None),
        dst_end_naive=dst_end.tz_localize(None),
    )


def _is_mst_window_naive(ts: pd.Series, dstw: DstWindow) -> pd.Series:
    """
    Within a single year's data, treat MST (standard-time) window as:
      timestamps < dst_start OR >= dst_end
    (All naive wall-time.)
    """
    return (ts < dstw.dst_start_naive) | (ts >= dstw.dst_end_naive)


def _is_mdt_window_naive(ts: pd.Series, dstw: DstWindow) -> pd.Series:
    return (ts >= dstw.dst_start_naive) & (ts < dstw.dst_end_naive)


# ----------------------------- Logger column helpers ----------------------------- #

def _logger_keys() -> List[str]:
    return [f"{s}{l}" for s in STRIPS for l in LOGGER_LOCATIONS]


def _normalize_logger_key(logger_key: str) -> str:
    s = str(logger_key).strip().upper()
    s = s.replace("-", "").replace("_", "").replace(" ", "")
    return s


def _suffix_for_logger(logger_key: str) -> str:
    # logger_key like "S2M" -> suffix "_S2_M"
    s = _normalize_logger_key(logger_key)
    if len(s) < 3:
        return ""
    return f"_{s[:2]}_{s[2:]}"


def _batt_col_for_logger(df: pd.DataFrame, logger_key: str) -> Optional[str]:
    suf = _suffix_for_logger(logger_key)
    cand = f"BattV_Min{suf}"
    if cand in df.columns:
        return cand
    batt_cols = [c for c in df.columns if str(c).startswith("BattV_Min_") and str(c).endswith(suf)]
    if batt_cols:
        return batt_cols[0]
    return None


def _logger_timestamp_set(df: pd.DataFrame, value_cols: List[str]) -> pd.Index:
    """
    Return timestamps where ANY of value_cols is non-null.
    """
    if not value_cols:
        return pd.Index([], dtype="datetime64[ns]")
    sub = df[["timestamp"] + value_cols].dropna(how="all", subset=value_cols)
    return pd.Index(sub["timestamp"].to_numpy(), dtype="datetime64[ns]")


# ----------------------------- Manual-jump detection (event-based) ----------------------------- #

@dataclass(frozen=True)
class JumpEvent:
    logger: str
    kind: str              # "forward" or "backward"
    prev_ts: pd.Timestamp
    ts: pd.Timestamp
    gap_min: float


def _find_jump_events(ts: pd.Series, *, step_min: float = 15.0) -> List[JumpEvent]:
    """
    Detect likely Set Clock events from timestamp diffs.

    Typical signature for "Set Clock forward by ~60 min" on a 15-min cadence:
      observed gap ~= 75 min (15 + 60)

    Typical signature for "Set Clock backward by ~60 min" would create negative diff:
      observed diff ~= -45 min ( -60 + 15 )  OR duplicates/overlaps.
    """
    out: List[JumpEvent] = []
    if ts.shape[0] < 3:
        return out

    # Ensure we have dense positional indexing; avoids KeyError when the Series index is not RangeIndex.
    ts2 = ts.reset_index(drop=True)

    diffs = ts2.diff()
    forward_mask = diffs > pd.Timedelta(minutes=step_min + 40)   # catch +60-ish on 15-min cadence
    backward_mask = diffs < -pd.Timedelta(minutes=40)            # catch ~ -45 (or more negative)

    # Iterate positions rather than labels.
    fpos = np.where(forward_mask.to_numpy(dtype=bool))[0]
    for i in fpos:
        if i <= 0:
            continue
        prev_ts = ts2.iloc[i - 1]
        cur_ts = ts2.iloc[i]
        gap = float((cur_ts - prev_ts).total_seconds() / 60.0)
        out.append(JumpEvent(logger="", kind="forward", prev_ts=prev_ts, ts=cur_ts, gap_min=gap))

    bpos = np.where(backward_mask.to_numpy(dtype=bool))[0]
    for i in bpos:
        if i <= 0:
            continue
        prev_ts = ts2.iloc[i - 1]
        cur_ts = ts2.iloc[i]
        gap = float((cur_ts - prev_ts).total_seconds() / 60.0)
        out.append(JumpEvent(logger="", kind="backward", prev_ts=prev_ts, ts=cur_ts, gap_min=gap))

    return out


# ----------------------------- Overlap-based seasonal misalignment test ----------------------------- #

@dataclass(frozen=True)
class OverlapResult:
    logger: str
    year: int
    ref_logger: str
    base_overlap: float
    overlap_shift_mst_minus1h: float
    overlap_shift_mdt_plus1h: float
    delta_mst_minus1h: float
    delta_mdt_plus1h: float


def _compute_overlap(a: pd.Index, b: pd.Index) -> float:
    if len(a) == 0:
        return 0.0
    inter = a.intersection(b)
    return float(len(inter) / len(a))


def _seasonal_shift_index(
    idx: pd.Index,
    ts_mask: pd.Series,
    shift: pd.Timedelta,
) -> pd.Index:
    """
    Shift only the subset of timestamps where ts_mask is True.
    idx is an Index of timestamps from the logger.
    ts_mask is a boolean Series aligned to idx values (same length).
    """
    vals = idx.to_numpy()
    shifted = vals.copy()
    mask_np = ts_mask.to_numpy(dtype=bool)
    if mask_np.size != shifted.size:
        raise ValueError("Internal error: ts_mask must be same length as idx.")
    if mask_np.any():
        shifted[mask_np] = (pd.to_datetime(shifted[mask_np]) + shift).to_numpy(dtype="datetime64[ns]")
    return pd.Index(shifted, dtype="datetime64[ns]")


def overlap_seasonal_test(
    df: pd.DataFrame,
    *,
    year: int,
    logger_key: str,
    ref_key: str,
    dstw: DstWindow,
) -> OverlapResult:
    suf = _suffix_for_logger(logger_key)
    ref_suf = _suffix_for_logger(ref_key)

    log_cols = [c for c in df.columns if str(c).endswith(suf) and c != "timestamp"]
    ref_cols = [c for c in df.columns if str(c).endswith(ref_suf) and c != "timestamp"]

    a = _logger_timestamp_set(df, log_cols)
    b = _logger_timestamp_set(df, ref_cols)

    base = _compute_overlap(a, b)

    a_ts = pd.Series(a.to_numpy(), dtype="datetime64[ns]")
    mst_mask = _is_mst_window_naive(a_ts, dstw)
    mdt_mask = _is_mdt_window_naive(a_ts, dstw)

    # H1 (always MDT): during MST window, subtract 1 hour.
    a_mst_minus1 = _seasonal_shift_index(a, mst_mask, pd.Timedelta(hours=-1))
    ov_mst_minus1 = _compute_overlap(a_mst_minus1, b)

    # H2 (always MST): during MDT window, add 1 hour.
    a_mdt_plus1 = _seasonal_shift_index(a, mdt_mask, pd.Timedelta(hours=+1))
    ov_mdt_plus1 = _compute_overlap(a_mdt_plus1, b)

    return OverlapResult(
        logger=_normalize_logger_key(logger_key),
        year=year,
        ref_logger=_normalize_logger_key(ref_key),
        base_overlap=base,
        overlap_shift_mst_minus1h=ov_mst_minus1,
        overlap_shift_mdt_plus1h=ov_mdt_plus1,
        delta_mst_minus1h=ov_mst_minus1 - base,
        delta_mdt_plus1h=ov_mdt_plus1 - base,
    )


# ----------------------------- Classification ----------------------------- #

@dataclass(frozen=True)
class ClockMode:
    logger: str
    year: int
    mode: str
    notes: str


def classify_logger(
    df: pd.DataFrame,
    *,
    year: int,
    logger_key: str,
    ref_key: str,
    dstw: DstWindow,
    overlap_gain_threshold: float = 0.10,
) -> ClockMode:
    """
    Heuristics:
    - If many jump events -> irregular_manual
    - If one forward jump after DST start (spring) and no backward -> seasonal_manual
    - If no jumps:
        * if overlap improves with MST -1h shift -> always_mdt
        * elif overlap improves with MDT +1h shift -> always_mst
        * else unknown
    """
    logger_key_n = _normalize_logger_key(logger_key)
    ref_key_n = _normalize_logger_key(ref_key)

    suf = _suffix_for_logger(logger_key_n)
    cols = [c for c in df.columns if str(c).endswith(suf) and c != "timestamp"]
    if not cols:
        return ClockMode(logger=logger_key_n, year=year, mode="unknown", notes="No columns for logger")

    sub = df[["timestamp"] + cols].dropna(how="all", subset=cols)
    if sub.empty:
        return ClockMode(logger=logger_key_n, year=year, mode="unknown", notes="No data rows for logger")

    ts = sub["timestamp"].reset_index(drop=True)
    events = _find_jump_events(ts, step_min=15.0)
    events = [JumpEvent(logger=logger_key_n, kind=e.kind, prev_ts=e.prev_ts, ts=e.ts, gap_min=e.gap_min) for e in events]

    fwd = [e for e in events if e.kind == "forward" and 40.0 <= e.gap_min <= 120.0]
    bwd = [e for e in events if e.kind == "backward" and -120.0 <= e.gap_min <= -40.0]

    if len(fwd) + len(bwd) >= 3:
        return ClockMode(
            logger=logger_key_n,
            year=year,
            mode="irregular_manual",
            notes=f"{len(fwd)} forward, {len(bwd)} backward jumps detected",
        )

    if len(fwd) == 1 and len(bwd) == 0:
        ev = fwd[0]
        if ev.ts >= dstw.dst_start_naive and ev.ts <= (dstw.dst_start_naive + pd.Timedelta(days=200)):
            return ClockMode(
                logger=logger_key_n,
                year=year,
                mode="seasonal_manual",
                notes=f"One forward jump {ev.gap_min:.1f} min at {ev.ts}",
            )

    ov = overlap_seasonal_test(df, year=year, logger_key=logger_key_n, ref_key=ref_key_n, dstw=dstw)

    if ov.delta_mst_minus1h >= overlap_gain_threshold and ov.delta_mst_minus1h > ov.delta_mdt_plus1h:
        return ClockMode(
            logger=logger_key_n,
            year=year,
            mode="always_mdt",
            notes=f"Overlap gain MST-1h: +{ov.delta_mst_minus1h:.3f} (base {ov.base_overlap:.3f})",
        )

    if ov.delta_mdt_plus1h >= overlap_gain_threshold and ov.delta_mdt_plus1h > ov.delta_mst_minus1h:
        return ClockMode(
            logger=logger_key_n,
            year=year,
            mode="always_mst",
            notes=f"Overlap gain MDT+1h: +{ov.delta_mdt_plus1h:.3f} (base {ov.base_overlap:.3f})",
        )

    return ClockMode(
        logger=logger_key_n,
        year=year,
        mode="unknown",
        notes=(
            f"Jumps: fwd={len(fwd)} bwd={len(bwd)}; "
            f"overlap gains: MST-1h={ov.delta_mst_minus1h:.3f}, MDT+1h={ov.delta_mdt_plus1h:.3f}"
        ),
    )


# ----------------------------- Reporting / reference selection ----------------------------- #

def _pick_default_reference_logger(df: pd.DataFrame) -> str:
    """
    Pick a logger with the most non-null BattV_Min points as a stable reference for overlap tests.
    """
    best: Optional[str] = None
    best_n = -1
    for lg in _logger_keys():
        col = _batt_col_for_logger(df, lg)
        if not col or col not in df.columns:
            continue
        n = int(pd.to_numeric(df[col], errors="coerce").notna().sum())
        if n > best_n:
            best_n = n
            best = lg
    return best or "S1T"


def _resolve_reference_logger(df: pd.DataFrame, requested: Optional[str]) -> str:
    """
    If requested is provided and appears in the dataset, use it.
    Otherwise fall back to auto-pick.
    """
    if requested:
        rk = _normalize_logger_key(requested)
        suf = _suffix_for_logger(rk)
        # accept if we can find any columns for it
        has_cols = any(str(c).endswith(suf) and c != "timestamp" for c in df.columns)
        if has_cols:
            return rk
    return _pick_default_reference_logger(df)


def run_year(
    year: int,
    *,
    overlap_gain_threshold: float = 0.10,
    requested_ref_logger: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    df = _read_raw_logger_parquet(year)
    dstw = _find_denver_dst_window(year, tz="America/Denver")

    ref = _resolve_reference_logger(df, requested_ref_logger)

    # 1) Special verification for S2M (requested)
    # If ref == S2M, choose a different reference (auto-pick) to avoid self-overlap.
    if _normalize_logger_key(ref) == "S2M":
        ref = _resolve_reference_logger(df, None)
        if _normalize_logger_key(ref) == "S2M":
            ref = "S1T"  # extremely defensive fallback

    s2m_ov = overlap_seasonal_test(df, year=year, logger_key="S2M", ref_key=ref, dstw=dstw)
    verify_rows = [{
        "year": year,
        "logger": s2m_ov.logger,
        "ref_logger": s2m_ov.ref_logger,
        "dst_start": str(dstw.dst_start_naive),
        "dst_end": str(dstw.dst_end_naive),
        "base_overlap": s2m_ov.base_overlap,
        "overlap_mst_minus1h": s2m_ov.overlap_shift_mst_minus1h,
        "delta_mst_minus1h": s2m_ov.delta_mst_minus1h,
        "overlap_mdt_plus1h": s2m_ov.overlap_shift_mdt_plus1h,
        "delta_mdt_plus1h": s2m_ov.delta_mdt_plus1h,
    }]
    verify_df = pd.DataFrame(verify_rows)

    # 2) Classification for all loggers
    modes: List[ClockMode] = []
    for lg in _logger_keys():
        mode = classify_logger(
            df,
            year=year,
            logger_key=lg,
            ref_key=ref,
            dstw=dstw,
            overlap_gain_threshold=overlap_gain_threshold,
        )
        modes.append(mode)

    modes_df = pd.DataFrame([{"year": m.year, "logger": m.logger, "mode": m.mode, "notes": m.notes} for m in modes])

    return verify_df, modes_df, ref


def main() -> int:
    ap = argparse.ArgumentParser(description="Clock mode analysis (DST/manual shifts) for logger data.")
    ap.add_argument("--years", type=int, nargs="+", required=True, help="Years to analyze (e.g., 2023 2024 2025).")
    ap.add_argument(
        "--overlap-gain-threshold",
        type=float,
        default=0.10,
        help="Minimum overlap improvement to call always_mdt/always_mst (default 0.10).",
    )
    ap.add_argument(
        "--write-csv",
        action="store_true",
        help="Write CSVs into biochar_app/diagnostics/reports/",
    )
    ap.add_argument(
        "--ref-logger",
        type=str,
        default=None,
        help="Reference logger (default: auto-select). Example: S1T",
    )

    args = ap.parse_args()

    years = [int(y) for y in args.years]

    reports_dir = Path(__file__).resolve().parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    stamp = date.today().isoformat()

    all_verify: List[pd.DataFrame] = []
    all_modes: List[pd.DataFrame] = []

    for y in years:
        print(f"\n=== YEAR {y} ===")
        verify_df, modes_df, ref_used = run_year(
            y,
            overlap_gain_threshold=float(args.overlap_gain_threshold),
            requested_ref_logger=args.ref_logger,
        )

        r = verify_df.iloc[0].to_dict()
        print(
            "S2M seasonal misalignment test (overlap vs reference):\n"
            f"  ref_logger           : {r['ref_logger']} (resolved: {ref_used})\n"
            f"  DST start/end        : {r['dst_start']}  →  {r['dst_end']}\n"
            f"  base_overlap         : {float(r['base_overlap']):.3f}\n"
            f"  overlap MST-1h       : {float(r['overlap_mst_minus1h']):.3f} (Δ {float(r['delta_mst_minus1h']):+.3f})\n"
            f"  overlap MDT+1h       : {float(r['overlap_mdt_plus1h']):.3f} (Δ {float(r['delta_mdt_plus1h']):+.3f})\n"
        )

        counts = modes_df["mode"].value_counts().to_dict()
        print("Clock mode counts:", counts)

        all_verify.append(verify_df)
        all_modes.append(modes_df)

    verify_all = pd.concat(all_verify, ignore_index=True) if all_verify else pd.DataFrame()
    modes_all = pd.concat(all_modes, ignore_index=True) if all_modes else pd.DataFrame()

    if args.write_csv:
        p1 = reports_dir / f"clock_verify_s2m_{stamp}.csv"
        p2 = reports_dir / f"clock_modes_{stamp}.csv"
        verify_all.to_csv(p1, index=False)
        modes_all.to_csv(p2, index=False)
        print("\n✅ Wrote:")
        print(f"  - {p1}")
        print(f"  - {p2}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
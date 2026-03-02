#!/usr/bin/env python3
"""
biochar_app/diagnostics/battery_replacements.py

Detect likely battery replacement events from BattV_Min_* columns, using a
"pre-dawn" (default ~5am) voltage series, and optionally merge in a manual
battery inventory log (known replacements + specs).

Why 5am?
--------
Around 5am, solar charging is minimal and battery voltage reflects overnight
state-of-charge. Weak/degrading batteries tend to sag overnight; a replacement
often produces a sharp upward step in pre-dawn voltage.

Outputs
-------
- Console summary
- Optional CSV(s) written to biochar_app/diagnostics/reports/
  * battery_replacements_candidates_YYYY-MM-DD.csv
  * battery_replacements_merged_YYYY-MM-DD.csv (if manual inventory provided)

Notes
-----
- This reads *processed* raw parquet(s) (biochar_app/data-processed/parquet/<year>/<year>_raw_logger.parquet)
  and does NOT modify any raw .dat files.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from biochar_app.scripts.config import PARQUET_DIR, YEARS  # type: ignore


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# Data model for manual battery inventory entries
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class BatterySpec:
    """
    Minimal spec set that will be useful later.

    Suggested fields:
    - chemistry: 'SLA' (sealed lead acid), 'FLA' (flooded lead acid), 'AGM', 'LiFePO4', etc.
    - nominal_voltage_v: typically 12.0 for your setup
    - capacity_ah: amp-hours (Ah). If unknown, leave None.
    - brand/model: helps trace purchasing batches
    - install_location: optional (e.g., "S3M box", "east edge", etc.)
    - notes: free text
    """
    chemistry: str
    nominal_voltage_v: float = 12.0
    capacity_ah: Optional[float] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    install_location: Optional[str] = None
    notes: Optional[str] = None


@dataclass(frozen=True)
class ManualReplacement:
    """
    Manual replacement record.

    logger: e.g. "S3_M" (matches BattV_Min_S3_M suffix) or "S3M" (we normalize).
    date: ISO date or datetime string (we use date component and assume local wall time)
    spec: BatterySpec
    """
    logger: str
    date: str
    spec: BatterySpec


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _normalize_logger_key(x: str) -> str:
    """
    Accept "S3_M", "S3M", "S3-M", "S3 M" -> "S3_M"
    """
    s = str(x).strip().upper().replace("-", "_").replace(" ", "_")
    if len(s) == 3 and s.startswith("S") and s[2] in ("T", "M", "B"):
        return f"{s[:2]}_{s[2]}"
    # already like S3_M
    return s


def _read_raw_logger_parquet(year: int) -> pd.DataFrame:
    path = Path(PARQUET_DIR) / str(year) / f"{year}_raw_logger.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Raw logger parquet not found for {year}: {path}")

    df = pd.read_parquet(path)
    if "timestamp" not in df.columns:
        raise ValueError(f"{path.name}: expected 'timestamp' column")

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].astype("datetime64[ns]")
    return df


def _list_batt_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if str(c).startswith("BattV_Min_")]


def _logger_from_batt_col(col: str) -> str:
    # "BattV_Min_S3_M" -> "S3_M"
    parts = str(col).split("BattV_Min_", 1)
    suf = parts[1] if len(parts) == 2 else str(col)
    return _normalize_logger_key(suf)


def _safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _window_df(df: pd.DataFrame, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    if start:
        start_ts = pd.to_datetime(start, errors="coerce")
        if pd.isna(start_ts):
            raise ValueError(f"Bad --start: {start!r}")
        df = df[df["timestamp"] >= start_ts]
    if end:
        end_ts = pd.to_datetime(end, errors="coerce")
        if pd.isna(end_ts):
            raise ValueError(f"Bad --end: {end!r}")
        df = df[df["timestamp"] <= end_ts]
    return df


# -----------------------------------------------------------------------------
# Core logic: build pre-dawn series and detect step events
# -----------------------------------------------------------------------------

def extract_predawn_series(
    df: pd.DataFrame,
    batt_col: str,
    *,
    target_hour: int = 5,
    window_minutes: int = 20,
) -> pd.DataFrame:
    """
    Return a small DataFrame with one row per day (where present):
      date, time, value

    We select samples whose time-of-day is within +/- window_minutes of target_hour:00.
    If multiple samples exist in that window for a day, we take the median (robust).
    """
    if batt_col not in df.columns:
        return pd.DataFrame(columns=["date", "time", "value"])

    sub = df[["timestamp", batt_col]].copy()
    sub = sub.dropna(subset=[batt_col])
    if sub.empty:
        return pd.DataFrame(columns=["date", "time", "value"])

    ts = sub["timestamp"]
    # Compute absolute minutes from target time (e.g., 5:00)
    tod_minutes = ts.dt.hour * 60 + ts.dt.minute
    target_minutes = int(target_hour) * 60
    delta = (tod_minutes - target_minutes).abs()

    sub = sub.loc[delta <= int(window_minutes)].copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "time", "value"])

    sub["date"] = sub["timestamp"].dt.floor("D")
    # For reproducibility, keep a representative time (median timestamp of the chosen rows)
    g = sub.groupby("date", sort=True)
    out = g.agg(
        time=("timestamp", "median"),
        value=(batt_col, "median"),
    ).reset_index()
    out["value"] = _safe_numeric(out["value"])
    out = out.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    return out


def detect_upward_steps(
    predawn: pd.DataFrame,
    *,
    min_step_v: float = 0.60,
    require_stable_days: int = 5,
    stable_floor_v: float = 12.0,
) -> pd.DataFrame:
    """
    Detect candidate replacement events as large upward steps in the pre-dawn series.

    Heuristic:
    - Find day-to-day deltas >= min_step_v
    - Optionally require that after the step, we see at least require_stable_days
      consecutive days with value >= stable_floor_v (suggests "new good battery")

    Returns rows with:
      prev_time, time, prev_v, v, delta_v, stable_streak_days
    """
    if predawn.empty or predawn.shape[0] < 2:
        return pd.DataFrame(columns=["prev_time", "time", "prev_v", "v", "delta_v", "stable_streak_days"])

    p = predawn.copy()
    p["prev_time"] = p["time"].shift(1)
    p["prev_v"] = p["value"].shift(1)
    p["delta_v"] = p["value"] - p["prev_v"]
    candidates = p[p["delta_v"] >= float(min_step_v)].copy()
    if candidates.empty:
        return pd.DataFrame(columns=["prev_time", "time", "prev_v", "v", "delta_v", "stable_streak_days"])

    # Compute stable streak length starting at each candidate index
    vals = p["value"].to_numpy()
    dates = p["date"].to_numpy()

    def stable_streak_from(i: int) -> int:
        # streak counting starts at i (the "after" point of the jump)
        n = 0
        for j in range(i, len(vals)):
            if np.isnan(vals[j]) or float(vals[j]) < float(stable_floor_v):
                break
            # ensure days are contiguous (allow 1-day gaps only? for now require consecutive dates)
            if n > 0:
                # dates are numpy datetime64[D] after floor; ensure +1 day continuity
                if (dates[j] - dates[j - 1]) != np.timedelta64(1, "D"):
                    break
            n += 1
            if n >= int(require_stable_days):
                # we can stop early; but returning full streak is useful
                continue
        return n

    # Map candidate rows back to index positions in p
    candidates["stable_streak_days"] = 0
    idx_map = {int(i): i for i in range(len(p))}
    for row_i in candidates.index:
        i = int(row_i)
        if i in idx_map:
            candidates.loc[row_i, "stable_streak_days"] = stable_streak_from(i)

    # Filter if stability is required
    if int(require_stable_days) > 0:
        candidates = candidates[candidates["stable_streak_days"] >= int(require_stable_days)]

    out = candidates.rename(columns={"value": "v"}).loc[
        :, ["prev_time", "time", "prev_v", "v", "delta_v", "stable_streak_days"]
    ]
    return out.reset_index(drop=True)


def summarize_predawn_streaks(
    predawn: pd.DataFrame,
    *,
    floor_v: float = 12.0,
    min_days: int = 5,
) -> List[Tuple[pd.Timestamp, pd.Timestamp, int]]:
    """
    Find streaks of days where predawn value >= floor_v for at least min_days.
    Returns list of (start_time, end_time, n_days).
    """
    if predawn.empty:
        return []

    p = predawn.copy()
    ok = p["value"] >= float(floor_v)
    # Identify contiguous runs in 'ok' with consecutive days
    streaks: List[Tuple[pd.Timestamp, pd.Timestamp, int]] = []

    start_idx: Optional[int] = None
    for i in range(len(p)):
        if bool(ok.iloc[i]):
            if start_idx is None:
                start_idx = i
            else:
                # break streak if non-consecutive date
                if (p["date"].iloc[i] - p["date"].iloc[i - 1]) != pd.Timedelta(days=1):
                    # close previous
                    n = i - start_idx
                    if n >= int(min_days):
                        streaks.append((p["time"].iloc[start_idx], p["time"].iloc[i - 1], n))
                    start_idx = i
        else:
            if start_idx is not None:
                n = i - start_idx
                if n >= int(min_days):
                    streaks.append((p["time"].iloc[start_idx], p["time"].iloc[i - 1], n))
                start_idx = None

    # tail
    if start_idx is not None:
        n = len(p) - start_idx
        if n >= int(min_days):
            streaks.append((p["time"].iloc[start_idx], p["time"].iloc[len(p) - 1], n))

    return streaks


# -----------------------------------------------------------------------------
# Manual inventory parsing (csv)
# -----------------------------------------------------------------------------

def load_manual_inventory_csv(path: Path) -> pd.DataFrame:
    """
    Expected CSV columns:

    logger,install_date,end_date,end_reason,reason_description,
    chemistry,nominal_voltage_v,capacity_ah,
    brand,model,install_location,notes

    Controlled vocabulary for end_reason:
        active
        replaced
        failed
        decommissioned
        temporary_remove
        unknown
    """

    allowed_reasons = {
        "active",
        "replaced",
        "failed",
        "decommissioned",
        "temporary_remove",
        "unknown",
    }

    df = pd.read_csv(path, comment="#")

    required = {"logger", "install_date", "chemistry"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in battery inventory CSV: {missing}")

    df = df.copy()

    # --- Normalize logger ---
    df["logger"] = (
        df["logger"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace("-", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    # --- Install date ---
    df["install_date"] = pd.to_datetime(df["install_date"], errors="coerce")
    if df["install_date"].isna().any():
        bad = df[df["install_date"].isna()]
        raise ValueError(f"Invalid install_date rows:\n{bad}")

    # --- End date (optional) ---
    if "end_date" not in df.columns:
        df["end_date"] = pd.NaT
    else:
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    # --- End reason (optional but validated if present) ---
    if "end_reason" not in df.columns:
        df["end_reason"] = "active"

    df["end_reason"] = df["end_reason"].fillna("active").astype(str).str.strip().str.lower()

    invalid_reasons = set(df["end_reason"]) - allowed_reasons
    if invalid_reasons:
        raise ValueError(
            f"Invalid end_reason values: {invalid_reasons}\n"
            f"Allowed: {sorted(allowed_reasons)}"
        )

    # --- Consistency rules ---
    # If end_date is null, reason must be 'active'
    mask_no_end = df["end_date"].isna()
    inconsistent_active = df.loc[mask_no_end & (df["end_reason"] != "active")]
    if not inconsistent_active.empty:
        raise ValueError(
            "Rows with no end_date must have end_reason='active':\n"
            f"{inconsistent_active}"
        )

    # If end_date exists, reason cannot be 'active'
    mask_has_end = df["end_date"].notna()
    inconsistent_closed = df.loc[mask_has_end & (df["end_reason"] == "active")]
    if not inconsistent_closed.empty:
        raise ValueError(
            "Rows with end_date must not have end_reason='active':\n"
            f"{inconsistent_closed}"
        )

    # Ensure chronological correctness per logger
    for lg, sub in df.sort_values("install_date").groupby("logger"):
        prev_end = None
        for _, row in sub.iterrows():
            start = row["install_date"]
            end = row["end_date"]

            if prev_end is not None and start < prev_end:
                raise ValueError(
                    f"Overlapping battery intervals for {lg}: "
                    f"{start} overlaps previous end {prev_end}"
                )

            if pd.notna(end) and end < start:
                raise ValueError(
                    f"end_date before install_date for {lg}: "
                    f"{start} → {end}"
                )

            prev_end = end if pd.notna(end) else prev_end

    return df

def validate_intervals(man: pd.DataFrame) -> None:
    for lg, sub in man.sort_values("install_date").groupby("logger"):
        prev_end = None
        for _, r in sub.iterrows():
            start = r["install_date"]
            end = r["end_date"]

            if prev_end is not None and start < prev_end:
                raise ValueError(
                    f"Overlapping battery intervals for {lg}: "
                    f"{start} overlaps previous ending {prev_end}"
                )

            prev_end = end if pd.notna(end) else prev_end

def get_active_battery(man: pd.DataFrame, logger: str, ts: pd.Timestamp) -> pd.Series | None:
    sub = man[man["logger"] == logger]

    for _, r in sub.iterrows():
        start = r["install_date"]
        end = r["end_date"]

        if pd.notna(start) and ts >= start:
            if pd.isna(end) or ts < end:
                return r

    return None
# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------

def print_logger_summary(
    logger_key: str,
    predawn: pd.DataFrame,
    steps: pd.DataFrame,
    *,
    floor_v: float,
    streak_days: int,
) -> None:
    print(f"\n=== {logger_key} ===")
    if predawn.empty:
        print("  No predawn points.")
        return

    vals = predawn["value"]
    q10 = float(vals.quantile(0.10))
    q50 = float(vals.quantile(0.50))
    q90 = float(vals.quantile(0.90))
    print(f"  predawn points: {predawn.shape[0]}")
    print(f"  min={float(vals.min()):.3f}  p10={q10:.3f}  p50={q50:.3f}  p90={q90:.3f}  max={float(vals.max()):.3f}")

    if steps.empty:
        print("  No replacement-like upward steps detected.")
    else:
        print("\n  Candidate replacement-like upward steps:")
        print(steps.to_string(index=False))

    streaks = summarize_predawn_streaks(predawn, floor_v=floor_v, min_days=streak_days)
    if streaks:
        print(f"\n  Streaks where predawn >= {floor_v:.2f} V for >= {streak_days} days:")
        for a, b, n in streaks:
            print(f"    {a} → {b}  ({n} days)")


def candidates_to_rows(
    logger_key: str,
    steps: pd.DataFrame,
    *,
    min_step_v: float,
    floor_v: float,
    streak_days: int,
) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    if steps.empty:
        return out
    for _, r in steps.iterrows():
        out.append(
            {
                "logger": logger_key,
                "candidate_time": pd.to_datetime(r["time"]).isoformat(sep=" "),
                "prev_time": pd.to_datetime(r["prev_time"]).isoformat(sep=" "),
                "candidate_v": float(r["v"]),
                "prev_v": float(r["prev_v"]),
                "delta_v": float(r["delta_v"]),
                "stable_streak_days": int(r.get("stable_streak_days", 0)),
                "min_step_v": float(min_step_v),
                "stable_floor_v": float(floor_v),
                "require_stable_days": int(streak_days),
            }
        )
    return out


def merge_manual_with_candidates(
    candidates: pd.DataFrame,
    manual_df: pd.DataFrame,
    *,
    max_days_apart: int = 10,
) -> pd.DataFrame:
    """
    Merge manual replacements onto nearest candidate in time (per logger) if within max_days_apart.
    Also keep manual entries even if no candidate matched.

    Returns one row per manual entry, with candidate fields prefixed as "cand_...".
    """
    # ---- candidates prep ----
    cand = candidates.copy()
    if cand.empty:
        cand = pd.DataFrame(
            columns=[
                "logger",
                "candidate_time",
                "prev_time",
                "candidate_v",
                "prev_v",
                "delta_v",
                "stable_streak_days",
            ]
        )

    if "logger" in cand.columns:
        cand["logger"] = cand["logger"].astype(str).map(_normalize_logger_key)

    if "candidate_time" in cand.columns:
        cand["candidate_time"] = pd.to_datetime(cand["candidate_time"], errors="coerce")

    # ---- manual prep ----
    if manual_df is None or manual_df.empty:
        return cand.iloc[0:0].copy() if candidates is None else cand.iloc[0:0].copy()  # empty output

    man = manual_df.copy()

    if "logger" not in man.columns:
        raise ValueError("manual_df must include a 'logger' column.")

    man["logger"] = man["logger"].astype(str).map(_normalize_logger_key)

    # Support a few plausible column names for the install date coming from CSV
    date_col: Optional[str] = None
    for c in ("install_date", "manual_date", "date"):
        if c in man.columns:
            date_col = c
            break
    if date_col is None:
        raise ValueError("manual_df must include one of: 'install_date', 'manual_date', or 'date'.")

    man["manual_date"] = pd.to_datetime(man[date_col], errors="coerce")

    # We'll build one output row per manual entry (even if unmatched)
    merged_rows: List[Dict[str, object]] = []

    # Group candidates by logger (sorted by candidate_time)
    cand_by_logger: Dict[str, pd.DataFrame] = {}
    if not cand.empty and "logger" in cand.columns:
        for lg, sub in cand.groupby("logger", sort=False):
            sub2 = sub.sort_values("candidate_time").reset_index(drop=True)
            cand_by_logger[str(lg)] = sub2

    used_candidate_pos: set[tuple[str, int]] = set()

    for _, mr in man.iterrows():
        lg = str(mr.get("logger", ""))
        md = mr.get("manual_date")

        # base row from manual record (coerce keys to str and NaN -> None)
        row: Dict[str, object] = {str(k): (None if pd.isna(v) else v) for k, v in mr.items()}

        # standard fields we always include
        row["logger"] = lg
        row["manual_date"] = None if pd.isna(md) else md
        row["matched_candidate"] = False
        row["match_abs_days"] = None

        csub = cand_by_logger.get(lg)
        best_pos: Optional[int] = None
        best_abs_days: Optional[float] = None

        if csub is not None and not csub.empty and pd.notna(md):
            # compute absolute day diffs to each candidate_time
            diffs = (csub["candidate_time"] - md).abs()
            diffs_days = diffs / pd.Timedelta(days=1)

            # If everything is NaT, skip
            if diffs_days.notna().any():
                # idxmin gives position because we reset_index(drop=True)
                pos = int(diffs_days.idxmin())
                abs_days = float(diffs_days.iloc[pos])

                if abs_days <= float(max_days_apart):
                    best_pos = pos
                    best_abs_days = abs_days

        if best_pos is not None and csub is not None:
            key = (lg, best_pos)
            if key not in used_candidate_pos:
                used_candidate_pos.add(key)

                cr = csub.iloc[best_pos].to_dict()
                for k, v in cr.items():
                    row[f"cand_{k}"] = None if pd.isna(v) else v

                row["matched_candidate"] = True
                row["match_abs_days"] = best_abs_days

        merged_rows.append(row)

    return pd.DataFrame(merged_rows)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Detect likely battery replacement events from BattV_Min_* columns.")
    ap.add_argument("--years", type=int, nargs="*", default=None, help="Years to scan (default: all YEARS).")
    ap.add_argument("--start", type=str, default=None, help="Optional start datetime (e.g., 2024-10-01).")
    ap.add_argument("--end", type=str, default=None, help="Optional end datetime (e.g., 2025-06-30).")

    ap.add_argument("--target-hour", type=int, default=5, help="Target pre-dawn hour (default 5).")
    ap.add_argument("--window-min", type=int, default=20, help="+/- minutes around target hour (default 20).")

    ap.add_argument("--min-step-v", type=float, default=0.60, help="Min upward step at pre-dawn to flag (V).")
    ap.add_argument("--stable-floor-v", type=float, default=12.0, help="Post-step 'good battery' floor (V).")
    ap.add_argument("--require-stable-days", type=int, default=5, help="Require this many stable days after step.")

    ap.add_argument("--only-loggers", type=str, nargs="*", default=None, help="Limit to specific loggers (e.g. S3_M S4_M).")

    ap.add_argument("--manual-inventory", type=str, default=None, help="Path to JSON with known replacements + specs.")
    ap.add_argument("--match-window-days", type=int, default=10, help="Manual↔candidate match window in days (default 10).")

    ap.add_argument("--write-csv", action="store_true", help="Write CSV outputs to diagnostics/reports/.")
    args = ap.parse_args()

    years = list(args.years) if args.years else list(YEARS)

    only_loggers: Optional[set[str]] = None
    if args.only_loggers:
        only_loggers = {_normalize_logger_key(x) for x in args.only_loggers}

    report_date = date.today().isoformat()

    candidate_rows: List[Dict[str, object]] = []

    print("🔋 battery_replacements starting")
    print(f"  years={years}")
    if args.start or args.end:
        print(f"  window={args.start or '[start]'} → {args.end or '[end]'}")
    print(f"  predawn={int(args.target_hour):02d}:00 ±{int(args.window_min)} min")
    print(f"  step>= {float(args.min_step_v):.2f} V, require_stable_days={int(args.require_stable_days)} at >= {float(args.stable_floor_v):.2f} V")
    if only_loggers:
        print(f"  only_loggers={sorted(only_loggers)}")

    for y in years:
        print(f"\n📅 YEAR {y}")
        try:
            df = _read_raw_logger_parquet(int(y))
        except FileNotFoundError as e:
            print(f"  ⚠️ {e}")
            continue

        df = _window_df(df, args.start, args.end)
        if df.empty:
            print("  (no rows in this window)")
            continue

        batt_cols = _list_batt_cols(df)
        if not batt_cols:
            print("  No BattV_Min_* columns found.")
            continue

        # Process each batt column
        for col in batt_cols:
            lg = _logger_from_batt_col(col)
            if only_loggers is not None and lg not in only_loggers:
                continue

            predawn = extract_predawn_series(
                df,
                col,
                target_hour=int(args.target_hour),
                window_minutes=int(args.window_min),
            )
            steps = detect_upward_steps(
                predawn,
                min_step_v=float(args.min_step_v),
                require_stable_days=int(args.require_stable_days),
                stable_floor_v=float(args.stable_floor_v),
            )

            print_logger_summary(
                lg,
                predawn,
                steps,
                floor_v=float(args.stable_floor_v),
                streak_days=int(args.require_stable_days),
            )

            candidate_rows.extend(
                candidates_to_rows(
                    lg,
                    steps,
                    min_step_v=float(args.min_step_v),
                    floor_v=float(args.stable_floor_v),
                    streak_days=int(args.require_stable_days),
                )
            )

    candidates_df = pd.DataFrame(candidate_rows)
    if candidates_df.empty:
        print("\n✅ No candidate replacement steps found (under current thresholds).")
    else:
        print(f"\n✅ Candidate events found: {len(candidates_df)}")

    wrote: List[Path] = []
    if args.write_csv:
        _ensure_dir(REPORTS_DIR)
        if not candidates_df.empty:
            p = REPORTS_DIR / f"battery_replacements_candidates_{report_date}.csv"
            candidates_df.to_csv(p, index=False)
            wrote.append(p)

    # Optional: merge manual inventory
    if args.manual_inventory:
        inv_path = Path(args.manual_inventory)
        if not inv_path.exists():
            raise FileNotFoundError(f"--manual-inventory not found: {inv_path}")
        manual = load_manual_inventory_csv(inv_path)
        merged = merge_manual_with_candidates(
            candidates_df,
            manual,
            max_days_apart=int(args.match_window_days),
        )
        print(f"\n📒 Manual inventory entries: {len(manual)}")
        if not merged.empty:
            print("  (merged manual ↔ candidates preview)")
            print(merged.head(10).to_string(index=False))

        if args.write_csv and not merged.empty:
            p = REPORTS_DIR / f"battery_replacements_merged_{report_date}.csv"
            merged.to_csv(p, index=False)
            wrote.append(p)

    if wrote:
        print("\n✅ Wrote:")
        for p in wrote:
            print(f"  - {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
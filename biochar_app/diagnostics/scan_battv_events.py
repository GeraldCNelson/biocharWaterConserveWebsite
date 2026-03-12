#!/usr/bin/env python3
"""
scan_battv_events.py

Scan all TOA5 logger .dat files for battery/timestamp anomalies and (optionally)
write per-event voltage-window PNGs.

Events:
- brownout: BattV_Min below threshold (contiguous runs)
- gap: timestamp gaps above a threshold  (ALWAYS computed, even if no BattV column)
- replacement_step: upward step in daily-min baseline (heuristic)
- high_voltage_warn: BattV >= warn threshold (contiguous runs)
- high_voltage_critical: BattV >= critical threshold (contiguous runs)

Notes on outputs (per your project convention):
- Reports go to: biochar_app/diagnostics/reports/
- Plots go to:   biochar_app/diagnostics/plots/voltage_events/

CLI:
  python -m biochar_app.diagnostics.scan_battv_events \
    --data-root biochar_app/data-raw \
    --start "2023-05-15 00:00" \
    --end   "2026-02-28 23:59" \
    --brownout 9.6 \
    --gap-minutes 60 \
    --hv-warn 16.0 \
    --hv-critical 18.0 \
    --out-csv biochar_app/diagnostics/reports/battv_events.csv \
    --out-gaps-csv biochar_app/diagnostics/reports/battv_gaps.csv \
    --write-plots \
    --plot-months 2 \
    --plot-max-per-logger 25

Key fixes vs prior version:
- If BattV column is missing, we STILL scan timestamp gaps in that file.
- Emit NA in min_v/max_v/gap_minutes (instead of blanks) by writing np.nan.
- Add logger_tag + year + file_path + file_kind metadata columns.
- Support reading late-2023 battv backfill files:
    S1M_Table1_late2023_withBattV.dat
  If present, they are included alongside the normal S1M_Table1.dat.
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


VOLTAGE_REGEX = re.compile(r"(batt|battery|volt|vbat|battv)", re.IGNORECASE)

# ---------------------------------------------------------------------
# Diagnostic output dirs (relative to this script)
# ---------------------------------------------------------------------
DIAG_DIR = Path(__file__).resolve().parent
REPORT_DIR = DIAG_DIR / "reports"
PLOT_DIR = DIAG_DIR / "plots" / "voltage_events"

REPORT_DIR.mkdir(parents=True, exist_ok=True)
PLOT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# TOA5 reader (ETL-consistent: header row is line 2)
# ---------------------------------------------------------------------
def _clean_col_name(col: object) -> str:
    s = str(col)
    s = s.lstrip("\ufeff").strip()
    s = s.strip('"').strip("'").strip()
    return s


def read_toa5_dat(path: Path) -> pd.DataFrame:
    with path.open("r", newline="") as f:
        r = csv.reader(f)
        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{path.name}: missing TOA5 header row (line 2).")

    cols = [_clean_col_name(c) for c in colnames]
    if "TIMESTAMP" not in cols and "timestamp" not in cols:
        raise ValueError(f"{path.name}: TOA5 header missing TIMESTAMP. Got: {cols}")

    df = pd.read_csv(
        path,
        skiprows=4,
        header=None,
        names=cols,
        na_values=["", "NA", "NAN"],
        engine="python",
    )
    df.columns = [_clean_col_name(c) for c in df.columns]
    return df


def detect_voltage_col(cols: Iterable[str]) -> Optional[str]:
    preferred = ["BattV_Min", "BattV_Avg", "BattV", "Battery", "VBatt", "Vbat"]
    cols_list = list(cols)
    for p in preferred:
        if p in cols_list:
            return p
    matches = [c for c in cols_list if VOLTAGE_REGEX.search(c)]
    return matches[0] if matches else None


def coerce_timestamp(s: pd.Series) -> pd.Series:
    ss = s.astype("string").str.strip().str.strip('"').str.strip("'")
    ts = pd.to_datetime(ss, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if ts.notna().mean() < 0.9:
        ts = pd.to_datetime(ss, errors="coerce")
    return ts


# ---------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------
@dataclass
class EventRow:
    logger_tag: str
    year: int
    file_path: str
    file_kind: str  # "main" | "late2023_withBattV"
    kind: str
    severity: str  # info | warn | critical
    start: pd.Timestamp
    end: pd.Timestamp
    details: str
    min_v: float | None
    max_v: float | None
    gap_minutes: float | None
    voltage_col: str | None
    plot_path: str | None = None


def find_gap_events(ts: pd.Series, *, gap_minutes: int = 60) -> list[tuple[pd.Timestamp, pd.Timestamp, float]]:
    """Return list of (prev_ts, next_ts, gap_minutes) where gap exceeds threshold."""
    t = pd.Series(ts).dropna().sort_values().reset_index(drop=True)
    if len(t) < 2:
        return []
    dt = t.diff().dropna()
    gap_mask = dt >= pd.Timedelta(minutes=gap_minutes)
    out: list[tuple[pd.Timestamp, pd.Timestamp, float]] = []
    for idx in gap_mask[gap_mask].index:
        prev_ts = pd.Timestamp(t.iloc[idx - 1])
        next_ts = pd.Timestamp(t.iloc[idx])
        mins = (next_ts - prev_ts).total_seconds() / 60.0
        out.append((prev_ts, next_ts, mins))
    return out


def find_brownout_runs(
    df: pd.DataFrame, ts_col: str, vcol: str, threshold: float
) -> list[tuple[pd.Timestamp, pd.Timestamp, float]]:
    """Contiguous runs where voltage < threshold; returns (start,end,minV)."""
    sub = df[[ts_col, vcol]].dropna().sort_values(ts_col)
    if sub.empty:
        return []
    below = sub[vcol] < threshold
    if not below.any():
        return []

    run_id = (below != below.shift()).cumsum()
    out: list[tuple[pd.Timestamp, pd.Timestamp, float]] = []
    for _, g in sub[below].groupby(run_id[below]):
        start = pd.Timestamp(g[ts_col].iloc[0])
        end = pd.Timestamp(g[ts_col].iloc[-1])
        minv = float(g[vcol].min())
        out.append((start, end, minv))
    return out


def find_high_voltage_runs(
    df: pd.DataFrame, ts_col: str, vcol: str, threshold: float
) -> list[tuple[pd.Timestamp, pd.Timestamp, float]]:
    """Contiguous runs where voltage >= threshold; returns (start,end,maxV)."""
    sub = df[[ts_col, vcol]].dropna().sort_values(ts_col)
    if sub.empty:
        return []
    above = sub[vcol] >= threshold
    if not above.any():
        return []

    run_id = (above != above.shift()).cumsum()
    out: list[tuple[pd.Timestamp, pd.Timestamp, float]] = []
    for _, g in sub[above].groupby(run_id[above]):
        start = pd.Timestamp(g[ts_col].iloc[0])
        end = pd.Timestamp(g[ts_col].iloc[-1])
        maxv = float(g[vcol].max())
        out.append((start, end, maxv))
    return out


def detect_replacement_steps(
    df: pd.DataFrame, ts_col: str, vcol: str, *, step_v: float = 0.6
) -> list[tuple[pd.Timestamp, float]]:
    """
    Heuristic: compute daily min series, then look for upward steps >= step_v.
    Returns list of (day, deltaV) where step occurs.
    """
    sub = df[[ts_col, vcol]].dropna().sort_values(ts_col)
    if sub.empty:
        return []
    series = pd.Series(sub[vcol].to_numpy(), index=pd.DatetimeIndex(sub[ts_col]))
    daily_min = series.resample("1D").min().dropna()
    if len(daily_min) < 10:
        return []

    d = daily_min.diff()
    hits = d[d >= step_v]
    return [(pd.Timestamp(k), float(v)) for k, v in hits.items()]


# ---------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------
def plot_event_window(
    df: pd.DataFrame,
    ts_col: str,
    vcol: str,
    *,
    logger_tag: str,
    kind: str,
    event_ts: pd.Timestamp,
    months: int,
    out_path: Path,
    brownout_threshold: float | None = None,
    hv_warn: float | None = None,
    hv_critical: float | None = None,
) -> None:
    """
    Save a per-event plot:
      - light raw BattV
      - daily minimum trend
      - vertical event line
      - optional brownout threshold line
      - optional high-voltage warn/critical lines
    """
    start = event_ts - pd.DateOffset(months=months)
    end = event_ts + pd.DateOffset(months=months)

    win = df[(df[ts_col] >= start) & (df[ts_col] <= end)].copy()
    if win.empty:
        return

    win = win.sort_values(ts_col)

    series = pd.Series(
        win[vcol].to_numpy(),
        index=pd.DatetimeIndex(win[ts_col]),
    ).dropna()

    daily_min = series.resample("1D").min().dropna()

    plt.figure(figsize=(14, 7))
    plt.plot(win[ts_col], win[vcol], alpha=0.25, linewidth=0.8, label=f"Raw {vcol}")
    if not daily_min.empty:
        plt.plot(daily_min.index, daily_min.values, linewidth=2.0, label="Daily minimum")

    plt.axvline(event_ts, linestyle="--", linewidth=2.0, label="Event")
    if brownout_threshold is not None:
        plt.axhline(brownout_threshold, linestyle=":", linewidth=2.0, label="Brownout threshold")

    if hv_warn is not None:
        plt.axhline(hv_warn, linestyle=":", linewidth=2.0, label=f"High-V warn ({hv_warn:g}V)")
    if hv_critical is not None:
        plt.axhline(hv_critical, linestyle=":", linewidth=2.0, label=f"High-V critical ({hv_critical:g}V)")

    plt.title(f"{logger_tag} {kind}: Battery Voltage around event {event_ts:%Y-%m-%d %H:%M}")
    plt.xlabel("Timestamp (logger local time)")
    plt.ylabel(vcol)
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.legend()
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close()


# ---------------------------------------------------------------------
def infer_logger_tag(path: Path) -> str:
    # S3M_Table1.dat -> S3M ; S3M_Table1_late2023_withBattV.dat -> S3M
    return path.name.split("_")[0]


def infer_year_from_parent(path: Path) -> int:
    # .../datfiles_2024/XYZ.dat -> 2024
    m = re.search(r"datfiles_(\d{4})", str(path))
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


def iter_dat_files(data_root: Path) -> list[tuple[Path, str]]:
    """
    Only scan "authoritative" locations:
      biochar_app/data-raw/datfiles_2023..datfiles_2026
    and only Table1 .dat files.

    Additionally include late-2023 backfill files (if present):
      S1M_Table1_late2023_withBattV.dat
    """
    out: list[tuple[Path, str]] = []
    for year in (2023, 2024, 2025, 2026):
        year_dir = data_root / f"datfiles_{year}"
        if not year_dir.exists():
            continue

        # main files
        for fp in sorted(year_dir.glob("S*_Table1.dat")):
            out.append((fp, "main"))

        # optional backfill files (typically in datfiles_2023)
        for fp in sorted(year_dir.glob("S*_Table1_late2023_withBattV.dat")):
            out.append((fp, "late2023_withBattV"))

    # De-dup exact same path, just in case
    seen: set[str] = set()
    uniq: list[tuple[Path, str]] = []
    for fp, kind in out:
        key = str(fp.resolve())
        if key in seen:
            continue
        seen.add(key)
        uniq.append((fp, kind))
    return uniq


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True, help="Path to data-raw (contains datfiles_YYYY/)")
    ap.add_argument("--start", required=True, help='Start timestamp, e.g. "2023-05-15 00:00"')
    ap.add_argument("--end", required=True, help='End timestamp, e.g. "2026-02-28 23:59"')
    ap.add_argument("--brownout", type=float, default=9.6, help="Voltage threshold to flag brownouts (default 9.6)")
    ap.add_argument("--gap-minutes", type=int, default=60, help="Gap threshold in minutes (default 60)")
    ap.add_argument(
        "--replacement-step",
        type=float,
        default=0.6,
        help="Daily-min upward step to flag replacement (default 0.6V)",
    )

    # High-voltage diagnostic thresholds
    ap.add_argument("--hv-warn", type=float, default=16.0, help="Flag BattV >= this as warn (default 16.0V)")
    ap.add_argument("--hv-critical", type=float, default=18.0, help="Flag BattV >= this as critical (default 18.0V)")

    # Reports
    ap.add_argument(
        "--out-csv",
        default=str(REPORT_DIR / "battv_events.csv"),
        help="Output CSV path (default: diagnostics/reports/battv_events.csv)",
    )
    ap.add_argument(
        "--out-gaps-csv",
        default=str(REPORT_DIR / "battv_gaps.csv"),
        help="Output CSV path for gaps-only report (default: diagnostics/reports/battv_gaps.csv)",
    )

    # Plot options
    ap.add_argument(
        "--write-plots",
        action="store_true",
        help="If set, write per-event PNGs under diagnostics/plots/voltage_events/",
    )
    ap.add_argument("--plot-months", type=int, default=2, help="Months before/after each event for plots (default 2)")
    ap.add_argument("--plot-max-per-logger", type=int, default=25, help="Cap plots per logger (default 25)")
    ap.add_argument(
        "--plot-kinds",
        default="brownout,gap,replacement_step,high_voltage_warn,high_voltage_critical",
        help="Comma-separated event kinds to plot (default all)",
    )
    args = ap.parse_args()

    data_root = Path(args.data_root).expanduser()
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)

    want_plot = bool(args.write_plots)
    plot_kinds = {k.strip() for k in str(args.plot_kinds).split(",") if k.strip()}

    rows: list[EventRow] = []
    gap_rows: list[EventRow] = []

    files = iter_dat_files(data_root)
    if not files:
        raise FileNotFoundError(
            f"No Table1 .dat files found under {data_root}/datfiles_2023..2026 "
            "matching S*_Table1.dat (and optional *_late2023_withBattV.dat)."
        )

    plots_written: dict[str, int] = {}

    for fp, file_kind in files:
        tag = infer_logger_tag(fp)
        year = infer_year_from_parent(fp)

        try:
            df = read_toa5_dat(fp)
        except Exception as e:
            rows.append(
                EventRow(
                    tag,
                    year,
                    str(fp),
                    file_kind,
                    "read_error",
                    "critical",
                    start,
                    end,
                    f"{fp.name}: {e}",
                    np.nan,
                    np.nan,
                    np.nan,
                    None,
                    None,
                )
            )
            continue

        ts_col = "TIMESTAMP" if "TIMESTAMP" in df.columns else "timestamp"
        df[ts_col] = coerce_timestamp(df[ts_col])
        df = df.dropna(subset=[ts_col]).sort_values(ts_col)

        # Restrict to requested window (for ALL events, including gaps)
        df = df[(df[ts_col] >= start) & (df[ts_col] <= end)].copy()
        if df.empty:
            continue

        # ---- gaps (ALWAYS)
        gaps = find_gap_events(df[ts_col], gap_minutes=args.gap_minutes)
        for prev_ts, next_ts, mins in gaps:
            er = EventRow(
                tag,
                year,
                str(fp),
                file_kind,
                "gap",
                "info",
                prev_ts,
                next_ts,
                f"{fp.name}",
                np.nan,
                np.nan,
                float(mins),
                None,
                None,
            )
            gap_rows.append(er)

            # Optional plotting for gaps requires voltage column; only do if we have it
            if want_plot and "gap" in plot_kinds:
                # we can still plot gaps if vcol exists; otherwise skip plot
                pass

            rows.append(er)

        # ---- voltage-based diagnostics (only if BattV col exists)
        vcol = detect_voltage_col(df.columns)
        if vcol is None:
            # Important: we do NOT "continue" before gaps (we already did gaps above).
            # We keep a single informational row for this file.
            rows.append(
                EventRow(
                    tag,
                    year,
                    str(fp),
                    file_kind,
                    "no_voltage_col",
                    "info",
                    start,
                    end,
                    f"{fp.name}: no BattV-like column",
                    np.nan,
                    np.nan,
                    np.nan,
                    None,
                    None,
                )
            )
            continue

        df[vcol] = pd.to_numeric(df[vcol], errors="coerce")

        # ---- brownouts
        for s, e, minv in find_brownout_runs(df, ts_col, vcol, args.brownout):
            er = EventRow(
                tag,
                year,
                str(fp),
                file_kind,
                "brownout",
                "warn",
                s,
                e,
                f"{fp.name}",
                float(minv),
                np.nan,
                np.nan,
                vcol,
                None,
            )

            if want_plot and "brownout" in plot_kinds:
                n = plots_written.get(tag, 0)
                if n < args.plot_max_per_logger:
                    event_ts = s
                    out_path = PLOT_DIR / tag / f"{tag}_brownout_{event_ts:%Y%m%d_%H%M}.png"
                    plot_event_window(
                        df,
                        ts_col,
                        vcol,
                        logger_tag=tag,
                        kind="brownout",
                        event_ts=event_ts,
                        months=args.plot_months,
                        out_path=out_path,
                        brownout_threshold=args.brownout,
                        hv_warn=args.hv_warn,
                        hv_critical=args.hv_critical,
                    )
                    er.plot_path = str(out_path)
                    plots_written[tag] = n + 1

            rows.append(er)

        # ---- replacement-like steps
        for day, dv in detect_replacement_steps(df, ts_col, vcol, step_v=args.replacement_step):
            er = EventRow(
                tag,
                year,
                str(fp),
                file_kind,
                "replacement_step",
                "info",
                day,
                day,
                f"{fp.name} daily_min +{dv:.2f}V",
                np.nan,
                np.nan,
                np.nan,
                vcol,
                None,
            )

            if want_plot and "replacement_step" in plot_kinds:
                n = plots_written.get(tag, 0)
                if n < args.plot_max_per_logger:
                    event_ts = day
                    out_path = PLOT_DIR / tag / f"{tag}_replacement_step_{event_ts:%Y%m%d_%H%M}_dV{dv:.2f}.png"
                    plot_event_window(
                        df,
                        ts_col,
                        vcol,
                        logger_tag=tag,
                        kind=f"replacement_step(+{dv:.2f}V)",
                        event_ts=event_ts,
                        months=args.plot_months,
                        out_path=out_path,
                        brownout_threshold=args.brownout,
                        hv_warn=args.hv_warn,
                        hv_critical=args.hv_critical,
                    )
                    er.plot_path = str(out_path)
                    plots_written[tag] = n + 1

            rows.append(er)

        # ---- high-voltage runs (warn + critical)
        for s, e, maxv in find_high_voltage_runs(df, ts_col, vcol, args.hv_warn):
            er = EventRow(
                tag,
                year,
                str(fp),
                file_kind,
                "high_voltage_warn",
                "warn",
                s,
                e,
                f"{fp.name} {vcol} >= {args.hv_warn:g}V",
                np.nan,
                float(maxv),
                np.nan,
                vcol,
                None,
            )

            if want_plot and "high_voltage_warn" in plot_kinds:
                n = plots_written.get(tag, 0)
                if n < args.plot_max_per_logger:
                    event_ts = s
                    out_path = PLOT_DIR / tag / f"{tag}_highV_warn_{event_ts:%Y%m%d_%H%M}_max{maxv:.2f}.png"
                    plot_event_window(
                        df,
                        ts_col,
                        vcol,
                        logger_tag=tag,
                        kind=f"high_voltage_warn(max={maxv:.2f}V)",
                        event_ts=event_ts,
                        months=args.plot_months,
                        out_path=out_path,
                        brownout_threshold=args.brownout,
                        hv_warn=args.hv_warn,
                        hv_critical=args.hv_critical,
                    )
                    er.plot_path = str(out_path)
                    plots_written[tag] = n + 1

            rows.append(er)

        for s, e, maxv in find_high_voltage_runs(df, ts_col, vcol, args.hv_critical):
            er = EventRow(
                tag,
                year,
                str(fp),
                file_kind,
                "high_voltage_critical",
                "critical",
                s,
                e,
                f"{fp.name} {vcol} >= {args.hv_critical:g}V",
                np.nan,
                float(maxv),
                np.nan,
                vcol,
                None,
            )

            if want_plot and "high_voltage_critical" in plot_kinds:
                n = plots_written.get(tag, 0)
                if n < args.plot_max_per_logger:
                    event_ts = s
                    out_path = PLOT_DIR / tag / f"{tag}_highV_critical_{event_ts:%Y%m%d_%H%M}_max{maxv:.2f}.png"
                    plot_event_window(
                        df,
                        ts_col,
                        vcol,
                        logger_tag=tag,
                        kind=f"high_voltage_critical(max={maxv:.2f}V)",
                        event_ts=event_ts,
                        months=args.plot_months,
                        out_path=out_path,
                        brownout_threshold=args.brownout,
                        hv_warn=args.hv_warn,
                        hv_critical=args.hv_critical,
                    )
                    er.plot_path = str(out_path)
                    plots_written[tag] = n + 1

            rows.append(er)

        # ---- optional: gap plots (only if vcol exists; now that we know it exists)
        if want_plot and "gap" in plot_kinds and gaps:
            for prev_ts, next_ts, mins in gaps:
                n = plots_written.get(tag, 0)
                if n >= args.plot_max_per_logger:
                    break
                event_ts = next_ts
                out_path = PLOT_DIR / tag / f"{tag}_gap_{event_ts:%Y%m%d_%H%M}_mins{int(mins)}.png"
                plot_event_window(
                    df,
                    ts_col,
                    vcol,
                    logger_tag=tag,
                    kind=f"gap_{mins:.0f}min",
                    event_ts=event_ts,
                    months=args.plot_months,
                    out_path=out_path,
                    brownout_threshold=args.brownout,
                    hv_warn=args.hv_warn,
                    hv_critical=args.hv_critical,
                )
                plots_written[tag] = n + 1

    out = pd.DataFrame([r.__dict__ for r in rows])
    if not out.empty:
        out = out.sort_values(["logger_tag", "year", "start", "kind"]).reset_index(drop=True)

    out_csv = Path(args.out_csv).expanduser()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    gaps_out = pd.DataFrame([r.__dict__ for r in gap_rows])
    if not gaps_out.empty:
        gaps_out = gaps_out.sort_values(["logger_tag", "year", "start"]).reset_index(drop=True)
    out_gaps_csv = Path(args.out_gaps_csv).expanduser()
    out_gaps_csv.parent.mkdir(parents=True, exist_ok=True)
    gaps_out.to_csv(out_gaps_csv, index=False)

    print(f"Scanned Table1 .dat files: {len(files)}")
    print(f"Wrote {len(out)} events -> {out_csv}")
    print(f"Wrote {len(gaps_out)} gap rows -> {out_gaps_csv}")

    if want_plot:
        total_plots = sum(plots_written.values())
        print(f"Wrote {total_plots} plots under -> {PLOT_DIR}")
        if total_plots == 0:
            print("Note: plotting was enabled, but no events matched (or plot limits were hit).")


if __name__ == "__main__":
    main()
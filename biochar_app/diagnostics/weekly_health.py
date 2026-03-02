#!/usr/bin/env python3
"""
biochar_app/diagnostics/weekly_health.py

Weekly (manual-for-now) diagnostics report for logger data health.

What it does
------------
- Loads processed raw logger parquet(s) for one or more years
- Summarizes:
  * timestamp continuity (duplicates, gaps, inferred step)
  * missingness (top NaN columns)
  * battery voltage (BattV_Min_*): percentiles + out-of-range checks
- Writes:
  * Markdown report to biochar_app/diagnostics/reports/weekly_health_YYYY-MM-DD.md
  * CSV summaries alongside the report (optional but handy)

Notes
-----
- Designed to be run manually for now (e.g., after you update .dat files and re-run ETL).
- Later can be scheduled wherever auto-download runs.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# We mirror the conventions used in your etl.py imports.
from biochar_app.scripts.config import PARQUET_DIR, YEARS  # type: ignore


REPORTS_DIR = Path(__file__).resolve().parent / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------- small helpers ----------------------------- #


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_raw_logger_parquet(year: int) -> pd.DataFrame:
    """
    Expected path (per your etl.py):
      PARQUET_DIR/<year>/<year>_raw_logger.parquet
    """
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

    # Force dtype for stable diff operations
    df["timestamp"] = df["timestamp"].astype("datetime64[ns]")
    return df


def _safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def _fmt_float(x: float) -> str:
    if np.isnan(x):
        return "NaN"
    return f"{x:.3f}"


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    if not rows:
        return "_(none)_\n"
    out: List[str] = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out) + "\n"


def _infer_step_minutes(ts: pd.Series) -> Optional[float]:
    """
    Infer the median positive timestep (minutes) from a timestamp series.
    """
    if ts.shape[0] < 3:
        return None

    diffs_raw = ts.diff().dropna()
    diffs_td = pd.to_timedelta(diffs_raw, errors="coerce").dropna()
    diffs_pos = diffs_td[diffs_td > pd.Timedelta(0)]

    if diffs_pos.empty:
        return None

    med = diffs_pos.median()
    med_td = pd.Timedelta(med)
    return med_td.total_seconds() / 60.0


# ----------------------------- checks ----------------------------- #


@dataclass(frozen=True)
class TimeHealth:
    n_rows: int
    inferred_step_min: Optional[float]
    n_duplicate_timestamps: int
    n_nonmonotonic: int
    n_large_gaps: int
    largest_gap_minutes: Optional[float]


def check_time_health(df: pd.DataFrame, gap_multiplier: float = 3.0) -> TimeHealth:
    ts = df["timestamp"]
    n = int(ts.shape[0])

    inferred = _infer_step_minutes(ts)

    # duplicates
    n_dup = int(ts.duplicated().sum())

    # diffs as timedeltas (mypy-safe + robust if dtype gets weird)
    diffs_raw = ts.diff()
    diffs_td = pd.to_timedelta(diffs_raw, errors="coerce")

    # non-monotonic (any negative diffs)
    n_nonmono = int((diffs_td < pd.Timedelta(0)).sum())

    # large gaps (based on inferred step; fall back to 15 min)
    step_min = inferred if inferred is not None else 15.0
    gap_thresh = pd.Timedelta(minutes=step_min * gap_multiplier)

    pos_diffs = diffs_td.dropna()
    pos_diffs = pos_diffs[pos_diffs > pd.Timedelta(0)]

    large_gaps = pos_diffs[pos_diffs > gap_thresh]
    n_gaps = int(large_gaps.shape[0])

    largest_gap_min: Optional[float] = None
    if not large_gaps.empty:
        max_gap_td = pd.Timedelta(large_gaps.max())
        largest_gap_min = max_gap_td.total_seconds() / 60.0

    return TimeHealth(
        n_rows=n,
        inferred_step_min=inferred,
        n_duplicate_timestamps=n_dup,
        n_nonmonotonic=n_nonmono,
        n_large_gaps=n_gaps,
        largest_gap_minutes=largest_gap_min,
    )


@dataclass(frozen=True)
class MissingnessRow:
    column: str
    nan_count: int
    nan_fraction: float


def compute_missingness(df: pd.DataFrame, top_n: int = 20) -> List[MissingnessRow]:
    # exclude timestamp
    cols = [c for c in df.columns if c != "timestamp"]
    if not cols:
        return []

    n = float(df.shape[0])
    nan_counts = df[cols].isna().sum().sort_values(ascending=False)

    out: List[MissingnessRow] = []
    for col, cnt in nan_counts.head(top_n).items():
        c = int(cnt)
        out.append(MissingnessRow(column=str(col), nan_count=c, nan_fraction=(c / n if n else 0.0)))
    return out


@dataclass(frozen=True)
class BatterySummary:
    column: str
    n: int
    min_v: float
    p01: float
    p50: float
    p99: float
    max_v: float
    out_of_range_count: int
    out_of_range_fraction: float


def battery_health(
    df: pd.DataFrame,
    vmin_ok: float = 11.0,
    vmax_ok: float = 13.0,
    top_n: int = 50,
) -> Tuple[List[BatterySummary], pd.DataFrame]:
    """
    Returns:
      - per-column battery summaries
      - a "violations" dataframe with timestamp/column/value for out-of-range rows (capped to top_n per column)
    """
    batt_cols = [c for c in df.columns if str(c).startswith("BattV_Min_")]
    if not batt_cols:
        return [], pd.DataFrame(columns=["timestamp", "column", "value", "reason"])

    all_violations: List[pd.DataFrame] = []
    summaries: List[BatterySummary] = []

    for col in batt_cols:
        s = _safe_numeric(df[col]).dropna()
        n = int(s.shape[0])
        if n == 0:
            continue

        q = s.quantile([0.01, 0.50, 0.99])
        min_v = float(s.min())
        p01 = float(q.loc[0.01])
        p50 = float(q.loc[0.50])
        p99 = float(q.loc[0.99])
        max_v = float(s.max())

        mask_low = s < vmin_ok
        mask_high = s > vmax_ok
        oor = mask_low | mask_high
        oor_count = int(oor.sum())
        oor_frac = float(oor_count / n) if n else 0.0

        summaries.append(
            BatterySummary(
                column=str(col),
                n=n,
                min_v=min_v,
                p01=p01,
                p50=p50,
                p99=p99,
                max_v=max_v,
                out_of_range_count=oor_count,
                out_of_range_fraction=oor_frac,
            )
        )

        if oor_count:
            # align to original df timestamps by index
            oor_idx = s[oor].index
            vdf = df.loc[oor_idx, ["timestamp"]].copy()
            vdf["column"] = str(col)
            vdf["value"] = _safe_numeric(df.loc[oor_idx, col])
            vdf["reason"] = np.where(vdf["value"] < vmin_ok, "below_min", "above_max")
            vdf = vdf.sort_values("timestamp").head(top_n)
            all_violations.append(vdf)

    summaries.sort(key=lambda x: (-x.out_of_range_fraction, x.column))

    if all_violations:
        violations_df = pd.concat(all_violations, ignore_index=True)
    else:
        violations_df = pd.DataFrame(columns=["timestamp", "column", "value", "reason"])

    return summaries, violations_df


# ----------------------------- report writing ----------------------------- #


def write_report(
    report_path: Path,
    year_blocks: List[Tuple[int, TimeHealth, List[MissingnessRow], List[BatterySummary]]],
    batt_vmin_ok: float,
    batt_vmax_ok: float,
) -> None:
    lines: List[str] = []
    lines.append("# Weekly Logger Health Report")
    lines.append("")
    lines.append(f"- Report date: **{date.today().isoformat()}**")
    lines.append(f"- Battery rule (BattV_Min): flag outside **[{batt_vmin_ok}, {batt_vmax_ok}] V**")
    lines.append("")

    for year, th, miss, batt in year_blocks:
        lines.append(f"## Year {year}")
        lines.append("")
        lines.append("### Timestamp health")
        lines.append("")
        lines.append(
            _md_table(
                ["rows", "inferred_step_min", "dup_timestamps", "nonmonotonic", "large_gaps", "largest_gap_min"],
                [[
                    str(th.n_rows),
                    "—" if th.inferred_step_min is None else f"{th.inferred_step_min:.3f}",
                    str(th.n_duplicate_timestamps),
                    str(th.n_nonmonotonic),
                    str(th.n_large_gaps),
                    "—" if th.largest_gap_minutes is None else f"{th.largest_gap_minutes:.1f}",
                ]],
            )
        )

        lines.append("### Missingness (top columns)")
        lines.append("")
        miss_rows = [[m.column, str(m.nan_count), _pct(m.nan_fraction)] for m in miss]
        lines.append(_md_table(["column", "nan_count", "nan_fraction"], miss_rows))

        lines.append("### Battery (BattV_Min_*)")
        lines.append("")
        if not batt:
            lines.append("_(no BattV_Min_* columns found)_\n")
        else:
            batt_rows: List[List[str]] = []
            for b in batt:
                batt_rows.append([
                    b.column,
                    str(b.n),
                    _fmt_float(b.min_v),
                    _fmt_float(b.p01),
                    _fmt_float(b.p50),
                    _fmt_float(b.p99),
                    _fmt_float(b.max_v),
                    str(b.out_of_range_count),
                    _pct(b.out_of_range_fraction),
                ])
            lines.append(
                _md_table(
                    ["column", "n", "min", "p01", "p50", "p99", "max", "out_of_range", "out_frac"],
                    batt_rows,
                )
            )

        lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Weekly logger health report (raw parquet diagnostics).")
    ap.add_argument("--year", type=int, default=None, help="Single year to analyze (default: latest from YEARS).")
    ap.add_argument("--years", type=int, nargs="*", default=None, help="Multiple years to analyze.")
    ap.add_argument("--batt-min", type=float, default=11.0, help="Lower bound for BattV_Min (V).")
    ap.add_argument("--batt-max", type=float, default=13.0, help="Upper bound for BattV_Min (V).")
    ap.add_argument("--gap-mult", type=float, default=3.0, help="Large-gap threshold multiplier vs inferred step.")
    ap.add_argument("--top-missing", type=int, default=20, help="Top N missingness columns to show.")
    ap.add_argument("--write-csv", action="store_true", help="Write CSV summaries alongside the MD report.")
    args = ap.parse_args()

    # Determine years to run
    years_list: List[int]
    if args.years:
        years_list = [int(y) for y in args.years]
    elif args.year is not None:
        years_list = [int(args.year)]
    else:
        try:
            years_list = [int(max(YEARS))]
        except Exception as e:
            raise ValueError("Could not determine default year from YEARS; pass --year or --years explicitly.") from e

    reports_dir = Path(__file__).resolve().parent / "reports"
    _ensure_dir(reports_dir)

    report_date = date.today().isoformat()
    report_md = reports_dir / f"weekly_health_{report_date}.md"

    print(
        "🩺 weekly_health starting\n"
        f"  years={years_list}\n"
        f"  reports_dir={reports_dir}\n"
        f"  batt_ok=[{float(args.batt_min):g}, {float(args.batt_max):g}] V\n"
        f"  gap_mult={float(args.gap_mult):g}\n"
        f"  top_missing={int(args.top_missing)}\n"
        f"  write_csv={bool(args.write_csv)}"
    )

    year_blocks: List[Tuple[int, TimeHealth, List[MissingnessRow], List[BatterySummary]]] = []

    # Optional CSV outputs
    missing_csv_rows: List[Dict[str, object]] = []
    batt_csv_rows: List[Dict[str, object]] = []
    batt_violations_all: List[pd.DataFrame] = []

    for y in years_list:
        print(f"— Processing {y} …")
        df = _read_raw_logger_parquet(y)

        th = check_time_health(df, gap_multiplier=float(args.gap_mult))
        miss = compute_missingness(df, top_n=int(args.top_missing))

        batt_summ, batt_viol = battery_health(
            df,
            vmin_ok=float(args.batt_min),
            vmax_ok=float(args.batt_max),
            top_n=200,
        )

        year_blocks.append((y, th, miss, batt_summ))

        if args.write_csv:
            for m in miss:
                missing_csv_rows.append(
                    {"year": y, "column": m.column, "nan_count": m.nan_count, "nan_fraction": m.nan_fraction}
                )
            for b in batt_summ:
                batt_csv_rows.append(
                    {
                        "year": y,
                        "column": b.column,
                        "n": b.n,
                        "min_v": b.min_v,
                        "p01": b.p01,
                        "p50": b.p50,
                        "p99": b.p99,
                        "max_v": b.max_v,
                        "out_of_range_count": b.out_of_range_count,
                        "out_of_range_fraction": b.out_of_range_fraction,
                        "batt_min_ok": float(args.batt_min),
                        "batt_max_ok": float(args.batt_max),
                    }
                )
            if not batt_viol.empty:
                batt_viol = batt_viol.copy()
                batt_viol.insert(0, "year", y)
                batt_violations_all.append(batt_viol)

    write_report(
        report_md,
        year_blocks=year_blocks,
        batt_vmin_ok=float(args.batt_min),
        batt_vmax_ok=float(args.batt_max),
    )

    wrote_paths: List[Path] = [report_md]

    if args.write_csv:
        if missing_csv_rows:
            p = reports_dir / f"weekly_missing_{report_date}.csv"
            pd.DataFrame(missing_csv_rows).to_csv(p, index=False)
            wrote_paths.append(p)
        if batt_csv_rows:
            p = reports_dir / f"weekly_battery_{report_date}.csv"
            pd.DataFrame(batt_csv_rows).to_csv(p, index=False)
            wrote_paths.append(p)
        if batt_violations_all:
            p = reports_dir / f"weekly_battery_violations_{report_date}.csv"
            pd.concat(batt_violations_all, ignore_index=True).to_csv(p, index=False)
            wrote_paths.append(p)

    print("✅ Wrote:")
    for p in wrote_paths:
        print(f"  - {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
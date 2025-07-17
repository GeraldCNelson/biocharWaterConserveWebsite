# DST correction summary script: categorize DST shifts by logger, year, and list major shift dates
import pandas as pd
import logging
from pathlib import Path
from biochar_app.scripts.config import DATA_RAW_DIR

# Base directory with subfolders datfiles_2023, datfiles_2024, datfiles_2025
BASE_DIR = Path(DATA_RAW_DIR)


def find_dst_summary_with_dates(base_dir: Path) -> dict[str, dict[int, dict[str, any]]]:
    """
    Scan each *_Table1.dat across year subdirs, detect DST jumps,
    count minor (<=1.25h) and major (>=5h) shifts per logger per year,
    and record the dates of the major shifts.

    Returns:
      {logger: { year: { 'minor': int, 'major': int, 'major_dates': list[str] } }}
    """
    summary: dict[str, dict[int, dict[str, any]]] = {}

    for year_dir in sorted(base_dir.glob('datfiles_*')):
        try:
            year = int(year_dir.name.split('_')[-1])
        except ValueError:
            continue

        for path in year_dir.glob('*_Table1.dat'):
            logger_name = path.stem.split('_')[0]
            # read only timestamp column as string
            try:
                df = pd.read_csv(
                    path,
                    skiprows=4,
                    header=None,
                    usecols=[0],
                    names=['timestamp'],
                    dtype={'timestamp': str},
                )
            except Exception as e:
                logging.warning(f"Failed to read {path.name}: {e}")
                continue

            # parse timestamps and restrict to same-year
            df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            df = df.dropna(subset=['timestamp'])
            df = df[df['timestamp'].dt.year == year]

            if df.empty:
                continue

            # compute diffs in hours
            diffs = df['timestamp'].diff().dt.total_seconds() / 3600.0

            # detect minor and major shifts
            minor_count = int(diffs.abs().le(1.25).sum())
            major_mask = diffs.abs().ge(5.0)
            major_count = int(major_mask.sum())

            # collect major shift dates
            major_dates = df.loc[major_mask, 'timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()

            # record
            logger_summary = summary.setdefault(logger_name, {})
            year_summary = logger_summary.setdefault(year, {})
            year_summary['minor'] = minor_count
            year_summary['major'] = major_count
            year_summary['major_dates'] = major_dates

    return summary


def main():
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"Base data directory not found: {BASE_DIR}")

    summary = find_dst_summary_with_dates(BASE_DIR)
    if not summary:
        print("No DST shift data found.")
        return

    # print summary
    for logger, years in sorted(summary.items()):
        print(f"{logger}:")
        for year, stats in sorted(years.items()):
            print(f"  {year}: minor shifts={stats['minor']}, major shifts={stats['major']}")
            if stats['major']:
                print("    Major shift dates:")
                for date in stats['major_dates']:
                    print(f"      - {date}")
        print()


if __name__ == '__main__':
    main()

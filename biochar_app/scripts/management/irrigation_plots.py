# irrigation_plots.py
from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, cast

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from biochar_app.scripts.management.irrigation_analysis import (
    DEPTH_INDEX_TO_INCHES,
    _validate_datetime_index,
)


def _is_missing(value: object) -> bool:
    return bool(pd.Series([value]).isna().iloc[0])


def _as_float_or_none(value: object) -> float | None:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if _is_missing(num):
        return None
    return float(num)


def _safe_filename(text: str) -> str:
    text = re.sub(r"[^\w\-\.]+", "_", text.strip())
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def _fmt1(value: object) -> str:
    num = _as_float_or_none(value)
    if num is None:
        return "NA"
    return f"{num:.1f}"


def _fmt_event_id(value: object) -> str:
    if value is None or _is_missing(value):
        return "NA"

    text = str(value)
    parts = text.split("_")

    if len(parts) >= 4 and re.match(r"^\d{4}-\d{2}-\d{2}$", parts[0]):
        date_label = pd.to_datetime(cast(Any, parts[0]), errors="coerce")
        date_text = (
            pd.Timestamp(date_label).strftime("%b %d")
            if not _is_missing(date_label)
            else parts[0]
        )
        return f"{parts[1]}/{parts[2]} {date_text}"

    return text[:22] + "…" if len(text) > 24 else text


def coerce_optional_timestamp(value: object) -> Optional[pd.Timestamp]:
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return None if _is_missing(value) else value

    ts = pd.to_datetime(cast(Any, value), errors="coerce")
    if _is_missing(ts):
        return None

    return pd.Timestamp(ts)


def _datetime_index_to_mpl_nums(index: pd.Index) -> np.ndarray:
    dt_index = pd.DatetimeIndex(index)
    py_dates = list(dt_index.to_pydatetime())
    return np.asarray(mdates.date2num(py_dates), dtype=float)


def _timestamp_to_mpl_num(ts: pd.Timestamp) -> float:
    return float(mdates.date2num(ts.to_pydatetime()))


def _get_strip_volume_and_flow(first_row: pd.Series) -> tuple[object, object, object]:
    gallons_strip = _as_float_or_none(first_row.get("gallons_strip", pd.NA))
    duration_hours = _as_float_or_none(first_row.get("event_duration_hours", pd.NA))
    avg_flow_gph_strip = _as_float_or_none(first_row.get("avg_flow_gph_strip", pd.NA))

    if (
        avg_flow_gph_strip is None
        and gallons_strip is not None
        and duration_hours is not None
        and duration_hours > 0
    ):
        avg_flow_gph_strip = gallons_strip / duration_hours

    return (
        pd.NA if gallons_strip is None else gallons_strip,
        pd.NA if duration_hours is None else duration_hours,
        pd.NA if avg_flow_gph_strip is None else avg_flow_gph_strip,
    )


def _get_plot_window_series(
    df: pd.DataFrame,
    sensor_col: str,
    irrigation_start: pd.Timestamp,
    hours_before: float,
    hours_after: float,
) -> pd.Series:
    plot_start = irrigation_start - pd.Timedelta(hours=hours_before)
    plot_end = irrigation_start + pd.Timedelta(hours=hours_after)

    series = pd.to_numeric(df[sensor_col], errors="coerce")
    return series.loc[plot_start:plot_end].dropna()


def _prepare_plot_window_df(
    df: pd.DataFrame,
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
) -> pd.DataFrame:
    _validate_datetime_index(df)
    return df.loc[pd.Timestamp(start):pd.Timestamp(end)].copy()


def _collect_multidepth_cols(
    strip: str,
    logger_position: str = "B",
    depths: Sequence[int] = (1, 2, 3),
) -> List[Tuple[str, str]]:
    cols: List[Tuple[str, str]] = []

    for depth in depths:
        if depth not in {1, 2, 3}:
            raise ValueError("depth values must be 1, 2, or 3")

        inches = DEPTH_INDEX_TO_INCHES[str(depth)]
        cols.append((f"VWC_{depth}_raw_{strip}_{logger_position}", f"{inches} in"))

    return cols


def compute_event_plot_ylim(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    strip_filter: Optional[Sequence[str]] = None,
    sensor_filter: Optional[Sequence[str]] = None,
    pad_fraction: float = 0.05,
) -> tuple[float, float] | None:
    if event_results.empty:
        return None

    work = event_results.copy()

    if strip_filter is not None:
        work = work[work["strip"].isin(strip_filter)].copy()

    if sensor_filter is not None:
        work = work[work["sensor_col"].isin(sensor_filter)].copy()

    mins: List[float] = []
    maxs: List[float] = []

    for _, row in work.iterrows():
        irrigation_start = coerce_optional_timestamp(row.get("irrigation_start"))
        sensor_col_obj = row.get("sensor_col")
        sensor_col = str(sensor_col_obj) if sensor_col_obj is not None else ""

        if irrigation_start is None or sensor_col not in df.columns:
            continue

        sub = _get_plot_window_series(
            df=df,
            sensor_col=sensor_col,
            irrigation_start=irrigation_start,
            hours_before=hours_before,
            hours_after=hours_after,
        )

        if sub.empty:
            continue

        mins.append(float(sub.min()))
        maxs.append(float(sub.max()))

    if not mins or not maxs:
        return None

    ymin = min(mins)
    ymax = max(maxs)
    yrange = ymax - ymin

    if yrange <= 0:
        return ymin - 1.0, ymax + 1.0

    pad = yrange * pad_fraction
    return ymin - pad, ymax + pad


def plot_irrigation_event_inspection(
    df: pd.DataFrame,
    event_row: pd.Series,
    output_path: Optional[str | Path] = None,
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    show: bool = False,
    y_limits: Optional[tuple[float, float]] = None,
    precip_col: Optional[str] = "precip_in",
) -> None:
    _validate_datetime_index(df)

    sensor_col = str(event_row["sensor_col"])
    if sensor_col not in df.columns:
        raise KeyError(f"Sensor column not found in dataframe: {sensor_col}")

    irrigation_start = coerce_optional_timestamp(event_row.get("irrigation_start"))
    irrigation_end = coerce_optional_timestamp(event_row.get("irrigation_end"))
    baseline_time = coerce_optional_timestamp(event_row.get("baseline_time"))
    peak_time = coerce_optional_timestamp(event_row.get("peak_time"))
    plateau_time = coerce_optional_timestamp(event_row.get("plateau_time"))

    if irrigation_start is None:
        raise ValueError("event_row is missing a valid irrigation_start")

    baseline_vwc = _as_float_or_none(event_row.get("baseline_vwc"))
    peak_vwc = _as_float_or_none(event_row.get("peak_vwc"))
    plateau_vwc = _as_float_or_none(event_row.get("plateau_vwc"))

    plot_start = irrigation_start - pd.Timedelta(hours=hours_before)
    plot_end = irrigation_start + pd.Timedelta(hours=hours_after)

    sub = _get_plot_window_series(
        df=df,
        sensor_col=sensor_col,
        irrigation_start=irrigation_start,
        hours_before=hours_before,
        hours_after=hours_after,
    )

    if sub.empty:
        raise ValueError(
            f"No data found for {sensor_col} between {plot_start} and {plot_end}"
        )

    strip = event_row.get("strip", "")
    year = event_row.get("year", "")
    event_id = event_row.get("event_id", "")
    plateau_method = event_row.get("plateau_method", "")
    duration_hr = event_row.get("event_duration_hours", pd.NA)
    t_peak_hr = event_row.get("time_to_peak_hours", pd.NA)
    t_plateau_hr = event_row.get("time_to_plateau_hours", pd.NA)

    fig, ax = plt.subplots(figsize=(13, 6))

    ax.plot(
        _datetime_index_to_mpl_nums(sub.index),
        np.asarray(sub.to_numpy(dtype=float), dtype=float),
        linewidth=1.8,
        label=sensor_col,
    )

    ax2 = None
    if precip_col and precip_col in df.columns:
        precip = (
            pd.to_numeric(df[precip_col], errors="coerce")
            .loc[plot_start:plot_end]
            .fillna(0)
        )
        if not precip.empty and float(precip.max()) > 0:
            ax2 = ax.twinx()
            ax2.bar(
                _datetime_index_to_mpl_nums(precip.index),
                np.asarray(precip.to_numpy(dtype=float), dtype=float),
                width=0.009,
                alpha=0.18,
                label=precip_col,
            )
            ax2.set_ylabel(precip_col)

    ax.axvline(
        _timestamp_to_mpl_num(irrigation_start),
        linestyle="--",
        linewidth=1.2,
        color="tab:blue",
        label="irrigation start",
    )

    if irrigation_end is not None:
        ax.axvspan(
            _timestamp_to_mpl_num(irrigation_start),
            _timestamp_to_mpl_num(irrigation_end),
            alpha=0.15,
            color="tab:blue",
            label="irrigation window",
        )
        ax.axvline(
            _timestamp_to_mpl_num(irrigation_end),
            linestyle="--",
            linewidth=1.2,
            color="tab:red",
            label="irrigation end",
        )

    if baseline_time is not None and baseline_vwc is not None:
        ax.scatter(
            [_timestamp_to_mpl_num(baseline_time)],
            [baseline_vwc],
            s=55,
            marker="o",
            label="baseline",
            zorder=5,
        )

    if peak_time is not None and peak_vwc is not None:
        ax.scatter(
            [_timestamp_to_mpl_num(peak_time)],
            [peak_vwc],
            s=70,
            marker="^",
            label="peak",
            zorder=6,
        )

    if plateau_time is not None and plateau_vwc is not None:
        ax.scatter(
            [_timestamp_to_mpl_num(plateau_time)],
            [plateau_vwc],
            s=70,
            marker="s",
            label="plateau",
            zorder=6,
        )

    title = (
        f"Strip: {strip} | Sensor: {sensor_col} | Event: {_fmt_event_id(event_id)} | "
        f"Year: {year}"
    )
    subtitle = (
        f"Duration (irrigation time): {_fmt1(duration_hr)} hr | "
        f"time_to_peak: {_fmt1(t_peak_hr)} hr | "
        f"time_to_plateau: {_fmt1(t_plateau_hr)} hr | "
        f"plateau_method: {plateau_method}"
    )

    ax.set_title(title + "\n" + subtitle)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("VWC (%)")
    ax.grid(True, alpha=0.3)

    ax.xaxis_date()
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d\n%H:%M"))

    if y_limits is not None:
        ax.set_ylim(*y_limits)

    handles1, labels1 = ax.get_legend_handles_labels()
    if ax2 is not None:
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(list(handles1) + list(handles2), list(labels1) + list(labels2), loc="best")
    else:
        ax.legend(loc="best")

    fig.autofmt_xdate(rotation=35, ha="right")
    fig.tight_layout()

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    if show:
        plt.show()

    plt.close(fig)


def save_irrigation_event_inspection_plots(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    output_dir: str | Path,
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    strip_filter: Optional[Sequence[str]] = None,
    sensor_filter: Optional[Sequence[str]] = None,
    max_plots: Optional[int] = None,
    skip_no_peak: bool = False,
    use_common_y_axis: bool = True,
    precip_col: Optional[str] = "precip_in",
) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame(
            columns=["event_id", "strip", "sensor_col", "output_file", "status"]
        )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    work = event_results.copy()

    if strip_filter is not None:
        work = work[work["strip"].isin(strip_filter)].copy()

    if sensor_filter is not None:
        work = work[work["sensor_col"].isin(sensor_filter)].copy()

    if skip_no_peak and "plateau_method" in work.columns:
        work = work[work["plateau_method"] != "no_peak"].copy()

    if max_plots is not None:
        work = work.head(max_plots).copy()

    y_limits = None
    if use_common_y_axis:
        y_limits = compute_event_plot_ylim(
            df=df,
            event_results=work,
            hours_before=hours_before,
            hours_after=hours_after,
        )

    log_rows: List[Dict[str, object]] = []

    for _, row in work.iterrows():
        event_id = row.get("event_id", "unknown")
        strip = row.get("strip", "unknown")
        sensor_col = row.get("sensor_col", "unknown")
        year = row.get("year", "unknown")

        output_file = out_dir / _safe_filename(
            f"{year}_{strip}_{sensor_col}_event_{event_id}.png"
        )

        try:
            plot_irrigation_event_inspection(
                df=df,
                event_row=row,
                output_path=output_file,
                hours_before=hours_before,
                hours_after=hours_after,
                show=False,
                y_limits=y_limits,
                precip_col=precip_col,
            )
            status = "written"
        except Exception as e:
            status = f"failed: {e}"

        log_rows.append(
            {
                "event_id": event_id,
                "strip": strip,
                "sensor_col": sensor_col,
                "output_file": str(output_file),
                "status": status,
            }
        )

    return pd.DataFrame(log_rows)


def plot_event_multidepth(
    df: pd.DataFrame,
    cols: Sequence[Tuple[str, str]],
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
    event_id: Optional[object] = None,
    strip: Optional[str] = None,
    year: Optional[int] = None,
    irrigation_start: Optional[pd.Timestamp | str] = None,
    irrigation_end: Optional[pd.Timestamp | str] = None,
    peaks: Optional[Mapping[str, pd.Timestamp | str]] = None,
    baselines: Optional[Mapping[str, pd.Timestamp | str]] = None,
    plateaus: Optional[Mapping[str, pd.Timestamp | str]] = None,
    output_path: Optional[str | Path] = None,
    show: bool = False,
    precip_col: Optional[str] = "precip_in",
    y_limits: Optional[Tuple[float, float]] = None,
    title_prefix: str = "Event Multi-depth VWC",
) -> None:
    sub = _prepare_plot_window_df(df, start=start, end=end)
    if sub.empty:
        raise ValueError("No data found in requested multi-depth plot window.")

    irrig_start_ts = coerce_optional_timestamp(irrigation_start)
    irrig_end_ts = coerce_optional_timestamp(irrigation_end)

    peak_ts_map = {
        k: ts
        for k, v in (peaks or {}).items()
        if (ts := coerce_optional_timestamp(v)) is not None
    }
    baseline_ts_map = {
        k: ts
        for k, v in (baselines or {}).items()
        if (ts := coerce_optional_timestamp(v)) is not None
    }
    plateau_ts_map = {
        k: ts
        for k, v in (plateaus or {}).items()
        if (ts := coerce_optional_timestamp(v)) is not None
    }

    fig, ax = plt.subplots(figsize=(14, 7))
    plotted_any = False

    for sensor_col, label in cols:
        if sensor_col not in sub.columns:
            continue

        series = pd.to_numeric(sub[sensor_col], errors="coerce")
        ax.plot(
            _datetime_index_to_mpl_nums(series.index),
            np.asarray(series.to_numpy(dtype=float), dtype=float),
            linewidth=2.0,
            label=label,
        )
        plotted_any = True

        marker_specs = [
            (baseline_ts_map.get(sensor_col), "o", 50),
            (peak_ts_map.get(sensor_col), "^", 70),
            (plateau_ts_map.get(sensor_col), "s", 65),
        ]

        for marker_time, marker, size in marker_specs:
            if marker_time is not None and marker_time in sub.index:
                marker_val = _as_float_or_none(sub.at[marker_time, sensor_col])
                if marker_val is not None:
                    ax.scatter(
                        [_timestamp_to_mpl_num(marker_time)],
                        [marker_val],
                        s=size,
                        marker=marker,
                        zorder=6,
                    )

    if not plotted_any:
        raise ValueError("None of the requested VWC columns were found in the plot window.")

    if irrig_start_ts is not None:
        ax.axvline(
            _timestamp_to_mpl_num(irrig_start_ts),
            linestyle="--",
            linewidth=1.3,
            color="tab:blue",
            label="Irrigation start",
        )

    if irrig_end_ts is not None:
        ax.axvline(
            _timestamp_to_mpl_num(irrig_end_ts),
            linestyle="--",
            linewidth=1.3,
            color="tab:red",
            label="Irrigation end",
        )

    if irrig_start_ts is not None and irrig_end_ts is not None:
        ax.axvspan(
            _timestamp_to_mpl_num(irrig_start_ts),
            _timestamp_to_mpl_num(irrig_end_ts),
            alpha=0.14,
            color="tab:blue",
            label="Irrigation window",
        )

    display_event_id = _fmt_event_id(event_id)

    title_bits: List[str] = []
    if strip is not None:
        title_bits.append(f"Strip: {strip}")
    title_bits.append(title_prefix)
    if event_id is not None:
        title_bits.append(f"Event: {display_event_id}")

    ax.set_title(" | ".join(title_bits), fontsize=13, fontweight="bold", loc="left")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("VWC (%)")
    ax.grid(True, alpha=0.25)

    ax.xaxis_date()
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d\n%H:%M"))

    if y_limits is not None:
        ax.set_ylim(*y_limits)

    marker_handles = [
        Line2D([0], [0], marker="o", linestyle="None", markersize=8, label="Baseline VWC"),
        Line2D([0], [0], marker="^", linestyle="None", markersize=8, label="Peak VWC"),
        Line2D([0], [0], marker="s", linestyle="None", markersize=8, label="Plateau VWC"),
    ]

    window_patch = Patch(
        facecolor="tab:blue",
        alpha=0.14,
        edgecolor="none",
        label="Irrigation window",
    )

    handles1, labels1 = ax.get_legend_handles_labels()

    filtered_handles: List[Any] = []
    filtered_labels: List[str] = []
    for handle, label in zip(handles1, labels1):
        if label == "Irrigation window":
            continue
        filtered_handles.append(handle)
        filtered_labels.append(str(label))

    legend_handles: List[Any] = filtered_handles + [window_patch] + marker_handles
    legend_labels: list[str] = (
            filtered_labels
            + ["Irrigation window"]
            + [str(h.get_label()) for h in marker_handles]
    )

    ax.legend(
        legend_handles,
        legend_labels,
        loc="center right",
        frameon=True,
        framealpha=0.9,
    )

    if year is not None:
        ax.text(
            0.01,
            -0.18,
            str(year),
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=10,
            fontweight="bold",
        )

    ax.text(
        0.01,
        -0.27,
        "All times local",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        style="italic",
    )

    fig.text(
        0.50,
        0.025,
        "Duration = irrigation time, measured from water start to water shutoff. "
        "Plot window extends before and after the event to show soil response.",
        ha="center",
        va="bottom",
        fontsize=9,
        bbox=dict(
            boxstyle="round,pad=0.45",
            facecolor="white",
            edgecolor="0.55",
            alpha=0.9,
        ),
    )

    fig.autofmt_xdate(rotation=35, ha="right")
    fig.subplots_adjust(left=0.07, right=0.97, top=0.90, bottom=0.22)

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    if show:
        plt.show()

    plt.close(fig)

def _event_id_mask(series: pd.Series, event_id: object) -> pd.Series:
    if _is_missing(event_id):
        return series.map(_is_missing)
    return series == event_id


def _event_label_for_filename(event_id: object, irrigation_start: pd.Timestamp, strip: str) -> str:
    if _is_missing(event_id):
        return f"{irrigation_start:%Y-%m-%d_%H%M}_{strip}"
    return str(event_id)


def plot_event_multidepth_from_results(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    strip: str,
    event_id: object,
    logger_position: str = "B",
    depths: Sequence[int] = (1, 2, 3),
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    output_path: Optional[str | Path] = None,
    show: bool = False,
    precip_col: Optional[str] = "precip_in",
    y_limits: Optional[Tuple[float, float]] = None,
) -> None:
    if event_results.empty:
        raise ValueError("event_results is empty.")

    work = event_results.copy()
    work = work[
        (work["strip"] == strip)
        & _event_id_mask(work["event_id"], event_id)
        & (work["logger_position"] == logger_position)
    ].copy()

    if work.empty:
        raise ValueError(
            f"No event_results rows found for strip={strip}, "
            f"event_id={event_id}, logger_position={logger_position}"
        )

    first_row = work.iloc[0]
    irrigation_start = coerce_optional_timestamp(first_row.get("irrigation_start"))
    irrigation_end = coerce_optional_timestamp(first_row.get("irrigation_end"))

    year_float = _as_float_or_none(first_row.get("year", None))
    year = int(year_float) if year_float is not None else None

    gallons_strip, duration_hours, avg_flow_gph_strip = _get_strip_volume_and_flow(first_row)

    gallons_strip_f = _as_float_or_none(gallons_strip)
    duration_hours_f = _as_float_or_none(duration_hours)
    avg_flow_gph_strip_f = _as_float_or_none(avg_flow_gph_strip)

    if gallons_strip_f is not None and duration_hours_f is not None and avg_flow_gph_strip_f is not None:
        title_prefix_with_flow = (
            f"Duration (irrigation time): {duration_hours_f:.2f} hr | "
            f"Strip volume: {gallons_strip_f:,.0f} gal | "
            f"Strip flow: {avg_flow_gph_strip_f:,.0f} gal/hr"
        )
    else:
        title_prefix_with_flow = "Multi-depth irrigation response"

    if irrigation_start is None:
        raise ValueError("Selected event has no valid irrigation_start.")

    start = irrigation_start - pd.Timedelta(hours=hours_before)
    end = irrigation_start + pd.Timedelta(hours=hours_after)

    baselines: Dict[str, pd.Timestamp] = {}
    peaks: Dict[str, pd.Timestamp] = {}
    plateaus: Dict[str, pd.Timestamp] = {}

    for _, row in work.iterrows():
        sensor_col = str(row["sensor_col"])

        baseline_time = coerce_optional_timestamp(row.get("baseline_time"))
        if baseline_time is not None:
            baselines[sensor_col] = baseline_time

        peak_time = coerce_optional_timestamp(row.get("peak_time"))
        if peak_time is not None:
            peaks[sensor_col] = peak_time

        plateau_time = coerce_optional_timestamp(row.get("plateau_time"))
        if plateau_time is not None:
            plateaus[sensor_col] = plateau_time

    plot_event_multidepth(
        df=df,
        cols=_collect_multidepth_cols(strip=strip, logger_position=logger_position, depths=depths),
        start=start,
        end=end,
        event_id=event_id,
        strip=strip,
        year=year,
        irrigation_start=irrigation_start,
        irrigation_end=irrigation_end,
        peaks=peaks,
        baselines=baselines,
        plateaus=plateaus,
        output_path=output_path,
        show=show,
        precip_col=precip_col,
        y_limits=y_limits,
        title_prefix=title_prefix_with_flow,
    )


def save_irrigation_event_multidepth_plots(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    output_dir: str | Path,
    strip_filter: Optional[Sequence[str]] = None,
    event_ids: Optional[Sequence[object]] = None,
    logger_position: str = "B",
    depths: Sequence[int] = (1, 2, 3),
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    max_plots: Optional[int] = None,
    precip_col: Optional[str] = "precip_in",
    use_common_y_axis: bool = True,
) -> pd.DataFrame:
    log_columns = [
        "event_id",
        "strip",
        "logger_position",
        "irrigation_start",
        "irrigation_end",
        "plot_start",
        "plot_end",
        "event_duration_hours",
        "gallons_strip",
        "avg_flow_gph_strip",
        "output_file",
        "status",
    ]

    if event_results.empty:
        return pd.DataFrame(columns=log_columns)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    work = event_results.copy()

    if strip_filter is not None:
        work = work[work["strip"].isin(strip_filter)].copy()

    if event_ids is not None:
        work = work[work["event_id"].isin(event_ids)].copy()

    work = work[work["logger_position"] == logger_position].copy()

    if work.empty:
        return pd.DataFrame(columns=log_columns)

    unique_events = (
        work[["strip", "event_id", "logger_position"]]
        .drop_duplicates()
        .sort_values(["strip", "event_id"])
        .reset_index(drop=True)
    )

    if max_plots is not None:
        unique_events = unique_events.head(max_plots).copy()

    y_limits: Optional[Tuple[float, float]] = None

    if use_common_y_axis:
        mins: List[float] = []
        maxs: List[float] = []

        for _, event_key in unique_events.iterrows():
            sub_rows = work[
                (work["strip"] == event_key["strip"])
                & _event_id_mask(work["event_id"], event_key["event_id"])
                & (work["logger_position"] == event_key["logger_position"])
            ].copy()

            if sub_rows.empty:
                continue

            irrigation_start = coerce_optional_timestamp(
                sub_rows.iloc[0].get("irrigation_start")
            )

            if irrigation_start is None:
                continue

            start = irrigation_start - pd.Timedelta(hours=hours_before)
            end = irrigation_start + pd.Timedelta(hours=hours_after)

            sub_df = _prepare_plot_window_df(df, start, end)

            for depth in depths:
                sensor_col = f"VWC_{depth}_raw_{event_key['strip']}_{logger_position}"

                if sensor_col not in sub_df.columns:
                    continue

                series = pd.to_numeric(sub_df[sensor_col], errors="coerce").dropna()

                if not series.empty:
                    mins.append(float(series.min()))
                    maxs.append(float(series.max()))

        if mins and maxs:
            ymin = min(mins)
            ymax = max(maxs)
            yrange = ymax - ymin

            y_limits = (
                (ymin - 1.0, ymax + 1.0)
                if yrange <= 0
                else (ymin - 0.05 * yrange, ymax + 0.05 * yrange)
            )

    log_rows: List[Dict[str, object]] = []

    for _, event_key in unique_events.iterrows():
        strip = str(event_key["strip"])
        event_id = event_key["event_id"]

        sub_rows = work[
            (work["strip"] == strip)
            & _event_id_mask(work["event_id"], event_key["event_id"])
            & (work["logger_position"] == logger_position)
        ].copy()

        if sub_rows.empty:
            continue

        first_row = sub_rows.iloc[0]

        irrigation_start = coerce_optional_timestamp(first_row.get("irrigation_start"))
        irrigation_end = coerce_optional_timestamp(first_row.get("irrigation_end"))

        gallons_strip, duration_hours, avg_flow_gph_strip = _get_strip_volume_and_flow(first_row)

        gallons_strip_f = _as_float_or_none(gallons_strip)
        duration_hours_f = _as_float_or_none(duration_hours)
        avg_flow_gph_strip_f = _as_float_or_none(avg_flow_gph_strip)

        if irrigation_start is None:
            continue

        plot_start = irrigation_start - pd.Timedelta(hours=hours_before)
        plot_end = irrigation_start + pd.Timedelta(hours=hours_after)

        irrig_start_str = irrigation_start.strftime("%Y-%m-%d_%H%M")

        event_label = _event_label_for_filename(event_id, irrigation_start, strip)

        filename = _safe_filename(
            f"{irrig_start_str}_{strip}_{logger_position}_event_{event_label}.png"
        )

        output_file = out_dir / filename

        try:
            plot_event_multidepth_from_results(
                df=df,
                event_results=work,
                strip=strip,
                event_id=event_id,
                logger_position=logger_position,
                depths=depths,
                hours_before=hours_before,
                hours_after=hours_after,
                output_path=output_file,
                show=False,
                precip_col=precip_col,
                y_limits=y_limits,
            )

            status = "written"

        except Exception as e:
            status = f"failed: {e}"

        log_rows.append(
            {
                "event_id": event_id,
                "strip": strip,
                "logger_position": logger_position,
                "irrigation_start": irrigation_start,
                "irrigation_end": irrigation_end,
                "plot_start": plot_start,
                "plot_end": plot_end,
                "event_duration_hours": (
                    round(duration_hours_f, 2) if duration_hours_f is not None else pd.NA
                ),
                "gallons_strip": (
                    round(gallons_strip_f, 0) if gallons_strip_f is not None else pd.NA
                ),
                "avg_flow_gph_strip": (
                    round(avg_flow_gph_strip_f, 1)
                    if avg_flow_gph_strip_f is not None
                    else pd.NA
                ),
                "output_file": str(output_file),
                "status": status,
            }
        )

    return pd.DataFrame(log_rows)
import os
import pandas as pd
from typing import Any, Dict, List, Optional, Union
from plotly import graph_objects as go
from flask import abort
from biochar_app.scripts.config import GSEASON_PERIODS, logger_location_mapping, human_label, UnitSystem, DEFAULT_UNITS, sensor_depth_mapping, TRACE_CHOICES
from biochar_app.scripts.config import PRECIP_COLS, UNIT_CONVERSIONS, DATA_PROCESSED_DIR
from pandas import Series
import json
from plotly.utils import PlotlyJSONEncoder
from biochar_app.scripts.routes_utils import load_logger_year
import logging

from biochar_app.scripts.gseason import (
    format_gseason_label,
    assign_gseason_periods,
    compute_summary_statistics,
get_gseason_summary,
get_flat_gseason_summary,
calculate_gseason_precip,
)

from biochar_app.scripts.plot_helpers import (
    sanitize_json,
    compute_global_min_max,
    common_xaxis_config,
    common_yaxis_config,
    common_yaxis2_config,
    get_unit_aware_label,
    human_label,
    parse_sensor_column,
    convert_units_for_download,
    load_irrigation_events,
    common_legend_config,
    label_name_mapping,
)


def init_time_figure(
        granularity: str,
        start: str,
        end: str
) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        xaxis=common_xaxis_config(granularity, start, end),
        template="plotly_white"
    )
    return fig


def prepare_plot_for_json(fig: go.Figure) -> dict[str, Any]:
    json_text = json.dumps(fig, cls=PlotlyJSONEncoder)
    return json.loads(json_text)


def add_raw_traces(
    fig: go.Figure,
    df: pd.DataFrame,
    variable: str,
    strip: str,
    logger_location: str,
    unit_system: str
) -> List[str]:
    """
    One line per depth at the chosen logger_location.
    Returns the list of df-columns actually plotted.
    """
    # pick out exactly the columns we want
    y_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_")
           and f"_raw_{strip}_{logger_location}" in c
    ]
    if not y_cols:
        abort(400, f"No raw columns for {variable}, {strip}, {logger_location}")

    # flatten everything into plain Python lists
    # build the common x axis once
    x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
    scale = 100 if variable == "VWC" else 1

    for col in y_cols:
        # depth label comes from your parse logic / config
        meta = parse_sensor_column(col, unit_system)
        y_series = df[col].astype(float) * scale
        y_vals = y_series.tolist()

        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="lines",
            name=meta["depth"],
            line=dict(width=2),
        ))

    return y_cols


def add_ratio_traces(
    fig: go.Figure,
    df: pd.DataFrame,
    variable: str,
    strip: str,
    logger_location: str
) -> List[str]:
    """
    One line per strip‐pair (S1/S2, S3/S4) at the chosen logger_location.
    Returns the list of df-columns actually plotted.
    """
    y_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_")
           and "_ratio_" in c
           and c.endswith(f"_{logger_location}")
    ]
    if not y_cols:
        abort(400, f"No ratio columns for {variable}, {strip}, {logger_location}")

    x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

    for col in y_cols:
        # extract “S1” and “S2” out of e.g. “VWC_1_ratio_S1_S2_T”
        pair = col.split("_ratio_")[1].rsplit("_", 1)[0]  # “S1_S2”
        y_vals = df[col].astype(float).tolist()

        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="lines",
            name=pair.replace("_", "/"),
            line=dict(width=2),
        ))

    return y_cols


def add_precipitation_bars(
    fig: go.Figure,
    df: pd.DataFrame,
    unit_system: str,
    granularity: str,
) -> None:
    """
    Add precipitation as bars on the secondary y-axis ("y2"),
    converting units if needed and sizing the bars according to granularity.
    """
    # 1) determine bar‐width in ms
    ms_per_day = 24 * 3600 * 1000
    width_map = {
        "15min": 15 * 60 * 1000,
        "hourly": 3600 * 1000,
        "daily": ms_per_day,
        "monthly": 30 * ms_per_day,
    }
    bar_width = width_map.get(granularity, ms_per_day)

    # 1a) widen daily bars by 50%
    if granularity == "daily":
        bar_width = int(bar_width * 1.5)

    # 2) map unit_system → preferred & fallback precip columns
    precip_col_map = {"metric": "precip_mm", "us": "precip_in"}
    other_col_map  = {"metric": "precip_in",  "us": "precip_mm"}
    primary_col   = precip_col_map[unit_system]
    fallback_col  = other_col_map[unit_system]

    # 3) pick & convert precipitation series
    precip_vals: Optional[pd.Series] = None
    if primary_col in df.columns:
        precip_vals = df[primary_col].astype(float)
    elif fallback_col in df.columns:
        vals = df[fallback_col].astype(float)
        # convert fallback → primary
        if unit_system == "metric":
            precip_vals = vals * 25.4
        else:
            precip_vals = vals / 25.4

    # 4) if we found precipitation, add it
    if precip_vals is not None:
        label = human_label("precip", unit_system)
        x_vals = df["timestamp"].tolist()
        y_vals = precip_vals.tolist()
        fig.add_trace(go.Bar(
            x         = x_vals,
            y         = y_vals,
            yaxis    = "y2",
            name     = label,
            width    = bar_width,
            marker   = dict(color="lightgrey"),
            opacity  = 0.6,
        ))
        # ensure the secondary axis is shown
        fig.update_layout(
            yaxis2=common_yaxis2_config(unit_system)
        )


def add_irrigation_shapes(
    fig: go.Figure,
    strip: str,
    year: int,
    unit_system: str,  # "us" or "metric"
) -> None:
    """
    Add a vertical line at each irrigation event start, annotate it, and
    then add a single dummy trace for the legend.
    """
    events = load_irrigation_events(strip, year)
    conv_irrig  = UNIT_CONVERSIONS["us_to_metric"]["irrigation"]

    # decode JSON if needed
    if isinstance(events, str):
        try:
            events = json.loads(events)
        except json.JSONDecodeError:
            return

    # normalize to a list of dicts
    if isinstance(events, pd.DataFrame):
        records = events.to_dict(orient="records")
    else:
        records = list(events)

    # draw one line + annotation per event
    for ev in records:
        raw_start = ev.get("start") or ev.get("timestamp")
        if not raw_start:
            continue
        try:
            ts = pd.to_datetime(raw_start)
        except Exception:
            continue

        # vertical line
        fig.add_shape(
            type="line",
            x0=ts, x1=ts,
            y0=0, y1=1,
            yref="paper",
            line=dict(color="sienna", dash="dot", width=2),
        )

        # annotate with k-gal or k-L
        vol_gal = ev.get("volume_gal")
        try:
            vol = float(vol_gal)
        except Exception:
            continue

        if unit_system == "metric":
            vol = conv_irrig(vol)
            unit = "k L"
        else:
            unit = "k gal"

        fig.add_annotation(
            x=ts,
            y=1.02,            # just above the plotting area
            yref="paper",
            text=f"{vol/1000:.0f} {unit}",
            showarrow=False,
            font=dict(size=10, color="sienna"),
            align="center",
        )

    # finally, one dummy trace so the legend shows exactly one entry
    legend_label = (
        "Irrig. Vol (000 L)"
        if unit_system == "metric"
        else "Irrig. Vol (000 gal)"
    )
    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode="lines",
        line=dict(color="sienna", dash="dot", width=2),
        name=legend_label,
        showlegend=True,
    ))


def configure_axes(
        fig: go.Figure,
        df: pd.DataFrame,
        y_cols: list[str],
        variable: str,
        unit_system: str,
        kind: str,
) -> None:
    scale = 100 if kind == "raw" and variable == "VWC" else 1
    df_scaled = df[y_cols].astype(float) * scale
    global_min, global_max = compute_global_min_max(df_scaled, y_cols)

    fig.update_layout(
        yaxis=common_yaxis_config(kind, variable, unit_system, global_min, global_max)
    )

    if kind == "raw":
        precip_col = f"precip_{PRECIP_COLS[unit_system]}"
        if precip_col in df.columns:
            fig.update_layout(
                yaxis2=common_yaxis2_config(unit_system)
            )


def make_raw_figure(
    df: pd.DataFrame,
    variable: str,
    strip: str,
    logger_location: str,
    depth: str,
    unit_system: str,
    year: int,
    granularity: str,
    start: str,
    end: str,
    trace_option: str,
) -> Dict[str, Any]:
    """
    Build a raw time‐series plot.
    trace_option=="depths": one line per depth at the chosen logger_location.
    trace_option=="locations": one line per logger_location at the chosen depth.
    """

    # 1) validate upfront
    if trace_option not in TRACE_CHOICES:
        abort(
            400,
            f"Unknown trace_option {trace_option!r}; must be one of {TRACE_CHOICES}",
        )

    # 2) nice y‐axis label
    human_var = label_name_mapping.get(variable, {}).get(unit_system, variable)

    # 3) init figure & scale
    fig = go.Figure()
    vwc_scale = 100 if variable == "VWC" else 1

    # 4) track exactly which df columns we plot
    y_cols: List[str] = []

    # 5) pick your traces
    if trace_option == TRACE_CHOICES[0]:  # "depths"
        for d, names in sensor_depth_mapping.items():
            col = f"{variable}_{d}_raw_{strip}_{logger_location}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y_vals = (df[col].astype(float) * vwc_scale).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=names[unit_system],
                    line=dict(width=2),
                )
            )

    elif trace_option == TRACE_CHOICES[1]:  # "locations"
        for loc_key, loc_name in logger_location_mapping.items():
            col = f"{variable}_{depth}_raw_{strip}_{loc_key}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y_vals = (df[col].astype(float) * vwc_scale).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=loc_name,
                    line=dict(width=2),
                )
            )

    # 6) overlays — *always* reachable, immediately after your two branches
    unsys_enum = UnitSystem(unit_system)
    add_precipitation_bars(fig, df, unsys_enum, granularity)
    add_irrigation_shapes(fig, strip, year, unit_system)

    # 7) final layout
    fig.update_layout(
        title={"text": f"Raw Plot for {human_var} in {strip}, {year}", "x": 0.5},
        xaxis=common_xaxis_config(granularity, start, end),
        yaxis=dict(title=human_var),
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin=dict(l=60, r=20, t=60, b=40),
        height=400,
        shapes=[],
    )

    # 8) auto‐scale based on the real df columns in y_cols
    configure_axes(
        fig=fig,
        df=df,
        y_cols=y_cols,
        variable=variable,
        unit_system=unit_system,
        kind="raw",
    )

    return prepare_plot_for_json(fig)

def make_ratio_figure(
        df: pd.DataFrame,
        variable: str,
        strip: str,
        logger_location: str,
        unit_system: str,
        granularity: str,
        year: int,
        start: str,
        end:   str,
        depth: str,
) -> dict[str, Any]:
    fig = go.Figure()

    # pick out ratio cols
    y_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_{depth}_ratio_")
           and c.endswith(f"_{logger_location}")
    ]
    if not y_cols:
        abort(400, "No ratio data available for the selected filters.")

    for col in y_cols:
        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
        y_vals = df[col].astype(float).tolist()
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="lines",
            name=f"{p1}/{p2}",
            line=dict(width=2),
        ))

    human_var = label_name_mapping.get(variable, {}).get(unit_system, variable)
    title = (
        f"{granularity.capitalize()} Ratio Plot for "
        f"{human_var} in {strip}, {year} ({logger_location.capitalize()} Logger)"
    )
    fig.update_layout(
        title={"text": title, "x": 0.5},
        xaxis=common_xaxis_config(granularity, start, end),
        legend=common_legend_config("Strip Ratios"),
        template="plotly_white",
        margin=dict(l=60, r=20, t=60, b=40),
        height=400,
        autosize=True,
    )

    configure_axes(
        fig=fig,
        df=df,
        y_cols=y_cols,
        variable=variable,
        unit_system=unit_system,
        kind="ratio",
    )

    return prepare_plot_for_json(fig)


def make_raw_gseason_figure(
    df: pd.DataFrame,
    variable: str,
    strip: str,
    logger_location: str,        # “T”, “M” or “B”, or "all"
    depth: str,
    unit_system: str,
    year: int,
) -> Dict[str, Any]:
    """
    Bar chart of raw growing-season means, plus seasonal precipitation on y2.
    """
    # 1) load the precomputed per-season summary
    summary = get_gseason_summary(year)

    # 2) human-friendly y-label
    human_var = label_name_mapping.get(variable, {}).get(unit_system, variable)

    # 3) seasons in order, with their labels
    seasons       = list(GSEASON_PERIODS.keys())
    season_labels = [GSEASON_PERIODS[s]["label"] for s in seasons]

    # 4) build the main bar traces (one per logger location)
    fig    = go.Figure()
    factor = 100 if variable == "VWC" else 1

    locs = [logger_location] if logger_location != "all" else list(logger_location_mapping)
    for loc in locs:
        y_vals = []
        for code in seasons:
            sd = summary.get(code, {})
            vd = sd.get(variable, {})

            # show me what's actually there
            print(f"gseason[{code!r}][{variable!r}] keys: {list(vd.keys())}")

            # correct key — no "_{loc}"
            strip_depth_key = f"{strip}_D{depth}"
            print(f"→ looking for key {strip_depth_key!r}")

            raw_stats = vd.get(strip_depth_key, {}).get("raw_statistics", {})
            mean = raw_stats.get("mean")
            print(f"→ mean for {code!r} @ {strip_depth_key!r} = {mean!r}")

            container = vd.get(strip_depth_key, {})
            print(f"container for {code!r}/{strip_depth_key!r} = {json.dumps(container, indent=2)}")

            raw_stats = container.get("raw_statistics", {})
            mean = raw_stats.get("mean")
            print(f"→ mean for {code!r} @ {strip_depth_key!r} = {mean!r}")

            y_vals.append(None if mean is None else float(mean) * factor)

        fig.add_trace(go.Bar(
            x=season_labels,
            y=y_vals,
            name=logger_location_mapping.get(loc, loc),
            marker=dict(opacity=0.8),
        ))

    # 5) add seasonal precipitation (on the same df, but computed separately)
    precip_df = calculate_gseason_precip(df, year, unit_system)
    precip_vals = [
        precip_df.loc[precip_df["period_code"] == code, "precip"].iat[0]
        if code in precip_df["period_code"].values else None
        for code in seasons
    ]
    precip_label = human_label("precip", unit_system)

    fig.add_trace(go.Bar(
        x=season_labels,
        y=precip_vals,
        name=precip_label,
        yaxis="y2",
        marker=dict(color="lightgrey"),
        opacity=0.4,
    ))

    # 6) dual-y layout
    fig.update_layout(
        barmode="group",
        title={
            "text": f"Raw Growing-Season Means for {human_var} in {strip}, {year}",
            "x": 0.5,
        },
        xaxis=dict(title="Season", type="category"),
        yaxis=dict(title=human_var),
        yaxis2=dict(
            title=label_name_mapping["precip"][unit_system],
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        template="plotly_white",
        margin=dict(l=60, r=60, t=60, b=40),
        height=400,
    )

    return prepare_plot_for_json(fig)


def make_ratio_gseason_figure(
    df: pd.DataFrame,
    variable: str,
    strip: str,
    logger_location: str,      # now actually used below
    depth: str,
    unit_system: str,
    year: int,
) -> Dict[str, Any]:
    """
    Bar chart of growing‐season strip‐ratios at a single logger location.
    Two bars per season: one for S1/S2 and one for S3/S4 at the chosen loc.
    """
    # 1) load the precomputed season summary
    summary = get_gseason_summary(year)

    # 2) friendly axis label (e.g. “Volumetric Water Content (%) Ratio”)
    human_var = label_name_mapping.get(variable, {}).get(unit_system, variable)

    # 3) seasons & labels
    seasons = list(GSEASON_PERIODS.keys())
    season_labels = [format_gseason_label(code) for code in seasons]

    # 4) the two strip‐pairs
    pairs = [("S1", "S2"), ("S3", "S4")]

    # 5) build one Bar trace per pair, _at the chosen logger_location_
    fig = go.Figure()
    for top, bottom in pairs:
        y_vals: List[Optional[float]] = []
        # note the loc suffix now included in the key
        key = f"{variable}_{depth}_ratio_{strip}_{top}_{bottom}_{logger_location}"
        for code in seasons:
            val = summary.get(code, {}).get(key)
            y_vals.append(0 if val is None else float(val))

        fig.add_trace(go.Bar(
            x=season_labels,
            y=y_vals,
            name=f"{top}/{bottom}",
            marker=dict(opacity=0.8),
        ))

    # 6) layout – grouped bars, axis titles, force y≥0
    fig.update_layout(
        barmode="group",
        title={
            "text": f"Growing-Season Ratios for {human_var} in {strip}, {year}",
            "x": 0.5,
        },
        xaxis=dict(title="Season (Pair)", type="category"),
        yaxis=dict(title=f"{human_var} Ratio", rangemode="tozero"),
        template="plotly_white",
        margin=dict(l=60, r=20, t=60, b=40),
        height=400,
        shapes=[],
    )

    return prepare_plot_for_json(fig)


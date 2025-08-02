#import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
#from flask import abort
#from typing import Any, Dict, List
#from fastapi import HTTPException
from typing import Any, Dict, List, Optional, Union
from flask import abort
from biochar_app.scripts.config import DEFAULT_GSEASON_PERIODS, logger_location_mapping, human_label, UnitSystem, DEFAULT_UNITS, sensor_depth_mapping, TRACE_CHOICES
from biochar_app.scripts.config import PRECIP_COLS, UNIT_CONVERSIONS, DATA_PROCESSED_DIR, bar_width_map
#from pandas import Series
import json
from plotly.utils import PlotlyJSONEncoder
#from biochar_app.scripts.routes_utils import load_logger_year

import logging
logger = logging.getLogger(__name__)


from biochar_app.scripts.gseason import (
    get_flat_gseason_summary,
    calculate_gseason_precip,
)

from biochar_app.scripts.get_weather_data import fetch_weather_data

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

from datetime import datetime, timedelta

# from .plot_utils import (
#     common_xaxis_config, common_legend_config,
#     prepare_plot_for_json, configure_axes,
#     bar_width_map, GSEASON_PERIODS, logger_location_mapping,
#     calculate_gseason_precip, human_label
# )


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
    bar_width = bar_width_map.get(granularity, bar_width_map["daily"])

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
    *,
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
    Build a raw time-series Plotly figure.

    trace_option=='depths': one line per depth at chosen logger_location.
    trace_option=='locations': one line per logger_location at chosen depth.
    """
    # 1) Validate trace_option
    if trace_option not in TRACE_CHOICES:
        abort(400, f"Unknown trace_option {trace_option!r}; must be one of {TRACE_CHOICES}")

    # 2) Human‐friendly Y label
    human_var = label_name_mapping.get(variable, {}).get(unit_system, variable)

    # 3) Init figure & scaling
    fig = go.Figure()
    vwc_scale = 100 if variable == "VWC" else 1

    # 4) Track which sensor‐traces we’ve added
    y_cols: List[str] = []

    # 5) Add sensor traces
    if trace_option == TRACE_CHOICES[0]:  # 'depths'
        for d, names in sensor_depth_mapping.items():
            col = f"{variable}_{d}_raw_{strip}_{logger_location}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y_vals = (df[col].astype(float) * vwc_scale).tolist()
            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode="lines",
                name=names[unit_system],
                line=dict(width=2),
            ))
    else:  # 'locations'
        for loc_key, loc_name in logger_location_mapping.items():
            col = f"{variable}_{depth}_raw_{strip}_{loc_key}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y_vals = (df[col].astype(float) * vwc_scale).tolist()
            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode="lines",
                name=loc_name,
                line=dict(width=2),
            ))

    # 6) Overlay precipitation (for VWC) and air‐temp (for T), then force y2 if either ran
    if variable == "VWC":
        # Precip bars
        logger.info(f"ℹ️ looking for precip columns (‘precip_in’/‘precip_mm’) in DataFrame")
        add_precipitation_bars(fig, df, unit_system, granularity)

    if variable == "T":
        # Air‐temp dotted line on y2
        for key in ("temp_air", "temp_air_C"):
            if key in df.columns:
                fig.add_trace(go.Scatter(
                    x=df["timestamp"],
                    y=df[key].astype(float).tolist(),
                    mode="lines",
                    name=label_name_mapping["temp_air"][unit_system],
                    yaxis="y2",
                    line=dict(dash="dot"),
                ))
                break

    if variable in ("VWC", "T"):
        # ensure the secondary axis shows up
        fig.update_layout(yaxis2=common_yaxis2_config(unit_system))

    # 7) Irrigation shapes (unchanged)
    add_irrigation_shapes(fig, strip, year, unit_system)

    # 8) Final layout (with Legend restored)
    fig.update_layout(
        title=    {"text": f"Raw Plot for {human_var} in {strip}, {year}", "x": 0.5},
        xaxis=    common_xaxis_config(granularity, start, end),
        yaxis=    {"title": human_var},
        yaxis2=   fig.layout.yaxis2,               # preserve y2 if set above
        legend=   common_legend_config("Legend"),
        template= "plotly_white",
        margin=   {"l": 60, "r": 20, "t": 60, "b": 40},
        height=   400,
        shapes=   [],
    )

    # 9) Auto‐scale
    configure_axes(
        fig=fig,
        df=df,
        y_cols=y_cols,
        variable=variable,
        unit_system=unit_system,
        kind="raw",
    )

    # 10) Marshalled for JSON
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
) -> Dict[str, Any]:
    """
    Build either a time‐series ratio (scatter) or a growing‐season ratio (bar) chart.
    """
    is_gseason = granularity.lower() == "gseason"
    fig = go.Figure()

    # pick out all matching ratio columns
    y_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_{depth}_ratio_")
           and c.endswith(f"_{logger_location}")
    ]
    if not y_cols:
        from flask import abort
        abort(400, "No ratio data available for the selected filters.")

    # add one trace per pair
    for col in y_cols:
        # label & values
        if is_gseason:
            x_vals = df["period_code"].tolist()
        else:
            x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        y_vals = df[col].astype(float).tolist()

        if is_gseason:
            # bar trace
            fig.add_trace(
                go.Bar(
                    x=x_vals,
                    y=y_vals,
                    name=f"{p1}/{p2}",
                    opacity=0.8,
                )
            )
        else:
            # line trace
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=f"{p1}/{p2}",
                    line=dict(width=2),
                )
            )

    # layout
    human_var = label_name_mapping.get(variable, {}).get(unit_system, variable)
    title = (
        f"{granularity.capitalize()} Ratio Plot for "
        f"{human_var} in {strip}, {year} ({logger_location.capitalize()} Logger)"
    )
    if is_gseason:
        xaxis_cfg = {"title": "Season", "type": "category"}
    else:
        xaxis_cfg = common_xaxis_config(granularity, start, end)

    fig.update_layout(
        title= {"text": title, "x": 0.5},
        xaxis=  xaxis_cfg,
        legend= common_legend_config("Strip Ratios"),
        template="plotly_white",
        margin= {"l": 60, "r": 20, "t": 60, "b": 40},
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
    *,
    df,
    periods: List[Any],
    variable: str,
    strip: str,
    logger_location: str,
    depth: int,
    unit_system: str,
    year: int,
    trace_option: str,             # "depths" or "loggerLocation"
) -> Dict[str, Any]:
    """
    Build a growing‐season bar‐chart Plotly figure.
    One bar per period, with either depths‐at‐one‐location or locations‐at‐one‐depth.
    """
    fig = go.Figure()
    human_var = label_name_mapping.get(variable, {}).get(unit_system, variable)
    scale = 100 if variable == "VWC" else 1

    # 1) x‐axis categories & ticktext (two‐line)
    codes = [p.code for p in periods]
    ticktext = [f"{p.label}<br>{p.start}–{p.end}" for p in periods]

    # 2) sensor bars
    y_cols: List[str] = []
    if trace_option == "depths":
        # one trace per depth at fixed logger_location
        for d, depth_labels in sensor_depth_mapping.items():
            col = f"{variable}_{d}_raw_{strip}_{logger_location}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            fig.add_trace(go.Bar(
                x=codes,
                y=(df[col].astype(float) * scale).tolist(),
                name=f"{human_var}, {depth_labels[unit_system]}",
            ))
    else:
        # one trace per logger location at fixed depth
        for loc_key, loc_label in logger_location_mapping.items():
            col = f"{variable}_{depth}_raw_{strip}_{loc_key}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            fig.add_trace(go.Bar(
                x=codes,
                y=(df[col].astype(float) * scale).tolist(),
                name=f"{human_var}, {loc_label}",
            ))

    # 3) precipitation overlay only for daily backend
    #    skip here because df has no timestamp; use add_precipitation_bars in the daily figure
    #    (we can add gseason‐precip later once we have period‐aggregated precip)
    # if variable == "VWC" and granularity.lower() != "gseason":
    #     add_precipitation_bars(fig, df, unit_system, granularity)
    #     fig.update_layout(yaxis2=common_yaxis2_config(unit_system))

    # 4) irrigation verticals
    add_irrigation_shapes(fig, strip, year, unit_system)

    # 5) layout
    fig.update_layout(
        title={
            "text": f"Raw Growing-Season Means for {human_var} in {strip}, {year}",
            "x": 0.5
        },
        xaxis={
            "title": "Season",
            "type": "category",
            "categoryorder": "array",
            "categoryarray": codes,
            "tickmode": "array",
            "tickvals": codes,
            "ticktext": ticktext,
        },
        yaxis=common_yaxis_config(
            kind="raw",
            variable=variable,
            unit_system=unit_system,
            global_min=df[y_cols].min(numeric_only=True).min(),
            global_max=df[y_cols].max(numeric_only=True).max(),
        ),
        # no yaxis2 for gseason
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 60, "b": 60},
        height=400,
    )

    # 6) auto‐scale
    configure_axes(
        fig=fig,
        df=df,
        y_cols=y_cols,
        variable=variable,
        unit_system=unit_system,
        kind="raw",
    )

    return prepare_plot_for_json(fig)


def make_ratio_gseason_figure(
    *,
    df: pd.DataFrame,
    periods: List[Any],           # could be dicts or PeriodSpec instances
    variable: str,
    strip: str,
    logger_location: str,
    depth: int,
    unit_system: str,
    year: int,
) -> Dict[str, Any]:
    """
    Build a growing‐season ratio bar‐chart:
     1) reuse the raw gseason figure
     2) swap title, drop irrigation axis, adjust y-axis for ratio
    """
    # 1) Build the underlying raw‐gseason bar chart (no granularity/start/end here)
    fig_json = make_raw_gseason_figure(
        df               = df,
        periods          = periods,
        variable         = variable,
        strip            = strip,
        logger_location  = logger_location,
        depth            = depth,
        unit_system      = unit_system,
        year             = year,
        trace_option     = "depths",   # always depths‐based for ratio bars
    )

    # 2) Relabel the x-axis ticks to show "Label\nstart-end"
    #    handle both dicts and PeriodSpec objects:
    x_labels = []
    for p in periods:
        if hasattr(p, "label"):
            lbl   = p.label
            start = p.start
            end   = p.end
        else:
            lbl   = p["label"]
            start = p["start"]
            end   = p["end"]
        x_labels.append(f"{lbl}\n{start}-{end}")

    fig_json["layout"]["xaxis"].update({
        "tickmode": "array",
        "tickvals": list(range(len(periods))),
        "ticktext": x_labels,
    })

    # 3) Swap title
    fig_json["layout"]["title"]["text"] = (
        fig_json["layout"]["title"]["text"]
        .replace("Raw", "Growing-Season Ratios")
    )

    # 4) Drop the secondary (irrigation) axis entirely
    fig_json["layout"].pop("yaxis2", None)

    # 5) Rescale the primary y-axis for ratio values
    fig_json["layout"]["yaxis"].update(
        common_yaxis_config(
            kind       = "ratio",
            variable   = variable,
            unit_system= unit_system,
            global_min = df.min(numeric_only=True).min(),
            global_max = df.max(numeric_only=True).max(),
        )
    )

    return fig_json



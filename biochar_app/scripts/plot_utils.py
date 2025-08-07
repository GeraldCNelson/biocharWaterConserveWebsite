import pandas as pd
import numpy as np
import plotly.graph_objects as go
from typing import Any, Dict, List, Optional, Union
from flask import abort
import json
from plotly.utils import PlotlyJSONEncoder
import logging

logger = logging.getLogger(__name__)

from biochar_app.scripts.config import (PRECIP_COLS, DATA_PROCESSED_DIR, bar_width_map, label_name_mapping,
   sensor_depth_mapping,
    TRACE_CHOICES,
    variable_name_abbrev,
    UNIT_CONVERSIONS,
)

from biochar_app.scripts.plot_helpers import (
    sanitize_json,
    compute_global_min_max,
    common_xaxis_config,
    common_yaxis_config,
    common_yaxis2_config,
    get_unit_aware_label,
    parse_sensor_column,
    convert_units_for_download,
    load_irrigation_events,
    common_legend_config,
)

from datetime import datetime, timedelta

from biochar_app.scripts.config import (
    sensor_depth_mapping, logger_location_mapping,
    variable_name_abbrev, label_name_mapping
)
from biochar_app.scripts.plot_helpers import (
    common_yaxis_config, common_yaxis2_config,
    common_legend_config
)


def init_time_figure(
    granularity: str,
    start: str,
    end: str
) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        xaxis=common_xaxis_config(granularity, start, end),
        template="plotly_white",
    )
    return fig


def prepare_plot_for_json(fig: go.Figure) -> Dict[str, Any]:
    raw = json.dumps(fig, cls=PlotlyJSONEncoder)
    return json.loads(raw)


def add_raw_traces(
    fig: go.Figure,
    df: pd.DataFrame,
    variable: str,
    strip: str,
    logger_location: str,
    unit_system: str,
) -> List[str]:
    y_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_")
           and f"_raw_{strip}_{logger_location}" in c
    ]
    if not y_cols:
        abort(400, f"No raw columns for {variable}, {strip}, {logger_location}")

    x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
    scale = 100 if variable == "VWC" else 1

    for col in y_cols:
        meta = parse_sensor_column(col, unit_system)
        y_series = df[col].astype(float) * scale
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_series.tolist(),
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
    logger_location: str,
) -> List[str]:
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
        pair = col.split("_ratio_")[1].rsplit("_", 1)[0].replace("_", "/")
        y_vals = df[col].astype(float).tolist()
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="lines",
            name=pair,
            line=dict(width=2),
        ))
    return y_cols


def add_precipitation_bars(
    fig: go.Figure,
    df: pd.DataFrame,
    unit_system: str,
    granularity: str,
) -> None:
    # bar‐width in ms, bump daily by 50%
    bw = bar_width_map.get(granularity, bar_width_map["daily"])
    if granularity == "daily":
        bw = int(bw * 1.5)

    primary = {"metric": "precip_mm", "us": "precip_in"}[unit_system]
    fallback = {"metric": "precip_in",  "us": "precip_mm"}[unit_system]

    if primary in df.columns:
        vals = df[primary].astype(float)
    elif fallback in df.columns:
        tmp = df[fallback].astype(float)
        vals = tmp * (25.4 if unit_system == "metric" else 1/25.4)
    else:
        return

    label = get_unit_aware_label("precip", unit_system)
    fig.add_trace(go.Bar(
        x=df["timestamp"].tolist(),
        y=vals.tolist(),
        yaxis="y2",
        name=label,
        width=bw,
        marker=dict(color="LightSteelBlue"),
        offsetgroup="0",
        opacity=0.6,
    ))
    fig.update_layout(yaxis2=common_yaxis2_config(unit_system))


def add_irrigation_shapes(
    fig: go.Figure,
    strip: str,
    year: int,
    unit_system: str,
    sum_only: bool = False,
    periods: Optional[List[dict]] = None,
) -> None:
    events = load_irrigation_events(strip, year)
    conv = UNIT_CONVERSIONS["us_to_metric"]["irrigation"]

    # Normalize to list-of‐dicts
    if isinstance(events, pd.DataFrame):
        recs = events.to_dict(orient="records")
    else:
        try:
            recs = json.loads(events) if isinstance(events, str) else list(events)
        except:
            recs = []

    # Summed‐per‐period
    if sum_only and periods:
        for p in periods:
            try:
                start = pd.to_datetime(p["start"])
                end   = pd.to_datetime(p["end"])
            except:
                continue

            vols = []
            for ev in recs:
                ts = pd.to_datetime(ev.get("start") or ev.get("timestamp"), errors="coerce")
                if not pd.isna(ts) and start <= ts <= end:
                    try:
                        vols.append(float(ev.get("volume_gal")))
                    except:
                        pass
            if not vols:
                continue

            total = sum(vols)
            if unit_system == "metric":
                total = conv(total)
                unit  = "k L"
            else:
                unit  = "k gal"

            fig.add_shape(
                type="line",
                x0=start, x1=start,
                y0=0,     y1=1,
                yref="paper",
                line=dict(color="sienna", dash="dot", width=2),
            )
            fig.add_annotation(
                x=start,
                y=1.02,
                yref="paper",
                text=f"{total/1000:.0f} {unit}",
                showarrow=False,
                font=dict(size=10, color="sienna"),
            )

    # Per‐event fallback
    elif not sum_only:
        for ev in recs:
            ts = pd.to_datetime(ev.get("start") or ev.get("timestamp"), errors="coerce")
            if pd.isna(ts):
                continue

            fig.add_shape(
                type="line",
                x0=ts, x1=ts,
                y0=0,  y1=1,
                yref="paper",
                line=dict(color="sienna", dash="dot", width=2),
            )
            try:
                vol = float(ev.get("volume_gal"))
            except:
                continue

            if unit_system == "metric":
                vol = conv(vol)
                unit = "k L"
            else:
                unit = "k gal"

            fig.add_annotation(
                x=ts,
                y=1.02,
                yref="paper",
                text=f"{vol/1000:.0f} {unit}",
                showarrow=False,
                font=dict(size=10, color="sienna"),
            )

    # Dummy legend trace
    legend_label = get_unit_aware_label("irrigation", unit_system)
    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode="lines",
        line=dict(color="sienna", dash="dot", width=2),
        name=legend_label,
        showlegend=True,
    ))


def configure_axes(
    fig: go.Figure,
    df: pd.DataFrame,
    y_cols: List[str],
    variable: str,
    unit_system: str,
    kind: str,
) -> None:
    scale = 100 if (kind == "raw" and variable == "VWC") else 1
    scaled = df[y_cols].astype(float) * scale
    gmin, gmax = compute_global_min_max(scaled, y_cols)

    fig.update_layout(
        yaxis=common_yaxis_config(kind, variable, unit_system, gmin, gmax)
    )
    if kind == "raw":
        precip_col = f"precip_{PRECIP_COLS[unit_system]}"
        if precip_col in df.columns:
            fig.update_layout(yaxis2=common_yaxis2_config(unit_system))


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
    if trace_option not in TRACE_CHOICES:
        abort(400, f"Unknown trace_option {trace_option!r}; must be one of {TRACE_CHOICES}")

    human_var = get_unit_aware_label(variable, unit_system)
    fig = go.Figure()
    scale = 100 if variable == "VWC" else 1
    y_cols: List[str] = []

    # sensor traces
    if trace_option == TRACE_CHOICES[0]:
        for d, names in sensor_depth_mapping.items():
            col = f"{variable}_{d}_raw_{strip}_{logger_location}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            x = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y = (df[col].astype(float) * scale).tolist()
            fig.add_trace(go.Scatter(
                x=x, y=y, mode="lines", name=names[unit_system], line=dict(width=2)
            ))
    else:
        for loc_key, loc_name in logger_location_mapping.items():
            col = f"{variable}_{depth}_raw_{strip}_{loc_key}"
            if col not in df.columns:
                continue
            y_cols.append(col)
            x = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y = (df[col].astype(float) * scale).tolist()
            fig.add_trace(go.Scatter(
                x=x, y=y, mode="lines", name=loc_name, line=dict(width=2)
            ))

    # precip / air‐temp overlays
    if variable == "VWC":
        logger.info("ℹ️ looking for precip columns (‘precip_in’/‘precip_mm’) in DataFrame")
        add_precipitation_bars(fig, df, unit_system, granularity)
    if variable == "T":
        for key in ("temp_air", "temp_air_C"):
            if key in df.columns:
                label = get_unit_aware_label("temp_air", unit_system)
                fig.add_trace(go.Scatter(
                    x=df["timestamp"],
                    y=df[key].astype(float).tolist(),
                    mode="lines",
                    name=label,
                    yaxis="y2",
                    line=dict(dash="dot"),
                ))
                break
    if variable in ("VWC", "T"):
        fig.update_layout(yaxis2=common_yaxis2_config(unit_system))

    # irrigation
    add_irrigation_shapes(fig, strip, year, unit_system)

    # layout
    fig.update_layout(
        title={"text": f"Raw Plot for {human_var} in {strip}, {year}", "x": 0.5},
        xaxis=common_xaxis_config(granularity, start, end),
        yaxis={"title": human_var},
        yaxis2=fig.layout.yaxis2,
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 60, "b": 40},
        height=400,
    )
    # new — narrow it to just the *_raw_* sensor columns you actually drew
    sensor_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_") and "_raw_" in c
    ]
    configure_axes(
        fig=fig,
        df=df,
        y_cols=y_cols,         #sensor_cols,
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
    end: str,
    depth: str,
) -> Dict[str, Any]:
    is_gs = granularity.lower() == "gseason"
    fig = go.Figure()

    y_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_{depth}_ratio_")
           and c.endswith(f"_{logger_location}")
    ]
    if not y_cols:
        abort(400, "No ratio data available for the selected filters.")

    for idx, col in enumerate(y_cols):
        if is_gs:
            x = df["period_code"].tolist()
        else:
            x = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        y = df[col].astype(float).tolist()

        if is_gs:
            fig.add_trace(go.Bar(
                x=x, y=y, name=f"{p1}/{p2}",
                offsetgroup=str(idx+1),
                opacity=0.8,
            ))
        else:
            fig.add_trace(go.Scatter(
                x=x, y=y, mode="lines", name=f"{p1}/{p2}", line=dict(width=2)
            ))

    human_var = get_unit_aware_label(variable, unit_system)
    title = (
        f"{granularity.capitalize()} Ratio Plot for "
        f"{human_var} in {strip}, {year} ({logger_location.capitalize()} Logger)"
    )
    xcfg = {"title":"Season","type":"category"} if is_gs else common_xaxis_config(granularity, start, end)

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={"text": title, "x": 0.5},
        xaxis=xcfg,
        legend=common_legend_config("Strip Ratios"),
        template="plotly_white",
        margin={"l":60,"r":20,"t":60,"b":40},
        height=400,
        autosize=True,
    )
    configure_axes(fig, df, y_cols, variable, unit_system, kind="ratio")
    return prepare_plot_for_json(fig)

def make_raw_gseason_figure(
    *,
    df: pd.DataFrame,
    periods: List[Any],           # list of PeriodSpec
    variable: str,
    strip: str,
    logger_location: str,
    depth: int,
    unit_system: str,
    year: int,
    trace_option: str,
) -> Dict[str, Any]:
    """
    Build a raw growing‐season bar chart with true grouped bars:
      - precipitation in its own offsetgroup on y2
      - sensor‐bars (percent, no extra *100)
      - summed irrigation‐lines (one per period)
    """

    # 1) category labels
    labels = [f"{p.label}\n{p.start}-{p.end}" for p in periods]

    fig = go.Figure()

    # 2) Precipitation (only for VWC) on true secondary axis
    precip_col = "precip_in" if unit_system == "us" else "precip_mm"
    if variable == "VWC" and precip_col in df.columns:
        fig.add_trace(go.Bar(
            x           = labels,
            y           = df[precip_col].tolist(),
            name        = label_name_mapping["precip"][unit_system],
            marker      = dict(color="LightSteelBlue"),
            yaxis       = "y2",
            offsetgroup = "0",
            opacity     = 0.6,
        ))

    # 3) Sensor bars (already in % units, no *100)
    human_var   = label_name_mapping[variable][unit_system]
    abbrev      = variable_name_abbrev[variable]
    legend_pref = f"{abbrev}, {{}}"

    if trace_option == "depths":
        # one location, multiple depths
        for idx, (d, depth_map) in enumerate(sensor_depth_mapping.items(), start=1):
            col = f"{variable}_{d}_raw_{strip}_{logger_location}"
            if col not in df.columns:
                continue
            fig.add_trace(go.Bar(
                x           = labels,
                y           = df[col].astype(float).tolist(),
                name        = legend_pref.format(depth_map[unit_system]),
                offsetgroup = str(idx),
            ))
    else:
        # one depth, multiple locations
        for idx, (loc_key, loc_label) in enumerate(logger_location_mapping.items(), start=1):
            col = f"{variable}_{depth}_raw_{strip}_{loc_key}"
            if col not in df.columns:
                continue
            fig.add_trace(go.Bar(
                x           = labels,
                y           = df[col].astype(float).tolist(),
                name        = legend_pref.format(loc_label),
                offsetgroup = str(idx),
            ))

    # 4) Summed irrigation‐lines (one per period)
    if variable == "VWC":
        add_irrigation_shapes(fig, strip, year, unit_system, sum_only=True, periods=periods)

    # 5a) pick exactly the G-season sensor cols we actually drew
    sensor_cols = (
        [f"{variable}_{d}_raw_{strip}_{logger_location}"
         for d in sensor_depth_mapping
         if f"{variable}_{d}_raw_{strip}_{logger_location}" in df.columns]
        if trace_option == "depths"
        else
        [f"{variable}_{depth}_raw_{strip}_{loc_key}"
         for loc_key in logger_location_mapping
         if f"{variable}_{depth}_raw_{strip}_{loc_key}" in df.columns]
    )

    # 5b) now lay out the axes using that same sensor_cols
    fig.update_layout(
        barmode      = "group",
        bargap       = 0.2,
        bargroupgap  = 0.1,
        title        = {
            "text": f"Raw Growing-Season Means for {human_var} in {strip}, {year}",
            "x": 0.5,
        },
        xaxis        = {
            "title":    "Season",
            "type":     "category",
            "showline": True,
            "linecolor":"black",
            "linewidth":1,
        },
        yaxis        = {
            **common_yaxis_config(
                kind        = "raw",
                variable    = variable,
                unit_system = unit_system,
                # compute global min/max only across the sensor columns, not precipitation
                global_min  = df[sensor_cols].min(numeric_only=True).min(),
                global_max  = df[sensor_cols].max(numeric_only=True).max(),
            ),
            "title": human_var,
        },
        yaxis2       = {
            **common_yaxis2_config(unit_system),
            "overlaying": "y",
            "side":       "right",
        },
        legend       = common_legend_config("Legend"),
        template     = "plotly_white",
        margin       = {"l":60, "r":20, "t":60, "b":40},
        height       = 400,
    )

    # 6) Auto‐scale the primary axis using only the sensor traces
    if trace_option == "depths":
        sensor_cols = [
            f"{variable}_{d}_raw_{strip}_{logger_location}"
            for d in sensor_depth_mapping
            if f"{variable}_{d}_raw_{strip}_{logger_location}" in df.columns
        ]
    else:
        sensor_cols = [
            f"{variable}_{depth}_raw_{strip}_{loc_key}"
            for loc_key in logger_location_mapping
            if f"{variable}_{depth}_raw_{strip}_{loc_key}" in df.columns
        ]

    configure_axes(
        fig         = fig,
        df          = df,
        y_cols      = sensor_cols,
        variable    = variable,
        unit_system = unit_system,
        kind        = "raw",
    )

    return prepare_plot_for_json(fig)

def make_ratio_gseason_figure(
    *,
    df: pd.DataFrame,
    periods: List[Any],
    variable: str,
    strip: str,
    logger_location: str,
    depth: int,
    unit_system: str,
    year: int,
) -> Dict[str, Any]:
    """
    Build a growing‐season ratio bar chart.
    One bar per strip‐pair (S1/S2, S3/S4) at the chosen logger_location.
    """
    # 1) category labels
    labels = [f"{p.label}\n{p.start}-{p.end}" for p in periods]

    # 2) pick out only the ratio columns for this depth/logger
    y_cols = [
        c for c in df.columns
        if c.startswith(f"{variable}_{depth}_ratio_")
           and c.endswith(f"_{logger_location}")
    ]
    if not y_cols:
        from flask import abort
        abort(400, "No ratio data available for the selected filters.")

    fig = go.Figure()

    # 3) one bar‐trace per pair
    for idx, col in enumerate(y_cols, start=1):
        p1, p2 = col.split("_ratio_")[1].split("_")[:2]  # e.g. ["S1","S2"]
        fig.add_trace(go.Bar(
            x=labels,
            y=df[col].astype(float).tolist(),
            name=f"{variable_name_abbrev[variable]} ratio {p1}/{p2}",
            offsetgroup=str(idx),
            opacity=0.8,
        ))

    # 4) layout
    depth_label = sensor_depth_mapping[str(depth)][unit_system]
    human_var    = label_name_mapping[variable][unit_system].split(" (")[0]
    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={
            "text": f"Growing-Season Ratios for {variable_name_abbrev[variable]} at {depth_label} in {strip}, {year}",
            "x": 0.5
        },
        xaxis={"title":"Season","type":"category","showline":True,"linecolor":"black","linewidth":1},
        yaxis={
            **common_yaxis_config(
                kind="ratio",
                variable=variable,
                unit_system=unit_system,
                global_min=df[y_cols].min(numeric_only=True).min(),
                global_max=df[y_cols].max(numeric_only=True).max(),
            ),
            "title": f"{human_var} Ratio"
        },
        legend=common_legend_config("Strip Ratios"),
        template="plotly_white",
        margin={"l":60,"r":20,"t":60,"b":40},
        height=400,
    )

    return prepare_plot_for_json(fig)
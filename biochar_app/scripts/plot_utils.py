import logging
import json
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder
from fastapi import HTTPException

from biochar_app.scripts.gseason_utils import periods_to_list_of_dicts

if TYPE_CHECKING:
    from biochar_app.scripts.routes import PeriodSpec  # noqa: F401

logger = logging.getLogger(__name__)

from biochar_app.scripts.config import (  # noqa: E402
    PRECIP_COLS,
    bar_width_map,
    label_name_mapping,
    sensor_depth_mapping,
    logger_location_mapping,
    TRACE_CHOICES,
    variable_name_abbrev,
    UNIT_CONVERSIONS,
    # IRR_COLOR,  # kept in config if other modules use it, but we use PLOT_COLORS here
    PLOT_COLORS,
)

def bad_request(msg: str) -> None:
    raise HTTPException(status_code=400, detail=msg)

def _depth_color(depth_key: str) -> Optional[str]:
    """
    Deterministic depth colors based on PLOT_COLORS keys:
      depth_1, depth_2, depth_3
    """
    return PLOT_COLORS.get(f"depth_{str(depth_key)}")


SWC_DEPTH_INCHES = {
    depth_key: float(labels["us"].split()[0])
    for depth_key, labels in sensor_depth_mapping.items()
}

from biochar_app.scripts.plot_helpers import (  # noqa: E402
    compute_global_min_max,
    common_xaxis_config,
    common_yaxis_config,
    common_yaxis2_config,
    get_unit_aware_label,
    parse_sensor_column,
    convert_units,
    load_irrigation_events,
    common_legend_config,
)


# ---------------------------------------------------------------------------
# Internal helper: safe scalar timestamp parsing
# ---------------------------------------------------------------------------

def _safe_parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
    """
    Best-effort conversion of a single value to a scalar Timestamp.

    Returns:
        - pd.Timestamp if we can parse a single scalar
        - None if the value is None, container-like, or could not be parsed
    """
    if value is None:
        return None

    if isinstance(value, (pd.Series, pd.DataFrame, pd.DatetimeIndex, np.ndarray, list, tuple)):
        return None

    ts = pd.to_datetime(value, errors="coerce")

    if isinstance(ts, (pd.Series, pd.DatetimeIndex)):
        if len(ts) == 0:
            return None
        ts = ts[0]

    if pd.isna(ts):
        return None

    return ts


def init_time_figure(granularity: str, start: str, end: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        xaxis=common_xaxis_config(granularity, start, end),
        template="plotly_white",
    )
    return fig


def prepare_plot_for_json(fig: go.Figure) -> Dict[str, Any]:
    """
    Ensures the returned structure is JSON-serializable (no numpy arrays, no Timestamps).
    PlotlyJSONEncoder handles numpy types well.
    """
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
        if c.startswith(f"{variable}_") and f"_raw_{strip}_{logger_location}" in c
    ]
    if not y_cols:
        bad_request( f"No raw columns for {variable}, {strip}, {logger_location}")

    x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

    for col in y_cols:
        meta = parse_sensor_column(col, unit_system)
        y_series = df[col].astype(float)

        # Derive depth key from the column name pattern: VAR_<depth>_raw_...
        # (e.g., T_1_raw_S1_T -> depth_key="1")
        depth_key = None
        try:
            depth_key = col.split("_")[1]
        except Exception:
            depth_key = None

        color = _depth_color(depth_key) if depth_key is not None else None

        line_kwargs: Dict[str, Any] = dict(width=2)
        if color:
            line_kwargs["color"] = color

        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=y_series.tolist(),
                mode="lines",
                name=meta["depth"],
                line=line_kwargs,
            )
        )
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
        bad_request( f"No ratio columns for {variable}, {strip}, {logger_location}")

    x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

    for col in y_cols:
        pair = col.split("_ratio_")[1].rsplit("_", 1)[0].replace("_", "/")  # S1/S2
        pair_key = col.split("_ratio_")[1].rsplit("_", 1)[0]  # S1_S2
        color = PLOT_COLORS.get(f"ratio_{pair_key}", None)

        y_vals = df[col].astype(float).tolist()
        trace_kwargs: Dict[str, Any] = dict(
            x=x_vals,
            y=y_vals,
            mode="lines",
            name=pair,
            line=dict(width=2),
        )
        if color:
            trace_kwargs["line"]["color"] = color

        fig.add_trace(go.Scatter(**trace_kwargs))

    return y_cols


def add_precipitation_bars(
        fig: go.Figure,
        df: pd.DataFrame,
        unit_system: str,
        granularity: str,
) -> None:
    bw = bar_width_map.get(granularity, bar_width_map["daily"])
    if granularity == "daily":
        bw = int(bw * 1.5)

    primary = {"metric": "precip_mm", "us": "precip_in"}[unit_system]
    fallback = {"metric": "precip_in", "us": "precip_mm"}[unit_system]

    if primary in df.columns:
        vals = df[primary].astype(float)
    elif fallback in df.columns:
        tmp = df[fallback].astype(float)
        vals = tmp * (25.4 if unit_system == "metric" else 1 / 25.4)
    else:
        return

    label = get_unit_aware_label("precip", unit_system)
    unit_suffix = "in" if unit_system == "us" else "mm"
    hovertemplate = "Precip: %{y:.2f} " + unit_suffix + "<extra></extra>"
    fig.add_trace(
        go.Bar(
            x=df["timestamp"].tolist(),
            y=vals.tolist(),
            yaxis="y2",
            name=label,
            width=bw,
            marker=dict(color=PLOT_COLORS.get("precip", "LightSteelBlue")),
            offsetgroup="0",
            opacity=0.55,
            hovertemplate=hovertemplate,
        )
    )
    fig.update_layout(yaxis2=common_yaxis2_config(unit_system))


def add_irrigation_shapes(
        fig: go.Figure,
        strip: str,
        year: int,
        unit_system: str,
        sum_only: bool = False,
        periods: Optional[List[Any]] = None,
        category_labels: Optional[List[str]] = None,
) -> None:
    """
    Draw irrigation:
      • sum_only=True  -> one vertical dotted line per period at the *category* position,
                          with "### k gal/k L" annotation above the plot.
      • sum_only=False -> one vertical line per event on a date axis.
      • Always adds a dummy legend entry matching the dotted line style.
    """
    events_df = load_irrigation_events(strip, year)
    recs = events_df.to_dict(orient="records")

    conv = UNIT_CONVERSIONS["us_to_metric"]["irrigation"]
    unit_lbl = "<br>k L" if unit_system == "metric" else "<br>k gal"

    irr_color = PLOT_COLORS.get("irrigation", "black")
    irr_anno_color = irr_color
    irr_opacity = 0.7

    if sum_only and periods:
        labels = category_labels or [
            (getattr(p, "label", None) or str(i + 1)) for i, p in enumerate(periods)
        ]

        for i, p in enumerate(periods):
            start_raw = getattr(p, "start", None)
            end_raw = getattr(p, "end", None)

            start_ts = _safe_parse_timestamp(start_raw)
            end_ts = _safe_parse_timestamp(end_raw)

            if start_ts is None or end_ts is None:
                continue

            total = 0.0
            for ev in recs:
                ts_raw = ev.get("start") or ev.get("timestamp")
                ts = _safe_parse_timestamp(ts_raw)
                if ts is None:
                    continue
                if ts < start_ts or ts > end_ts:
                    continue

                try:
                    total += float(ev.get("volume_gal", 0))
                except (TypeError, ValueError):
                    pass

            if total <= 0:
                continue
            if unit_system == "metric":
                total = conv(total)

            cat = labels[i]

            fig.add_shape(
                type="line",
                xref="x",
                x0=cat,
                x1=cat,
                yref="paper",
                y0=0,
                y1=1,
                line=dict(color=irr_color, dash="dot", width=2),
                opacity=irr_opacity,
            )
            fig.add_annotation(
                xref="x",
                x=cat,
                yref="paper",
                y=1.02,
                text=f"{total/1000:.0f} {unit_lbl}",
                showarrow=False,
                font=dict(size=10, color=irr_anno_color),
            )

    elif not sum_only:
        for ev in recs:
            ts_raw = ev.get("start") or ev.get("timestamp")
            ts = _safe_parse_timestamp(ts_raw)
            if ts is None:
                continue

            fig.add_shape(
                type="line",
                xref="x",
                x0=ts,
                x1=ts,
                yref="paper",
                y0=0,
                y1=1,
                line=dict(color=irr_color, dash="dot", width=2),
                opacity=irr_opacity,
            )

            try:
                vol = float(ev.get("volume_gal", 0))
            except (TypeError, ValueError):
                continue
            if unit_system == "metric":
                vol = conv(vol)

            fig.add_annotation(
                x=ts,
                y=1.02,
                yref="paper",
                text=f"{vol/1000:.0f} {unit_lbl}",
                showarrow=False,
                font=dict(size=10, color=irr_anno_color),
            )

    legend_label = get_unit_aware_label("irrigation", unit_system)
    fig.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color=irr_color, dash="dot", width=2),
            name=legend_label,
            showlegend=True,
        )
    )


def configure_primary_yaxis(
        fig: go.Figure,
        df: pd.DataFrame,
        y_cols: List[str],
        variable: str,
        unit_system: str,
        kind: str,
) -> None:
    if not y_cols:
        return

    scaled = df[y_cols].astype(float)
    gmin, gmax = compute_global_min_max(scaled, y_cols)

    if kind == "ratio" and variable in ("VWC", "SWC"):
        gmin = min(0.0, gmin)

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
        bad_request( f"Unknown trace_option {trace_option!r}; must be one of {TRACE_CHOICES}")

    display_variable = variable
    source_variable = "VWC" if variable == "SWC" else variable

    df_plot = df.copy()

    human_var = get_unit_aware_label(display_variable, unit_system)
    human_logger_loc = logger_location_mapping.get(logger_location, logger_location)

    fig = go.Figure()
    y_cols: List[str] = []
    use_secondary_y = False

    def swc_from_vwc(series: pd.Series, depth_key: str) -> pd.Series:
        vwc_pct = pd.to_numeric(series, errors="coerce")
        depth_in = SWC_DEPTH_INCHES.get(str(depth_key))
        if depth_in is None:
            return pd.Series(np.nan, index=series.index)

        swc_in = (vwc_pct / 100.0) * depth_in
        if unit_system == "metric":
            return UNIT_CONVERSIONS["us_to_metric"]["swc"](swc_in)
        return swc_in

    if trace_option == TRACE_CHOICES[0]:
        for d, names in sensor_depth_mapping.items():
            base_col = f"{source_variable}_{d}_raw_{strip}_{logger_location}"
            if base_col not in df_plot.columns:
                continue

            if display_variable == "SWC":
                df_plot[base_col] = swc_from_vwc(df_plot[base_col], d)

            y_cols.append(base_col)
            x_vals = df_plot["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y_vals = pd.to_numeric(df_plot[base_col], errors="coerce").tolist()

            line_kwargs: Dict[str, Any] = dict(width=2)
            depth_col = _depth_color(str(d))
            if depth_col:
                line_kwargs["color"] = depth_col

            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=names[unit_system],
                    line=line_kwargs,
                )
            )
    else:
        for loc_key, loc_name in logger_location_mapping.items():
            base_col = f"{source_variable}_{depth}_raw_{strip}_{loc_key}"
            if base_col not in df_plot.columns:
                continue

            if display_variable == "SWC":
                df_plot[base_col] = swc_from_vwc(df_plot[base_col], depth)

            y_cols.append(base_col)
            x_vals = df_plot["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y_vals = pd.to_numeric(df_plot[base_col], errors="coerce").tolist()

            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=loc_name,
                    line=dict(width=2),
                )
            )

    if not y_cols:
        abort(
            400,
            (
                f"No valid data to plot for '{display_variable}' "
                f"@ strip='{strip}', loc='{logger_location}', depth='{depth}' "
                f"between {start} and {end}."
            ),
        )

    if display_variable in ("VWC", "SWC"):
        logger.info("ℹ️ looking for precip columns (‘precip_in’/‘precip_mm’) in DataFrame")
        add_precipitation_bars(fig, df, unit_system, granularity)
        use_secondary_y = True

    if display_variable == "T":
        temp_col = None
        if unit_system == "metric" and "temp_air_degC" in df.columns:
            temp_col = "temp_air_degC"
        elif "temp_air_degF" in df.columns:
            temp_col = "temp_air_degF"

        if temp_col is not None:
            label = get_unit_aware_label("temp_air", unit_system)
            fig.add_trace(
                go.Scatter(
                    x=df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                    y=pd.to_numeric(df[temp_col], errors="coerce").tolist(),
                    mode="lines",
                    name=label,
                    yaxis="y2",
                    line=dict(
                        dash="dot",
                        color=PLOT_COLORS.get("air_temp", None),
                        width=2,
                    ),
                )
            )
            use_secondary_y = True

    if use_secondary_y:
        fig.update_layout(yaxis2=common_yaxis2_config(unit_system))

    add_irrigation_shapes(fig, strip, year, unit_system)

    title_text = (
        f"{granularity.capitalize()} Data Plot for {human_var} "
        f"in Strip {strip}, {year} ({human_logger_loc} Logger)"
    )

    layout_kwargs: Dict[str, Any] = dict(
        title={"text": title_text, "x": 0.5, "font": {"size": 18}},
        xaxis=common_xaxis_config(granularity, start, end),
        yaxis={"title": {"text": human_var, "font": {"size": 14}}},
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 60, "b": 40},
        height=400,
        autosize=True,
    )

    if use_secondary_y:
        # keep the y2 definition created earlier (secondary_y=True traces)
        layout_kwargs["yaxis2"] = fig.layout.yaxis2

        # give enough room for y2 ticks/title + legend so Plotly doesn't shrink plot width
        layout_kwargs["margin"]["r"] = 240

        # place legend outside the plotting area (in the right margin)
        base_legend = common_legend_config("Legend") or {}
        layout_kwargs["legend"] = {
            **base_legend,
            "x": 1.02,
            "xanchor": "left",
            "y": 1.0,
            "yanchor": "top",
        }

    fig.update_layout(**layout_kwargs)

    fig.update_layout(font={"size": 12})

    configure_primary_yaxis(
        fig=fig,
        df=df_plot,
        y_cols=y_cols,
        variable=display_variable,
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

    ratio_prefix = "VWC" if variable == "SWC" else variable

    y_cols = [
        c for c in df.columns
        if c.startswith(f"{ratio_prefix}_{depth}_ratio_")
           and c.endswith(f"_{logger_location}")
    ]
    if not y_cols:
        bad_request( "No ratio data available for the selected filters.")

    df_plot = convert_units(df, unit_system)

    for idx, col in enumerate(y_cols):
        if is_gs:
            x = df_plot["period_code"].tolist()
        else:
            x = df_plot["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        y = df_plot[col].astype(float).tolist()
        name = f"{variable_name_abbrev.get(variable, variable)} ratio {p1}/{p2}"

        pair_key = f"{p1}_{p2}"
        pair_color = PLOT_COLORS.get(f"ratio_{pair_key}", None)

        if is_gs:
            bar_kwargs: Dict[str, Any] = dict(
                x=x, y=y, name=name,
                offsetgroup=str(idx + 1),
                opacity=0.8,
            )
            if pair_color:
                bar_kwargs["marker"] = dict(color=pair_color)
            fig.add_trace(go.Bar(**bar_kwargs))
        else:
            line_kwargs: Dict[str, Any] = dict(width=2)
            if pair_color:
                line_kwargs["color"] = pair_color
            fig.add_trace(go.Scatter(
                x=x, y=y, mode="lines", name=name, line=line_kwargs
            ))

    human_var = get_unit_aware_label(variable, unit_system)
    human_logger_loc = logger_location_mapping.get(logger_location, logger_location)
    var_abbrev = variable  # already something like "VWC", "EC", "T", etc.

    title = (
        f"{granularity.capitalize()} Ratio Plot for "
        f"{var_abbrev} in {year} ({human_logger_loc} Logger)"
    )
    xcfg = {"title": "Season", "type": "category"} if is_gs else common_xaxis_config(granularity, start, end)

    fig.add_shape(
        type="line",
        xref="paper", x0=0, x1=1,
        yref="y", y0=1, y1=1,
        line=dict(color=PLOT_COLORS.get("zero_line", "rgba(0,0,0,0.5)"), width=1),
    )

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={"text": title, "x": 0.5},
        xaxis=xcfg,
        legend=common_legend_config("Strip Ratios"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 60, "b": 40},
        height=400,
        autosize=True,
    )
    configure_primary_yaxis(fig, df_plot, y_cols, variable, unit_system, kind="ratio")
    return prepare_plot_for_json(fig)


def make_temperature_delta_figure(
        df: pd.DataFrame,
        depth: int,
        logger_location: str,
        unit_system: str,
        granularity: str,
        year: int,
        start: str,
        end: str,
) -> Dict[str, Any]:
    loc = logger_location

    col_s1 = f"T_{depth}_raw_S1_{loc}"
    col_s2 = f"T_{depth}_raw_S2_{loc}"
    col_s3 = f"T_{depth}_raw_S3_{loc}"
    col_s4 = f"T_{depth}_raw_S4_{loc}"

    missing = [c for c in (col_s1, col_s2, col_s3, col_s4) if c not in df.columns]
    if missing:
        raise ValueError(f"make_temperature_delta_figure: missing temperature columns: {missing}")

    delta_12 = (df[col_s1] - df[col_s2]).astype(float)
    delta_34 = (df[col_s3] - df[col_s4]).astype(float)

    x_vals = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
    d12_vals = pd.to_numeric(delta_12, errors="coerce").tolist()
    d34_vals = pd.to_numeric(delta_34, errors="coerce").tolist()

    unit_label = "°F" if unit_system.lower().startswith("us") else "°C"
    y_label = f"Soil temperature difference ({unit_label})"

    depth_label = sensor_depth_mapping.get(str(depth), {}).get(unit_system, f"{depth}")
    loc_label = logger_location_mapping.get(loc, loc)

    title = (
        f"{granularity.capitalize()} Soil Temperature Difference Between Strips "
        f"(S1–S2, S3–S4), {depth_label}, {loc_label} Logger, {year}"
    )

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=x_vals,
            y=d12_vals,
            mode="lines",
            name="S1 − S2",
            line=dict(
                width=2,
                color=PLOT_COLORS.get("delta_T_S1_S2", None),
            ),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=x_vals,
            y=d34_vals,
            mode="lines",
            name="S3 − S4",
            line=dict(
                width=2,
                color=PLOT_COLORS.get("delta_T_S3_S4", None),
            ),
        )
    )

    fig.add_shape(
        type="line",
        xref="paper", x0=0, x1=1,
        yref="y", y0=0, y1=0,
        line=dict(
            color=PLOT_COLORS.get("zero_line", "rgba(0,0,0,0.5)"),
            width=1,
            dash="dash",
        ),
    )

    max_abs = max(
        float(np.nanmax(np.abs(np.asarray(d12_vals, dtype=float)))),
        float(np.nanmax(np.abs(np.asarray(d34_vals, dtype=float)))),
    )
    if not np.isfinite(max_abs) or max_abs <= 0:
        max_abs = 1.0
    max_abs *= 1.05

    fig.update_layout(
        title={"text": title, "x": 0.5},
        xaxis=common_xaxis_config(granularity, start, end),
        yaxis={
            "title": y_label,
            "zeroline": False,
            "range": [-max_abs, max_abs],
            "autorange": False,
            # add the left y-axis line
            "showline": True,
            "linecolor": "black",
            "linewidth": 1,
            # optional: also draw top/right if you like the boxed look
            # "mirror": True,
        },
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 70, "b": 40},
        height=400,
        autosize=True,
    )

    return prepare_plot_for_json(fig)


def make_raw_gseason_figure(
        *,
        df: pd.DataFrame,
        periods: List[Any],
        variable: str,
        strip: str,
        logger_location: str,
        depth: int,
        unit_system: str,
        year: int,
        trace_option: str,
) -> Dict[str, Any]:
    """
    Growing-season RAW figure.

    For most variables this uses the usual `{variable}_{depth}_raw_{strip}_{logger}`
    columns (mirroring the non-seasonal plots).

    For SWC we use the aggregated volume columns:

        SWC_vol_gal_{strip}_{logger}_{depth}   (US)
        SWC_vol_L_{strip}_{logger}_{depth}     (metric)

    and apply the same 'depths' vs 'locations' logic for traces.
    """
    df = convert_units(df, unit_system)

    norm_periods = periods_to_list_of_dicts(periods or [])
    labels = [f"{p['label']} ({p['start']}-{p['end']})" for p in norm_periods]

    fig = go.Figure()

    # ------------------------------------------------------------------ #
    # Precipitation overlay (VWC only, as before)
    # ------------------------------------------------------------------ #
    precip_col_us = "precip_in"
    precip_col_mm = "precip_mm"
    have_precip = (
            variable == "VWC"
            and (precip_col_us in df.columns or precip_col_mm in df.columns)
    )

    precip_vals = None
    if have_precip:
        if unit_system == "metric" and precip_col_mm in df.columns:
            precip_col = precip_col_mm
        else:
            precip_col = precip_col_us

        precip_vals = pd.to_numeric(df[precip_col], errors="coerce").astype(float)

        unit_suffix = "in" if unit_system == "us" else "mm"
        fig.add_trace(
            go.Bar(
                x=labels,
                y=precip_vals.tolist(),
                name=label_name_mapping["precip"][unit_system],
                marker=dict(color=PLOT_COLORS.get("precip", "LightSteelBlue")),
                yaxis="y2",
                offsetgroup="0",
                opacity=0.55,
                text=[
                    f"{v:.2f} {unit_suffix}"
                    if (v is not None and np.isfinite(v))
                    else ""
                    for v in precip_vals
                ],
                textposition="outside",
                textfont=dict(size=12),
                cliponaxis=False,
                hovertemplate="Precip: %{y:.2f} " + unit_suffix,
            )
        )

    # ------------------------------------------------------------------ #
    # Sensor bars
    # ------------------------------------------------------------------ #
    human_var = label_name_mapping[variable][unit_system]
    abbr = variable_name_abbrev.get(variable, variable)
    legend_fmt = f"{abbr}, {{}}"

    sensor_cols_plotted: List[str] = []

    # ----- SWC special case (use SWC_vol_* columns) -------------------- #
    if variable == "SWC":
        vol_suffix = "gal" if unit_system == "us" else "L"
        base = f"SWC_vol_{vol_suffix}"

        if trace_option == "depths":
            # One bar per depth for the chosen strip & logger location.
            for idx, (d, depth_map) in enumerate(sensor_depth_mapping.items(), start=1):
                col = f"{base}_{strip}_{logger_location}_{d}"
                if col not in df.columns:
                    continue
                sensor_cols_plotted.append(col)

                series = pd.to_numeric(df[col], errors="coerce").astype(float)

                bar_kwargs: Dict[str, Any] = dict(
                    x=labels,
                    y=series.tolist(),
                    name=legend_fmt.format(depth_map[unit_system]),
                    offsetgroup=str(idx),
                    opacity=0.85,
                )
                depth_col = _depth_color(str(d))
                if depth_col:
                    bar_kwargs["marker"] = dict(color=depth_col)

                fig.add_trace(go.Bar(**bar_kwargs))
        else:
            # One bar per logger location at the chosen depth.
            for idx, (loc_key, loc_label) in enumerate(
                    logger_location_mapping.items(), start=1
            ):
                col = f"{base}_{strip}_{loc_key}_{depth}"
                if col not in df.columns:
                    continue
                sensor_cols_plotted.append(col)

                series = pd.to_numeric(df[col], errors="coerce").astype(float)

                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=series.tolist(),
                        name=legend_fmt.format(loc_label),
                        offsetgroup=str(idx),
                        opacity=0.85,
                    )
                )

    # ----- All other variables (VWC, EC, T, etc.) ---------------------- #
    else:
        if trace_option == "depths":
            for idx, (d, depth_map) in enumerate(sensor_depth_mapping.items(), start=1):
                col = f"{variable}_{d}_raw_{strip}_{logger_location}"
                if col not in df.columns:
                    continue
                sensor_cols_plotted.append(col)

                bar_kwargs: Dict[str, Any] = dict(
                    x=labels,
                    y=pd.to_numeric(df[col], errors="coerce").astype(float).tolist(),
                    name=legend_fmt.format(depth_map[unit_system]),
                    offsetgroup=str(idx),
                    opacity=0.85,
                )
                depth_col = _depth_color(str(d))
                if depth_col:
                    bar_kwargs["marker"] = dict(color=depth_col)

                fig.add_trace(go.Bar(**bar_kwargs))
        else:
            for idx, (loc_key, loc_label) in enumerate(
                    logger_location_mapping.items(), start=1
            ):
                col = f"{variable}_{depth}_raw_{strip}_{loc_key}"
                if col not in df.columns:
                    continue
                sensor_cols_plotted.append(col)
                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=pd.to_numeric(df[col], errors="coerce")
                        .astype(float)
                        .tolist(),
                        name=legend_fmt.format(loc_label),
                        offsetgroup=str(idx),
                        opacity=0.85,
                        )
                )

    # Irrigation overlays (still VWC-only for now)
    if variable == "VWC" and norm_periods:
        add_irrigation_shapes(
            fig=fig,
            strip=strip,
            year=year,
            unit_system=unit_system,
            sum_only=True,
            periods=periods,
            category_labels=labels,
        )

    if sensor_cols_plotted:
        primary_min = df[sensor_cols_plotted].min(numeric_only=True).min()
        primary_max = df[sensor_cols_plotted].max(numeric_only=True).max()
    else:
        primary_min = None
        primary_max = None

    yaxis_cfg = common_yaxis_config(
        "raw", variable, unit_system, primary_min, primary_max
    )
    y2_cfg: Dict[str, Any] = common_yaxis2_config(unit_system)

    if have_precip and precip_vals is not None:
        pvals = precip_vals.to_numpy(dtype=float)
        good = pvals[np.isfinite(pvals)]
        pmax = float(good.max()) if good.size else 0.0
        y2_cfg["range"] = [0.0, (pmax * 1.15) if pmax > 0 else 1.0]

    title_text = (
        f"Growing-season Data Plot for {human_var} in Strip {strip}, {year} "
        f"({logger_location_mapping[logger_location]} Logger)"
    )

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={"text": title_text, "x": 0.5, "font": {"size": 18}},
        xaxis={
            "title": "Season",
            "type": "category",
            "showline": True,
            "linecolor": "black",
            "linewidth": 1,
        },
        yaxis={**yaxis_cfg, "title": human_var},
        yaxis2=y2_cfg,
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 70, "b": 40},
        height=400,
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
    Growing-season RATIO figure.

    * For VWC (and other ratio-ready variables) we use the precomputed
      `{variable}_{depth}_ratio_*_{logger}` columns.

    * For SWC we build ratios on the fly:

        SWC ratio S1/S2  =  SWC_vol_{unit}_S1_{logger}_{depth}
                           / SWC_vol_{unit}_S2_{logger}_{depth}

        SWC ratio S3/S4  =  SWC_vol_{unit}_S3_{logger}_{depth}
                           / SWC_vol_{unit}_S4_{logger}_{depth}

      so the growing-season SWC ratio plot has the same structure as the
      VWC ratio plot: two bars per season (S1/S2 and S3/S4).
    """
    df = convert_units(df, unit_system)

    # Normalized periods + x-axis labels
    norm_periods = periods_to_list_of_dicts(periods or [])
    labels = [f"{p['label']} ({p['start']}-{p['end']})" for p in norm_periods]

    fig = go.Figure()

    # ------------------------------------------------------------------ #
    # Shared label / title helpers (auto-detect what to show)
    # ------------------------------------------------------------------ #
    # Abbreviation like "VWC", "SWC", "EC", ...
    abbr = variable_name_abbrev.get(variable, variable)

    # Full unit-aware label (e.g. "Volumetric Water Content (%)")
    full_label = label_name_mapping[variable][unit_system]
    human_base = full_label.split(" (")[0]  # drop units → "Volumetric Water Content"

    # Logger location label, e.g. "Top"
    logger_label = logger_location_mapping.get(logger_location, "")
    logger_suffix = f" ({logger_label} Logger)" if logger_label else ""

    def _make_title(no_data: bool = False) -> str:
        """
        Build a ratio title that is consistent with the daily ratio plots:
        - Use abbreviation (VWC, SWC, ...)
        - Mention year and logger location
        - Mention that these are strip ratios S1/S2 and S3/S4
        - Optionally append a 'no data' message.
        """
        base = f"Growing-season Ratio Plot for {abbr} in {year}{logger_suffix} " \
               "(Strip Ratios S1/S2 and S3/S4)"
        if no_data:
            base += " — no ratio data available for selected filters"
        return base

    # ------------------------------------------------------------------ #
    # SWC: construct S1/S2 and S3/S4 ratios from SWC_vol_* columns
    # ------------------------------------------------------------------ #
    if variable == "SWC":
        vol_suffix = "gal" if unit_system == "us" else "L"
        base = f"SWC_vol_{vol_suffix}"

        s1_col = f"{base}_S1_{logger_location}_{depth}"
        s2_col = f"{base}_S2_{logger_location}_{depth}"
        s3_col = f"{base}_S3_{logger_location}_{depth}"
        s4_col = f"{base}_S4_{logger_location}_{depth}"

        # Debug: check that at least some SWC columns exist for this slice
        swc_cols = [c for c in df.columns if c.startswith("SWC_")]
        logger.info("[SWC gseason ratio] available SWC columns: %s", swc_cols)

        ratios: Dict[str, pd.Series] = {}

        if s1_col in df.columns and s2_col in df.columns:
            s1 = pd.to_numeric(df[s1_col], errors="coerce")
            s2 = pd.to_numeric(df[s2_col], errors="coerce")
            r12 = (s1 / s2).replace([np.inf, -np.inf], np.nan)
            ratios["S1/S2"] = r12

        if s3_col in df.columns and s4_col in df.columns:
            s3 = pd.to_numeric(df[s3_col], errors="coerce")
            s4 = pd.to_numeric(df[s4_col], errors="coerce")
            r34 = (s3 / s4).replace([np.inf, -np.inf], np.nan)
            ratios["S3/S4"] = r34

        if not ratios:
            # Nothing to plot – return an empty but valid figure with a clear title
            logger.warning(
                "[SWC gseason ratio] No usable SWC_vol columns for "
                "strip=%s, logger=%s, depth=%s",
                strip,
                logger_location,
                depth,
            )
            fig.update_layout(
                title={"text": _make_title(no_data=True), "x": 0.5},
                xaxis={
                    "title": "Season",
                    "type": "category",
                    "showline": True,
                    "linecolor": "black",
                    "linewidth": 1,
                },
            )
            return prepare_plot_for_json(fig)

        # Build traces
        all_vals: List[pd.Series] = []
        for idx, (pair_label, series) in enumerate(ratios.items(), start=1):
            vals = pd.to_numeric(series, errors="coerce").astype(float)
            all_vals.append(vals)

            color_key = f"ratio_SWC_{pair_label.replace('/', '_')}"
            color_val = PLOT_COLORS.get(color_key, None)

            bar_kwargs: Dict[str, Any] = dict(
                x=labels,
                y=vals.tolist(),
                name=f"{abbr} ratio {pair_label}",
                offsetgroup=str(idx),
                opacity=0.8,
            )
            if color_val is not None:
                bar_kwargs["marker"] = dict(color=color_val)

            fig.add_trace(go.Bar(**bar_kwargs))

        # Global y-range from all ratio series
        combined = pd.concat(all_vals, axis=0)
        global_min = float(combined.min()) if not combined.empty else None
        global_max = float(combined.max()) if not combined.empty else None

        fig.update_layout(
            barmode="group",
            bargap=0.2,
            bargroupgap=0.1,
            title={"text": _make_title(no_data=False), "x": 0.5},
            xaxis={
                "title": "Season",
                "type": "category",
                "showline": True,
                "linecolor": "black",
                "linewidth": 1,
            },
            yaxis={
                **common_yaxis_config(
                    kind="ratio",
                    variable="SWC",
                    unit_system=unit_system,
                    global_min=global_min,
                    global_max=global_max,
                ),
                "title": f"{human_base} Ratio",
            },
            legend=common_legend_config("Strip Ratios"),
            template="plotly_white",
            margin={"l": 60, "r": 20, "t": 60, "b": 40},
            height=400,
        )

        return prepare_plot_for_json(fig)

    # ------------------------------------------------------------------ #
    # Default path: use precomputed *_ratio_* columns (e.g. VWC)
    # ------------------------------------------------------------------ #
    y_cols = [
        c
        for c in df.columns
        if c.startswith(f"{variable}_{depth}_ratio_")
           and c.endswith(f"_{logger_location}")
    ]

    if not y_cols:
        logger.warning(
            "[gseason ratio] No ratio columns for variable=%s, depth=%s, "
            "strip=%s, logger=%s",
            variable,
            depth,
            strip,
            logger_location,
        )
        fig.update_layout(
            title={"text": _make_title(no_data=True), "x": 0.5},
            xaxis={
                "title": "Season",
                "type": "category",
                "showline": True,
                "linecolor": "black",
                "linewidth": 1,
            },
        )
        return prepare_plot_for_json(fig)

    # Reuse the same figure object; add traces for each ratio column
    for idx, col in enumerate(y_cols, start=1):
        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        pair_color = PLOT_COLORS.get(f"ratio_{p1}_{p2}", None)

        bar_kwargs: Dict[str, Any] = dict(
            x=labels,
            y=pd.to_numeric(df[col], errors="coerce").astype(float).tolist(),
            name=f"{abbr} ratio {p1}/{p2}",
            offsetgroup=str(idx),
            opacity=0.8,
        )
        if pair_color:
            bar_kwargs["marker"] = dict(color=pair_color)

        fig.add_trace(go.Bar(**bar_kwargs))

    global_min = df[y_cols].min(numeric_only=True).min()
    global_max = df[y_cols].max(numeric_only=True).max()

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={"text": _make_title(no_data=False), "x": 0.5},
        xaxis={
            "title": "Season",
            "type": "category",
            "showline": True,
            "linecolor": "black",
            "linewidth": 1,
        },
        yaxis={
            **common_yaxis_config(
                kind="ratio",
                variable=variable,
                unit_system=unit_system,
                global_min=global_min,
                global_max=global_max,
            ),
            "title": f"{human_base} Ratio",
        },
        legend=common_legend_config("Strip Ratios"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 60, "b": 40},
        height=400,
    )

    return prepare_plot_for_json(fig)
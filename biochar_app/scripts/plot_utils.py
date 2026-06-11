"""
plot_utils.py

Plotly figure builders + small helpers used by routes.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Sequence, cast

import pandas as pd
import plotly.graph_objects as go
from fastapi import HTTPException
from plotly.utils import PlotlyJSONEncoder

from biochar_app.scripts.gseason_utils import periods_to_list_of_dicts
from biochar_app.scripts.type_utils import (
    NAN,
    NEG_INF,
    POS_INF,
    UnitSystem,
    df_cols,
    finite_min_max,
    safe_timestamp,
    safe_tolist,
    to_float_series,
)

if TYPE_CHECKING:
    from biochar_app.scripts.routes import PeriodSpec  # noqa: F401

logger = logging.getLogger(__name__)

from biochar_app.config.core import (
    PLOT_COLORS,
    TITLE_FONT_SIZE,
    TRACE_CHOICES,
    bar_width_map,
    LOGGER_LOCATION_MAPPING,
    SENSOR_DEPTH_LABELS,
    VARIABLE_NAME_ABBREV,
)
from biochar_app.config.units import (
    UNIT_CONVERSIONS,
    label_name_mapping,
)

from biochar_app.scripts.plot_helpers import (
    common_legend_config,
    common_xaxis_config,
    common_yaxis2_config,
    common_yaxis_config,
    convert_units,
    get_unit_aware_label,
    load_irrigation_events,
)

PLOT_MARGINS = {
    "standard": {"l": 60, "r": 120, "t": 60, "b": 40},
    "standard_tall": {"l": 60, "r": 120, "t": 70, "b": 40},
    "dual_axis_us": {"l": 60, "r": 150, "t": 60, "b": 40},
    "dual_axis_metric": {"l": 60, "r": 165, "t": 60, "b": 40},
    "dual_axis_tall_us": {"l": 60, "r": 150, "t": 70, "b": 40},
    "dual_axis_tall_metric": {"l": 60, "r": 165, "t": 70, "b": 40},
}


# ---------------------------------------------------------------------------
# Small helpers worth keeping
# ---------------------------------------------------------------------------


def bad_request(msg: str) -> None:
    raise HTTPException(status_code=400, detail=msg)


def coerce_unit_system(unit_system: str) -> UnitSystem:
    s = (unit_system or "").strip().lower()
    if s in {"us", "usa", "imperial", "customary"}:
        return "us"
    if s in {"metric", "si"}:
        return "metric"
    return "us"


def _depth_color(depth_key: str) -> Optional[str]:
    return PLOT_COLORS.get(f"depth_{str(depth_key)}")


def _plot_margin(preset: str) -> Dict[str, int]:
    return dict(PLOT_MARGINS[preset])


SWC_DEPTH_INCHES: Dict[str, float] = {
    str(depth_key): float(str(labels["us"]).split()[0])
    for depth_key, labels in SENSOR_DEPTH_LABELS.items()
}


def _ensure_timestamp_datetime(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def _x_time_strings(df: pd.DataFrame) -> List[str]:
    if "timestamp" not in df.columns:
        return []
    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    return ts.dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()


def prepare_plot_for_json(fig: go.Figure) -> Dict[str, Any]:
    raw = json.dumps(fig, cls=PlotlyJSONEncoder)
    return cast(Dict[str, Any], json.loads(raw))


def _compact_unit_phrase(label: str) -> str:
    s = str(label).strip()
    replacements = {
        " inches": " in",
        " inch": " in",
        " centimeters": " cm",
        " centimeter": " cm",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    return s


def _depth_display_label(depth_key: str | int, usys: UnitSystem, *, compact: bool = False) -> str:
    dkey = str(depth_key)
    label = SENSOR_DEPTH_LABELS.get(dkey, {}).get(
        usys,
        SENSOR_DEPTH_LABELS.get(dkey, {}).get("us", dkey),
    )
    return _compact_unit_phrase(label) if compact else str(label)


def _logger_display_label(logger_location: str) -> str:
    return str(LOGGER_LOCATION_MAPPING.get(logger_location, logger_location))


def _normalize_trace_grouping(trace_option: str) -> str:
    mode = str(trace_option or "").strip().lower()
    if mode in {"depth", "depths"}:
        return "depth"
    if mode in {"loggerlocation", "logger_location", "logger-location"}:
        return "loggerLocation"
    return mode


def build_raw_plot_title(
    *,
    granularity: str,
    human_var: str,
    strip: str,
    year: int,
    trace_option: str,
    logger_location: str,
    depth: str | int,
    unit_system: str,
    is_gseason: bool = False,
) -> str:
    usys: UnitSystem = coerce_unit_system(unit_system)
    grouping = _normalize_trace_grouping(trace_option)

    if grouping == "depth":
        fixed_label = f"{_logger_display_label(logger_location)} Logger"
    else:
        fixed_label = _depth_display_label(depth, usys, compact=True)

    if is_gseason:
        return f"Growing-season Data Plot for {human_var} in Strip {strip}, {year} ({fixed_label})"

    return f"{granularity.capitalize()} {human_var} in Strip {strip}, {year} ({fixed_label})"


def build_ratio_plot_title(
    *,
    granularity: str,
    variable: str,
    logger_location: str,
    depth: str | int,
    unit_system: str,
    year: int,
    is_gseason: bool = False,
    no_data: bool = False,
) -> str:
    usys: UnitSystem = coerce_unit_system(unit_system)
    logger_label = f"{_logger_display_label(logger_location)} Logger"
    depth_label = _depth_display_label(depth, usys, compact=True)

    prefix = "Growing-season Ratio Plot" if is_gseason else f"{granularity.capitalize()} Ratio Plot"
    title_text = f"{prefix} for {variable} in {year} ({logger_label}, {depth_label})"

    if no_data:
        title_text += " — no ratio data available for selected filters"

    return title_text


# ---------------------------------------------------------------------------
# Overlays / shared layout helpers
# ---------------------------------------------------------------------------


def add_precipitation_bars(
    fig: go.Figure,
    df: pd.DataFrame,
    unit_system: str,
    granularity: str,
) -> None:
    usys: UnitSystem = coerce_unit_system(unit_system)
    df = _ensure_timestamp_datetime(df)

    bw = bar_width_map.get(granularity, bar_width_map.get("daily", 0))
    if granularity == "daily":
        try:
            bw = int(float(bw) * 1.5)
        except (TypeError, ValueError):
            pass

    primary = {"metric": "precip_mm", "us": "precip_in"}[usys]
    fallback = {"metric": "precip_in", "us": "precip_mm"}[usys]

    vals: Optional[pd.Series] = None
    if primary in df.columns:
        vals = to_float_series(df[primary])
    elif fallback in df.columns:
        tmp = to_float_series(df[fallback])
        vals = tmp * (25.4 if usys == "metric" else 1.0 / 25.4)

    if vals is None or "timestamp" not in df.columns:
        return

    unit_suffix = "mm" if usys == "metric" else "in"

    fig.add_trace(
        go.Bar(
            x=safe_tolist(df["timestamp"]),
            y=safe_tolist(vals),
            yaxis="y2",
            name="Precip",
            width=bw,
            marker=dict(color=PLOT_COLORS.get("precip", "LightSteelBlue")),
            offsetgroup="0",
            opacity=0.55,
            hovertemplate=f"Precip: %{{y:.2f}} {unit_suffix}<extra></extra>",
        )
    )
    fig.update_layout(yaxis2=common_yaxis2_config(usys))


def add_irrigation_shapes(
    fig: go.Figure,
    strip: str,
    year: int,
    unit_system: str,
    sum_only: bool = False,
    periods: Optional[Sequence[Any]] = None,
    category_labels: Optional[List[str]] = None,
) -> None:
    usys: UnitSystem = coerce_unit_system(unit_system)

    events_df = load_irrigation_events(strip, year)
    recs = events_df.to_dict(orient="records")

    conv = UNIT_CONVERSIONS["us_to_metric"]["irrigation"]
    unit_lbl = "<br>k L" if usys == "metric" else "<br>k gal"

    irr_color = PLOT_COLORS.get("irrigation", "black")
    irr_anno_color = irr_color
    irr_opacity = 0.7

    if sum_only and periods:
        labels = category_labels or [
            (getattr(p, "label", None) or str(i + 1)) for i, p in enumerate(periods)
        ]

        for i, p in enumerate(periods):
            start_ts = safe_timestamp(getattr(p, "start", None))
            end_ts = safe_timestamp(getattr(p, "end", None))
            if start_ts is None or end_ts is None:
                continue

            total = 0.0
            for ev in recs:
                ts = safe_timestamp(ev.get("start") or ev.get("timestamp"))
                if ts is None or ts < start_ts or ts > end_ts:
                    continue
                try:
                    total += float(ev.get("gallons_strip", 0) or 0.0)
                except (TypeError, ValueError):
                    pass

            if total <= 0:
                continue

            if usys == "metric":
                total = float(conv(total))

            cat = labels[i] if i < len(labels) else str(i + 1)

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
                text=f"{total / 1000.0:.0f} {unit_lbl}",
                showarrow=False,
                font=dict(size=10, color=irr_anno_color),
            )

    elif not sum_only:
        for ev in recs:
            ts = safe_timestamp(ev.get("start") or ev.get("timestamp"))
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
                vol = float(ev.get("gallons_strip", 0) or 0.0)
            except (TypeError, ValueError):
                continue

            if usys == "metric":
                vol = float(conv(vol))

            fig.add_annotation(
                x=ts,
                y=1.02,
                yref="paper",
                text=f"{vol / 1000.0:.0f} {unit_lbl}",
                showarrow=False,
                font=dict(size=10, color=irr_anno_color),
            )

    fig.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            line=dict(color=irr_color, dash="dot", width=2),
            name="Irrig",
            showlegend=True,
            hoverinfo="skip",
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

    usys: UnitSystem = coerce_unit_system(unit_system)
    block = df_cols(df, y_cols)
    gmin, gmax = finite_min_max(block)
    if gmin is None or gmax is None:
        return

    if kind == "raw" and variable == "VWC":
        gmin = 0.0
        gmax = 50.0

    elif kind == "ratio" and variable in ("VWC", "SWC"):
        gmin = min(0.0, gmin)

    fig.update_layout(
        yaxis=common_yaxis_config(
            kind,
            variable,
            usys,
            gmin,
            gmax,
        )
    )


# ---------------------------------------------------------------------------
# RAW (time series)
# ---------------------------------------------------------------------------


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
    usys: UnitSystem = coerce_unit_system(unit_system)
    grouping = _normalize_trace_grouping(trace_option)

    if grouping not in {"depth", "loggerLocation"}:
        bad_request(f"Unknown trace_option {trace_option!r}; must be one of {TRACE_CHOICES}")

    display_variable = variable
    source_variable = "VWC" if variable == "SWC" else variable

    df_plot = _ensure_timestamp_datetime(df)
    if "timestamp" not in df_plot.columns:
        raise HTTPException(status_code=400, detail="No timestamp column available for plotting.")

    human_var = get_unit_aware_label(display_variable, usys)

    fig = go.Figure()
    y_cols: List[str] = []
    use_secondary_y = False

    def swc_from_vwc(series: pd.Series, depth_key: str) -> pd.Series:
        vwc_pct = to_float_series(series)
        depth_in = SWC_DEPTH_INCHES.get(str(depth_key))
        if depth_in is None:
            return pd.Series([NAN] * len(series), index=series.index)

        swc_in = (vwc_pct / 100.0) * float(depth_in)
        if usys == "metric":
            conv_swc = UNIT_CONVERSIONS["us_to_metric"]["swc"]
            return swc_in.apply(lambda x: float(conv_swc(x)) if pd.notna(x) else NAN)
        return swc_in

    x_vals = _x_time_strings(df_plot)

    if grouping == "depth":
        for d, _names in SENSOR_DEPTH_LABELS.items():
            d_str = str(d)
            base_col = f"{source_variable}_{d_str}_raw_{strip}_{logger_location}"
            if base_col not in df_plot.columns:
                continue

            if display_variable == "SWC":
                df_plot[base_col] = swc_from_vwc(df_plot[base_col], d_str)

            y_cols.append(base_col)
            y_vals = safe_tolist(to_float_series(df_plot[base_col]))

            line_kwargs: Dict[str, Any] = {"width": 2}
            depth_col = _depth_color(d_str)
            if depth_col:
                line_kwargs["color"] = depth_col

            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=_depth_display_label(d_str, usys, compact=True),
                    line=line_kwargs,
                )
            )
    else:
        depth_str = str(depth)
        for loc_key in LOGGER_LOCATION_MAPPING:
            base_col = f"{source_variable}_{depth_str}_raw_{strip}_{loc_key}"
            if base_col not in df_plot.columns:
                continue

            if display_variable == "SWC":
                df_plot[base_col] = swc_from_vwc(df_plot[base_col], depth_str)

            y_cols.append(base_col)
            y_vals = safe_tolist(to_float_series(df_plot[base_col]))

            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=_logger_display_label(loc_key),
                    line=dict(width=2),
                )
            )

    if not y_cols:
        raise HTTPException(
            status_code=400,
            detail=(
                f"No valid data to plot for '{display_variable}' "
                f"@ strip='{strip}', loc='{logger_location}', depth='{depth}' "
                f"between {start} and {end}."
            ),
        )

    if display_variable in ("VWC", "SWC"):
        add_precipitation_bars(fig, df_plot, usys, granularity)
        use_secondary_y = True

    if display_variable == "T":
        temp_col: Optional[str] = None
        if usys == "metric" and "temp_air_degC" in df_plot.columns:
            temp_col = "temp_air_degC"
        elif "temp_air_degF" in df_plot.columns:
            temp_col = "temp_air_degF"

        if temp_col is not None:
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=safe_tolist(to_float_series(df_plot[temp_col])),
                    mode="lines",
                    name="Air Temp",
                    line=dict(
                        dash="dot",
                        color=PLOT_COLORS.get("air_temp", None),
                        width=2,
                    ),
                )
            )

    if use_secondary_y:
        fig.update_layout(yaxis2=common_yaxis2_config(usys))

    if display_variable in ("VWC", "SWC"):
        add_irrigation_shapes(fig, strip, year, usys)

    title_text = build_raw_plot_title(
        granularity=granularity,
        human_var=human_var,
        strip=strip,
        year=year,
        trace_option=grouping,
        logger_location=logger_location,
        depth=depth,
        unit_system=usys,
        is_gseason=False,
    )

    layout_kwargs: Dict[str, Any] = dict(
        title={"text": title_text, "x": 0.5, "font": {"size": TITLE_FONT_SIZE}},
        xaxis=common_xaxis_config(granularity, start, end),
        yaxis={"title": {"text": human_var, "font": {"size": 14}}},
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin=_plot_margin("standard"),
        height=400,
        autosize=True,
    )

    if use_secondary_y:
        layout_kwargs["yaxis2"] = fig.layout.yaxis2
        layout_kwargs["margin"] = _plot_margin("dual_axis_metric" if usys == "metric" else "dual_axis_us")

    fig.update_layout(**layout_kwargs)
    fig.update_layout(font={"size": 12})

    configure_primary_yaxis(
        fig=fig,
        df=df_plot,
        y_cols=y_cols,
        variable=display_variable,
        unit_system=usys,
        kind="raw",
    )

    return prepare_plot_for_json(fig)


# ---------------------------------------------------------------------------
# Ratio (time series or gseason)
# ---------------------------------------------------------------------------


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
    usys: UnitSystem = coerce_unit_system(unit_system)
    is_gs = granularity.lower() == "gseason"

    fig = go.Figure()

    ratio_prefix = "VWC" if variable == "SWC" else variable
    depth_str = str(depth)
    abbr = VARIABLE_NAME_ABBREV.get(variable, variable)

    y_cols = [
        c
        for c in df.columns
        if c.startswith(f"{ratio_prefix}_{depth_str}_ratio_")
        and c.endswith(f"_{logger_location}")
    ]

    if not y_cols:
        bad_request("No ratio data available for the selected filters.")

    df_plot = convert_units(_ensure_timestamp_datetime(df), usys).copy()

    if not is_gs and "timestamp" in df_plot.columns:
        df_plot["timestamp"] = pd.to_datetime(df_plot["timestamp"], errors="coerce")
        df_plot = df_plot.loc[df_plot["timestamp"].notna()].copy()

        start_ts = pd.to_datetime(start, errors="coerce")
        end_ts = pd.to_datetime(end, errors="coerce")

        if pd.notna(start_ts) and pd.notna(end_ts):
            df_plot = df_plot.loc[
                (df_plot["timestamp"] >= start_ts)
                & (df_plot["timestamp"] <= end_ts)
            ].copy()

    for idx, col in enumerate(y_cols, start=1):
        x = safe_tolist(df_plot.get("period_code")) if is_gs else _x_time_strings(df_plot)

        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        y = safe_tolist(to_float_series(df_plot[col]))
        pair_label = f"{p1}/{p2}"

        pair_color = PLOT_COLORS.get(f"ratio_{p1}_{p2}", None)

        if is_gs:
            bar_kwargs: Dict[str, Any] = {
                "x": x,
                "y": y,
                "name": pair_label,
                "offsetgroup": str(idx),
                "opacity": 0.8,
            }
            if pair_color:
                bar_kwargs["marker"] = dict(color=pair_color)
            fig.add_trace(go.Bar(**bar_kwargs))
        else:
            line_kwargs: Dict[str, Any] = {"width": 2}
            if pair_color:
                line_kwargs["color"] = pair_color

            fig.add_trace(
                go.Scatter(
                    x=x,
                    y=y,
                    mode="lines",
                    name=pair_label,
                    line=line_kwargs,
                )
            )

    title = build_ratio_plot_title(
        granularity=granularity,
        variable=variable,
        logger_location=logger_location,
        depth=depth,
        unit_system=usys,
        year=year,
        is_gseason=is_gs,
        no_data=False,
    )

    xcfg: Dict[str, Any] = (
        {"title": "Season", "type": "category"}
        if is_gs
        else common_xaxis_config(granularity, start, end)
    )

    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        x1=1,
        yref="y",
        y0=1,
        y1=1,
        line=dict(
            color=PLOT_COLORS.get("zero_line", "rgba(0,0,0,0.5)"),
            width=1,
        ),
    )

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={"text": title, "x": 0.5, "font": {"size": TITLE_FONT_SIZE}},
        xaxis=xcfg,
        legend=common_legend_config(f"{abbr} ratio"),
        template="plotly_white",
        margin=_plot_margin(
            "standard" if is_gs else ("dual_axis_metric" if usys == "metric" else "dual_axis_us")
        ),
        height=400,
        autosize=True,
    )

    configure_primary_yaxis(
        fig=fig,
        df=df_plot,
        y_cols=y_cols,
        variable=variable,
        unit_system=usys,
        kind="ratio",
    )

    return prepare_plot_for_json(fig)


# ---------------------------------------------------------------------------
# Temperature delta (time series)
# ---------------------------------------------------------------------------


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
    usys: UnitSystem = coerce_unit_system(unit_system)
    df2 = _ensure_timestamp_datetime(df)
    if "timestamp" not in df2.columns:
        bad_request("No timestamp column available for temperature delta plot.")

    depth_str = str(depth)
    loc = logger_location

    col_s1 = f"T_{depth_str}_raw_S1_{loc}"
    col_s2 = f"T_{depth_str}_raw_S2_{loc}"
    col_s3 = f"T_{depth_str}_raw_S3_{loc}"
    col_s4 = f"T_{depth_str}_raw_S4_{loc}"

    missing = [c for c in (col_s1, col_s2, col_s3, col_s4) if c not in df2.columns]
    if missing:
        raise ValueError(f"make_temperature_delta_figure: missing temperature columns: {missing}")

    s1 = to_float_series(df2[col_s1])
    s2 = to_float_series(df2[col_s2])
    s3 = to_float_series(df2[col_s3])
    s4 = to_float_series(df2[col_s4])

    delta_12 = (s1 - s2).astype(float)
    delta_34 = (s3 - s4).astype(float)

    x_vals = _x_time_strings(df2)
    d12_vals = safe_tolist(delta_12)
    d34_vals = safe_tolist(delta_34)

    unit_label = "°F" if usys == "us" else "°C"
    y_label = f"Soil temperature difference ({unit_label})"

    depth_label = _depth_display_label(depth_str, usys, compact=True)
    loc_label = _logger_display_label(loc)

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
            line=dict(width=2, color=PLOT_COLORS.get("delta_T_S1_S2", None)),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=x_vals,
            y=d34_vals,
            mode="lines",
            name="S3 − S4",
            line=dict(width=2, color=PLOT_COLORS.get("delta_T_S3_S4", None)),
        )
    )

    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        x1=1,
        yref="y",
        y0=0,
        y1=0,
        line=dict(color=PLOT_COLORS.get("zero_line", "rgba(0,0,0,0.5)"), width=1, dash="dash"),
    )

    arr12 = pd.Series(d12_vals, dtype="float64")
    arr34 = pd.Series(d34_vals, dtype="float64")
    max12 = arr12.abs().max(skipna=True)
    max34 = arr34.abs().max(skipna=True)
    max_abs = float(max(max12 if pd.notna(max12) else 0.0, max34 if pd.notna(max34) else 0.0))
    if max_abs <= 0:
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
            "showline": True,
            "linecolor": "black",
            "linewidth": 1,
        },
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin=_plot_margin("standard_tall"),
        height=400,
        autosize=True,
    )

    return prepare_plot_for_json(fig)


# -----------------------------------------------------------------------------
# RAW gseason (categorical)
# -----------------------------------------------------------------------------


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
    usys: UnitSystem = coerce_unit_system(unit_system)
    grouping = _normalize_trace_grouping(trace_option)

    df2 = convert_units(df, usys).copy()
    norm_periods = periods_to_list_of_dicts(periods or [])
    labels = [f"{p['label']} ({p['start']}-{p['end']})" for p in norm_periods]

    fig = go.Figure()

    precip_col_us = "precip_in"
    precip_col_mm = "precip_mm"
    have_precip = variable == "VWC" and (precip_col_us in df2.columns or precip_col_mm in df2.columns)

    precip_vals: Optional[pd.Series] = None
    if have_precip:
        precip_col = precip_col_mm if (usys == "metric" and precip_col_mm in df2.columns) else precip_col_us
        precip_vals = to_float_series(df2[precip_col])

        unit_suffix = "in" if usys == "us" else "mm"
        precip_list = safe_tolist(precip_vals)
        precip_text: List[str] = []
        for v in precip_list:
            try:
                fv = float(v)
                precip_text.append(f"{fv:.2f} {unit_suffix}" if pd.notna(fv) else "")
            except (TypeError, ValueError):
                precip_text.append("")

        fig.add_trace(
            go.Bar(
                x=labels,
                y=safe_tolist(precip_vals),
                name=label_name_mapping["precip"][usys],
                marker=dict(color=PLOT_COLORS.get("precip", "LightSteelBlue")),
                yaxis="y2",
                offsetgroup="0",
                opacity=0.55,
                text=precip_text,
                textposition="outside",
                textfont=dict(size=12),
                cliponaxis=False,
                hovertemplate="Precip: %{y:.2f} " + unit_suffix,
            )
        )

    human_var = label_name_mapping[variable][usys]
    abbr = VARIABLE_NAME_ABBREV.get(variable, variable)
    legend_fmt = f"{abbr}, {{}}"
    sensor_cols_plotted: List[str] = []
    depth_str = str(depth)

    if variable == "SWC":
        vol_suffix = "gal" if usys == "us" else "L"
        base = f"SWC_vol_{vol_suffix}"

        if grouping == "depth":
            for idx, (d, depth_map) in enumerate(SENSOR_DEPTH_LABELS.items(), start=1):
                d_str = str(d)
                col = f"{base}_{strip}_{logger_location}_{d_str}"
                if col not in df2.columns:
                    continue
                sensor_cols_plotted.append(col)

                series = to_float_series(df2[col])
                bar_kwargs: Dict[str, Any] = {
                    "x": labels,
                    "y": safe_tolist(series),
                    "name": legend_fmt.format(depth_map[usys]),
                    "offsetgroup": str(idx),
                    "opacity": 0.85,
                }
                depth_col = _depth_color(d_str)
                if depth_col:
                    bar_kwargs["marker"] = dict(color=depth_col)
                fig.add_trace(go.Bar(**bar_kwargs))
        else:
            for idx, (loc_key, loc_label) in enumerate(LOGGER_LOCATION_MAPPING.items(), start=1):
                col = f"{base}_{strip}_{loc_key}_{depth_str}"
                if col not in df2.columns:
                    continue
                sensor_cols_plotted.append(col)

                series = to_float_series(df2[col])
                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=safe_tolist(series),
                        name=legend_fmt.format(loc_label),
                        offsetgroup=str(idx),
                        opacity=0.85,
                    )
                )

    else:
        if grouping == "depth":
            for idx, (d, depth_map) in enumerate(SENSOR_DEPTH_LABELS.items(), start=1):
                d_str = str(d)
                col = f"{variable}_{d_str}_raw_{strip}_{logger_location}"
                if col not in df2.columns:
                    continue
                sensor_cols_plotted.append(col)

                series = to_float_series(df2[col])
                bar_kwargs2: Dict[str, Any] = {
                    "x": labels,
                    "y": safe_tolist(series),
                    "name": legend_fmt.format(depth_map[usys]),
                    "offsetgroup": str(idx),
                    "opacity": 0.85,
                }
                depth_col = _depth_color(d_str)
                if depth_col:
                    bar_kwargs2["marker"] = dict(color=depth_col)
                fig.add_trace(go.Bar(**bar_kwargs2))
        else:
            for idx, (loc_key, loc_label) in enumerate(LOGGER_LOCATION_MAPPING.items(), start=1):
                col = f"{variable}_{depth_str}_raw_{strip}_{loc_key}"
                if col not in df2.columns:
                    continue
                sensor_cols_plotted.append(col)

                series = to_float_series(df2[col])
                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=safe_tolist(series),
                        name=legend_fmt.format(loc_label),
                        offsetgroup=str(idx),
                        opacity=0.85,
                    )
                )

    if variable == "VWC" and norm_periods:
        add_irrigation_shapes(
            fig=fig,
            strip=strip,
            year=year,
            unit_system=usys,
            sum_only=True,
            periods=periods,
            category_labels=labels,
        )

    primary_min: Optional[float]
    primary_max: Optional[float]
    if sensor_cols_plotted:
        block = df_cols(df2, sensor_cols_plotted)
        primary_min, primary_max = finite_min_max(block)
    else:
        primary_min = None
        primary_max = None

    yaxis_cfg = common_yaxis_config("raw", variable, usys, primary_min, primary_max)
    y2_cfg: Dict[str, Any] = common_yaxis2_config(usys)

    if have_precip and precip_vals is not None:
        p_block = pd.DataFrame({"p": to_float_series(precip_vals)})
        _pmin, pmax = finite_min_max(p_block)
        pmax_val = float(pmax) if pmax is not None else 0.0
        y2_cfg["range"] = [0.0, (pmax_val * 1.15) if pmax_val > 0 else 1.0]

    title_text = build_raw_plot_title(
        granularity="gseason",
        human_var=human_var,
        strip=strip,
        year=year,
        trace_option=grouping,
        logger_location=logger_location,
        depth=depth,
        unit_system=usys,
        is_gseason=True,
    )

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={"text": title_text, "x": 0.5, "font": {"size": TITLE_FONT_SIZE}},
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
        margin=_plot_margin(
            "dual_axis_tall_metric" if have_precip and usys == "metric"
            else "dual_axis_tall_us" if have_precip
            else "standard_tall"
        ),
        height=400,
    )

    return prepare_plot_for_json(fig)


# -----------------------------------------------------------------------------
# Ratio gseason (categorical)
# -----------------------------------------------------------------------------


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
    usys: UnitSystem = coerce_unit_system(unit_system)
    df2 = convert_units(df, usys).copy()
    norm_periods = periods_to_list_of_dicts(periods or [])
    labels = [f"{p['label']} ({p['start']}-{p['end']})" for p in norm_periods]

    fig = go.Figure()

    abbr = VARIABLE_NAME_ABBREV.get(variable, variable)
    full_label = label_name_mapping[variable][usys]
    human_base = str(full_label).split(" (")[0]
    depth_str = str(depth)

    if variable == "SWC":
        vol_suffix = "gal" if usys == "us" else "L"
        base = f"SWC_vol_{vol_suffix}"

        s1_col = f"{base}_S1_{logger_location}_{depth_str}"
        s2_col = f"{base}_S2_{logger_location}_{depth_str}"
        s3_col = f"{base}_S3_{logger_location}_{depth_str}"
        s4_col = f"{base}_S4_{logger_location}_{depth_str}"

        ratios: Dict[str, pd.Series] = {}

        if s1_col in df2.columns and s2_col in df2.columns:
            s1 = to_float_series(df2[s1_col])
            s2 = to_float_series(df2[s2_col])
            ratios["S1/S2"] = (s1 / s2).replace([POS_INF, NEG_INF], NAN)

        if s3_col in df2.columns and s4_col in df2.columns:
            s3 = to_float_series(df2[s3_col])
            s4 = to_float_series(df2[s4_col])
            ratios["S3/S4"] = (s3 / s4).replace([POS_INF, NEG_INF], NAN)

        if not ratios:
            logger.warning(
                "[SWC gseason ratio] No usable SWC_vol columns for strip=%s, logger=%s, depth=%s",
                strip,
                logger_location,
                depth_str,
            )
            fig.update_layout(
                title={
                    "text": build_ratio_plot_title(
                        granularity="gseason",
                        variable=abbr,
                        logger_location=logger_location,
                        depth=depth,
                        unit_system=usys,
                        year=year,
                        is_gseason=True,
                        no_data=True,
                    ),
                    "x": 0.5,
                },
                xaxis={
                    "title": "Season",
                    "type": "category",
                    "showline": True,
                    "linecolor": "black",
                    "linewidth": 1,
                },
            )
            return prepare_plot_for_json(fig)

        all_vals: List[pd.Series] = []
        for idx, (pair_label, series) in enumerate(ratios.items(), start=1):
            vals = to_float_series(series)
            all_vals.append(vals)

            color_key = f"ratio_SWC_{pair_label.replace('/', '_')}"
            color_val = PLOT_COLORS.get(color_key, None)

            bar_kwargs: Dict[str, Any] = {
                "x": labels,
                "y": safe_tolist(vals),
                "name": pair_label,
                "offsetgroup": str(idx),
                "opacity": 0.8,
            }
            if color_val is not None:
                bar_kwargs["marker"] = dict(color=color_val)

            fig.add_trace(go.Bar(**bar_kwargs))

        combined = pd.concat(all_vals, axis=0)
        global_min_val = combined.min()
        global_max_val = combined.max()
        global_min = float(global_min_val) if pd.notna(global_min_val) else None
        global_max = float(global_max_val) if pd.notna(global_max_val) else None

        fig.update_layout(
            barmode="group",
            bargap=0.2,
            bargroupgap=0.1,
            title={
                "text": build_ratio_plot_title(
                    granularity="gseason",
                    variable=abbr,
                    logger_location=logger_location,
                    depth=depth,
                    unit_system=usys,
                    year=year,
                    is_gseason=True,
                    no_data=False,
                ),
                "x": 0.5,
            },
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
                    unit_system=usys,
                    global_min=global_min,
                    global_max=global_max,
                ),
                "title": f"{human_base} Ratio",
            },
            legend=common_legend_config(f"{abbr} ratio"),
            template="plotly_white",
            margin=_plot_margin("standard"),
            height=400,
        )

        return prepare_plot_for_json(fig)

    y_cols = [
        c
        for c in df2.columns
        if c.startswith(f"{variable}_{depth_str}_ratio_") and c.endswith(f"_{logger_location}")
    ]

    if not y_cols:
        logger.warning(
            "[gseason ratio] No ratio columns for variable=%s, depth=%s, strip=%s, logger=%s",
            variable,
            depth_str,
            strip,
            logger_location,
        )
        fig.update_layout(
            title={
                "text": build_ratio_plot_title(
                    granularity="gseason",
                    variable=abbr,
                    logger_location=logger_location,
                    depth=depth,
                    unit_system=usys,
                    year=year,
                    is_gseason=True,
                    no_data=True,
                ),
                "x": 0.5,
            },
            xaxis={
                "title": "Season",
                "type": "category",
                "showline": True,
                "linecolor": "black",
                "linewidth": 1,
            },
        )
        return prepare_plot_for_json(fig)

    for idx, col in enumerate(y_cols, start=1):
        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        pair_color = PLOT_COLORS.get(f"ratio_{p1}_{p2}", None)

        series = to_float_series(df2[col])
        bar_kwargs3: Dict[str, Any] = {
            "x": labels,
            "y": safe_tolist(series),
            "name": f"{p1}/{p2}",
            "offsetgroup": str(idx),
            "opacity": 0.8,
        }
        if pair_color:
            bar_kwargs3["marker"] = dict(color=pair_color)

        fig.add_trace(go.Bar(**bar_kwargs3))

    block = df_cols(df2, y_cols)
    global_min, global_max = finite_min_max(block)

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={
            "text": build_ratio_plot_title(
                granularity="gseason",
                variable=abbr,
                logger_location=logger_location,
                depth=depth,
                unit_system=usys,
                year=year,
                is_gseason=True,
                no_data=False,
            ),
            "x": 0.5,
        },
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
                unit_system=usys,
                global_min=global_min,
                global_max=global_max,
            ),
            "title": f"{human_base} Ratio",
        },
        legend=common_legend_config(f"{abbr} ratio"),
        template="plotly_white",
        margin=_plot_margin("standard"),
        height=400,
    )

    return prepare_plot_for_json(fig)
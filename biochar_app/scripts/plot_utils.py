import pandas as pd
import numpy as np
import plotly.graph_objects as go
from typing import Any, Dict, List, Tuple, Optional
from flask import abort
import json
from plotly.utils import PlotlyJSONEncoder
import logging

from biochar_app.scripts.gseason_utils import periods_to_list_of_dicts
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from biochar_app.scripts.routes import PeriodSpec

logger = logging.getLogger(__name__)

from biochar_app.scripts.config import (
    PRECIP_COLS,
    DATA_PROCESSED_DIR,
    bar_width_map,
    label_name_mapping,
    sensor_depth_mapping,
    logger_location_mapping,
    TRACE_CHOICES,
    variable_name_abbrev,
    UNIT_CONVERSIONS,
    IRR_COLOR,
)

SWC_DEPTH_INCHES = {
    depth_key: float(labels["us"].split()[0])
    for depth_key, labels in sensor_depth_mapping.items()
}

from biochar_app.scripts.plot_helpers import (
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

    for col in y_cols:
        meta = parse_sensor_column(col, unit_system)
        y_series = df[col].astype(float)
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

    conv     = UNIT_CONVERSIONS["us_to_metric"]["irrigation"]
    unit_lbl = "k L" if unit_system == "metric" else "k gal"

    if sum_only and periods:
        # use category positions, not domain fractions
        labels = category_labels or [
            (getattr(p, "label", None) or str(i + 1)) for i, p in enumerate(periods)
        ]

        for i, p in enumerate(periods):
            start_ts = pd.to_datetime(getattr(p, "start", None))
            end_ts   = pd.to_datetime(getattr(p, "end", None))
            if pd.isna(start_ts) or pd.isna(end_ts):
                continue

            total = 0.0
            for ev in recs:
                ts = pd.to_datetime(ev.get("start") or ev.get("timestamp"), errors="coerce")
                if pd.isna(ts) or not (start_ts <= ts <= end_ts):
                    continue
                try:
                    total += float(ev.get("volume_gal", 0))
                except (TypeError, ValueError):
                    pass

            if total <= 0:
                continue
            if unit_system == "metric":
                total = conv(total)

            cat = labels[i]  # categorical x position
            # vertical dotted line at the *category*, spanning the full plot height
            fig.add_shape(
                type="line",
                xref="x", x0=cat, x1=cat,
                yref="paper", y0=0, y1=1,
                line=dict(color="sienna", dash="dot", width=2),
            )
            # “### k gal/k L” above the plot, centered over the same category
            fig.add_annotation(
                xref="x", x=cat,
                yref="paper", y=1.02,
                text=f"{total/1000:.0f} {unit_lbl}",
                showarrow=False,
                font=dict(size=10, color="sienna"),
            )

    elif not sum_only:
        # date-time axis case
        for ev in recs:
            ts = pd.to_datetime(ev.get("start") or ev.get("timestamp"), errors="coerce")
            if pd.isna(ts):
                continue

            fig.add_shape(
                type="line",
                xref="x", x0=ts, x1=ts,
                yref="paper", y0=0, y1=1,
                line=dict(color="sienna", dash="dot", width=2),
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
                font=dict(size=10, color="sienna"),
            )

    # Dummy legend entry that matches the dotted line style
    legend_label = get_unit_aware_label("irrigation", unit_system)
    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode="lines",
        line=dict(color="sienna", dash="dot", width=2),
        name=legend_label,
        showlegend=True,
    ))


def configure_primary_yaxis(
    fig: go.Figure,
    df: pd.DataFrame,
    y_cols: List[str],
    variable: str,
    unit_system: str,
    kind: str,
) -> None:
    """
    Compute a sensible global min/max from the plotted columns and
    delegate to common_yaxis_config, with a couple of extra tweaks:

    - For ratio plots of water-content variables (VWC, SWC) we clamp
      the lower bound at 0 so the darker "1.0" line is visually clear.
    - For temperature and EC ratios we do *not* force the minimum to 0,
      which avoids a lot of wasted vertical space.
    """
    if not y_cols:
        # Nothing to scale – leave layout as-is.
        return

    scaled = df[y_cols].astype(float)
    gmin, gmax = compute_global_min_max(scaled, y_cols)

    # Only clamp ratio plots to 0.0 for water-content variables
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
    """
    Build the top (raw) Plotly figure.

    - For VWC, T, EC: plot the raw sensor values from the Parquet slice.
    - For SWC: derive soil-water content from the underlying VWC(%)
      for each depth using the physical depth in inches, then convert
      to cm when metric units are selected.

    Titles and y-axis labels always use the *display* variable
    (VWC, Soil Temperature, EC, Soil Water Content), while the
    underlying column patterns may reuse VWC columns for SWC.
    """
    if trace_option not in TRACE_CHOICES:
        abort(
            400,
            f"Unknown trace_option {trace_option!r}; must be one of {TRACE_CHOICES}",
        )

    # What we're *displaying* vs what columns we read from
    display_variable = variable                      # "VWC", "T", "EC", or "SWC"
    source_variable  = "VWC" if variable == "SWC" else variable

    # Copy so we can safely manipulate values (for SWC) without
    # affecting the cached dataset upstream.
    df_plot = df.copy()

    # Human-readable variable & logger location for titles/labels
    human_var        = get_unit_aware_label(display_variable, unit_system)
    human_logger_loc = logger_location_mapping.get(logger_location, logger_location)

    fig = go.Figure()
    y_cols: List[str] = []
    use_secondary_y = False

    # Small helper: convert a VWC(%) series to SWC (in or cm)
    def swc_from_vwc(series: pd.Series, depth_key: str) -> pd.Series:
        vwc_pct = pd.to_numeric(series, errors="coerce")
        depth_in = SWC_DEPTH_INCHES.get(str(depth_key))
        if depth_in is None:
            return pd.Series(np.nan, index=series.index)

        # VWC is stored as percent (0–100) in Parquet
        swc_in = (vwc_pct / 100.0) * depth_in
        if unit_system == "metric":
            return UNIT_CONVERSIONS["us_to_metric"]["swc"](swc_in)
        else:
            return swc_in

    # ── 1) Sensor traces on primary y-axis ──────────────────────────────
    if trace_option == TRACE_CHOICES[0]:  # group by depths at one logger
        for d, names in sensor_depth_mapping.items():
            base_col = f"{source_variable}_{d}_raw_{strip}_{logger_location}"
            if base_col not in df_plot.columns:
                continue

            # If plotting SWC, rewrite the underlying series in-place
            # so downstream scaling uses SWC units.
            if display_variable == "SWC":
                df_plot[base_col] = swc_from_vwc(df_plot[base_col], d)

            y_cols.append(base_col)
            x_vals = df_plot["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            y_vals = pd.to_numeric(df_plot[base_col], errors="coerce").tolist()

            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=y_vals,
                    mode="lines",
                    name=names[unit_system],
                    line=dict(width=2),
                )
            )
    else:
        # group by logger locations at a fixed depth
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

    # If we genuinely found no usable columns, bail out cleanly.
    if not y_cols:
        abort(
            400,
            (
                f"No valid data to plot for '{display_variable}' "
                f"@ strip='{strip}', loc='{logger_location}', depth='{depth}' "
                f"between {start} and {end}. Found columns: "
                f"{[c for c in df_plot.columns if c.startswith(source_variable + '_')]}"
            ),
        )

    # ── 2) Precipitation overlay (for VWC & SWC) ────────────────────────
    if display_variable in ("VWC", "SWC"):
        logger.info("ℹ️ looking for precip columns (‘precip_in’/‘precip_mm’) in DataFrame")
        # Uses precip_in/precip_mm directly; df vs df_plot doesn’t matter here
        add_precipitation_bars(fig, df, unit_system, granularity)
        use_secondary_y = True

    # ── 3) Air temperature overlay (for soil-temperature plots) ─────────
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
                    x=df["timestamp"],
                    y=pd.to_numeric(df[temp_col], errors="coerce").tolist(),
                    mode="lines",
                    name=label,
                    yaxis="y2",
                    line=dict(dash="dot"),
                )
            )
            use_secondary_y = True

    # If we added anything on the secondary axis, configure it now.
    if use_secondary_y:
        fig.update_layout(yaxis2=common_yaxis2_config(unit_system))

    # ── 4) Irrigation overlay (same for all variables) ──────────────────
    add_irrigation_shapes(fig, strip, year, unit_system)

    # ── 5) Layout & axis ranges ─────────────────────────────────────────
    title_text = (
        f"{granularity.capitalize()} Data Plot for {human_var} "
        f"in Strip {strip}, {year} ({human_logger_loc} Logger)"
    )

    layout_kwargs: Dict[str, Any] = dict(
        title={"text": title_text, "x": 0.5},
        xaxis=common_xaxis_config(granularity, start, end),
        yaxis={"title": human_var},
        legend=common_legend_config("Legend"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 60, "b": 40},
        height=400,
        autosize=True,
    )
    if use_secondary_y:
        layout_kwargs["yaxis2"] = fig.layout.yaxis2

    fig.update_layout(**layout_kwargs)

    # Use the actually plotted columns (now in correct units) for y-range
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

    # 🔁 SWC ratios reuse the VWC ratio columns (volume scale cancels).
    ratio_prefix = "VWC" if variable == "SWC" else variable

    y_cols = [
        c for c in df.columns
        if c.startswith(f"{ratio_prefix}_{depth}_ratio_")
           and c.endswith(f"_{logger_location}")
    ]
    if not y_cols:
        abort(400, "No ratio data available for the selected filters.")

    df_plot = convert_units(df, unit_system)

    for idx, col in enumerate(y_cols):
        if is_gs:
            x = df_plot["period_code"].tolist()
        else:
            x = df_plot["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

        p1, p2 = col.split("_ratio_")[1].split("_")[:2]
        y = df_plot[col].astype(float).tolist()
        name = f"{variable_name_abbrev.get(variable, variable)} ratio {p1}/{p2}"

        if is_gs:
            fig.add_trace(go.Bar(
                x=x, y=y, name=name,
                offsetgroup=str(idx+1),
                opacity=0.8,
            ))
        else:
            fig.add_trace(go.Scatter(
                x=x, y=y, mode="lines", name=name, line=dict(width=2)
            ))

    human_var = get_unit_aware_label(variable, unit_system)
    human_logger_loc = logger_location_mapping.get(logger_location, logger_location)

    title = (
        f"{granularity.capitalize()} Ratio Plot for "
        f"{human_var} in {year} ({human_logger_loc} Logger)"
    )
    xcfg = {"title": "Season", "type": "category"} if is_gs else common_xaxis_config(granularity, start, end)

    # Horizontal reference line at y=1
    fig.add_shape(
        type="line",
        xref="paper", x0=0, x1=1,
        yref="y", y0=1, y1=1,
        line=dict(color="rgba(0,0,0,0.5)", width=1),
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

# plot_utils.py

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
    Growing-season raw bar chart:

      - sensor means on primary y-axis
      - precipitation on secondary y-axis (bars)
      - irrigation totals as vertical dotted lines (with annotations)
      - precipitation labels drawn just above each precip bar
    """

    # Convert logger values for display units (VWC %, temp °F/°C, etc.)
    df = convert_units(df, unit_system)

    # --- Normalize periods -> list[dict] with keys: code, label, start, end ----
    norm_periods = periods_to_list_of_dicts(periods or [])
    labels = [f"{p['label']} ({p['start']}-{p['end']})" for p in norm_periods]

    fig = go.Figure()

    # -------------------------------------------------------------------------
    # 1) Precipitation on y2 (only shown for VWC)
    # -------------------------------------------------------------------------
    precip_col_us = "precip_in"
    precip_col_mm = "precip_mm"
    have_precip = (variable == "VWC") and (
        precip_col_us in df.columns or precip_col_mm in df.columns
    )

    precip_vals = None
    if have_precip:
        # Choose which column to use based on unit_system
        if unit_system == "metric" and precip_col_mm in df.columns:
            precip_col = precip_col_mm
        else:
            precip_col = precip_col_us

        precip_vals = pd.to_numeric(df[precip_col], errors="coerce").astype(float)
        try:
            vals_for_log = [
                None if (v is None or pd.isna(v)) else float(v)
                for v in precip_vals.tolist()
            ]
        except Exception:
            vals_for_log = precip_vals.tolist()

        logger.info("🌧️ G-season precip (%s) → %s", precip_col, vals_for_log)
        if len(vals_for_log) == len(labels):
            logger.info(
                "🌧️ per-period precip (label → value): %s",
                list(zip(labels, vals_for_log)),
            )

        # Precipitation bar trace on y2
        fig.add_trace(
            go.Bar(
                x=labels,
                y=precip_vals.tolist(),
                name=label_name_mapping["precip"][unit_system],
                marker=dict(color="LightSteelBlue"),
                yaxis="y2",
                offsetgroup="0",
                opacity=0.6,
                hovertemplate="Precip: %{y:.2f}"
                + (" in" if unit_system == "us" else " mm"),
            )
        )

        # Add numeric labels above each precip bar (on y2 axis)
        unit_suffix = "in" if unit_system == "us" else "mm"
        for lab, val in zip(labels, precip_vals):
            if pd.isna(val) or val <= 0:
                continue
            fig.add_annotation(
                x=lab,
                xref="x",
                y=float(val) * 1.03,  # a bit above the bar
                yref="y2",
                text=f"{val:.2f} {unit_suffix}",
                showarrow=False,
                font=dict(size=10),
            )

    # -------------------------------------------------------------------------
    # 2) Sensor bars on primary y-axis
    # -------------------------------------------------------------------------
    human_var = label_name_mapping[variable][unit_system]
    abbrev = variable_name_abbrev[variable]
    legend_fmt = f"{abbrev}, {{}}"

    sensor_cols_plotted: List[str] = []

    if trace_option == "depths":
        # One bar per depth at a single logger location
        for idx, (d, depth_map) in enumerate(sensor_depth_mapping.items(), start=1):
            col = f"{variable}_{d}_raw_{strip}_{logger_location}"
            if col not in df:
                continue
            sensor_cols_plotted.append(col)
            fig.add_trace(
                go.Bar(
                    x=labels,
                    y=pd.to_numeric(df[col], errors="coerce")
                    .astype(float)
                    .tolist(),
                    name=legend_fmt.format(depth_map[unit_system]),
                    offsetgroup=str(idx),
                    opacity=0.85,
                )
            )
    else:
        # One bar per logger location at a fixed depth
        for idx, (loc_key, loc_label) in enumerate(
            logger_location_mapping.items(), start=1
        ):
            col = f"{variable}_{depth}_raw_{strip}_{loc_key}"
            if col not in df:
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

    # -------------------------------------------------------------------------
    # 3) Irrigation lines + annotations (VWC only)
    # -------------------------------------------------------------------------
    if variable == "VWC" and norm_periods:
        evs = load_irrigation_events(strip, year).to_dict(orient="records")
        conv = UNIT_CONVERSIONS["us_to_metric"]["irrigation"]
        unit_lbl = "k L" if unit_system == "metric" else "k gal"

        # Build absolute timestamps for each period (handles wrap-around)
        start_end_pairs: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        for p in norm_periods:
            s = str(p["start"])  # "MM-DD"
            e = str(p["end"])
            try:
                sm = int(s[:2])
                em = int(e[:2])
            except Exception:
                sm = em = 1  # fallback (won't wrap)
            start_year = year - 1 if sm > em else year
            end_year = year
            start_ts = pd.Timestamp(f"{start_year}-{s}")
            # end inclusive to end-of-day
            end_ts = (
                pd.Timestamp(f"{end_year}-{e}")
                + pd.Timedelta(days=1)
                - pd.Timedelta(seconds=1)
            )
            start_end_pairs.append((start_ts, end_ts))

        for (start_ts, end_ts), cat in zip(start_end_pairs, labels):
            total = 0.0
            for ev in evs:
                ts = pd.to_datetime(
                    ev.get("start") or ev.get("timestamp"), errors="coerce"
                )
                if pd.isna(ts) or not (start_ts <= ts <= end_ts):
                    continue
                try:
                    total += float(ev.get("volume_gal", 0))
                except (TypeError, ValueError):
                    pass

            if total <= 0:
                continue
            if unit_system == "metric":
                total = conv(total)

            fig.add_shape(
                type="line",
                xref="x",
                x0=cat,
                x1=cat,
                yref="paper",
                y0=0,
                y1=1,
                line=dict(color=IRR_COLOR, dash="dot", width=2),
            )
            fig.add_annotation(
                xref="x",
                x=cat,
                yref="paper",
                y=1.06,  # a bit higher to clear legend
                text=f"{total/1000:.0f} {unit_lbl}",
                showarrow=False,
                font=dict(size=10, color="sienna"),
            )

        # Dummy legend entry (use NaN so it doesn’t render a point)
        legend_label = get_unit_aware_label("irrigation", unit_system)
        fig.add_trace(
            go.Scatter(
                x=[labels[0]],
                y=[float("nan")],
                mode="lines",
                line=dict(color=IRR_COLOR, dash="dot", width=2),
                name=legend_label,
                showlegend=True,
                hoverinfo="skip",
                legendgroup="irrigation",
            )
        )

    # -------------------------------------------------------------------------
    # 4) Axis scaling
    # -------------------------------------------------------------------------
    # Primary y: only use the columns we actually plotted
    if sensor_cols_plotted:
        primary_min = (
            df[sensor_cols_plotted].min(numeric_only=True).min()
        )
        primary_max = (
            df[sensor_cols_plotted].max(numeric_only=True).max()
        )
    else:
        primary_min = None
        primary_max = None

    yaxis_cfg = common_yaxis_config(
        "raw", variable, unit_system, primary_min, primary_max
    )

    # Secondary y (precipitation)
    y2_cfg: dict[str, Any] = common_yaxis2_config(unit_system)

    if have_precip and precip_vals is not None:
        pvals = precip_vals.to_numpy(dtype=float)
        good = pvals[np.isfinite(pvals)]
        if good.size:
            pmax = float(good.max())
        else:
            pmax = 0.0

        if pmax <= 0:
            ymax = 1.0
        else:
            ymax = pmax * 1.15

        # force 0 → top range; no negatives
        y2_cfg["range"] = [0.0, ymax]

    # -------------------------------------------------------------------------
    # 5) Layout
    # -------------------------------------------------------------------------
    depth_label = sensor_depth_mapping[str(depth)][unit_system]
    title_text = (
        f"Growing-season Data Plot for {human_var} in Strip {strip}, {year} "
        f"({logger_location_mapping[logger_location]} Logger)"
    )

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={"text": title_text, "x": 0.5},
        xaxis={
            "title": "Season",
            "type": "category",
            "showline": True,
            "linecolor": "black",
            "linewidth": 1,
        },
        yaxis={**yaxis_cfg, "title": human_var},
        yaxis2=y2_cfg,
        legend=dict(
            **common_legend_config("Legend"),
            bgcolor="rgba(255,255,255,0.7)",  # slightly transparent
            bordercolor="rgba(0,0,0,0.15)",
            borderwidth=1,
        ),
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
    Build a growing‐season ratio bar chart.
    One bar per strip‐pair (S1/S2, S3/S4) at the chosen logger_location.
    """
    df = convert_units(df, unit_system)

    # 1) category labels
    labels = [f"{p.label} ({p.start}-{p.end})" for p in periods]

    # 2) pick out only the ratio columns for this depth/logger
    y_cols = [
        c
        for c in df.columns
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
        fig.add_trace(
            go.Bar(
                x=labels,
                y=df[col].astype(float).tolist(),
                name=f"{variable_name_abbrev[variable]} ratio {p1}/{p2}",
                offsetgroup=str(idx),
                opacity=0.8,
            )
        )

    # 4) layout
    depth_label = sensor_depth_mapping[str(depth)][unit_system]
    human_var = label_name_mapping[variable][unit_system].split(" (")[0]

    fig.update_layout(
        barmode="group",
        bargap=0.2,
        bargroupgap=0.1,
        title={
            "text": (
                f"Growing-season Ratio Plot for {human_var} at {depth_label} in Strip {strip}, {year} "
                "(Strip Ratios S1/S2 and S3/S4)"
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
                unit_system=unit_system,
                global_min=df[y_cols].min(numeric_only=True).min(),
                global_max=df[y_cols].max(numeric_only=True).max(),
            ),
            "title": f"{human_var} Ratio",
        },
        legend=common_legend_config("Strip Ratios"),
        template="plotly_white",
        margin={"l": 60, "r": 20, "t": 60, "b": 40},
        height=400,
    )

    return prepare_plot_for_json(fig)
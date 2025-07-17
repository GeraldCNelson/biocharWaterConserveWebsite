
def prepare_plot_figure(
    filtered_df, unit_system, y_cols, y2_cols, variable,
    trace_option, depth, logger_location, kind, global_min, global_max,
    granularity, strip, year, legend_labels=None
):
    fig = go.Figure()

    # Build title and legend label
    title, legend_title = build_plot_title_and_legend_label(
        granularity, variable, strip, year,
        trace_option, logger_location, depth, unit_system, kind
    )

    # Dynamic primary axis label
    yaxis_title = label_name_mapping.get(variable, {}).get(unit_system, variable)

    # Primary y-axis traces
    for idx, col in enumerate(y_cols):
        trace_label = legend_labels[idx] if legend_labels and idx < len(legend_labels) else col
        fig.add_trace(go.Scatter(
            x=filtered_df["timestamp"],
            y=filtered_df[col],
            name=trace_label,
            mode="lines",
            yaxis="y1"
        ))

    # Secondary y-axis traces (only if kind == "raw")
    if kind == "raw":
        for col in y2_cols:
            name = col.replace("_", " ")
            ts = filtered_df["timestamp"]
            vals = filtered_df[col]

            if "precip" in col:
                fig.add_trace(go.Bar(x=ts, y=vals, name=name, yaxis="y2", opacity=0.4, marker=dict(color="lightblue")))
            elif "temp_air" in col:
                fig.add_trace(go.Scatter(x=ts, y=vals, name=name, mode="lines",
                                         line=dict(dash="dot", color="gray"), yaxis="y2"))
            elif "irrigation" in col:
                fig.add_trace(go.Bar(x=ts, y=vals, name=name, yaxis="y2", opacity=0.4, marker=dict(color="orange")))

        # 🟫 Add irrigation overlays as rectangles with annotations
        irrigation_events = load_irrigation_events(year, strip)
        if irrigation_events is not None:
            for _, row in irrigation_events.iterrows():
                start = pd.to_datetime(row["start"])
                end = pd.to_datetime(row["end"])
                volume = row["volume"]

                fig.add_shape(
                    type="rect",
                    x0=start,
                    x1=end,
                    y0=global_min or 0,
                    y1=global_max or 1,
                    xref="x",
                    yref="y",
                    line=dict(width=0),
                    fillcolor="rgba(139,69,19,0.15)",  # light brown
                    layer="below"
                )

                midpoint = start + (end - start) / 2
                fig.add_annotation(
                    x=midpoint,
                    y=global_max * 0.98 if global_max else 1,
                    text=f"{int(volume / 1000)}k",
                    showarrow=False,
                    font=dict(size=10, color="brown"),
                    yanchor="top"
                )

    # Secondary axis label
    secondary_label = ""
    if kind == "raw":
        if any("precip" in col for col in y2_cols):
            secondary_label = label_name_mapping.get("precip_mm", {}).get(unit_system, "Precipitation")
        elif any("temp_air" in col for col in y2_cols):
            secondary_label = label_name_mapping.get("temp_air", {}).get(unit_system, "Air Temperature")
        elif any("irrigation" in col for col in y2_cols):
            secondary_label = label_name_mapping.get("irrigation", {}).get(unit_system, "Irrigation")

    layout_config = dict(
        title=dict(text=title, x=0.5),
        xaxis=dict(title="Date", type="date", tickformat="%b %Y", linecolor="black", linewidth=1),
        yaxis=dict(
            title=yaxis_title,
            linecolor="black", linewidth=1,
            rangemode="tozero",
            range=[global_min, global_max] if global_min is not None and global_max is not None else None
        ),
        legend=dict(
            title=dict(text=legend_title),
            bgcolor="rgba(255,255,255,0.5)",
            x=1, xanchor="right", y=1, yanchor="top"
        ),
        template="plotly_white"
    )

    if kind == "raw":
        layout_config["yaxis2"] = dict(
            title=secondary_label,
            overlaying="y",
            side="right",
            showline=True,
            linecolor="black",
            linewidth=1
        )

    fig.update_layout(**layout_config)

    return prepare_plot_for_json(fig)

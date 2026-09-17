# Irrigation flow and water-balance research outputs

The complete analysis report is [irrigation_flow_water_balance_report.md](irrigation_flow_water_balance_report.md).

Supporting files:

- `flow_arrival_event_analysis.csv`: event-level arrival and water-balance data
- `flow_arrival_paired_analysis.csv`: matched strip-pair comparisons
- `field_runoff_notes.csv`: management notes relevant to runoff and field-end flow
- `flow_infiltration_results.json`: machine-readable model results and diagnostics
- `zone_capacity_summary.csv`: modeled upper retained-water capacity by strip zone
- `typical_event_summary.csv`: median and middle-50% water accounting by strip

Research figure:

- `zone_capacity_field_layout.png`: nominal 47-ft logger zones shaded by modeled capacity

Regenerate all seven outputs with `python biochar_app/scripts/management/irrigation_analysis/analyze_flow_infiltration.py`. Regenerate the map with `python biochar_app/scripts/management/irrigation_analysis/plot_zone_capacity_field_layout.py`.

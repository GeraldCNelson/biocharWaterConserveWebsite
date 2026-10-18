# Soil-health results inventory, 2026-10-17

This dated inventory records the preservation and initial classification of a
small set of historical soil-health analysis files. The files had been stored
under the ignored `biochar_app/results/` directory, which meant they were easy
to overlook and were not protected by Git history.

**Inventory date:** 2026-10-17

**Analysis period represented:** 2023–2025

**Current location:** `biochar_app/docs/research/soil_health/`

**Publication status:** Preserved research background; not yet approved for the
public Technical Analyses section.

## Preservation decision

All 14 substantive files were retained: one interpretive Markdown report and
13 CSV tables. Together they occupy about 33 KB, so file size is not a reason
to exclude them from Git. The files are potentially useful for future work on
soil health, nutrient needs, fertilizer effects, and treated-versus-control
biochar comparisons.

The tables were placed in `supporting_tables/`. No table was deleted merely
because its name includes `first_pass`, `rounded`, `rebuilt`, or `updated`.
Those names indicate possible relationships among the files, but the original
generation code and a complete provenance record were not found. Retaining the
whole compact family is safer than guessing which version is expendable.

## Known source and method limits

The column names indicate that the analysis combined several kinds of project
data:

- soil chemistry and soil-health measurements;
- biomass and plant-tissue nutrient measurements;
- fertilizer applications;
- strip areas and assumed bulk densities; and
- treatment labels for the four experimental strips.

These inputs are inferred from the tables rather than confirmed by a preserved
generation command. Before the analysis is refreshed or published, the project
should identify the exact canonical input files, document unit conversions and
bulk-density assumptions, and implement a reproducible builder.

The four-strip experiment can support informative paired descriptions, but the
strip comparisons should not be presented as independently replicated causal
effects. Year, irrigation, fertilizer, location, and sampling differences may
also influence the observed contrasts. In addition, some annual tables have
blank fall values for 2025.

## File-by-file classification

| File | Initial classification | Retention reason or follow-up |
|---|---|---|
| [`biochar_microbe_nutrient_summary_2023-2025.md`](biochar_microbe_nutrient_summary_2023-2025.md) | Historical interpretation | Useful synthesis and source of testable hypotheses; causal wording was qualified during preservation. |
| [`nutrient_budget_8in_2023_2025.csv`](supporting_tables/nutrient_budget_8in_2023_2025.csv) | Integrated working table | Combines biomass, uptake, fertilizer, soil stocks, and unexplained balances; assumptions need reconstruction. |
| [`nutrient_flow_2023_2025_annual.csv`](supporting_tables/nutrient_flow_2023_2025_annual.csv) | Annual analysis table | Adds treatment labels, budgets, and use-efficiency measures to annual nutrient flows. |
| [`nutrient_flow_2023_2025_annual_rebuilt.csv`](supporting_tables/nutrient_flow_2023_2025_annual_rebuilt.csv) | Possible intermediate | Appears to be a narrower rebuilt annual table; retained until its relationship to the enriched table is reproduced. |
| [`nutrient_flow_2023_2025_effects.csv`](supporting_tables/nutrient_flow_2023_2025_effects.csv) | Derived contrast table | Contains S1–S2 and S3–S4 differences; useful for checking future paired analyses. |
| [`nutrient_flow_2023_first_pass.csv`](supporting_tables/nutrient_flow_2023_first_pass.csv) | Detailed first-pass table | Preserves biomass, tissue nutrient, uptake, and fertilizer calculations that may help reconstruct the method. |
| [`nutrient_flow_2023_first_pass_rounded.csv`](supporting_tables/nutrient_flow_2023_first_pass_rounded.csv) | Rounded derivative | Likely presentation-oriented version of the first-pass table; retained pending exact comparison. |
| [`nutrient_flow_2023_summary.csv`](supporting_tables/nutrient_flow_2023_summary.csv) | Condensed 2023 table | Smaller selection of biomass, nutrient, uptake, and budget fields. |
| [`nutrient_flow_2023_summary_rounded.csv`](supporting_tables/nutrient_flow_2023_summary_rounded.csv) | Rounded derivative | Likely presentation-oriented version of the 2023 summary; retained pending exact comparison. |
| [`p_turnover_efficiency_2023_2025.csv`](supporting_tables/p_turnover_efficiency_2023_2025.csv) | Exploratory metric table | Contains constructed buffering, access, and turnover metrics whose definitions need scientific and computational review. |
| [`p_turnover_efficiency_pairs_2023_2025.csv`](supporting_tables/p_turnover_efficiency_pairs_2023_2025.csv) | Derived contrast table | Summarizes treated/control ratios for the exploratory phosphorus metrics. |
| [`soil_stock_8in_2023_2025.csv`](supporting_tables/soil_stock_8in_2023_2025.csv) | Derived soil-stock table | Converts phosphorus and potassium measurements to estimated eight-inch stocks using strip-specific bulk density. |
| [`soil_stock_8in_annual_2023_2025.csv`](supporting_tables/soil_stock_8in_annual_2023_2025.csv) | Annual derived table | Pairs spring and fall estimates and computes changes; 2025 fall fields are incomplete. |
| [`soil_stock_8in_updated_2023_2025.csv`](supporting_tables/soil_stock_8in_updated_2023_2025.csv) | Enriched working table | Retains the widest set of laboratory fields and derived stock columns; exact source version and update meaning need documentation. |

## Appropriate future use

This family can seed a reproducible, updated analysis rather than serve as its
final answer. The next scientific pass should:

1. map every field to a canonical laboratory, biomass, fertilizer, or
   experiment-configuration source;
2. reconstruct the calculations in a maintained research script;
3. distinguish measured variables from derived indices and hypotheses;
4. state the limits of paired strip comparisons;
5. extend the data through the latest complete sampling period; and
6. review the narrative and selected tables for a future Technical Analyses
   website section.

Until then, cite these files as a **historical preliminary analysis from
2023–2025**, not as the project's current conclusion about biochar effects.

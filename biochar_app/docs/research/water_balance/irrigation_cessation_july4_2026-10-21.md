# July 4 irrigation-cessation screening analysis

**Analysis date:** 2026-11-15

## Bottom line

- **BC50/CON50 (S1/S2):** On July 4, 2026, the biochar strip contained +0.507 inch relative to its control in the represented profile. If that difference were fully plant-available, it equals +2.2 days of pasture use at 0.23 inch/day. Using each strip's own same-year empirical dry reference, the modeled durations were 9.8 days for biochar and 8.8 days for control.
- **BC100/CON100 (S3/S4):** On July 4, 2026, the biochar strip contained +0.120 inch relative to its control in the represented profile. If that difference were fully plant-available, it equals +0.5 days of pasture use at 0.23 inch/day. Using each strip's own same-year empirical dry reference, the modeled durations were 6.3 days for biochar and 8.9 days for control.

These are **full-water-use days to an empirical dry reference**, not days until the soil is empty, crop death, or a measured wilting point. The calculation assumes no effective rainfall or further irrigation. Actual crop use would decline as water stress develops.

## Question

If irrigation stopped on July 4, how many days of midsummer pasture water use could the monitored soil-water reserve support, and did the biochar strip begin with or retain more water than its paired control?

## Method and assumptions

The three horizontal CS650 sensors represent adjacent six-inch bands spanning approximately 3--21 inches below the surface. At every 15-minute timestamp, the calculation requires valid 6-, 12-, and 18-inch readings at the Top, Middle, and Bottom positions. Position profiles are weighted by their mapped logger influence-zone areas. A daily result requires at least 72 complete observations, or 18 hours of coverage, and uses their median.

For each strip and year, the empirical dry reference is the 5th percentile of eligible daily median profile water from April 1 through the last complete day of the irrigation season. The modeled reserve is July 4 profile water minus that reference, with negative reserves set to zero.

Pasture use is evaluated at 0.20, 0.23, and 0.30 inch/day. The central 0.23-inch/day value corresponds to approximately 7 inches during July. CSU reports this magnitude for pasture near Delta, Colorado; FAO midseason coefficients for managed pasture are broadly consistent with pasture water use near grass-reference evapotranspiration.

Sources: [CSU pasture irrigation guidance](https://tra.extension.colostate.edu/wp-content/uploads/sites/9/2016/02/Pasture-Establishment-Renovation.pdf) and [FAO crop coefficients](https://www.fao.org/4/x0490e/x0490e0b.htm).

## Results by strip and year

| Year | Strip | July 4 observations | July 4 profile water (in) | Empirical dry reference (in) | Reserve above reference (in) | Days at 0.20 in/day | Days at 0.23 in/day | Days at 0.30 in/day |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 2023 | BC50 (S1) | 96 | 5.009 | 3.346 | 1.662 | 8.3 | 7.2 | 5.5 |
| 2023 | CON50 (S2) | 96 | 3.885 | 2.338 | 1.548 | 7.7 | 6.7 | 5.2 |
| 2023 | BC100 (S3) | 47 | -- | -- | -- | -- | -- | -- |
| 2023 | CON100 (S4) | 46 | -- | -- | -- | -- | -- | -- |
| 2024 | BC50 (S1) | 96 | 5.628 | 2.915 | 2.713 | 13.6 | 11.8 | 9.0 |
| 2024 | CON50 (S2) | 96 | 3.288 | 1.963 | 1.325 | 6.6 | 5.8 | 4.4 |
| 2024 | BC100 (S3) | 96 | 4.912 | 3.096 | 1.816 | 9.1 | 7.9 | 6.1 |
| 2024 | CON100 (S4) | 44 | -- | -- | -- | -- | -- | -- |
| 2025 | BC50 (S1) | 96 | 6.048 | 3.348 | 2.700 | 13.5 | 11.7 | 9.0 |
| 2025 | CON50 (S2) | 96 | 5.794 | 2.472 | 3.322 | 16.6 | 14.4 | 11.1 |
| 2025 | BC100 (S3) | 96 | 6.027 | 3.740 | 2.287 | 11.4 | 9.9 | 7.6 |
| 2025 | CON100 (S4) | 96 | 6.213 | 3.678 | 2.535 | 12.7 | 11.0 | 8.5 |
| 2026 | BC50 (S1) | 96 | 5.556 | 3.297 | 2.259 | 11.3 | 9.8 | 7.5 |
| 2026 | CON50 (S2) | 96 | 5.049 | 3.027 | 2.023 | 10.1 | 8.8 | 6.7 |
| 2026 | BC100 (S3) | 96 | 5.184 | 3.729 | 1.455 | 7.3 | 6.3 | 4.8 |
| 2026 | CON100 (S4) | 96 | 5.064 | 3.015 | 2.049 | 10.2 | 8.9 | 6.8 |

A missing result means July 4 had fewer than 72 complete observations across all nine sensors. No nearby date was substituted.

## Paired interpretation at the central rate

| Year | Pair | July 4 biochar minus control water (in) | Equivalent pasture-use days if fully available | Biochar days to own dry reference | Control days to own dry reference | Difference in modeled duration |
|---:|---|---:|---:|---:|---:|---:|
| 2023 | BC50/CON50 (S1/S2) | +1.124 | +4.9 | 7.2 | 6.7 | +0.5 |
| 2023 | BC100/CON100 (S3/S4) | -- | -- | -- | -- | -- |
| 2024 | BC50/CON50 (S1/S2) | +2.340 | +10.2 | 11.8 | 5.8 | +6.0 |
| 2024 | BC100/CON100 (S3/S4) | -- | -- | -- | -- | -- |
| 2025 | BC50/CON50 (S1/S2) | +0.254 | +1.1 | 11.7 | 14.4 | -2.7 |
| 2025 | BC100/CON100 (S3/S4) | -0.186 | -0.8 | 9.9 | 11.0 | -1.1 |
| 2026 | BC50/CON50 (S1/S2) | +0.507 | +2.2 | 9.8 | 8.8 | +1.0 |
| 2026 | BC100/CON100 (S3/S4) | +0.120 | +0.5 | 6.3 | 8.9 | -2.6 |

The two paired measures answer different questions. The July 4 water difference asks whether one strip started with more monitored water. The duration comparison asks how long each strip would take to reach its own observed low-storage reference. They can point in different directions because the empirical reference also differs among strips.

## Limitations

- The represented profile spans approximately 3--21 inches. It is not a complete pasture root zone, and it omits water above and below that interval while extrapolating local sensor readings across mapped field zones.
- The empirical dry reference is an observed lower-envelope statistic, not a laboratory-derived permanent wilting point or a crop-specific stress threshold.
- Absolute VWC differences among sensors can reflect calibration, installation contact, soil texture, and local field conditions as well as real water storage.
- The constant-use scenarios omit rainfall, irrigation, drainage, upward flow, and reduced evapotranspiration under stress.
- Cutting or grazing changes canopy area and crop coefficients. July 4 does not represent the same regrowth stage in every year.
- There is one strip per treatment within each irrigation regime, so the paired differences are descriptive rather than independently replicated treatment effects.

## Reproducibility

Run from the repository root:

```bash
.biochar_py313/bin/python -m biochar_app.scripts.management.irrigation_analysis.analyze_irrigation_cessation
```

Supporting tables:

- [`irrigation_cessation_july4_strip_summary_2026-10-21.csv`](supporting_tables/irrigation_cessation_july4_strip_summary_2026-10-21.csv)
- [`irrigation_cessation_july4_pair_summary_2026-10-21.csv`](supporting_tables/irrigation_cessation_july4_pair_summary_2026-10-21.csv)

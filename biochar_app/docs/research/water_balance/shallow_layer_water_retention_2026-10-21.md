# Shallow-layer water retention in biochar and control strips

**Analysis date:** 2026-11-15

**Logger data included through:** 2026-10-24 00:30:00

## Bottom line

- **BC50/CON50 (S1/S2):** Across matched irrigation events, the biochar strip averaged +0.253 inch (+4.2 VWC percentage points) relative to its control. The 2026 matched-event mean was +0.384 inch (+6.4 VWC percentage points). Across eligible days in the 2026 irrigation season, the daily differences totaled +95.4 water-inch days.
- **BC100/CON100 (S3/S4):** Across matched irrigation events, the biochar strip averaged +0.193 inch (+3.2 VWC percentage points) relative to its control. The 2026 matched-event mean was -0.026 inch (-0.4 VWC percentage points). Across eligible days in the 2026 irrigation season, the daily differences totaled -1.3 water-inch days.

A **water-inch day** is analogous to a degree-day: it combines magnitude and duration. For example, a 0.5-inch difference sustained for 10 eligible days equals 5 water-inch days. It does not mean that 5 inches of water were saved or applied, and it does not by itself establish that the additional water was plant-available or used by the crop.

The pooled averages are descriptive, not stand-alone treatment estimates. The S1/S2 difference became positive after 2023 and remained positive through 2026. The S3/S4 difference declined from a large positive value in 2023 to approximately zero in 2025--2026. Thus the current record supports a persistent shallow-water difference for S1 versus S2, but not a stable shallow-water advantage for S3 versus S4.

The complementary April--October calculation is reported separately for each year below. Daily differences and water-inch days are not summed across years.

## Question and measurement boundary

This report asks how much more or less water was present in the shallow sensor-represented soil band in each biochar strip than in its paired non-biochar strip. BC50 (S1) is compared with CON50 (S2) under the reduced-irrigation, approximately monthly regime. BC100 (S3) is compared with CON100 (S4) under the full-irrigation, approximately biweekly regime.

The nominal 6-inch CS650 sensor represents an approximately six-inch-thick band centered at the sensor, or roughly 3--9 inches below the surface. It overlaps the biochar-injected layer but is not a direct measurement of only the 0--6-inch injected soil. The report therefore uses **shallow sensor band**, not **0--6-inch layer**.

## Method

For each strip and timestamp, Top, Middle, and Bottom shallow VWC readings were converted to equivalent inches of water and weighted by their mapped logger influence-zone areas. All three positions had to be valid. Irrigation intervals were paired within S1/S2 and S3/S4. Positive differences mean the biochar strip contained more water. One VWC percentage point across a six-inch band equals 0.06 inch of water.

The 24--36-hour median is the primary redistributed post-irrigation reference. Later checkpoints describe persistence during drying. Descriptive bootstrap intervals resample repeated events within years. They describe these particular strip pairs; they do not create independent field replication.

### Data sources and eligibility

The analysis reads the maintained annual raw-logger parquet files, `biochar_app/data-processed/management/irrigation/irrigation_clean.csv`, and the processed 15-minute CoAgMet precipitation files. It performs no network retrieval. An interval must have a subsequent irrigation in the same calendar year, last no more than 45 days, and provide a matched biochar/control event. Each reported strip value also requires valid shallow readings at all three logger positions. The interval-mean metric additionally requires at least 75% complete temporal coverage in each strip.

### Coverage

- **BC50/CON50 (S1/S2):** 23 matched irrigation intervals; 23 had a complete 24--36-hour reference value, and 23 met the paired 75% coverage requirement for the interval mean.
- **BC100/CON100 (S3/S4):** 41 matched irrigation intervals; 34 had a complete 24--36-hour reference value, and 24 met the paired 75% coverage requirement for the interval mean.

Counts vary among later checkpoints because a checkpoint must occur before the next irrigation and must have complete Top, Middle, and Bottom shallow readings in both strips.

## Irrigation-season daily results

This complementary calculation uses every calendar day from April 1 through October 31 for which both strips have at least 72 simultaneous valid 15-minute observations—at least 18 of the day's 24 hours. It first averages the simultaneous biochar-minus-control observations within each day. The seasonal mean gives the average difference on eligible days.

**Water-inch days** are the signed sum of those daily mean water-depth differences. For example, maintaining one extra inch for ten complete days equals ten water-inch days. This is a time-integrated storage difference, not ten inches of irrigation water saved or delivered. Missing days are omitted rather than extrapolated.

### Results by irrigation season

| Pair | Year | Eligible days / source-available days | Mean daily difference (VWC percentage points) | Mean daily difference (in) | Approx. gal/acre present on an average eligible day | Water-inch days | Biochar higher days |
|---|---:|---:|---:|---:|---:|---:|---:|
| BC50/CON50 (S1/S2) | 2023 | 150/214 | +4.9 | +0.291 | +7,903 | +43.7 | 89% |
| BC50/CON50 (S1/S2) | 2024 | 214/214 | +9.7 | +0.582 | +15,783 | +124.5 | 100% |
| BC50/CON50 (S1/S2) | 2025 | 214/214 | +6.9 | +0.417 | +11,312 | +89.2 | 96% |
| BC50/CON50 (S1/S2) | 2026 | 206/206 | +7.7 | +0.463 | +12,571 | +95.4 | 99% |
| BC100/CON100 (S3/S4) | 2023 | 0/214 | -- | -- | -- | -- | -- |
| BC100/CON100 (S3/S4) | 2024 | 126/214 | +3.0 | +0.182 | +4,931 | +22.9 | 87% |
| BC100/CON100 (S3/S4) | 2025 | 214/214 | +0.9 | +0.055 | +1,500 | +11.8 | 69% |
| BC100/CON100 (S3/S4) | 2026 | 143/206 | -0.2 | -0.009 | -248 | -1.3 | 62% |

The 2026 source ends at 2026-10-24 00:30:00, so 2026-10-23 is the last complete day in the daily calculation. The 2023 BC100/CON100 record has no days meeting the 75% paired-coverage requirement and is not extrapolated.

## Main matched-event results

| Pair | Irrigation regime | Measurement | Events | Difference (VWC percentage points) | Biochar minus control (in) | Approx. gal/acre | Descriptive bootstrap 95% interval (in) | Biochar higher |
|---|---|---|---:|---:|---:|---:|---:|---:|
| BC50/CON50 (S1/S2) | reduced irrigation (monthly) | Redistributed water, 24--36 h median | 23 | +4.2 | +0.253 | +6,858 | +0.218 to +0.290 | 83% |
| BC50/CON50 (S1/S2) | reduced irrigation (monthly) | Water at 72 h | 23 | +5.6 | +0.339 | +9,199 | +0.300 to +0.375 | 96% |
| BC50/CON50 (S1/S2) | reduced irrigation (monthly) | Water at 7 d | 22 | +9.1 | +0.548 | +14,869 | +0.490 to +0.603 | 100% |
| BC50/CON50 (S1/S2) | reduced irrigation (monthly) | Water before next irrigation | 23 | +8.4 | +0.503 | +13,637 | +0.431 to +0.569 | 100% |
| BC50/CON50 (S1/S2) | reduced irrigation (monthly) | Mean water over irrigation interval | 23 | +7.9 | +0.473 | +12,835 | +0.434 to +0.504 | 96% |
| BC100/CON100 (S3/S4) | full irrigation (biweekly) | Redistributed water, 24--36 h median | 34 | +3.2 | +0.193 | +5,231 | +0.126 to +0.264 | 62% |
| BC100/CON100 (S3/S4) | full irrigation (biweekly) | Water at 72 h | 35 | +3.2 | +0.192 | +5,209 | +0.139 to +0.252 | 77% |
| BC100/CON100 (S3/S4) | full irrigation (biweekly) | Water at 7 d | 33 | +3.2 | +0.191 | +5,196 | +0.139 to +0.245 | 79% |
| BC100/CON100 (S3/S4) | full irrigation (biweekly) | Water before next irrigation | 28 | +0.2 | +0.014 | +376 | -0.052 to +0.076 | 57% |
| BC100/CON100 (S3/S4) | full irrigation (biweekly) | Mean water over irrigation interval | 24 | +1.0 | +0.063 | +1,697 | +0.018 to +0.108 | 79% |

## Interpretation

- **BC50/CON50 (S1/S2):** the pooled redistributed shallow-band difference was +0.253 inch (+6,858 gal/acre). Most repeated matched events favored the biochar strip.
- **BC100/CON100 (S3/S4):** the pooled redistributed shallow-band difference was +0.193 inch (+5,231 gal/acre). The pooled mean was positive, but individual event directions were mixed.

## Rain-free sensitivity

The sensitivity subset retains checkpoints with no more than 0.01 inch of accumulated rain after irrigation. It should be checked alongside the all-event estimates because rainfall can add water independently of the irrigation treatment.

| Pair | Measurement | Rain-free events | Biochar minus control (in) | Approx. gal/acre |
|---|---|---:|---:|---:|
| BC50/CON50 (S1/S2) | Redistributed water, 24--36 h median | 22 | +0.263 | +7,124 |
| BC50/CON50 (S1/S2) | Water at 7 d | 9 | +0.519 | +14,092 |
| BC100/CON100 (S3/S4) | Redistributed water, 24--36 h median | 28 | +0.152 | +4,135 |
| BC100/CON100 (S3/S4) | Water at 7 d | 19 | +0.174 | +4,725 |

## Year-to-year stability

The year summary should be treated as a required diagnostic, not an optional detail. A treatment interpretation is weak when its direction changes among years, even if a pooled repeated-event interval excludes zero.

| Pair | Year | Events | 24--36 h difference (in) | 72 h difference (in) | Pre-next difference (in) |
|---|---:|---:|---:|---:|---:|
| BC50/CON50 (S1/S2) | 2023 | 6 | -0.054 | +0.083 | +0.606 |
| BC50/CON50 (S1/S2) | 2024 | 6 | +0.416 | +0.499 | +0.553 |
| BC50/CON50 (S1/S2) | 2025 | 6 | +0.286 | +0.341 | +0.392 |
| BC50/CON50 (S1/S2) | 2026 | 5 | +0.384 | +0.452 | +0.451 |
| BC100/CON100 (S3/S4) | 2023 | 6 | +0.750 | +0.594 | +0.222 |
| BC100/CON100 (S3/S4) | 2024 | 9 | +0.228 | +0.204 | +0.119 |
| BC100/CON100 (S3/S4) | 2025 | 10 | +0.024 | +0.056 | -0.041 |
| BC100/CON100 (S3/S4) | 2026 | 9 | -0.026 | +0.018 | -0.041 |

## Limitations

- There is one biochar strip and one control strip per irrigation regime. Logger positions and repeated irrigation events are subsamples, not independent treatment replicates.
- Because treatment is confounded with strip identity, an absolute S1/S2 or S3/S4 water difference cannot be attributed entirely to biochar. Pre-existing soil, topographic, hydraulic, or sensor differences may contribute.
- The shallow sensor band overlaps but does not exactly equal the biochar-injected layer.
- The analysis estimates water present, not plant-available water and not water physically located inside biochar pores.
- This analysis measures absolute water present after irrigation. It is not the separate event-storage-gain quantity (post-irrigation water minus the strip's own pre-irrigation baseline).
- Spatial weighting assumes each logger represents its mapped Top, Middle, or Bottom influence zone.
- Intervals ending after the last recorded irrigation cannot be evaluated because a next-irrigation boundary is required.

## Reproducibility

Run from the repository root:

```bash
.biochar_py313/bin/python -m biochar_app.scripts.management.irrigation_analysis.analyze_shallow_layer_retention
```

Supporting tables:

Related analysis:

- [`irrigation_cessation_july4_2026-10-21.md`](irrigation_cessation_july4_2026-10-21.md)

- [`shallow_layer_pair_summary_2026-10-21.csv`](supporting_tables/shallow_layer_pair_summary_2026-10-21.csv)
- [`shallow_layer_year_summary_2026-10-21.csv`](supporting_tables/shallow_layer_year_summary_2026-10-21.csv)
- [`shallow_layer_matched_events_2026-10-21.csv`](supporting_tables/shallow_layer_matched_events_2026-10-21.csv)
- [`shallow_layer_irrigation_season_summary_2026-10-21.csv`](supporting_tables/shallow_layer_irrigation_season_summary_2026-10-21.csv)
- [`shallow_layer_daily_pair_differences_2026-10-21.csv`](supporting_tables/shallow_layer_daily_pair_differences_2026-10-21.csv)

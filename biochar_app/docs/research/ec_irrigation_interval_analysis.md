---
output:
  word_document: default
  html_document: default
---
# Six-inch EC behavior between irrigation events

## Bottom line

This analysis separates the moisture, temperature, and paired-strip components
of the six-inch EC signal. It uses 67 complete
inter-irrigation intervals and 6,495 strip-position daily records from
2023-05-24 through
2026-09-28.

The results remain paired field contrasts, not replicated treatment effects.
S1/S2 estimates the observed biochar-control contrast under the monthly regime;
S3/S4 estimates it under the approximately biweekly regime. Each treatment
combination is represented by one physical strip, so biochar remains confounded
with persistent strip, soil, and field-position differences.

## What the results suggest

The median temperature-corrected bulk EC declines within every pair-position-
treatment cell during the drydown, ranging from -7.3% to
-2.6% per day. VWC declines at the same time. This is strong
evidence that the repeated rise-and-fall pattern is dominated by soil-water
content and conductive-path continuity.

Temperature normalization changes the six overall treatment ratios by at most
1.8%, so temperature is not the explanation for the
large pair and position differences. VWC modeling matters much more:

| pair | position | measured_bulk_ratio | ec25_ratio | vwc_modeled_ratio |
| --- | --- | --- | --- | --- |
| S1/S2 | Bottom | 4.797 | 4.733 | 2.403 |
| S1/S2 | Middle | 0.999 | 1.006 | 0.889 |
| S1/S2 | Top | 1.686 | 1.704 | 0.857 |
| S3/S4 | Bottom | 0.427 | 0.434 | 0.460 |
| S3/S4 | Middle | 1.146 | 1.146 | 1.004 |
| S3/S4 | Top | 1.642 | 1.619 | 1.134 |

The adjustment does not reveal a uniform biochar signal. In S1/S2, the Top
contrast reverses from higher measured bulk EC in S1 to a lower VWC-modeled
index, while the Bottom contrast remains strongly above 1. In S3/S4, the
Bottom contrast remains strongly below 1, Middle moves close to 1, and Top is
still moderately above 1 under the empirical model. The opposite Bottom
directions are especially difficult to reconcile with one common soluble-salt
effect of biochar.

The VWC-normalized index sometimes rises and sometimes falls through a drydown.
That mixed behavior is not evidence for general salt precipitation. It is
consistent with a combination of concentration, redistribution, sensor/soil
differences, and an imperfect VWC-only conductivity model.

## What was calculated

1. Fifteen-minute six-inch EC, VWC, and temperature measurements were matched
   within sensor, strip, and logger position.
2. Measured bulk EC was normalized to 25 C before daily aggregation using
   `EC25 = EC / (1 + 0.02 × (soil temperature C - 25))`.
3. Irrigation dates themselves were excluded because a daily median on those
   dates mixes pre-irrigation and post-irrigation conditions.
4. EC25 was plotted against VWC separately within each complete interval.
5. A common moisture exponent was estimated within each pair and logger
   position after removing interval-by-strip means. The apparent pore-EC index
   is `EC25 / theta^m`, where `theta = VWC / 100`.
6. Prespecified `m = 1.0`, `1.5`, and `2.0` results are retained as sensitivity
   checks. Treatment ratios give each irrigation interval equal weight.

The processed logger data do **not** contain dielectric permittivity. Therefore
the Hilhorst method cannot be applied, and the apparent pore-EC index must not be
called measured pore-water EC, saturation-extract EC, or salinity. It omits a
soil solid/surface-conductivity term and an independently calibrated tortuosity
function. Biochar could itself affect those terms.

## Empirical VWC exponents

| pair | position | intervals | days | estimate_95_interval |
| --- | --- | --- | --- | --- |
| S1/S2 | Bottom | 24 | 1090 | 1.15 (1.07–1.23) |
| S1/S2 | Middle | 24 | 1090 | 1.26 (1.18–1.34) |
| S1/S2 | Top | 24 | 1090 | 1.13 (1.07–1.20) |
| S3/S4 | Bottom | 43 | 976 | 1.02 (0.92–1.11) |
| S3/S4 | Middle | 43 | 1098 | 0.88 (0.82–0.93) |
| S3/S4 | Top | 43 | 1151 | 1.04 (0.99–1.10) |

The empirical exponent is a descriptive drydown relationship, not a universal
soil constant. Differences between the biochar and control exponent estimates
are available in `moisture_exponents.csv` and are one way biochar-related pore
structure or persistent strip differences could enter the measurement.

## Paired apparent pore-EC contrasts

| pair | position | intervals | ratio_95_interval |
| --- | --- | --- | --- |
| S1/S2 | Bottom | 24 | 2.40 (2.17–2.69) |
| S1/S2 | Middle | 24 | 0.89 (0.84–0.94) |
| S1/S2 | Top | 24 | 0.86 (0.81–0.91) |
| S3/S4 | Bottom | 35 | 0.46 (0.43–0.50) |
| S3/S4 | Middle | 38 | 1.00 (0.97–1.05) |
| S3/S4 | Top | 42 | 1.13 (1.09–1.19) |

A ratio above 1 means that the VWC-normalized index is higher in the biochar
strip than in its paired control. It does not by itself identify more total
salt, a biochar chemical effect, or an agronomic benefit. Compare these values
with `pair_ratio_summary.csv` to see whether conclusions change under the three
prespecified moisture exponents.

## Change during drydown

| pair | position | treatment | intervals | median_percent_change_per_day |
| --- | --- | --- | --- | --- |
| S1/S2 | Bottom | biochar | 24 | -1.418 |
| S1/S2 | Bottom | control | 24 | 0.343 |
| S1/S2 | Middle | biochar | 24 | -0.983 |
| S1/S2 | Middle | control | 24 | 1.604 |
| S1/S2 | Top | biochar | 24 | -0.120 |
| S1/S2 | Top | control | 24 | 0.143 |
| S3/S4 | Bottom | biochar | 42 | 1.340 |
| S3/S4 | Bottom | control | 32 | -1.041 |
| S3/S4 | Middle | biochar | 38 | 0.701 |
| S3/S4 | Middle | control | 43 | 0.145 |
| S3/S4 | Top | biochar | 42 | 0.064 |
| S3/S4 | Top | control | 43 | 0.909 |

If measured bulk EC declines while this index rises, the pattern is consistent
with the remaining pore water becoming more concentrated while bulk conduction
paths disappear. If both decline, salt removal, redistribution, adsorption, or
precipitation becomes more plausible, but cannot be distinguished here.

## Figures

- `ec25_vs_vwc_by_pair_position.png` shows every complete interval as a faint
  trajectory, separated by treatment pair and logger position.
- `apparent_pore_ec_drydown.png` shows the moisture-normalized index relative to
  days 1–2 of each interval.
- `biochar_control_ec_ratio_sensitivity.png` compares measured EC, EC25, three
  prespecified VWC models, and the empirical VWC model.

## Outputs and reproducibility

- `irrigation_intervals.csv`: event boundaries used in the analysis.
- `six_in_daily_drydowns.csv`: daily sensor-level analysis data.
- `six_in_daily_pair_contrasts.csv`: date-matched biochar/control comparisons.
- `moisture_exponents.csv`: pooled and treatment-specific drydown exponents.
- `pair_ratio_summary.csv`: all-years interval-equal treatment contrasts.
- `pair_ratio_by_year.csv`: year-specific treatment contrasts.
- `drydown_interval_trends.csv` and `drydown_trend_summary.csv`: interval and
  aggregate trends with time since irrigation.

Regenerate with:

```bash
.biochar_py313/bin/python biochar_app/scripts/research/analyze_ec_irrigation_intervals.py
```

# CS650 bulk-EC temperature-normalization assessment

## Recommendation

Do not replace, relabel, or rewrite the measured CS650 electrical-conductivity
series. If EC25 is offered, offer it only as a separate, optional research
series named **“Bulk EC normalized to 25 °C (generic manufacturer
sensitivity)”**. Keep measured bulk EC as the default.

For an eventual research/advanced display or download:

1. use EC and temperature from the same CS650 sensor, depth, strip, logger
   position, and timestamp;
2. convert the processed soil temperature from °F back to °C;
3. correct each observation before any monthly or seasonal aggregation;
4. require finite EC and temperature and a finite, positive denominator;
5. leave EC25 missing at or below 0 °C rather than extrapolating through frozen
   conditions;
6. preserve measured EC and EC25 as separately labeled fields; and
7. calculate pair comparisons as ratios of means over the same simultaneous
   valid observations in both strips.

This is appropriate as a sensitivity view, not as a validated site-specific
calibration. Do not describe EC25 as pore-water EC, extract EC, salinity, or a
biochar benefit score. Bulk EC remains moisture dependent after temperature
normalization.

## Manufacturer basis

Campbell Scientific describes the CS650 result as bulk electrical
conductivity and gives the following generic conversion:

\[
EC_{25}=\frac{EC_T}{1+0.02(T_{soil,C}-25)}.
\]

The manufacturer also says that the generic correction worked in coarse sand,
should be used cautiously in other soil types, and is preferably replaced by a
soil-specific correction. The CS650/CS655 firmware does not apply temperature
correction automatically. The temperature sensor is in the epoxy sensor head,
so installation orientation and burial affect how closely it represents soil
temperature.

Primary sources:

- [Temperature correction of soil electrical conductivity](https://help.campbellsci.com/cs650-cs655/cs650-cs655/operation/temp-correction-soil-electric-conduct.htm)
- [Separate temperature dependence and correction guidance for volumetric water content](https://help.campbellsci.com/cs650-cs655/cs650-cs655/operation/temp-dependence-correction.htm)
- [Operational specifications](https://help.campbellsci.com/cs650-cs655/cs650-cs655/specifications/operational-specifications.htm)

The EC equation above is not the VWC temperature-correction equation. The two
must not be conflated.

## What the soil information can and cannot change

The Campbell guidance does not provide a coefficient lookup by texture, a
different equation for loam or sandy loam, or a method for calculating the
temperature coefficient from routine soil chemistry. It says the generic
2%/°C correction worked in coarse sand, should be used cautiously in other
soils, and is preferably replaced by a soil-specific correction. Consequently,
the available soil information can tell us that the generic result is a
sensitivity calculation; it cannot defensibly supply a replacement coefficient.

The June 2023 NRCS report maps the 2.2-acre area of interest as:

- 93.2% Sagrlite loam. Its typical surface profile is loam from 0 to 13 inches,
  underlain by silt loam.
- 6.8% Glenton very fine sandy loam. Its typical surface profile is very fine
  sandy loam from 0 to 14 inches, underlain by sandy loam.

That context is directly relevant to the 6-inch sensors and does not match the
coarse-sand medium in which Campbell says the generic correction worked. The
NRCS salinity ranges are map-unit interpretations, not measurements at the
individual loggers. Likewise, the 28 Ward samples have 1:1 laboratory EC from
0.07 to 1.81 dS/m, but laboratory extract EC is not the CS650's in-situ bulk EC
and does not determine a temperature coefficient.

A defensible site coefficient would require an empirical calibration using the
same soil and sensors over controlled temperature changes while holding water
content and soluble-ion conditions stable, repeated over the relevant moisture
range. The resulting coefficient could then replace 0.02 in the same algebraic
form if the data support a linear response. That is a separate calibration
experiment, not a calculation that can be made from the existing soil tests
alone.

## Logger and ETL inspection

### Logger-side processing

The retained logger programs under
`biochar_app/data-raw/datfiles_2026/sys/bin/` read the CS650/CS655 response,
alias the sensor-reported EC and temperature values, and average them into the
15-minute table. No retained program contains an EC25 equation or another EC
temperature correction.

The status tables identify the archived program name and signature at all 12
stations through September 13, 2026. Ten stations report
`CSU_3depths.CR2`; S2T and S3T report `CSU_3depths_M3.CR2`. All retained
variants store the sensor-reported EC and temperature separately without EC25
normalization. Because the status archive ends before the latest processed
observations, this is strong but not absolute evidence that no program changed
between September 13 and October 7, 2026.

### ETL processing and units

Raw TOA5 headers identify EC as dS/m and soil temperature as degrees Celsius.
The ETL renames the fields and then explicitly converts every
`T_<depth>_raw_<strip>_<position>` value from °C to °F before writing canonical
parquets. It does not temperature-correct EC. Therefore, applying Campbell's
equation directly to processed `T_*_raw_*` values without first converting
them back to °C would be wrong.

The source schemas were verified for 2023-2026. Each 15-minute parquet has 36
EC and 36 co-located temperature columns covering three depths, four strips,
and three logger positions. The 2026 source ends at October 7, 2026 00:45.

## Assessment design

The assessment uses the canonical files under
`biochar_app/data-processed/parquet/summary/15min/`. For every physical sensor:

- measured EC is paired with its simultaneous processed soil temperature;
- temperature is converted from °F to °C;
- the denominator and correction factor are calculated explicitly;
- `EC25_sensitivity_*` retains all formula-valid observations so freezing
  influence can be audited; and
- `EC25_nonfreezing_*` requires temperature above 0 °C and is the primary
  assessment series.

Season labels follow the repository convention within each calendar-year
file: April-October is `growing_apr_oct`, and January-March plus
November-December is `winter_nov_mar`. Consequently, 2023 and 2026 seasonal
coverage is partial at the dataset boundaries.

S1/S2 and S3/S4 comparisons use S1 and S3 as treated strips. Each measured and
corrected ratio of means uses an identical simultaneous observation mask in
the treated and control strip. The supplemental instantaneous-ratio file is
kept separate and is not used for the principal comparison.

## Focused result: April-October at 6 inches

The attached dashboard chart uses the **mean of individual simultaneous
15-minute treated/control ratios** for EC. It does not use the ratio of the two
seasonal means. The focused before/after figure deliberately reproduces that
same estimator so the corrected panel is directly comparable to the supplied
plot. The ratio-of-means results remain the principal cross-check because a
mean of individual ratios can be sensitive to small values in the denominator.

Across the 24 year-pair-position bars in the attached-chart metric:

- the median absolute change from generic EC25 normalization is 1.20%;
- the largest absolute change is 3.96%, for 2023 S3/S4 Middle;
- no bar changes direction relative to parity (1.0); and
- the strong Bottom-position pattern remains.

The Bottom results are:

| Year | Pair | Measured ratio | Generic EC25 ratio | Change |
|---|---|---:|---:|---:|
| 2023 | S1/S2 | 4.833 | 4.750 | -1.72% |
| 2023 | S3/S4 | 1.366 | 1.411 | +3.26% |
| 2024 | S1/S2 | 9.988 | 9.741 | -2.48% |
| 2024 | S3/S4 | 0.468 | 0.475 | +1.53% |
| 2025 | S1/S2 | 3.580 | 3.576 | -0.12% |
| 2025 | S3/S4 | 0.321 | 0.324 | +0.86% |
| 2026 | S1/S2 | 3.098 | 3.118 | +0.67% |
| 2026 | S3/S4 | 0.302 | 0.309 | +2.35% |

Thus, the generic temperature adjustment does not restore the descriptive
expectation of ratios near 1. For 2024-2026 the Bottom S1/S2 ratios are well
above 1 while Bottom S3/S4 ratios are well below 1. Those opposite directions
also do not resemble a single, uniform soluble-material effect of biochar.
They point instead to position/pair-specific effects or confounding that a
temperature-only correction cannot resolve. Because bulk EC remains strongly
moisture dependent, treatment-related water-content differences are one of the
next variables to evaluate.

This is a descriptive sensitivity result, not a formal hypothesis test. A
confirmatory test still needs a prespecified practical-equivalence margin and
an uncertainty method that respects temporal autocorrelation, repeated
measurements, logger position, year, and treatment-pair structure.

## Coverage and formula diagnostics

| Year | Source sensor-rows | Formula-valid | Valid coverage | Frozen among valid | Missing EC or temperature | Temperature range (°C) |
|---|---:|---:|---:|---:|---:|---:|
| 2023 | 759,708 | 590,315 | 77.70% | 0.87% | 169,393 | -2.18 to 43.31 |
| 2024 | 1,264,896 | 1,210,867 | 95.73% | 1.11% | 54,029 | -1.81 to 33.83 |
| 2025 | 1,260,468 | 1,259,217 | 99.90% | 2.28% | 1,251 | -3.59 to 31.90 |
| 2026 | 964,224 | 953,852 | 98.92% | 0.11% | 10,372 | -0.43 to 37.20 |

Across all years there are 4,014,251 formula-valid sensor-observations and
3,965,940 nonfreezing observations. No finite paired observation had a
nonpositive formula denominator. There are 48,311 formula-valid observations
at or below freezing; they are retained only in the all-temperature
sensitivity fields.

Among nonfreezing observations, the EC25/measured-EC factor has median 1.223,
5th percentile 0.985, and 95th percentile 1.881. The observed range is 0.732
to just under 2.0. Including frozen observations increases the maximum factor
to 2.335.

## Absolute EC effect

The correction changes absolute seasonal means substantially in cool soil:

| Season | Sensor-season cells | Median change in mean EC | Range |
|---|---:|---:|---:|
| April-October | 144 | +10.99% | +3.08% to +17.62% |
| November-March | 141 | +66.67% | +57.92% to +81.90% |

This large cool-season change is expected from normalization to 25 °C, but it
also increases the risk that users interpret a generic sensitivity result as
a directly measured or site-calibrated value.

## Paired-strip conclusions

The effect on paired ratios is much smaller because treated and control
sensors usually have similar seasonal temperatures. Across the 141 available
nonfreezing year-season-pair-depth-position cells:

- median absolute change in the ratio of means is 0.96%;
- the 90th percentile absolute change is 2.47%;
- the 95th percentile absolute change is 2.69%;
- 140 of 141 changes are within 5%; and
- the maximum change is +6.45% for S3/S4, depth 1, Middle, during the 2023
  April-October period (1.717 measured to 1.828 EC25).

Seasonal treated-minus-control mean temperatures range from -1.80 to +0.96 °C.
The measured seasonal ratios themselves range from 0.086 to 9.256, far wider
than the temperature-normalization effect.

Two cells cross the exact value 1.0, but both remain inside a descriptive ±5%
parity band:

| Cell | Measured ratio | EC25 ratio |
|---|---:|---:|
| S1/S2, 2025 winter, depth 1 Middle | 1.024 | 0.998 |
| S3/S4, 2026 growing, depth 1 Middle | 1.003 | 0.993 |

Only one cell changes the descriptive ±5% band: S3/S4, 2025 winter, depth 2
Middle moves from 0.953 to 0.948. This is a threshold crossing, not a
statistical significance result.

Including frozen observations changes some seasonal EC25 ratios materially:
the largest all-temperature versus nonfreezing difference is 9.87%. This
supports leaving EC25 missing at or below 0 °C in any user-facing product.

Overall, temperature normalization does not resolve the strong and sometimes
opposite position/depth patterns already present in measured EC. It should not
be used to turn those patterns into a causal biochar claim.

## Relationship to the existing adjusted-ratio analysis

`biochar_app/scripts/research/analyze_ec_adjustment.py` fits a robust model to
daily 6-inch log EC ratios while controlling for moisture, temperature,
irrigation, antecedent moisture, year, pair, and position. It does not derive
EC25 and should not be described as temperature normalization.

There is also a unit issue to audit separately before relying on that model's
temperature-specific numerical statements: it reads processed `T_*_raw_*`
values, which are °F, but labels and screens them as °C. In particular, its
`between(-20, 60)` check is applied to °F values. This assessment does not
modify that script or its already-dirty report.

## Before a production offer

Before promoting EC25 beyond a research/advanced sensitivity view:

1. evaluate a site- and soil-specific temperature response across relevant
   moisture states in treated and control soil;
2. confirm sensor-head burial and orientation, because the reported
   temperature is measured in the head rather than averaged along the rods;
3. retain a visible missing-value policy for frozen or invalid observations;
4. show measurement coverage and the applied correction factor; and
5. avoid comparison with laboratory extract salinity thresholds unless a
   separate validated relationship is developed.

No deployment or production-product change is recommended from this
assessment alone.

## Reproducibility and outputs

Run with the repository Python environment:

```bash
.biochar_py313/bin/python \
  biochar_app/scripts/research/analyze_ec25_temperature_sensitivity.py
```

The script writes only to
`biochar_app/data-processed/research/ec25_temperature_sensitivity/` and does
not alter canonical parquets. Outputs include:

- `series/<year>_ec25_research_15min.parquet`: separate 15-minute temperature,
  factor, all-temperature sensitivity, and nonfreezing EC25 series;
- `sensor_year_coverage.csv`: missing, invalid-denominator, freezing, and
  temperature-range diagnostics;
- `sensor_seasonal_means.csv` and `sensor_monthly_means.csv`;
- `pair_seasonal_ratios_of_means.csv` and
  `pair_monthly_ratios_of_means.csv`;
- `instantaneous_ratio_summary_supplemental.csv`;
- `growing_season_6in_attached_metric_temperature_comparison.csv` and
  `growing_season_6in_measured_vs_ec25.png`, which directly reproduce the
  estimator used in the supplied April-October 6-inch chart;
- `assessment_metadata.json`, including source-file hashes and method details;
  and
- four PNG summaries of temporal patterns, correction factors, and seasonal
  pair-ratio changes.

The script was independently spot-checked against the source 2026 parquet.
Temperature conversion, factor, and EC25 differences were below floating-point
roundoff; the nonfreezing missing-value mask had zero mismatches; and a
recalculated 2026 S1/S2 growing-season depth-3 Top pair matched the exported
observation count and ratios.

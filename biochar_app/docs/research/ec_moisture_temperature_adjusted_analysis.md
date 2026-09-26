# Logger EC adjusted for water content and temperature

## Question

Do the logger electrical-conductivity (EC) contrasts between each biochar strip and its paired non-biochar strip persist after accounting for simultaneous volumetric water content (VWC) and soil temperature, and do those adjusted contrasts agree with laboratory ion measurements?

The treatment ratios are S1/S2 and S3/S4. A ratio above 1 means the logger or laboratory value was higher in the biochar strip; a ratio below 1 means it was lower.

## Data and method

- Logger data: 15-minute EC, VWC, and temperature observations during April–October of 2023–2026.
- Pairing: simultaneous observations within each year, strip pair, logger position (Top, Middle, Bottom), and sensor depth (6, 12, 18 inches).
- To reduce serial repetition and isolated spikes, simultaneous records were summarized to hourly medians.
- The response was the logarithm of the paired EC ratio. Robust regressions accounted for paired VWC and temperature differences and for nonlinear mean moisture and temperature conditions. The adjusted ratio is the model-estimated contrast when the paired strips have the same VWC and temperature.
- Laboratory data: one composite sample per strip and sampling date, at 0–8 or 0–12 inches. The comparison uses the laboratory EC, sulfate-S, sodium, potassium, nitrate-N, and Olsen phosphorus ratios.

This is an observational adjustment, not a randomized estimate of a biochar effect. In particular, the laboratory samples do not identify Top/Middle/Bottom locations or match the three logger depths.

## Moisture- and temperature-adjusted logger EC

The table combines all three sensor depths. Entries are the median adjusted treatment ratio across the 12 year-by-position strata, followed by the interquartile range. The final column is the unadjusted geometric-median ratio for comparison.

| Strip pair | Depth | Adjusted EC ratio | Interquartile range | Unadjusted ratio |
|---|---:|---:|---:|---:|
| S1/S2 | 6 in | 1.115 | 0.911–1.542 | 1.737 |
| S1/S2 | 12 in | 1.117 | 0.999–1.864 | 1.155 |
| S1/S2 | 18 in | 1.824 | 0.998–2.576 | 2.130 |
| S3/S4 | 6 in | 0.948 | 0.616–1.077 | 1.251 |
| S3/S4 | 12 in | 0.791 | 0.586–1.228 | 0.990 |
| S3/S4 | 18 in | 0.688 | 0.482–1.076 | 0.605 |

Adjustment reduces several of the very large raw ratios, so unequal water content and temperature explain part of the apparent EC contrast. They do not eliminate the main spatial pattern: S1 tends to remain higher than S2, especially at 18 inches, whereas S3 tends to remain lower than S4, especially at 12 and 18 inches.

Position is at least as important as depth:

| Strip pair | Position | Adjusted EC ratio | Interquartile range | Unadjusted ratio |
|---|---|---:|---:|---:|
| S1/S2 | Top | 0.926 | 0.900–1.035 | 0.987 |
| S1/S2 | Middle | 1.043 | 0.947–1.502 | 1.119 |
| S1/S2 | Bottom | 2.472 | 2.085–4.166 | 3.803 |
| S3/S4 | Top | 1.239 | 1.135–1.504 | 1.605 |
| S3/S4 | Middle | 0.791 | 0.605–0.954 | 0.990 |
| S3/S4 | Bottom | 0.531 | 0.373–0.601 | 0.271 |

The opposing Top-to-Bottom gradients are not consistent with one uniform biochar response. They instead point toward persistent spatial differences in salt transport, drainage, irrigation advance, antecedent conditions, or soil properties. A biochar contribution remains possible, but it cannot be separated from these spatial effects using the EC ratios alone.

## Comparison with laboratory ions

Across the 14 sampling-date-by-pair contrasts, laboratory EC ratios were most closely associated with sulfate and sodium ratios.

| Laboratory constituent | Number of contrasts | Spearman correlation with laboratory EC ratio | p-value | Interpretation |
|---|---:|---:|---:|---|
| Sulfate-S | 14 | 0.789 | 0.001 | Strong positive association |
| Sodium | 14 | 0.846 | <0.001 | Strong positive association |
| Potassium | 14 | 0.305 | 0.288 | Weak/inconclusive |
| Nitrate-N | 4 | -0.600 | 0.400 | Too few measured contrasts |
| Olsen phosphorus | 14 | 0.112 | 0.703 | Little association |

The directions are also informative. S1/S2 sulfate and sodium ratios exceeded 1 on six of seven sampling dates. S3/S4 sulfate exceeded 1 on only two of seven dates, and sodium exceeded 1 on only one. This mirrors the broad logger pattern of higher EC in S1 than S2 and lower EC in S3 than S4.

## Interpretation

1. **Logger EC is not acting as a direct nutrient-availability measure.** Its paired-strip contrasts are much more consistent with sulfate and sodium than with nitrate, phosphorus, or potassium.
2. **Moisture and temperature matter, but they are not the whole explanation.** Adjustment attenuates the most extreme ratios without removing the contrasting S1/S2 and S3/S4 patterns.
3. **The strongest signal is spatial salt redistribution.** The large and opposite bottom-of-field contrasts suggest movement and accumulation of soluble ions, with drainage or irrigation hydraulics likely contributing.
4. **A single project-wide “biochar raises EC” or “biochar lowers EC” conclusion is not supported.** The direction depends on strip pair and field position.
5. **The chemistry evidence points primarily to salinity-related ions.** Sodium and sulfate should be analyzed as the leading explanatory candidates; nitrate and phosphorus should remain separate nutrient-availability outcomes.

## Limitations and next tests

- Laboratory samples are strip composites from shallow intervals. They cannot validate individual logger positions or the 18-inch sensors.
- The 2026 growing season is incomplete.
- The adjusted ratios summarize repeated observations; formal confidence intervals should use day- or irrigation-event-level resampling rather than treating hours as independent.
- Before causal treatment claims, fit a joint model with strip pair, position, depth, year, irrigation timing, and antecedent moisture, and test a biochar-by-pair and biochar-by-position interaction.
- The most informative new sampling would collect matched Top/Middle/Bottom samples from S1–S4 at depths aligned with the sensors, measuring saturated-paste EC or another calibrated salinity measure together with sulfate, sodium, chloride, nitrate, potassium, and phosphorus.

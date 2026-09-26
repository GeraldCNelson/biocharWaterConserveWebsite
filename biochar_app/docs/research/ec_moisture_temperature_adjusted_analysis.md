# Six-inch soil electrical-conductivity patterns and shallow laboratory chemistry

## What an EC ratio means

Electrical conductivity (EC) measures how readily the soil–water system conducts electricity. Conductivity increases as the concentration and mobility of dissolved ions increase. A ratio above 1 therefore means **higher EC in the biochar strip than in its paired non-biochar strip**; it does not automatically mean “better.”

- Some ions contributing to EC are plant nutrients.
- Excessive soluble salts can impair water uptake and crop growth.
- Low EC can reflect low salinity, but it can also accompany low nutrient-ion concentrations.
- The field sensor reports bulk-soil EC, which is affected by water content, temperature, texture, and ion concentration. It is not interchangeable with a standardized laboratory salinity threshold.

Accordingly, the analysis treats higher and lower ratios as differences to explain, not benefit scores.

## Scope

This revised analysis reports only the **6-inch sensor depth**. The laboratory samples combine the upper 8–12 inches of each entire strip. They overlap the 6-inch measurement zone and may include soil near the upper edge of the nominal 12-inch zone, but they cannot directly validate the 12- or 18-inch sensors.

Top, Middle, and Bottom are retained as separate logger positions in the field analysis. Whole-strip laboratory samples cannot be assigned to those positions, so no position-specific laboratory interpretation is attempted.

## Daily paired data

For each year, strip pair, and logger position, simultaneous 15-minute measurements were summarized to daily medians during April–October. The two treatment pairs are S1/S2 and S3/S4, with S1 and S3 treated with biochar. The response is

\[
y_{d,p,l}=\log\left(\frac{EC_{B,d,p,l}}{EC_{C,d,p,l}}\right),
\]

where \(d\) is day, \(p\) is strip pair, \(l\) is logger position, \(B\) is the biochar strip, and \(C\) is its paired control strip.

The simultaneous covariates are

\[
\Delta VWC=VWC_B-VWC_C,\qquad \overline{VWC}=(VWC_B+VWC_C)/2,
\]

\[
\Delta T=T_B-T_C,\qquad \overline{T}=(T_B+T_C)/2.
\]

The previous day's \(\Delta VWC\) and \(\overline{VWC}\) represent antecedent moisture. Irrigation controls are days since the most recent paired-strip irrigation and an indicator for an irrigation day.

## Joint robust model

The fitted equation is

\[
\begin{aligned}
y={}&\beta_0+\beta_P P+\beta_M M+\beta_D D
+\sum_{k=2024}^{2026}\gamma_k I(year=k)\\
&+\beta_{PM}(P\times M)+\beta_{PD}(P\times D)\\
&+\beta_1\Delta VWC+\beta_2\overline{VWC}+\beta_3\overline{VWC}^{2}\\
&+\beta_4\Delta T+\beta_5\overline{T}+\beta_6\overline{T}^{2}\\
&+\beta_7\Delta VWC_{d-1}+\beta_8\overline{VWC}_{d-1}\\
&+\beta_9 DaysSinceIrrigation+\beta_{10}IrrigationDay+\varepsilon.
\end{aligned}
\]

Here \(P=1\) for S3/S4, \(M=1\) for Middle, and \(D=1\) for Bottom; S1/S2 Top in 2023 is the reference category. Continuous variables are centered on their analysis means. The pair and position terms are the treatment-contrast interactions because the modeled outcome is already biochar divided by control.

Coefficients were estimated by Huber iteratively reweighted least squares. In standardized-residual form the objective is

\[
\min_\beta\sum_i \rho\left(\frac{y_i-X_i\beta}{s}\right),
\]

with tuning constant \(c=1.345\) and

\[
\rho(u)=
\begin{cases}
u^2/2,& |u|\le c,\\
c|u|-c^2/2,& |u|>c.
\end{cases}
\]

This limits the influence of extreme daily ratios. Confidence intervals use 1,000 calendar-week block-bootstrap samples, keeping measurements from the same week together. Adjusted ratios are \(\exp(X\widehat\beta)\), evaluated at equal paired VWC and temperature differences, average continuous conditions, equal weighting of the four years, and the stated pair and position.

## Adjusted 6-inch EC contrasts

| Strip pair | Logger position | Adjusted EC ratio | Week-block 95% interval | Direction |
|---|---|---:|---:|---|
| S1/S2 | Top | 1.093 | 1.010–1.169 | S1 slightly higher |
| S1/S2 | Middle | 0.985 | 0.734–1.178 | No clear difference |
| S1/S2 | Bottom | 3.132 | 2.725–3.601 | S1 much higher |
| S3/S4 | Top | 1.392 | 1.307–1.582 | S3 higher |
| S3/S4 | Middle | 1.348 | 1.300–1.475 | S3 higher |
| S3/S4 | Bottom | 0.500 | 0.467–0.565 | S3 much lower |

The model confirms strong, opposite position-specific patterns. This is a description of paired strips, not a clean causal estimate of biochar: each treatment is represented by only one physical strip within each pair, so treatment remains inseparable from persistent strip characteristics.

## Soil-temperature differences

Temperature differs between some paired strips, but the differences are small compared with the EC contrasts.

| Pair | Position | Mean biochar minus control temperature | Week-block 95% interval |
|---|---|---:|---:|
| S1/S2 | Top | -0.47 °C | -0.81 to -0.12 |
| S1/S2 | Middle | +0.26 °C | +0.07 to +0.47 |
| S1/S2 | Bottom | +0.22 °C | +0.02 to +0.44 |
| S3/S4 | Top | +0.34 °C | -0.11 to +0.78 |
| S3/S4 | Middle | +0.02 °C | -0.26 to +0.30 |
| S3/S4 | Bottom | +0.02 °C | -0.16 to +0.19 |

The paired-temperature coefficient in the EC model is a 1.020 multiplier per 1 °C difference, with a week-block interval spanning no effect (log coefficient interval -0.006 to 0.051). Mean soil temperature has a detectable but modest association with EC. Removing all temperature terms changes the six adjusted EC ratios by only about 0.2%–2.5%. Temperature adjustment is therefore appropriate, but it is not the explanation for the large Bottom contrasts.

## What “laboratory EC ratio” means

The laboratory file contains a soil-test EC value (`ec_1_1`) for each whole-strip composite. For a given sampling date:

\[
Laboratory\ EC\ ratio=\frac{soil\text{-}test\ EC\ in\ S1\ (or\ S3)}{soil\text{-}test\ EC\ in\ S2\ (or\ S4)}.
\]

These are **soil-test values**, not logger values. The ratios vary substantially:

| Sampling date | S1/S2 lab EC ratio | S3/S4 lab EC ratio |
|---|---:|---:|
| 2023-03-31 | 1.79 | 1.80 |
| 2023-10-09 | 3.94 | 0.86 |
| 2024-03-20 | 2.50 | 0.47 |
| 2024-11-05 | 4.08 | 0.88 |
| 2025-03-31 | 1.06 | 0.48 |
| 2025-11-03 | 1.33 | 0.11 |
| 2026-04-28 | 0.64 | 0.34 |

Thus the laboratory EC evidence is not a stable treatment effect. It varies by pair and date, and S1/S2 even changes direction in 2026.

## Two distinct laboratory comparisons

### 1. Which measured ions accompany laboratory EC?

This comparison stays entirely within each soil sample. Across 14 date-by-pair contrasts, laboratory EC ratios track sodium and sulfate most clearly. It asks what may be contributing to the soil-test conductivity; it does **not** validate the logger.

| Ion ratio | n | Spearman correlation with laboratory EC ratio | p-value |
|---|---:|---:|---:|
| Sulfate-S | 14 | 0.789 | 0.001 |
| Sodium | 14 | 0.846 | <0.001 |
| Potassium | 14 | 0.305 | 0.288 |
| Nitrate-N | 14 | 0.556 | 0.039 |
| Olsen phosphorus | 14 | 0.112 | 0.703 |

### 2. Do shallow laboratory ions track the adjusted 6-inch logger ratio?

For each soil-sampling date with logger coverage, the adjusted 6-inch logger ratios were summarized over a ±7-day window and then averaged equally across the available Top/Middle/Bottom positions. Twelve date-by-pair comparisons were available.

| Ion ratio | n | Spearman correlation with adjusted 6-inch logger EC ratio | p-value |
|---|---:|---:|---:|
| Sulfate-S | 12 | 0.476 | 0.118 |
| Sodium | 12 | 0.797 | 0.002 |
| Potassium | 12 | 0.867 | <0.001 |
| Nitrate-N | 12 | 0.112 | 0.729 |
| Olsen phosphorus | 12 | 0.615 | 0.033 |

These correlations are exploratory. There are only 12 observations, five ions were tested, several sample dates fall outside the growing-season fitting period, and the whole-strip sample is being compared with an equal-weight average of three point locations. Pair and year can drive both variables. Sodium is the most consistent candidate across both comparisons. Potassium and phosphorus track the adjusted logger contrast in this small matched set but do not track laboratory EC consistently, so they should not yet be interpreted as conductivity drivers.

## Conclusions

1. A ratio above 1 means higher conductivity in the biochar strip, not necessarily a beneficial condition.
2. Moisture, temperature, irrigation timing, antecedent moisture, year, pair, and position do not remove the strong 6-inch spatial contrasts.
3. Temperature differences are generally less than 0.5 °C, and including temperature changes the adjusted ratios only slightly.
4. The opposing Bottom results—S1/S2 near 3.13 and S3/S4 near 0.50—cannot be explained as one uniform biochar response.
5. Whole-strip laboratory chemistry cannot explain location-specific logger patterns. It can only provide a shallow, strip-wide comparison.
6. Sodium is the most reproducible ion association. Other ion results require additional matched sampling before interpretation.

## Causal limitation and next sampling step

The joint model improves adjustment but does not create treatment replication. Biochar is confounded with strip identity, and position-specific field effects are large. A defensible causal biochar estimate would require additional treated and untreated strips or another design that separates treatment from strip.

The most useful next sampling round would collect matched Top/Middle/Bottom samples in all four strips, using depth intervals aligned with the 6-inch sensors. Laboratory analyses should include a calibrated salinity measure plus sodium, sulfate, chloride, potassium, nitrate, and phosphorus.

## Reproducibility

The analysis is generated by `biochar_app/scripts/research/analyze_ec_adjustment.py`. Machine-readable results are written to `biochar_app/data-processed/research/ec_adjustment/`.

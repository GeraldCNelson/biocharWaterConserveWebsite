Plot Download Notes - Ratio
---------------------------

Contents

- Ratio downloads contain treated/control comparison columns when available.
- Ratio columns are limited to the selected sensor depth.
- Ratio values are unitless.
- For Seasonal Periods VWC downloads and bars, each ratio is the numerator's seasonal mean divided by the denominator's seasonal mean, using matching timestamps with finite readings in both strips at the same depth and logger position. No ratio is reported if no pairs exist or the denominator mean is nonpositive. It is not an average of individual ratios.
- Nonseasonal ratio time series are unchanged. Individual-ratio variability is distinct from the seasonal ratio of means; see the summary download README for matched-observation counts and coverage.
- Temperature ratios are generally not used; temperature comparisons may use difference columns instead.

Column naming notes

- Ratio columns identify paired strip comparisons, such as S1/S2 or S3/S4.
- The selected strip determines which treatment/control pair is used.

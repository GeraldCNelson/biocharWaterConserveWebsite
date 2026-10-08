Summary Statistics Notes
------------------------

Contents

- raw_summary.csv contains summary statistics for raw logger measurements.
- ratio_summary.csv contains summary statistics for treated/control ratios when available.
- For temperature variables, ratio statistics are omitted because temperature ratios are not used.

Column naming notes

- The Row column describes the measurement series summarized.
- Raw rows usually identify strip and logger location, such as Top, Middle, or Bottom.
- Ratio rows identify paired strip comparisons, such as S1/S2 or S3/S4.
- Summary statistic columns include minimum, mean, maximum, and standard deviation values when available.

Seasonal VWC ratios

- The headline seasonal VWC ratio is ratio_of_means: mean S1 / mean S2, or mean S3 / mean S4, at the same depth and logger position. Both means use only matching timestamps with finite readings in both strips. A missing pair or a nonpositive denominator mean produces no seasonal ratio.
- ratio_of_means_n and ratio_of_means_coverage_pct describe these matched observations. Coverage uses the expected 15-minute observations through the elapsed part of the season; an unfinished season is not a full-season result.
- The seasonal table's Mean in VWC ratio sections displays this ratio of means. Its Valid n and Coverage describe the matched observations.
- ratio_min, ratio_mean, ratio_max and ratio_std are retained in the CSV as statistics of individual 15-minute ratios. In particular, ratio_mean is the mean of individual ratios, NOT the headline seasonal ratio. The table's Min, Max and SD also describe individual ratios; SD is not uncertainty in the ratio of seasonal means.
- Raw means may use more observations than matched-pair means, so dividing the displayed raw means need not reproduce the seasonal ratio.
- Other variables and the 15-minute ratio time series are unchanged.

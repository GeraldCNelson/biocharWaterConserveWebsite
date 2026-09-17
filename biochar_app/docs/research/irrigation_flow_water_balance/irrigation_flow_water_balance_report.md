# Flow rate, wetting-front advance, and the unresolved water balance

## Bottom line

This analysis uses the corrected 2023–2026 logger timestamps and treats Top→Bottom travel as the primary horizontal-advance outcome. Middle timing is retained as a separate spatial-coverage diagnostic; strict Top→Middle→Bottom order is no longer required for the primary model.

That revision restores S3 to the analysis and changes the conclusion. A doubling of recorded pair-level flow is associated with an estimated **18% shorter Top→Bottom travel time**, but the 95% interval ranges from 41% shorter to 15% longer (event-clustered normal-approximation p=0.249). The full-field flow effect is therefore uncertain.

The stricter subset that requires Top→Middle→Bottom order estimates a **36% shorter** Top→Bottom time when flow doubles (95% interval 15% to 51% shorter; p=0.002). Because that restriction excludes every S3 observation and many nonuniform events, it is reported as sensitivity evidence rather than the principal result.

## Experimental design

The field combines two treatment dimensions. S1 is biochar with monthly irrigation, S2 is its non-biochar monthly control, S3 is biochar with approximately biweekly irrigation, and S4 is its non-biochar biweekly control. Consequently:

- S1−S2 estimates the biochar contrast under monthly irrigation.
- S3−S4 estimates the biochar contrast under biweekly irrigation.
- A difference between those two contrasts is consistent with a biochar × irrigation-frequency interaction, including effects mediated by antecedent soil moisture.

However, each of the four treatment combinations is represented by only one strip. Irrigation frequency is therefore confounded with strip pair and east/west field position, and biochar effects within a regime are confounded with individual strip differences. The results are repeated-event comparisons within these four experimental units, not replicated treatment estimates.

## Data and eligibility

- There are 146 strip-events; 117 have all three 6-inch arrival estimates.
- 102 have Bottom after Top. Of these, 87 records from 59 irrigation events also pass canonical event QC and enter the primary model.
- Primary records by strip are S1=16, S2=18, S3=31, and S4=22.
- 62 records have strict Top→Middle→Bottom order. S3 has 32 Top→Bottom-ordered records, 31 of which pass QC, but no strict three-position records.
- The water-balance analysis uses 78 eligible records. The 77 end-of-field proxy records remain upper-bound estimates, not measured runoff.

## Horizontal advance and spatial coverage

Top arrival itself is earlier at higher flow: doubling flow is associated with an estimated **44% earlier** Top response (95% interval 23% to 59% earlier; p<0.001). By contrast, the primary Top→Bottom model is uncertain, and its unadjusted Spearman correlation is -0.090 (permutation p=0.405).

When stratified by irrigation regime, doubling flow is associated with **34% shorter** Top→Bottom time in the monthly S1/S2 observations (95% interval 25% to 41% shorter; p<0.001). In the biweekly S3/S4 observations, the estimate is only **7% shorter** and is highly uncertain (95% interval ranges from 45% shorter to 59% longer; p=0.800). These regime-specific estimates are descriptive because regime and strip pair cannot be separated statistically.

The corrected 2024 S3 plots commonly show Top first, Bottom shortly afterward, and Middle near the end of irrigation. The traces are smooth and the Middle response is a genuine VWC increase, so this pattern no longer looks like a simple clock error. It indicates nonuniform spatial delivery or a different water pathway at S3M. Middle timing should therefore be analyzed as a response characteristic rather than used as a gate that discards otherwise credible Top→Bottom observations.

## Monitored storage and residual water

Holding applied gallons, strip, and year constant:

- Doubling flow is associated with **11.6 percentage points less monitored storage** (95% interval 4.0 to 19.1 points less; p=0.003).
- The complementary residual fraction is **12.3 percentage points larger** (95% interval 5.6 to 19.0 points; p<0.001).
- Absolute monitored storage is estimated to be **21.8% lower** when flow doubles at fixed applied volume (95% interval 12.4% to 30.2% lower; p<0.001).

The storage association is similar in the two timing regimes: doubling flow is associated with 11.1 percentage points less monitored storage in the monthly pair (p=0.026) and 10.2 points less in the biweekly pair (p=0.039). This similarity does not remove the lack of replicated strips.

These are observational associations. The residual is not measured runoff: it may contain tailwater, deep percolation, lateral redistribution, continued infiltration, unequal strip delivery, and measurement error.

## Biochar-control comparisons

For 16 eligible monthly-irrigation S1–S2 events, the biochar strip S1 has Top→Bottom travel averaging **217 minutes longer** than control strip S2 (bootstrap 95% interval 161 to 271; sign-randomization p<0.001).

For 12 eligible biweekly-irrigation S3–S4 events, the biochar strip S3 has Top→Bottom travel averaging **284 minutes shorter** than control strip S4 (bootstrap 95% interval 211 to 371 minutes shorter; sign-randomization p<0.001).

The mean residual is 10.0 percentage points higher in S1 than S2 under monthly irrigation and 4.4 points lower in S3 than S4 under biweekly irrigation. These opposing within-regime contrasts may reflect an interaction between biochar and irrigation timing through antecedent moisture, but they may also reflect fixed differences between the individual strips, unequal gate flow, or east/west field conditions. They do not justify either a pooled biochar conclusion or a standalone irrigation-frequency conclusion.

## Field observations and next measurements

Only 7 management records contain runoff, field-end, flooding, or cross-strip language. These provide valuable checks but are too sparse to estimate runoff across all events.

The most useful next measurements remain strip-specific flow and field-end tailwater. The next analytical step is to summarize Middle timing by strip, year, and event and compare it with irrigation duration and field notes. Deep drainage requires measurements below the monitored profile or a defensible calibrated drainage model.

## Reproducibility

The event table, paired-event table, field-note subset, and machine-readable results were regenerated by `biochar_app/scripts/management/irrigation_analysis/analyze_flow_infiltration.py`. Canonical inputs were read only.

Two reader-facing tables accompany the detailed outputs. `zone_capacity_summary.csv` reports the modeled upper retained-water state for each Top, Middle, and Bottom zone. `typical_event_summary.csv` reports strip-level medians and middle-50% ranges from eligible observed irrigation events. Its component medians are calculated independently and therefore are descriptive rather than a synthetic water budget for one event.

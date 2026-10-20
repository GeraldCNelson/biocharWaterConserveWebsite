# Soil-water storage and irrigation water-balance equations

This document is the maintained technical reference for the soil-water
storage and irrigation water-balance quantities used by the project. It is
intended to replace a small family of LaTeX, generated HTML, and Word copies
that described a simpler calculation.

**Last reviewed:** 2026-10-19

**Status:** Maintained technical reference. Approved by the maintainer on
2026-10-19. The equations describe the implemented calculation; they do not
turn the calculated residual into a runoff measurement.

## Purpose

The project estimates the increase in soil-water storage associated with an
irrigation event and compares it with the recorded irrigation volume. The
difference is called the **water-balance residual** or, after negative values
are clipped to zero, **unretained water in the measured profile**.

The residual is not measured runoff. It can include tailwater, drainage below
the monitored profile, lateral redistribution, continuing infiltration,
evapotranspiration, unequal water delivery, unmeasured storage, and measurement
or spatial-representation error.

## Notation

| Symbol | Meaning |
|---|---|
| $I$ | Recorded irrigation volume allocated to one strip, in gallons. |
| $j$ | Logger influence zone: Top, Middle, or Bottom. |
| $d$ | Sensor depth index corresponding to the nominal 6-, 12-, or 18-inch installation depth. |
| $\theta_{b,j,d}$ | Baseline volumetric water content at zone $j$ and depth $d$, recorded as a percentage. |
| $\theta_{p,j,d}$ | Accepted plateau volumetric water content after irrigation, recorded as a percentage. |
| $\Delta z_d$ | Soil-layer thickness represented by depth $d$, in inches. |
| $A_j$ | Field area represented by logger zone $j$, in square feet. |
| $c$ | Conversion factor, $0.623$ gallon per square foot per inch of water. |
| $\Delta h_{j,d}$ | Estimated change in water depth for one represented soil layer, in inches. |
| $\Delta S_j$ | Estimated change in monitored soil-water storage for one logger zone, in gallons. |
| $\Delta S_{\mathrm{strip}}$ | Estimated change in monitored storage for the whole strip, in gallons. |
| $R$ | Signed water-balance residual, $I-\Delta S_{\mathrm{strip}}$, in gallons. |
| $U$ | Nonnegative unretained-water quantity, $\max(R,0)$, in gallons. |

## Monitored profile and field zones

The three sensors are installed at nominal depths of 6, 12, and 18 inches.
The configured CS650 vertical sensitivity radius is three inches, so each
sensor represents a six-inch-thick, non-overlapping band centered on its
installation depth. The resulting bands span approximately 3–9, 9–15, and
15–21 inches: an 18-inch-thick monitored profile whose upper boundary is about
three inches below the soil surface.

Several existing output names contain `0_18in`. That label means the project's
nominal three-layer, 18-inch profile; it should not be read as a literal claim
that the configured sensor-centered bands span exactly 0–18 inches.

Each strip is also divided into Top, Middle, and Bottom influence zones. Zone
boundaries are placed halfway between adjacent logger positions, with the end
segments assigned to the nearest logger. The implemented zone areas and their
sources are defined in
[`field_management_metadata.py`](../../../config/field_management_metadata.py).

## Change in water depth for one layer

Because VWC is stored as a percentage, it is divided by 100 before it is
multiplied by the represented layer thickness:

$$
\Delta h_{j,d}
=
\frac{\theta_{p,j,d}-\theta_{b,j,d}}{100}
\Delta z_d.
$$

The calculation accepts a layer only when baseline and plateau values are
available, the plateau method represents a usable response, and the plateau is
not below baseline. Missing or rejected layers remain missing; they are not
replaced with zero.

## Storage for one logger influence zone

For a zone with all three represented layers accepted, the change in storage
is

$$
\Delta S_j
=
c A_j \sum_d \Delta h_{j,d}.
$$

Here $c=0.623$ converts one inch of water over one square foot to gallons. A
partial layer sum may be retained as a diagnostic, but it is not eligible for
the whole-strip water balance.

## Whole-strip monitored storage

When the Top, Middle, and Bottom zones all have complete accepted profiles,

$$
\Delta S_{\mathrm{strip}}
=
\Delta S_{T}+\Delta S_{M}+\Delta S_{B}.
$$

If any zone lacks a complete profile, the whole-strip estimate is missing.
This prevents an absent sensor response from being treated as zero storage and
artificially increasing the residual.

The monitored-storage fraction is

$$
f_S
=
\frac{\Delta S_{\mathrm{strip}}}{I},
\qquad I>0.
$$

## Water-balance residual and unretained water

The signed residual is

$$
R
=
I-\Delta S_{\mathrm{strip}}.
$$

The descriptive nonnegative quantity used in current outputs is

$$
U
=
\max(R,0),
$$

with fraction

$$
f_U
=
\frac{U}{I},
\qquad I>0.
$$

The signed residual is retained because negative values—estimated monitored
storage greater than recorded applied water—are diagnostically important. The
clipped quantity $U$ is convenient for descriptive summaries but must not hide
those inconsistencies.

## Physical interpretation

A fuller event-scale balance would need to account for water pathways that are
not directly measured by the present calculation. Conceptually,

$$
I + P
=
\Delta S_{\mathrm{monitored}}
+ \Delta S_{\mathrm{unmonitored}}
+ Q + D + L + E_T + \varepsilon,
$$

where $P$ is precipitation during the accounting interval, $Q$ is surface
runoff or tailwater, $D$ is deep drainage, $L$ is lateral redistribution,
$E_T$ is evaporation plus plant water use, and $\varepsilon$ collects
measurement and representation error. Not all of these terms are currently
observed. Consequently, $R$ cannot be assigned to any single pathway.

Field-end arrival is analyzed separately as a hydraulic timing indicator. It
can support interpretation of possible tailwater, but it does not convert the
residual into a runoff measurement.

## Implementation and related results

The maintained implementation is in
[`holding_capacity.py`](../../../scripts/management/irrigation_analysis/holding_capacity.py),
particularly `build_event_storage_by_zone()` and
`build_event_storage_by_event()`. Sensor depths and represented layer
thicknesses are configured in
[`experiment_config.py`](../../../config/experiment_config.py).

The current scientific interpretation is reported in
[`irrigation_flow_water_balance_report.md`](../irrigation_flow_water_balance/irrigation_flow_water_balance_report.md).
That report treats the residual as unresolved water and documents the
experimental-design and measurement limits.

The comparison with the older LaTeX, HTML, and Word copies is recorded in
[`water_balance_document_review_2026-10-18.md`](../../project/water_balance_document_review_2026-10-18.md).

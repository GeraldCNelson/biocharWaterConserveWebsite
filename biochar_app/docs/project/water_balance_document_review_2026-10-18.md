# Water-balance document review, 2026-10-18

This review explains how the project's overlapping water-balance documents
were compared and how their useful content was incorporated into one maintained
Markdown source.

**Review date:** 2026-10-18

**Maintained canonical source approved 2026-10-19:**
[`water_balance_equations.md`](../research/water_balance/water_balance_equations.md)

## Why equivalence is not a byte-for-byte comparison

The old files use different formats and are not identical. One LaTeX copy adds
a notation section, while the other LaTeX copy, generated HTML, and Word copy
contain the same shorter sequence of equations. The older interpretation also
labels applied irrigation minus monitored storage as runoff. Current code and
research correctly describe that quantity as an unresolved residual.

Content equivalence therefore means that every scientifically useful term,
equation, unit conversion, and explanatory qualification has either:

1. been preserved in the canonical Markdown document; or
2. been deliberately corrected, with the reason recorded below.

Git history remains the recovery path for the retired representations.

## Files compared

| File | Size | SHA-256 | Finding |
|---|---:|---|---|
| `references/waterbalance_code.tex` | 2,032 bytes | `0d28858eb53e32318b9bbf837c4fb6e8afb1ff232baade613fb8e5d7afaed259` | Most complete old copy because it includes notation; contains corrupted question-mark characters and the obsolete runoff interpretation. |
| `biochar_app/docs/research/water_balance/waterbalance_code.tex` | 1,467 bytes | `7b31fcfb422ec2b44727c5aa246e889e95f2a97e9de6c6579272dfc578f83470` | Same four equation sections as the reference copy, but without its notation section. |
| `biochar_app/markdown/waterbalance_code.md` | 6,085 bytes | `72a82f4f4451676e281704789eb7d2a1fa3d2b7386670e83f6c8ebd37b76b1a2` | Pandoc-generated standalone HTML stored with an `.md` extension; no unique scientific content was found. |
| `biochar_app/docs/waterbalance_code.docx` | 11,728 bytes | `ec8312c0e2d1e19e7eec9f9394df295a2a204bcdc5b50231abee6dd82198c169` | Word rendering of the shorter document; extracted text contains no unique equation or interpretation. |

## Content crosswalk

| Older content | Canonical Markdown location | Disposition |
|---|---|---|
| Symbols for irrigation, storage, runoff, drainage, evaporation/plant use, VWC, layer thickness, and area | “Notation” and “Physical interpretation” | Preserved, clarified, and extended to distinguish measured from unmeasured terms. |
| Weighted average SWC, $\sum w_i\theta_i$ | “Change in water depth for one layer” | Replaced by the current implemented layer-thickness calculation. The project does not use the old generic weighted-average equation for the event water balance. |
| Trapezoidal integration between sensor depths | “Monitored profile and field zones,” “Change in water depth for one layer,” and “Storage for one logger influence zone” | Replaced by the implemented three non-overlapping sensor-centered layers and Top/Middle/Bottom influence-zone areas. |
| Applied irrigation minus storage | “Water-balance residual and unretained water” | Preserved as the signed residual $R=I-\Delta S_{\mathrm{strip}}$. |
| Labeling the residual as runoff | “Purpose” and “Physical interpretation” | Corrected. The residual is not a runoff measurement and may contain several unmeasured pathways and errors. |
| Cubic metres to litres conversion | Zone-storage conversion using $0.623$ gallon per square foot per inch | Replaced by the conversion actually used in the current US-unit implementation. The generic metric identity is valid but is not part of this calculation. |
| No rule for missing layers or zones | Layer, zone, and whole-strip eligibility sections | Added from the current implementation so missing observations are not silently counted as zero. |
| No distinction between signed and clipped residuals | “Water-balance residual and unretained water” | Added from the current implementation. |

## Verification performed

- Compared the two LaTeX files directly; their only structural difference is
  the notation section in the `references/` copy.
- Inspected the generated HTML and extracted the text from the DOCX; neither
  contains a unique equation or scientific qualification.
- Traced the maintained equations to `build_event_storage_by_zone()` and
  `build_event_storage_by_event()` in `holding_capacity.py`.
- Traced sensor depths and represented layer thicknesses to
  `experiment_config.py` and zone-area conversion to
  `field_management_metadata.py`.
- Checked the terminology against the current irrigation flow and water-balance
  report, which explicitly states that the residual is not measured runoff.

## Retirement decision

The maintainer approved the canonical Markdown source on 2026-10-19. The four
older files were therefore removed together in one documentation-only commit,
with this review retained as its evidence. The equations do not require a
retained LaTeX source: Markdown preserves the math notation, and a
print-oriented PDF can be generated from the maintained Markdown later if one
becomes useful.

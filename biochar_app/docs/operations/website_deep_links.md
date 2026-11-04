# Website links for reports and other documents

**Updated:** 2026-11-04

The website supports stable links to a major tab and, where useful, to a
specific heading within that tab. These links are intended for the project
report, presentations, email, and other documents outside the website.

## Link format

Use the public website address followed by a `tab` parameter:

```text
https://biocharresearch.org/?tab=soilbio
```

Add a heading anchor after `#` to open the tab and move directly to a section:

```text
https://biocharresearch.org/?tab=soilbio#soilbio-total-living-microbial-biomass
```

The website creates stable heading anchors from the visible heading text.
Anchors are lowercase, use hyphens in place of spaces and punctuation, and
begin with the tab's content ID. For example, **Total Living Microbial
Biomass** within the `soilbio` tab becomes
`soilbio-total-living-microbial-biomass`.

## Supported tab names

| Website content | `tab` value |
|---|---|
| Introduction | `intro` |
| Experiment Design | `experiment` |
| Interactive monitoring plots | `monitoring` |
| Summary Statistics | `summary` |
| Seasonal comparison | `seasons` |
| Biochar characteristics and analysis | `biochar-analysis` |
| Pasture quality | `pasture-quality` |
| Biomass samples and fertilizer recommendations | `biomass` |
| Soil Biological Health | `soilbio` |
| Soil Chemistry | `soilchem` |
| Technical Details | `technical-details` |
| Bulk Downloads | `downloads` |
| Glossary | `glossary` |
| Acknowledgements | `acknowledgements` |

Selecting a tab updates the browser address, so the tab-level link can be
copied from the address bar. Existing Summary Statistics links may continue
to include `year`, `granularity`, `variable`, `strip`, and `depth` parameters.

## Adding a link in Microsoft Word

1. Select the words that should become the link.
2. Choose **Insert > Link** or press **Command-K** on macOS.
3. Paste the full website link into the **Address** field.
4. Select **OK**, then test the link in the exported PDF as well as in Word.

When a report cites a particular table or indicator, use a section link rather
than only the tab link. The section link remains meaningful even if more
material is later added above it.

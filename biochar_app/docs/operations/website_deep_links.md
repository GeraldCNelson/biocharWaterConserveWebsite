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
copied from the address bar.

## Linking to selected monitoring or summary results

Continuous Monitoring and Summary Statistics links can include any combination
of these optional parameters:

| Selection | Parameter | Supported values |
|---|---|---|
| Year | `year` | A year offered on the website, such as `2026` |
| Data time resolution | `granularity` | `15min`, `hourly`, `daily`, `monthly`, or `gseason` |
| Variable | `variable` | `VWC`, `EC`, `T`, or `SWC` |
| Strip | `strip` | `S1`, `S2`, `S3`, or `S4` |
| Sensor depth | `depth` | `1` (6 inches), `2` (12 inches), or `3` (18 inches) |

For example, this link opens Continuous Monitoring with EC selected:

```text
https://biocharresearch.org/?tab=monitoring&variable=EC
```

This more specific example opens the 2026 daily EC results for Strip 1 at the
6-inch sensor depth:

```text
https://biocharresearch.org/?tab=monitoring&year=2026&granularity=daily&variable=EC&strip=S1&depth=1
```

The same filter parameters work with `tab=summary`. Parameter values must use
the spelling and capitalization shown above. A missing or unrecognized value
leaves that selection at the website default.

## Adding a link in Microsoft Word

1. Select the words that should become the link.
2. Choose **Insert > Link** or press **Command-K** on macOS.
3. Paste the full website link into the **Address** field.
4. Select **OK**, then test the link in the exported PDF as well as in Word.

When a report cites a particular table or indicator, use a section link rather
than only the tab link. The section link remains meaningful even if more
material is later added above it.

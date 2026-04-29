# Critique: Fruita Biochar Project Codebase
## Code Review & Carbon Efficiency Ratio Implementation

**Repository:** [biocharWaterConserveWebsite (etl-refactor branch)](https://github.com/GeraldCNelson/biocharWaterConserveWebsite/tree/etl-refactor)

**Date:** March 20, 2026

---

## 1. Overall Architecture Assessment

The codebase is a **Python/FastAPI dashboard** with an ETL pipeline that processes Campbell Scientific logger data (VWC, temperature, EC) across four experimental strips (two biochar-amended, two control). The code is well-documented, defensively written, and handles edge cases carefully. However, several structural and analytical issues limit its value.

### Strengths
- Thorough timestamp handling (DST, clock corrections, logger-specific stitching)
- Defensive `safe_ratio()` function that guards against division-by-zero and ±∞
- Clean separation of ETL (`etl.py`), processing (`process_data.py`), aggregation (`aggregation.py`), and plotting (`plot_utils.py`)
- Good use of Parquet for intermediate storage
- Value-bounds enforcement with reporting

### Weaknesses (addressed in detail below)
- Heavy code duplication across `etl.py` and `process_data.py`
- No carbon efficiency calculations despite being a biochar project
- Ratio calculations are purely VWC-based — no carbon mass balance metrics
- Missing statistical significance testing on treatment vs. control comparisons
- Excessive `.copy()` calls creating memory pressure

---

## 2. Specific Code Critiques

### 2.1 Code Duplication: `etl.py` vs. `process_data.py`

These two files contain near-identical functions:

| Function | `process_data.py` | `etl.py` |
|---|---|---|
| `rename_logger_columns()` | ✓ | ✓ (slightly different split logic) |
| `replace_bad_values()` | ✓ | ✓ |
| `scale_vwc_to_percent()` | ✓ | ✓ |
| `add_swc_cylinder_volumes()` | ✓ | ✓ (uses `"1","2","3"` vs `DEPTHS`) |
| `merge_all_loggers()` | ✓ | ✓ |
| `read_logger_data()` | ✓ | ✓ (more robust TOA5 parsing) |

**Recommendation:** Extract shared transformations into a `transforms.py` module. The `etl.py` versions are generally more robust (e.g., proper TOA5 header parsing vs. hardcoded `skiprows=4`). Deprecate the `process_data.py` equivalents.

```python
# biochar_app/scripts/transforms.py
"""Shared data transformations used by both ETL and legacy processing."""

def replace_bad_values(df_in, threshold=DEFAULT_BAD_VALUE_THRESHOLD):
    ...

def scale_vwc_to_percent(df_in):
    ...

def add_swc_cylinder_volumes(df_in):
    ...
```

### 2.2 Excessive DataFrame Copies

Almost every function starts with `df_out = df_in.copy()`. For a 15-minute time series over a full year (35,040 rows × hundreds of columns), this is expensive. Many of these copies are unnecessary because the function is the sole consumer in a pipeline.

**Current pattern:**
```python
def scale_vwc_to_percent(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()  # Unnecessary full copy
    vwc_cols = [c for c in df_out.columns if c.startswith("VWC_") and "_raw_" in c]
    for col_name in vwc_cols:
        df_out[col_name] = pd.to_numeric(df_out[col_name], errors="coerce") * 100.0
    return df_out
```

**Recommended pattern — use an `inplace` flag:**
```python
def scale_vwc_to_percent(df_in: pd.DataFrame, *, copy: bool = True) -> pd.DataFrame:
    df = df_in.copy() if copy else df_in
    vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
    for col_name in vwc_cols:
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce") * 100.0
    return df
```

Then in the orchestration pipeline where you control the data flow:
```python
df = replace_bad_values(df, copy=False)
df = scale_vwc_to_percent(df, copy=False)
df = convert_soil_t_to_fahrenheit(df, copy=False)
```

### 2.3 `calculate_ratios()` — Silent Failures and Missing Validation

```python
def calculate_ratios(df_in: pd.DataFrame) -> pd.DataFrame:
    ...
    for var in ["VWC", "T", "EC"]:
        for s1, s2 in pairings:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    c1 = f"{var}_{d}_raw_{s1}_{loc}"
                    c2 = f"{var}_{d}_raw_{s2}_{loc}"
                    out_col = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"
                    if c1 in df_out.columns and c2 in df_out.columns:
                        df_out[out_col] = safe_ratio(df_out[c1], df_out[c2])
                    else:
                        df_out[out_col] = pd.NA  # Silent failure
```

**Issues:**
1. When columns are missing, the ratio is silently set to `pd.NA` with no log warning. A missing column likely indicates a logger outage — this should be logged.
2. Temperature ratios are computed here but then explicitly excluded from display (per the tech details page noting "Temperature Ratios Are Not Reported"). The code should skip `"T"` entirely to avoid computing ratios that are never used and can mislead developers.
3. The 4-level nested loop creates O(variables × pairings × locations × depths) columns — currently 3 × 2 × 3 × 3 = 54 ratio columns. This is fine, but the column-naming convention is inconsistent between VWC/EC ratios (`VWC_1_ratio_S1_S2_T`) and SWC ratios (`SWC_vol_gal_1_ratio_S1_S2_T` — note different position of depth index).

**Recommended fix:**
```python
RATIO_VARIABLES = ["VWC", "EC"]  # Exclude T; ratios are not meaningful

def calculate_ratios(df_in: pd.DataFrame, *, copy: bool = True) -> pd.DataFrame:
    df_out = df_in.copy() if copy else df_in
    pairings = [("S1", "S2"), ("S3", "S4")]

    for var in RATIO_VARIABLES:
        for s1, s2 in pairings:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    c1 = f"{var}_{d}_raw_{s1}_{loc}"
                    c2 = f"{var}_{d}_raw_{s2}_{loc}"
                    out_col = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"
                    if c1 in df_out.columns and c2 in df_out.columns:
                        df_out[out_col] = safe_ratio(df_out[c1], df_out[c2])
                    else:
                        logger.warning(
                            f"Missing column(s) for ratio {out_col}: "
                            f"{c1}={'present' if c1 in df_out.columns else 'MISSING'}, "
                            f"{c2}={'present' if c2 in df_out.columns else 'MISSING'}"
                        )
                        df_out[out_col] = NAN
    return df_out
```

### 2.4 `safe_ratio()` — The Epsilon Threshold

```python
def safe_ratio(num, denom, eps=1e-3):
    denom_safe = denom_f.copy()
    small_mask = denom_safe.abs() < float(eps)
    denom_safe.loc[small_mask] = NAN
```

An `eps=1e-3` threshold on VWC (0–100%) means any control strip reading below 0.1% VWC triggers NaN. This is physically reasonable for VWC (bone-dry soil), but for EC (which can have small legitimate values in dS/m), this threshold may be too aggressive. Consider making `eps` variable-specific or at least documenting the choice.

### 2.5 Aggregation — Growing Season Period Handling

In `write_gseason_summary()`, the cross-year period handling is well-implemented but has a subtle issue:

```python
stats_series = window[value_cols].agg(agg_map).round(3)
```

This applies `"sum"` for precip columns and `"mean"` for everything else. But for ratio columns (e.g., `VWC_1_ratio_S1_S2_T`), taking the **mean of ratios** is not the same as the **ratio of means**. For a growing-season summary, the ratio of the seasonal means would be more interpretable:

```
Mean(VWC_S1) / Mean(VWC_S2)  ≠  Mean(VWC_S1 / VWC_S2)
```

The latter (what the code does) gives equal weight to every timestep, including dry periods where ratios blow up. The former weights by actual water content and is more physically meaningful.

### 2.6 `plot_utils.py` — Over-Engineering

The plotting code at ~500+ lines is excessively complex for what is essentially: (a) a time-series line chart with optional precipitation bars, and (b) a ratio line chart. The multiple layers of helper functions (`_compact_legend_label`, `_depth_display_label`, `_normalize_trace_grouping`, etc.) make it harder to modify than a simpler approach would.

**Recommendation:** Consider using Plotly Express for the core chart construction, which handles most of this automatically, and reserve `go.Figure` only for custom overlays (irrigation shapes, precipitation bars).

### 2.7 Type Hint Workarounds

The codebase has extensive comments like:
```python
# ✅ Fix PyCharm warning: DatetimeIndex has no ".index"
# ✅ Keep common_end_date unambiguously a 'date' for PyCharm
```

These indicate the project is fighting PyCharm's type checker rather than using proper type annotations. Consider adding a `py.typed` marker and using `pandas-stubs` (or switching to `pyright` which handles pandas better).

---

## 3. Carbon Efficiency Ratios: What's Missing and How to Add Them

### 3.1 What "C Efficiency Ratio" Means

In the biochar literature, the carbon efficiency ratio (also called **C sequestration efficiency**) of a treatment compares how much of the original feedstock carbon ends up as stable biochar carbon. The key formulas from the literature are:

**A. Carbon Yield (C-yield):**
$$C_{yield} = \frac{Y_{biochar} \times C_{biochar}}{C_{feedstock}}$$

Where:
- $Y_{biochar}$ = mass yield of biochar (kg biochar / kg feedstock)
- $C_{biochar}$ = carbon content of biochar (fraction)
- $C_{feedstock}$ = carbon content of feedstock (fraction)

**B. Carbon Sequestration Potential (CS) — from [Comparative analysis of biochar carbon stability, Sci. Total Environ. 2024](https://www.sciencedirect.com/science/article/pii/S0048969723082372):**
$$CS = \frac{M \times ch \times C_{ch} \times R_{50}}{M \times C_f}$$

Where $R_{50}$ is the recalcitrance index (fraction of C remaining after thermal oxidation).

**C. Long-term sequestration efficiency ($BC_{+100}$) — from [IBI methodology](https://biochar-international.org/wp-content/uploads/2018/04/IBI_Report_Biochar_Stability_Test_Method_Final.pdf):**

Based on the H/C_org molar ratio:
- H/C_org < 0.4 → $BC_{+100}$ ≈ 70% (very stable)
- H/C_org 0.4–0.7 → $BC_{+100}$ ≈ 50–65%
- H/C_org > 0.7 → biochar is less stable; $BC_{+100}$ drops significantly

**D. Net C efficiency across treatments:**
$$\eta_C = \frac{C_{biochar,stable} - C_{process,emissions}}{C_{feedstock,input}}$$

This accounts for process energy emissions (pyrolysis fuel, transport, etc.)

### 3.2 Why the Current Code Doesn't Calculate Any of This

The existing code calculates **VWC ratios** (biochar-strip / control-strip) which measure the **water retention effect** of biochar in soil, not carbon efficiency. The project site confirms the biochar is pistachio-shell-derived from VGrid Energy Systems.

The current ratios answer: "How much more water does biochar-amended soil hold?"
The missing ratios answer: "How efficiently did different treatments (feedstocks, temperatures, methods) convert biomass carbon into stable biochar carbon?"

### 3.3 Proposed Implementation

To add C efficiency ratio calculations, you need a new module. The data inputs would come from lab analysis of the biochar (which may need to be added to the data pipeline) or from published values for pistachio-shell biochar.

```python
# biochar_app/scripts/carbon_efficiency.py
"""
Carbon efficiency ratio calculations for biochar production treatments.

Implements:
  - C-yield: fraction of feedstock C retained in biochar
  - BC+100: estimated C remaining after 100 years (from H/Corg)
  - Net sequestration efficiency: accounting for process emissions
  
References:
  - Woolf et al. (2021), European J. Soil Sci., doi:10.1111/ejss.13396
  - IBI (2018), Biochar Carbon Stability Test Method
  - ACR Carbon (2023), Methodology for Biochar Projects
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class FeedstockProperties:
    """Characterization of biomass feedstock."""
    name: str
    carbon_content_fraction: float        # C fraction (dry basis), e.g., 0.48
    moisture_content_fraction: float       # wet-basis moisture, e.g., 0.10
    ash_content_fraction: float            # ash fraction (dry basis), e.g., 0.02
    hydrogen_content_fraction: float       # H fraction (dry basis), optional
    oxygen_content_fraction: float         # O fraction (dry basis), optional

    def __post_init__(self):
        if not 0 < self.carbon_content_fraction < 1:
            raise ValueError(f"C content must be 0–1, got {self.carbon_content_fraction}")


@dataclass
class PyrolysisConditions:
    """Treatment / production conditions."""
    treatment_name: str
    temperature_celsius: float             # Peak pyrolysis temperature
    residence_time_minutes: float          # Hold time at peak temperature
    heating_rate_celsius_per_min: float    # Ramp rate
    process_type: str = "slow_pyrolysis"   # slow_pyrolysis, fast_pyrolysis, gasification, HTC


@dataclass
class BiocharProperties:
    """Measured properties of the produced biochar."""
    carbon_content_fraction: float         # C fraction (dry basis)
    hydrogen_content_fraction: float       # H fraction (dry basis)
    oxygen_content_fraction: float         # O fraction (dry basis)
    ash_content_fraction: float            # Ash (dry basis)
    mass_yield_fraction: float             # kg biochar / kg dry feedstock
    fixed_carbon_fraction: float           # From proximate analysis
    volatile_matter_fraction: float        # From proximate analysis

    def h_to_c_org_molar_ratio(self) -> float:
        """
        Molar H/Corg ratio — the key stability indicator.
        H/Corg < 0.4 → highly carbonized (graphite-like)
        H/Corg > 0.7 → less stable
        """
        c_org = self.carbon_content_fraction - (self.ash_content_fraction * 0.0)
        if c_org <= 0:
            return float('inf')
        # Molar ratio: (H/1.008) / (Corg/12.011)
        return (self.hydrogen_content_fraction / 1.008) / (c_org / 12.011)

    def o_to_c_molar_ratio(self) -> float:
        """Molar O/C ratio — secondary stability indicator."""
        if self.carbon_content_fraction <= 0:
            return float('inf')
        return (self.oxygen_content_fraction / 15.999) / (self.carbon_content_fraction / 12.011)


@dataclass
class CarbonEfficiencyResult:
    """Complete carbon efficiency assessment for one treatment."""
    treatment_name: str
    feedstock_name: str

    # Core efficiency metrics
    c_yield: float                         # Fraction of feedstock C retained in biochar
    mass_yield: float                      # kg biochar / kg dry feedstock
    h_corg_ratio: float                    # Molar H/Corg
    o_c_ratio: float                       # Molar O/C

    # Stability & sequestration
    bc_plus_100: float                     # Estimated fraction stable after 100 yr
    net_sequestration_efficiency: float    # c_yield × bc_plus_100

    # Process metadata
    pyrolysis_temp_c: float
    residence_time_min: float

    def to_dict(self) -> Dict:
        return {
            "treatment": self.treatment_name,
            "feedstock": self.feedstock_name,
            "mass_yield_pct": round(self.mass_yield * 100, 1),
            "c_yield_pct": round(self.c_yield * 100, 1),
            "H_Corg_molar": round(self.h_corg_ratio, 3),
            "O_C_molar": round(self.o_c_ratio, 3),
            "BC_plus_100_pct": round(self.bc_plus_100 * 100, 1),
            "net_C_sequestration_pct": round(self.net_sequestration_efficiency * 100, 1),
            "pyrolysis_temp_C": self.pyrolysis_temp_c,
            "residence_time_min": self.residence_time_min,
        }


def estimate_bc_plus_100(h_corg: float) -> float:
    """
    Estimate the fraction of biochar C remaining after 100 years
    based on the H/Corg molar ratio.

    Calibration from IBI (2018) two-component model:
      H/Corg = 0.4 → BC+100 ≈ 0.70
      H/Corg = 0.7 → BC+100 ≈ 0.50

    Linear interpolation within the calibrated range (0.1–0.7);
    clamped outside.
    """
    if h_corg <= 0.1:
        return 0.80  # Highly carbonized (approaching graphite)
    if h_corg >= 0.7:
        return max(0.0, 0.50 - 0.5 * (h_corg - 0.7))  # Rapid decline above 0.7
    # Linear fit through (0.1, 0.80) and (0.7, 0.50)
    return 0.80 - (h_corg - 0.1) * (0.30 / 0.60)


def compute_c_yield(
    feedstock: FeedstockProperties,
    biochar: BiocharProperties,
) -> float:
    """
    Carbon yield = (mass_yield × C_biochar) / C_feedstock

    Returns fraction (0–1).
    """
    if feedstock.carbon_content_fraction <= 0:
        return 0.0
    return (
        biochar.mass_yield_fraction * biochar.carbon_content_fraction
        / feedstock.carbon_content_fraction
    )


def evaluate_treatment(
    feedstock: FeedstockProperties,
    conditions: PyrolysisConditions,
    biochar: BiocharProperties,
    process_c_emissions_kg_per_kg_feedstock: float = 0.0,
) -> CarbonEfficiencyResult:
    """
    Full carbon efficiency evaluation for one treatment.

    Parameters
    ----------
    feedstock : FeedstockProperties
    conditions : PyrolysisConditions
    biochar : BiocharProperties
    process_c_emissions_kg_per_kg_feedstock : float
        CO2-equivalent C emissions per kg of dry feedstock processed.
        Set to 0 if not accounting for process emissions.
    """
    c_yield = compute_c_yield(feedstock, biochar)
    h_corg = biochar.h_to_c_org_molar_ratio()
    o_c = biochar.o_to_c_molar_ratio()
    bc100 = estimate_bc_plus_100(h_corg)

    # Net efficiency: stable C retained minus process C lost
    gross_stable_c = c_yield * bc100
    net_eff = max(0.0, gross_stable_c - process_c_emissions_kg_per_kg_feedstock)

    return CarbonEfficiencyResult(
        treatment_name=conditions.treatment_name,
        feedstock_name=feedstock.name,
        c_yield=c_yield,
        mass_yield=biochar.mass_yield_fraction,
        h_corg_ratio=h_corg,
        o_c_ratio=o_c,
        bc_plus_100=bc100,
        net_sequestration_efficiency=net_eff,
        pyrolysis_temp_c=conditions.temperature_celsius,
        residence_time_min=conditions.residence_time_minutes,
    )


def compare_treatments(
    results: List[CarbonEfficiencyResult],
) -> pd.DataFrame:
    """
    Build a comparison DataFrame from multiple treatment evaluations.
    Sorted by net sequestration efficiency (descending).
    """
    rows = [r.to_dict() for r in results]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("net_C_sequestration_pct", ascending=False)
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Example: pistachio shell biochar used in the Fruita experiment
# ---------------------------------------------------------------------------

def fruita_pistachio_example() -> CarbonEfficiencyResult:
    """
    Example calculation for the VGrid pistachio-shell biochar
    used in the Fruita CSU experiment.

    Values below are representative; replace with actual lab analysis.
    """
    feedstock = FeedstockProperties(
        name="Pistachio shells",
        carbon_content_fraction=0.465,     # ~46.5% C typical for nut shells
        moisture_content_fraction=0.08,
        ash_content_fraction=0.015,
        hydrogen_content_fraction=0.062,
        oxygen_content_fraction=0.44,
    )

    conditions = PyrolysisConditions(
        treatment_name="VGrid slow pyrolysis (pistachio)",
        temperature_celsius=550,           # Typical for VGrid
        residence_time_minutes=60,
        heating_rate_celsius_per_min=10,
        process_type="slow_pyrolysis",
    )

    biochar = BiocharProperties(
        carbon_content_fraction=0.78,      # ~78% C typical for 550°C nut-shell biochar
        hydrogen_content_fraction=0.025,
        oxygen_content_fraction=0.06,
        ash_content_fraction=0.08,
        mass_yield_fraction=0.30,          # ~30% yield typical at 550°C
        fixed_carbon_fraction=0.72,
        volatile_matter_fraction=0.18,
    )

    return evaluate_treatment(feedstock, conditions, biochar)


if __name__ == "__main__":
    # Quick demo
    result = fruita_pistachio_example()
    print(f"C-yield: {result.c_yield:.1%}")
    print(f"H/Corg: {result.h_corg_ratio:.3f}")
    print(f"BC+100: {result.bc_plus_100:.1%}")
    print(f"Net sequestration efficiency: {result.net_sequestration_efficiency:.1%}")
```

### 3.4 Integration with the Existing Dashboard

To integrate C efficiency ratios into the existing FastAPI app, add a new route and template tab:

```python
# In routes.py, add:
from biochar_app.scripts.carbon_efficiency import (
    compare_treatments, evaluate_treatment,
    FeedstockProperties, PyrolysisConditions, BiocharProperties,
)

@app.get("/carbon-efficiency")
async def carbon_efficiency_page(request: Request):
    """Display C efficiency comparison table for different treatments."""
    # Load treatment data from a CSV/JSON config file
    treatments = load_treatment_configs()  # New function to read lab data
    results = [evaluate_treatment(**t) for t in treatments]
    comparison_df = compare_treatments(results)
    
    return templates.TemplateResponse(
        "carbon_efficiency.outputs_html",
        {
            "request": request,
            "table_html": comparison_df.to_html(
                classes="table table-striped",
                index=False,
                float_format="%.1f",
            ),
            "results_json": comparison_df.to_dict(orient="records"),
        },
    )
```

### 3.5 Data Requirements

To calculate real C efficiency ratios, you need lab data that the project may or may not already have:

| Parameter | Source | Currently Available? |
|---|---|---|
| Feedstock C content (%) | Elemental analysis of pistachio shells | Likely from VGrid spec sheet |
| Biochar C content (%) | Ward Labs or similar | May be in soil analysis data |
| Biochar H content (%) | Elemental analysis | Probably not collected |
| Biochar yield (mass) | Production records from VGrid | Likely available |
| H/Corg molar ratio | Calculated from C, H analysis | Needs H analysis |
| Fixed carbon (%) | Proximate analysis (ASTM D1762) | May need to commission |

**Minimum viable calculation:** If you only have biochar C content and feedstock C content, you can compute C-yield and skip the stability metrics. This alone is valuable for comparing treatments.

---

## 4. Additional Recommendations

### 4.1 Add Statistical Testing

The current ratio plots show S1/S2 and S3/S4 over time but provide no confidence intervals or significance tests. For a research project in its fourth year, this is a notable gap.

```python
# In aggregation or a new stats module:
from scipy import stats

def test_treatment_effect(df, var, depth, loc, period_start, period_end):
    """
    Paired t-test or Wilcoxon signed-rank test for biochar vs. control.
    """
    mask = (df.timestamp >= period_start) & (df.timestamp <= period_end)
    s1 = df.loc[mask, f"{var}_{depth}_raw_S1_{loc}"].dropna()
    s2 = df.loc[mask, f"{var}_{depth}_raw_S2_{loc}"].dropna()
    
    # Align on common timestamps
    common_idx = s1.index.intersection(s2.index)
    s1, s2 = s1.loc[common_idx], s2.loc[common_idx]
    
    if len(common_idx) < 10:
        return {"n": len(common_idx), "p_value": None, "significant": None}
    
    stat, p = stats.wilcoxon(s1, s2)
    return {
        "n": len(common_idx),
        "mean_diff": float((s1 - s2).mean()),
        "p_value": float(p),
        "significant": p < 0.05,
    }
```

### 4.2 Soil Carbon Stock Changes

The project already collects soil chemistry data from Ward Labs. If those reports include total organic carbon (TOC) or soil organic matter (SOM), you can track whether the biochar strips are building soil carbon over time — a direct measure of the biochar's C-sequestration effectiveness in the field:

$$\Delta SOC = SOC_{biochar,t2} - SOC_{biochar,t1} - (SOC_{control,t2} - SOC_{control,t1})$$

### 4.3 Water Use Efficiency (WUE) as a Complementary Metric

The project has both irrigation volumes and yield data. A valuable derived metric would be:

$$WUE = \frac{Dry\ biomass\ yield\ (kg/ha)}{Total\ water\ applied\ (mm)}$$

Comparing WUE across biochar vs. control strips would directly quantify biochar's value proposition for western Colorado agriculture.

### 4.4 Config / Constants Improvements

The `DEPTHS` constant uses `["1", "2", "3"]` as string keys mapped to physical depths (6, 12, 18 inches). This is error-prone. Consider a named mapping:

```python
SENSOR_DEPTHS = {
    "1": {"inches": 6, "cm": 15.24},
    "2": {"inches": 12, "cm": 30.48},
    "3": {"inches": 18, "cm": 45.72},
}
```

---

## 5. Summary of Priority Actions

| Priority | Action | Effort |
|---|---|---|
| **High** | Add `carbon_efficiency.py` module (Section 3.3) | Medium |
| **High** | Eliminate duplication between `etl.py` and `process_data.py` | Medium |
| **High** | Add statistical significance testing to ratio comparisons | Low |
| **Medium** | Fix mean-of-ratios vs. ratio-of-means in gseason summaries | Low |
| **Medium** | Reduce unnecessary `.copy()` calls | Low |
| **Medium** | Add soil ΔC tracking from Ward Labs data | Medium |
| **Low** | Simplify `plot_utils.py` with Plotly Express | High |
| **Low** | Clean up PyCharm type-hint workarounds | Low |

---

## Sources

- Woolf et al. (2021), "The importance of biochar quality and pyrolysis yield for soil carbon sequestration," [European J. Soil Sci.](https://bsssjournals.onlinelibrary.wiley.com/doi/10.1111/ejss.13396)
- IBI (2018), "An assessment of methods to determine biochar carbon stability," [Biochar International](https://biochar-international.org/wp-content/uploads/2018/04/IBI_Report_Biochar_Stability_Test_Method_Final.pdf)
- ACR Carbon (2023), "Methodology for Biochar Projects," [ACR Carbon](https://acrcarbon.org/wp-content/uploads/2023/03/Biochar-Methodology-Public-Comment-Draft.pdf)
- Global Biochar C-Sink (2025), "Collection of formulas and emission factors," [Carbon Standards](https://www.carbon-standards.com/docs/transfer/4000115EN.pdf)
- Elshakry et al. (2024), "Biochar production under different pyrolysis temperatures," [Nature Scientific Reports](https://www.nature.com/articles/s41598-024-52336-5)

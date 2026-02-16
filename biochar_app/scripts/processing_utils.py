# biochar_app/scripts/processing_utils.py
import pandas as pd

from biochar_app.scripts.config import (
    UNIT_CONVERSIONS,
    STRIPS,
    LOGGER_LOCATIONS,
    DEPTHS,
    cylinder_volume_m3,
)

def add_swc_cylinder_volumes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # cylinder_volume_m3() should return cm^3
    cyl_vol_cm3 = cylinder_volume_m3()
    cyl_vol_L   = cyl_vol_cm3 / 1_000.0
    cyl_vol_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_vol_L)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in DEPTHS:
                col = f"VWC_{depth}_raw_{strip}_{loc}"
                if col not in df:
                    continue

                frac     = df[col].astype(float) / 100.0
                df[f"SWC_vol_L_{strip}_{loc}_{depth}"]   = frac * cyl_vol_L
                df[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = frac * cyl_vol_gal

    return df

def calculate_ratios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for var in ["VWC", "T", "EC", "SWC"]:
        for (s1, s2) in [("S1", "S2"), ("S3", "S4")]:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    # SWC was only stored at depth '1'
                    if var == "SWC" and d != "1":
                        continue
                    c1  = f"{var}_{d}_raw_{s1}_{loc}"
                    c2  = f"{var}_{d}_raw_{s2}_{loc}"
                    out = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"
                    if c1 in df and c2 in df:
                        df[out] = df[c1] / df[c2]
                    else:
                        df[out] = pd.NA
    return df
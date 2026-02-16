# biochar_app/utils.py

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
import json
from dataclasses import dataclass
from biochar_app.scripts.config import DATA_PROCESSED_DIR, DEFAULT_GSEASON_PERIODS, UNIT_CONVERSIONS, LOGGER_LOCATIONS, DEPTHS
from datetime import datetime
import plotly.graph_objects as go
from typing import List

@dataclass
class LoggerFileInfo:
    filename: str
    start_date: str
    end_date: str
    granularity: str

loaded_datasets = {}


def calculate_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute pairwise strip ratios for each variable/depth/location,
    replacing any infinite results with NA.
    """
    df = df.copy()
    for var in ["VWC", "T", "EC", "SWC"]:
        for s1, s2 in [("S1", "S2"), ("S3", "S4")]:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    # SWC only at depth 1
                    if var == "SWC" and d != "1":
                        continue

                    c1 = f"{var}_{d}_raw_{s1}_{loc}"
                    c2 = f"{var}_{d}_raw_{s2}_{loc}"
                    out = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"

                    if c1 in df.columns and c2 in df.columns:
                        # suppress divide-by-zero warnings, compute ratio
                        with np.errstate(divide='ignore', invalid='ignore'):
                            ratio = df[c1] / df[c2]
                        # replace any ±Inf with NA
                        ratio = ratio.replace([np.inf, -np.inf], pd.NA)
                        df[out] = ratio
                    else:
                        # if either column missing, fill with NA
                        df[out] = pd.NA

    return df


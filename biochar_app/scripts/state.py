# biochar_app/scripts/state.py
from __future__ import annotations

import pandas as pd
from typing import Dict, Tuple

# ─────────────────────────────────────────────────────────────
# In-memory caches shared across the app
# ─────────────────────────────────────────────────────────────

# (year, granularity) -> DataFrame
DATAFRAME_CACHE: Dict[Tuple[int, str], pd.DataFrame] = {}

# year -> granularity/key -> {"min": "YYYY-MM-DD", "max": "YYYY-MM-DD"}
DATE_RANGES: Dict[int, Dict[str, Dict[str, str]]] = {}
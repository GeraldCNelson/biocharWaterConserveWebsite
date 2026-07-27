# biochar_app/scripts/state.py
from __future__ import annotations

import pandas as pd

# ─────────────────────────────────────────────────────────────
# In-memory caches shared across the app
# ─────────────────────────────────────────────────────────────

# (year, granularity) -> DataFrame
DATAFRAME_CACHE: dict[tuple[int, str], pd.DataFrame] = {}

# year -> granularity/key -> {"min": "YYYY-MM-DD", "max": "YYYY-MM-DD"}
DATE_RANGES: dict[int, dict[str, dict[str, str]]] = {}
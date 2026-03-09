#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt

p = "biochar_app/data-processed/parquet/2023_15min.parquet"

df = pd.read_parquet(p)[["timestamp"]].dropna().sort_values("timestamp")
t = pd.to_datetime(df["timestamp"]).drop_duplicates().reset_index(drop=True)

d = t.diff()
g = pd.DataFrame({"start": t.shift(1), "end": t, "dt": d}).dropna()
g = g[g["dt"] > pd.Timedelta(minutes=15)]

plt.figure(figsize=(14,2))
plt.hlines(1, g["start"], g["end"], linewidth=6)
plt.yticks([])
plt.title(f"2023 logger data gaps ({len(g)} gaps)")
plt.xlabel("Date")
plt.tight_layout()
plt.show()
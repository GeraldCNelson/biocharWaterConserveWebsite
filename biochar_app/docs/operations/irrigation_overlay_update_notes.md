# 🛠️ Irrigation Overlay Update Notes

This document summarizes the integration of irrigation overlay functionality into the Plotly plot generation process.

---

## ✅ Function Added: `add_irrigation_overlay`

**Location:** `plot_utils.py`

### Purpose:
This function overlays irrigation volumes onto the plot using vertical dashed lines at corresponding timestamps. It includes:
- Optional volume thresholding (e.g., ignore values below 1000 gallons).
- Label annotations showing the amount of irrigation applied.

---

## 🔧 Supporting Logic

### Example Usage:
This function should be called **after** the plot traces are created and **before** returning the plot JSON:

```python
fig = go.Figure()
# ... add traces
fig = add_irrigation_overlay(fig, variable, kind, strip, year)
```

---

## 🔍 Function Definition
```python
def add_irrigation_overlay(fig, variable, kind, strip, year):
    if kind != "raw" or variable != "VWC":
        return fig  # Only overlay for raw VWC plots

    irrigation_path = os.path.join(DATA_PROCESSED_DIR, f"irrigation_volume_{year}.csv")
    if not os.path.exists(irrigation_path):
        logging.warning(f"⚠️ Irrigation file not found: {irrigation_path}")
        return fig

    try:
        df = pd.read_csv(irrigation_path, parse_dates=["timestamp"])
        df = df[df["strip"] == strip]
        df = df[df["volume_gal"] > 1000]  # threshold

        for _, row in df.iterrows():
            fig.add_shape(
                type="line",
                x0=row["timestamp"], x1=row["timestamp"],
                y0=0, y1=1,
                xref="x", yref="paper",
                line=dict(color="orange", dash="dot"),
                layer="below"
            )
            fig.add_annotation(
                x=row["timestamp"],
                y=1.02,
                xref="x", yref="paper",
                showarrow=False,
                text=f"{int(row['volume_gal'])}k",
                font=dict(size=10, color="orange"),
                align="center"
            )
    except Exception as e:
        logging.exception("❌ Failed to add irrigation overlay")
    
    return fig
```

---

## 📁 Notes
- You can tune the `yref`, `y1`, and annotation `y` values to shift the lines and text as needed.
- Colors, thresholds, and annotation formats are all customizable.
- Assumes irrigation files are named: `irrigation_volume_<year>.csv`.

---

## ✅ Result
You should see orange dotted lines with volume labels near the top of the plot for each significant irrigation event.


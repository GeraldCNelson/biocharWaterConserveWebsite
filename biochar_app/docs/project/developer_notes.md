
# 🛠 Developer Notes

This file collects hard-won solutions and best practices for maintaining and extending the Biochar Water Conservation App frontend and backend.

---

## 📆 Datetime Handling for Plotly (X-Axis)

**Problem**: Plotly expects `Date`-compatible x-values but pandas often uses `datetime64[ns]` or timestamps that must be serialized.

**✅ Solution**:
- Parse timestamps immediately:
  ```python
  df = pd.read_csv(..., parse_dates=["timestamp"])
  df["timestamp"] = pd.to_datetime(df["timestamp"])
  ```
- In `prepare_plot_for_json()`:
  ```python
  trace.x = [pd.Timestamp(x).isoformat() for x in trace.x]
  ```
- In Plotly layout:
  ```python
  xaxis=dict(
      title="Date",
      tickformat="%b %Y",
      tickangle=-30
  )
  ```

---

## 🕳 Handling Missing Values in Plotly

**Problem**: NAs (`np.nan`) in data will silently break Plotly visualizations.

**✅ Solution**:
- Convert all y-values to float or `None`:
  ```python
  trace.y = [None if pd.isna(y) else float(y) for y in trace.y]
  ```

---

## 🔁 Data Trace Labeling (Legend Titles)

Use `build_plot_title_and_legend_label()` to dynamically generate:
- Plot title
- Legend title

Call it from `prepare_plot_figure(...)`.

---

## 🔧 Utility Suggestions

- Move x/y safety handling to a shared helper:
  ```python
  def standardize_x_y_for_plotly(x_vals, y_vals):
      x_safe = [pd.Timestamp(x).isoformat() for x in x_vals]
      y_safe = [None if pd.isna(y) else float(y) for y in y_vals]
      return x_safe, y_safe
  ```

---

## 🧱 Recommended Structure

```
/biochar_app/
├── routes.py
├── templates/
├── static/
│   ├── js/
│   ├── css/
│   └── data/
├── utils/
│   ├── plot_utils.py
│   ├── routes_utils.py
│   ├── config.py
│   └── ...
├── docs/
│   ├── developer_notes.md       ◀ YOU ARE HERE
│   ├── function_docs_report.md  ◀ (Auto-generated from function_doc_checker.py)
│   └── app_structure.md         ◀ (Suggested map of app components)
```

---

## 📌 TODO

- [ ] Add markdown maps for component relationships
- [ ] Document `config.py` vs `config.js`
- [ ] Log known data column inconsistencies (e.g., depth vs depth_mapping)


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

## 🧱 Structure and configuration notes

The maintained current-state map is
[`app_structure.md`](app_structure.md). Do not infer proposed module moves from
this older working-notes file.

Configuration locations with similar names have different roles:

- `biochar_app/config/` is the maintained Python configuration package.
- `biochar_app/scripts/config.py` is a compatibility shim for legacy imports.
- `biochar_app/static/js/config.js` fetches the server-generated narrative-page
  mapping and defines a browser debug flag. It is not a duplicate of the Python
  configuration package.

New Python code should import the specific module under `biochar_app.config`
when practical. Remove legacy shim imports only after an import audit and
regression checks.

---

## 📌 TODO

### Priority 1 — Architecture and documentation restructuring

- [x] Compare the architecture-document headings with the repository directory
  structure and the website navigation, then agree on a harmonized structure
  before undertaking broad code refactoring.
- [x] Organize the documentation around both project components and user needs:
  biochar researchers comparing results, researchers adapting the tools to other
  experiments, field and data-collection users, and project maintainers.
- [x] Incorporate the detailed architecture-document review comments, including
  Markdown table repairs; clearer water-availability terminology; links to the
  ETL and Word-to-Markdown entry points; a fuller description of ETL operations,
  the master workbook, and source-versus-generated files; and revision of the
  laboratory-data workflow.
- [x] Define and document the intended boundaries among the public website,
  scientific analysis, ETL/data preparation, field operations, and reusable
  developer tools.
- [x] Add Markdown maps for component relationships.
- [x] Update or replace superseded developer guidance, including the roles of
  `config.py` and `config.js`.
- [ ] Log known data-column inconsistencies (for example, `depth` versus
  `depth_mapping`).

### Priority 2 — Research analysis and reuse

- [ ] Plan expanded treated-versus-control analyses covering soil health,
  nutrient requirements, and fertilizer effects, while stating the limits of
  the experimental design and replication.
- [ ] Identify the experiment-specific configuration needed to reuse the data
  pipeline and analysis tools in another agricultural experiment.
- [ ] Test portability with a second example dataset, such as a rainfed maize
  experiment, and document its minimum data-collection requirements.
- [ ] Make reusable tools discoverable through a user-oriented catalog with
  inputs, outputs, examples, limitations, and installation or access guidance.
- [ ] Document maintenance roles, expected recurring work, hardware inventory,
  and the factors that drive operating and replacement costs.

### Priority 3 — Prepare field-data tools for the next field season

Field work is largely complete for the current year. Use the off-season to
design and test these tools, with operational deployment targeted for the next
field season.

- [ ] Write a field-capture workflow and data dictionary before changing the
  application: required fields, timestamps, review responsibilities, validation
  rules, offline behavior, and the designated-record boundary.
- [ ] Stabilize the existing irrigation-entry prototype, including its page
  route, authentication, start/end event linkage, photo storage, validation,
  and automated tests.
- [ ] Make the phone interface installable and offline-capable, with durable
  queued events and photographs, retry-safe synchronization, visible sync
  status, and retention of original images and provenance.
- [ ] Add a general field-observation workflow for multiple photographs,
  repeatable photo points, crop and soil observations, notes, and optional
  location data.
- [ ] Integrate reviewed field records and photographs with the canonical photo
  inventory, quality-control process, ETL pipeline, and research datasets.
- [ ] Conduct a pre-season field test that includes poor connectivity, closing
  and reopening the app, duplicate-upload prevention, and recovery from an
  interrupted synchronization.
- [ ] Define a question-led drone pilot using repeatable RGB flights and paired
  ground observations; document flight metadata, source-file preservation,
  processing provenance, strip-level outputs, costs, maintenance, and operating
  requirements before considering more specialized sensors.

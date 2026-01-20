// static/js/nir_tab.js
//
// Updated to the new standard payload shape (same as soilchem/soilbio):
// {
//   title: "Pasture Quality Metrics",
//   sets: [
//     {
//       key: "nir_set1",
//       label: "Pasture Quality Metrics — Set 1",
//       periods: [{key,label}, ...],
//       variables: [{key,label}, ...],
//       rows: ["strip_1", ...],
//       rowLabels: {"strip_1":"STRIP 1", ...},
//       data: { varKey: { rowKey: { periodKey: value|null } } }
//     },
//     ...
//   ]
// }

function isObject(x) {
  return x !== null && typeof x === "object" && !Array.isArray(x);
}

function formatNumber(v) {
  if (v === null || v === undefined) return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(2);
}

function validatePayloadShape(payload) {
  if (!isObject(payload)) return false;
  if (typeof payload.title !== "string") return false;
  if (!Array.isArray(payload.sets)) return false;

  for (const s of payload.sets) {
    if (!isObject(s)) return false;
    if (typeof s.key !== "string") return false;
    if (typeof s.label !== "string") return false;
    if (!Array.isArray(s.periods)) return false;
    if (!Array.isArray(s.variables)) return false;
    if (!Array.isArray(s.rows)) return false;
    if (!isObject(s.rowLabels)) return false;
    if (!isObject(s.data)) return false;
  }

  return true;
}

function buildTableForVariable(setPayload, variableKey, variableLabel) {
  const periods = setPayload.periods || [];
  const rows = setPayload.rows || [];
  const rowLabels = setPayload.rowLabels || {};
  const data = setPayload.data || {};

  const varBlock = data[variableKey];
  if (!isObject(varBlock)) {
    const div = document.createElement("div");
    div.className = "alert alert-warning";
    div.textContent = `No data found for variable: ${variableLabel}`;
    return div;
  }

  const wrapper = document.createElement("div");
  wrapper.className = "nir-var-block";

  const h5 = document.createElement("h5");
  h5.className = "nir-var-title";
  h5.textContent = variableLabel;
  wrapper.appendChild(h5);

  const table = document.createElement("table");
  table.className = "table table-sm table-striped table-bordered align-middle nir-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");

  const th0 = document.createElement("th");
  th0.textContent = "Row";
  // If you want sticky first column like soil tables, you can add inline style:
  // th0.style.position = "sticky"; th0.style.left = "0"; th0.style.background = "white"; th0.style.zIndex = "2";
  headRow.appendChild(th0);

  for (const p of periods) {
    const th = document.createElement("th");
    th.textContent = (p && (p.label || p.key)) ? (p.label || p.key) : "";
    headRow.appendChild(th);
  }

  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");

  for (const r of rows) {
    const tr = document.createElement("tr");

    const tdLabel = document.createElement("td");
    tdLabel.textContent = rowLabels[r] || r;
    tr.appendChild(tdLabel);

    const rowBlock = varBlock[r];
    for (const p of periods) {
      const key = p?.key;
      const td = document.createElement("td");
      const raw = rowBlock && key ? rowBlock[key] : null;
      td.textContent = formatNumber(raw);
      tr.appendChild(td);
    }

    tbody.appendChild(tr);
  }

  table.appendChild(tbody);
  wrapper.appendChild(table);

  return wrapper;
}

async function fetchJson(url) {
  const resp = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`GET ${url} failed: ${resp.status} ${text}`);
  }

  return await resp.json();
}

function makeSetSectionTitle(titleText, subtitleText = "") {
  const section = document.createElement("div");
  section.className = "nir-set-section";

  const h4 = document.createElement("h4");
  h4.className = "nir-set-title";
  h4.textContent = titleText;
  section.appendChild(h4);

  if (subtitleText) {
    const p = document.createElement("p");
    p.className = "text-muted nir-set-subtitle";
    p.textContent = subtitleText;
    section.appendChild(p);
  }

  return section;
}

function renderOneSetFromPayload(sectionEl, setPayload) {
  // tables per variable
  for (const v of setPayload.variables) {
    const key = v?.key;
    const label = v?.label || key;
    if (!key) continue;

    const block = buildTableForVariable(setPayload, key, label);
    sectionEl.appendChild(block);
  }
}

/**
 * Public: render NIR tables into the NIR tab.
 * New standard uses ONE endpoint returning all sets:
 *   GET /api/get_nir_table
 */
export async function renderNirTables() {
  const container = document.getElementById("nir-content");
  if (!container) {
    console.warn("NIR container #nir-content not found.");
    return;
  }

  // Prevent double-render if the user clicks tabs repeatedly
  if (container.dataset.rendered === "true") return;

  container.innerHTML = "";

  // Loading indicator
  const loading = document.createElement("div");
  loading.className = "text-muted";
  loading.textContent = "Loading…";
  container.appendChild(loading);

  try {
    const payload = await fetchJson("/api/get_nir_table");
    loading.remove();

    if (!validatePayloadShape(payload)) {
      const pre = document.createElement("pre");
      pre.className = "alert alert-warning";
      pre.textContent =
        "Endpoint returned JSON but not in a recognized shape.\n\n" +
        JSON.stringify(payload, null, 2);
      container.appendChild(pre);
      return;
    }

    // Optional top title (if you want it displayed inside the tab)
    // const top = document.createElement("h4");
    // top.className = "mb-3";
    // top.textContent = payload.title;
    // container.appendChild(top);

    for (const setPayload of payload.sets) {
      // If you want custom subtitles, you can add a subtitle field in the backend later.
      const section = makeSetSectionTitle(setPayload.label);
      container.appendChild(section);
      renderOneSetFromPayload(section, setPayload);
    }

    container.dataset.rendered = "true";
  } catch (err) {
    console.error("Failed to render NIR tables:", err);
    loading.remove();

    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load NIR tables. Check server logs.";
    container.appendChild(div);
  }
}
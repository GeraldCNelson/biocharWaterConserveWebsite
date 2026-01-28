// static/js/nir_tab.js
//
// Robust renderer for the Pasture Quality Metrics tab.
//
// Supports BOTH payload shapes:
//
// 1) Standard multi-set:
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
//
// 2) Legacy / single-set:
// {
//   title: "...",
//   periods: [...],
//   variables: [...],
//   rows: [...],
//   rowLabels: {...},
//   data: {...}
// }
//
// Also tolerates drift where set.key/set.label/variables/rows/rowLabels are missing,
// and derives them from `data` where possible.

import { makeSetSectionTitle } from "./tab_ui.js";

function isObject(x) {
  return x !== null && typeof x === "object" && !Array.isArray(x);
}

function formatNumber(v) {
  if (v === null || v === undefined) return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(2);
}

function safeStr(x, fallback = "") {
  return (typeof x === "string" && x.trim()) ? x : fallback;
}

function normalizePeriod(p) {
  if (!p) return { key: "", label: "" };
  if (typeof p === "string") return { key: p, label: p };
  if (isObject(p)) {
    const key = safeStr(p.key, "");
    const label = safeStr(p.label, key || "");
    return { key, label };
  }
  return { key: "", label: "" };
}

function normalizeVariable(v) {
  if (!v) return { key: "", label: "" };
  if (typeof v === "string") return { key: v, label: v };
  if (isObject(v)) {
    const key = safeStr(v.key, "");
    const label = safeStr(v.label, key || "");
    return { key, label };
  }
  return { key: "", label: "" };
}

function deriveVariablesFromData(dataObj) {
  if (!isObject(dataObj)) return [];
  const keys = Object.keys(dataObj);
  return keys.map((k) => ({ key: k, label: k }));
}

function deriveRowsFromData(dataObj) {
  // data: { varKey: { rowKey: { periodKey: value } } }
  if (!isObject(dataObj)) return [];
  const vars = Object.keys(dataObj);
  const rowSet = new Set();
  for (const v of vars) {
    const varBlock = dataObj[v];
    if (!isObject(varBlock)) continue;
    for (const r of Object.keys(varBlock)) rowSet.add(r);
  }
  return Array.from(rowSet);
}

function deriveRowLabels(rows) {
  const out = {};
  for (const r of rows) out[r] = r;
  return out;
}

function normalizeOneSet(setPayload, setIndex = 0) {
  const s = isObject(setPayload) ? { ...setPayload } : {};

  // key / label
  if (typeof s.key !== "string" || !s.key.trim()) {
    s.key = `nir_set_${setIndex + 1}`;
  }
  if (typeof s.label !== "string" || !s.label.trim()) {
    s.label = `Set ${setIndex + 1}`;
  }

  // periods
  if (!Array.isArray(s.periods)) s.periods = [];
  s.periods = s.periods.map(normalizePeriod).filter((p) => p.key);

  // data
  if (!isObject(s.data)) s.data = {};

  // variables: if missing, derive from data keys
  if (!Array.isArray(s.variables) || s.variables.length === 0) {
    const derived = deriveVariablesFromData(s.data);
    if (derived.length > 0) {
      console.warn("[nir_tab] set.variables missing; derived from data keys:", derived.map(d => d.key));
    }
    s.variables = derived;
  }
  s.variables = s.variables.map(normalizeVariable).filter((v) => v.key);

  // rows: if missing, derive from data
  if (!Array.isArray(s.rows) || s.rows.length === 0) {
    const derivedRows = deriveRowsFromData(s.data);
    if (derivedRows.length > 0) {
      console.warn("[nir_tab] set.rows missing; derived from data row keys:", derivedRows);
    }
    s.rows = derivedRows;
  }

  // rowLabels: if missing, derive identity labels
  if (!isObject(s.rowLabels)) {
    console.warn("[nir_tab] set.rowLabels missing; using identity labels.");
    s.rowLabels = deriveRowLabels(s.rows);
  } else {
    // ensure every row has a label
    for (const r of s.rows) {
      if (!s.rowLabels[r]) s.rowLabels[r] = r;
    }
  }

  return s;
}

function normalizePayload(payload) {
  // Returns canonical shape:
  // { title: string, sets: [ {key,label,periods,variables,rows,rowLabels,data} ... ] }
  if (!isObject(payload)) return null;

  const title = safeStr(payload.title, "Pasture Quality Metrics");

  // If already in standard shape
  if (Array.isArray(payload.sets)) {
    const sets = payload.sets.map((s, i) => normalizeOneSet(s, i));
    return { title, sets };
  }

  // If it looks like a single-set payload
  const looksSingleSet =
    Array.isArray(payload.periods) ||
    Array.isArray(payload.variables) ||
    Array.isArray(payload.rows) ||
    isObject(payload.data);

  if (looksSingleSet) {
    const single = normalizeOneSet(
      {
        key: "nir_set_1",
        label: title,
        periods: payload.periods,
        variables: payload.variables,
        rows: payload.rows,
        rowLabels: payload.rowLabels,
        data: payload.data,
      },
      0
    );
    return { title, sets: [single] };
  }

  return null;
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

function renderOneSetFromPayload(sectionEl, setPayload) {
  // tables per variable
  for (const v of setPayload.variables || []) {
    const key = v?.key;
    const label = v?.label || key;
    if (!key) continue;

    const block = buildTableForVariable(setPayload, key, label);
    sectionEl.appendChild(block);
  }
}

/**
 * Public: render NIR tables into the NIR tab.
 * Standard uses ONE endpoint returning all sets:
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
    const rawPayload = await fetchJson("/api/get_nir_table");
    loading.remove();

    const payload = normalizePayload(rawPayload);

    if (!payload || !Array.isArray(payload.sets) || payload.sets.length === 0) {
      const pre = document.createElement("pre");
      pre.className = "alert alert-warning";
      pre.textContent =
        "Endpoint returned JSON but not in a recognized shape.\n\n" +
        JSON.stringify(rawPayload, null, 2);
      container.appendChild(pre);
      return;
    }

    for (const setPayload of payload.sets) {
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
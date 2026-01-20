// static/js/soil_tab.js
//
// Updated to the new standard payload shape (parallel to updated nir_tab.js):
// {
//   title: "Soil Biological Health",
//   sets: [
//     {
//       key: "functional_groups_biomass",
//       label: "Functional Groups — Biomass (Phospholipid Fatty Acids; nanogram per gram soil)",
//       periods: [{key,label}, ...],
//       variables: [{key,label}, ...],
//       rows: ["strip_1", ...],
//       rowLabels: {"strip_1":"STRIP 1", ...},
//       data: { varKey: { rowKey: { periodKey: value|null } } }
//     },
//     ...
//   ]
// }

import { debugLog } from "./plots.js";

function isObject(x) {
  return x !== null && typeof x === "object" && !Array.isArray(x);
}

function humanizeKey(key) {
  if (!key) return "";
  // snake_case -> Title Case, keep common acronyms readable
  const s = String(key)
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

  return s.replace(/\b\w/g, (m) => m.toUpperCase());
}

function formatValue(v, decimals = 3) {
  if (v === null || v === undefined || Number.isNaN(v)) return "";

  // If backend accidentally sends strings, still round nicely.
  const n = Number(v);
  if (Number.isFinite(n)) return n.toFixed(decimals);

  return String(v);
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
  wrapper.className = "mb-4";

  const h5 = document.createElement("h5");
  h5.className = "mb-2";
  // IMPORTANT: use provided label (includes units). Fallback to humanized key.
  h5.textContent = variableLabel || humanizeKey(variableKey);
  wrapper.appendChild(h5);

  const tableResponsive = document.createElement("div");
  tableResponsive.className = "table-responsive";
  wrapper.appendChild(tableResponsive);

  const table = document.createElement("table");
  table.className = "table table-sm table-striped table-bordered align-middle";
  tableResponsive.appendChild(table);

  // ---- THEAD
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");

  const th0 = document.createElement("th");
  th0.textContent = "Row";
  th0.style.position = "sticky";
  th0.style.left = "0";
  th0.style.background = "white";
  th0.style.zIndex = "2";
  headRow.appendChild(th0);

  for (const p of periods) {
    const th = document.createElement("th");
    th.textContent = (p && (p.label || p.key)) ? (p.label || p.key) : "";
    headRow.appendChild(th);
  }

  thead.appendChild(headRow);
  table.appendChild(thead);

  // ---- TBODY
  const tbody = document.createElement("tbody");

  for (const r of rows) {
    const tr = document.createElement("tr");

    const tdLabel = document.createElement("td");
    tdLabel.innerHTML = `<b>${rowLabels[r] || r}</b>`;
    tdLabel.style.position = "sticky";
    tdLabel.style.left = "0";
    tdLabel.style.background = "white";
    tdLabel.style.zIndex = "1";
    tr.appendChild(tdLabel);

    const rowBlock = varBlock[r] || {};
    for (const p of periods) {
      const key = p?.key;
      const td = document.createElement("td");
      const raw = key ? rowBlock[key] : null;

      // Round everything; this fixes S1/S2 and S3/S4 long decimals.
      td.textContent = formatValue(raw, 3);
      tr.appendChild(td);
    }

    tbody.appendChild(tr);
  }

  table.appendChild(tbody);

  return wrapper;
}

function makeSetSectionTitle(titleText, subtitleText = "") {
  const section = document.createElement("div");
  section.className = "soil-set-section mb-4";

  const h4 = document.createElement("h4");
  h4.className = "soil-set-title mb-2";
  h4.textContent = titleText;
  section.appendChild(h4);

  if (subtitleText) {
    const p = document.createElement("p");
    p.className = "text-muted soil-set-subtitle";
    p.textContent = subtitleText;
    section.appendChild(p);
  }

  return section;
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`${url} failed: ${res.status} ${res.statusText} — ${txt}`);
  }
  return await res.json();
}

function renderAllSetsIntoContainer(container, payload) {
  for (const setPayload of payload.sets) {
    // Sets indicated here:
    const section = makeSetSectionTitle(setPayload.label);
    container.appendChild(section);

    // One table per variable within the set
    for (const v of setPayload.variables || []) {
      const key = v?.key;
      if (!key) continue;

      // Variable titles should NOT be snake_case; labels from backend include units.
      const label = v?.label || humanizeKey(key);
      section.appendChild(buildTableForVariable(setPayload, key, label));
    }
  }
}

export async function renderSoilChemTable() {
  const container = document.getElementById("soilchem-content");
  if (!container) return;

  if (container.dataset.rendered === "true") return;

  container.innerHTML = `<div class="text-muted">Loading soil chemistry…</div>`;
  try {
    const payload = await fetchJson("/api/get_soilchem_table");
    container.innerHTML = "";

    if (!validatePayloadShape(payload)) {
      container.innerHTML =
        `<pre class="alert alert-warning">` +
        `Endpoint returned JSON but not in a recognized shape.\n\n` +
        `${JSON.stringify(payload, null, 2)}` +
        `</pre>`;
      return;
    }

    renderAllSetsIntoContainer(container, payload);

    container.dataset.rendered = "true";
    debugLog("✅ Soil chemistry tables rendered (sets)");
  } catch (err) {
    console.error("❌ renderSoilChemTable failed:", err);
    container.innerHTML = `<div class="text-danger">Failed to load soil chemistry table.</div>`;
  }
}

export async function renderSoilBioTable() {
  const container = document.getElementById("soilbio-content");
  if (!container) return;

  if (container.dataset.rendered === "true") return;

  container.innerHTML = `<div class="text-muted">Loading soil biological health…</div>`;
  try {
    const payload = await fetchJson("/api/get_soilbio_table");
    container.innerHTML = "";

    if (!validatePayloadShape(payload)) {
      container.innerHTML =
        `<pre class="alert alert-warning">` +
        `Endpoint returned JSON but not in a recognized shape.\n\n` +
        `${JSON.stringify(payload, null, 2)}` +
        `</pre>`;
      return;
    }

    renderAllSetsIntoContainer(container, payload);

    container.dataset.rendered = "true";
    debugLog("✅ Soil biological health tables rendered (sets)");
  } catch (err) {
    console.error("❌ renderSoilBioTable failed:", err);
    container.innerHTML = `<div class="text-danger">Failed to load soil biological health table.</div>`;
  }
}
// static/js/soil_tab.js
//
// Payload shape (new standard):
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
  const s = String(key)
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();

  return s.replace(/\b\w/g, (m) => m.toUpperCase());
}

function looksLikePercent(variableKey, variableLabel) {
  const k = (variableKey || "").toLowerCase();
  const l = (variableLabel || "").toLowerCase();
  return (
      k.endsWith("_pct") ||
      k.includes("percent") ||
      l.includes("percent") ||
      l.includes("%")
  );
}

function formatValue(v, decimals = 3) {
  if (v === null || v === undefined || Number.isNaN(v)) return "";

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

/**
 * If a set already indicates “— Percent ...”, don't repeat “— Percent” in each variable title.
 * Example:
 *   Set: "Functional Groups — Percent (percent of total PLFA)"
 *   Var: "Total Bacteria — Percent (percent)"
 * becomes:
 *   "Total Bacteria (percent)"
 */
function dedupePercentTitle(setLabel, variableLabel) {
  const s = String(setLabel || "");
  let v = String(variableLabel || "");

  const setHasPercent = /—\s*Percent\b/i.test(s);
  const varHasPercentDash = /—\s*Percent\b/i.test(v);

  if (setHasPercent && varHasPercentDash) {
    v = v.replace(/—\s*Percent\b\s*/i, "").trim();
  }

  // Optional extra polish: if set already explains percent context, keep var suffix minimal
  // e.g. "(percent)" is fine; but if you ever had "(percent of total PLFA)" in both, you'd handle it here.

  return v;
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
    div.textContent = `No data found for variable: ${variableLabel || variableKey}`;
    return div;
  }

  const wrapper = document.createElement("div");
  wrapper.className = "mb-4";

  // Title
  const h5 = document.createElement("h5");
  h5.className = "mb-2";

  const rawLabel = variableLabel || humanizeKey(variableKey);
  h5.textContent = dedupePercentTitle(setPayload.label, rawLabel);
  wrapper.appendChild(h5);

  // ✅ NEW: variable-level note (if provided by backend)
  // Expect setPayload.variables items to optionally include: {key, label, note}
  const vars = Array.isArray(setPayload.variables) ? setPayload.variables : [];
  const vMeta = vars.find((v) => v && typeof v === "object" && v.key === variableKey);
  const vNote = vMeta && typeof vMeta.note === "string" ? vMeta.note.trim() : "";

  if (vNote) {
    const note = document.createElement("div");
    note.className = "text-muted small mb-2 soil-variable-note";
    note.textContent = vNote;
    wrapper.appendChild(note);
  }

  // Responsive table wrapper
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

  // Choose decimals: percent → 2, everything else → 3
  const decimals = looksLikePercent(variableKey, rawLabel) ? 2 : 3;

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

      // This keeps S1/S2 and S3/S4 nicely rounded, too.
      td.textContent = formatValue(raw, decimals);
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

function renderOneSetIntoSection(sectionEl, setPayload) {
  // tables per variable
  const vars = Array.isArray(setPayload.variables) ? setPayload.variables : [];

  for (const v of vars) {
    const key = v?.key;
    const label = v?.label || key;
    if (!key) continue;

    const block = buildTableForVariable(setPayload, key, label);
    sectionEl.appendChild(block);
  }
}

function renderAllSetsIntoContainer(container, payload) {
  for (const setPayload of payload.sets) {
    const section = makeSetSectionTitle(setPayload.label);
    section.classList.add("mb-4");

    // ✅ Notes directly under the section title, like the Soil Chem page
    if (setPayload.notes) {
      const p = document.createElement("p");
      p.className = "text-muted soil-set-notes";
      p.textContent = setPayload.notes;
      section.appendChild(p);
    }

    // Render tables for this set (soil_tab.js implementation)
    renderOneSetIntoSection(section, setPayload);

    container.appendChild(section);
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
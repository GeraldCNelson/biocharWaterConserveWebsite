// static/js/nir_tab.js

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
  if (!Array.isArray(payload.periods)) return false;
  if (!Array.isArray(payload.variables)) return false;
  if (!Array.isArray(payload.rows)) return false;
  if (!isObject(payload.data)) return false;
  return true;
}

function buildTableForVariable(payload, variableKey, variableLabel) {
  const periods = payload.periods || [];
  const rows = payload.rows || [];
  const data = payload.data || {};

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
    tdLabel.textContent = r;
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

async function renderOneSet(sectionEl, endpointUrl) {
  // loading indicator inside section
  const loading = document.createElement("div");
  loading.className = "text-muted";
  loading.textContent = "Loading…";
  sectionEl.appendChild(loading);

  try {
    const payload = await fetchJson(endpointUrl);

    // remove loading
    loading.remove();

    if (!validatePayloadShape(payload)) {
      const pre = document.createElement("pre");
      pre.className = "alert alert-warning";
      pre.textContent =
        "Endpoint returned JSON but not in a recognized shape.\n\n" +
        JSON.stringify(payload, null, 2);
      sectionEl.appendChild(pre);
      return;
    }

    // tables per variable
    for (const v of payload.variables) {
      const key = v?.key;
      const label = v?.label || key;
      if (!key) continue;

      const block = buildTableForVariable(payload, key, label);
      sectionEl.appendChild(block);
    }
  } catch (err) {
    console.error("Failed to render NIR set:", endpointUrl, err);
    loading.remove();
    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load NIR tables. Check server logs.";
    sectionEl.appendChild(div);
  }
}

/**
 * Public: render Sets 1–4 into the NIR tab.
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

  // --- Set 1 ---
  const set1 = makeSetSectionTitle(
    "Set 1: Pasture Quality Metrics",
    "Core forage-quality indicators (dry-basis where applicable)."
  );
  container.appendChild(set1);
  await renderOneSet(set1, "/api/get_nir_set1_table");

  // --- Set 2 ---
  const set2 = makeSetSectionTitle(
    "Set 2: Carbohydrates & Energy Partitioning",
    "Energy-related metrics and carbohydrate fractions."
  );
  container.appendChild(set2);
  await renderOneSet(set2, "/api/get_nir_set2_table");

  // --- Set 3 ---
  const set3 = makeSetSectionTitle(
    "Set 3: Minerals & Ash",
    "Ash/mineral indicators (placeholders until finalized)."
  );
  container.appendChild(set3);
  await renderOneSet(set3, "/api/get_nir_set3_table");

  // --- Set 4 ---
  const set4 = makeSetSectionTitle(
    "Set 4: Digestibility Metrics",
    "Digestibility-oriented indicators (placeholders until finalized)."
  );
  container.appendChild(set4);
  await renderOneSet(set4, "/api/get_nir_set4_table");

  container.dataset.rendered = "true";
}
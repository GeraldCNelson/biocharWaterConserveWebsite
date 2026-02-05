// api_requests.js
import { getDropdownValue } from "./ui_utils.js";

/**
 * Simple helper to capitalize the first letter of a string.
 */
function capitalizeFirst(str) {
  return str ? str.charAt(0).toUpperCase() + str.slice(1) : "";
}

/**
 * Update the Summary tab table + title for non-gseason and gseason cases.
 * (Used when you want to drive things explicitly from data already fetched.)
 */
export function updateMainDataDisplay(data, options) {
  console.log("📊 Updating Main Data Display...");

  const year        = getDropdownValue("summary-year", true);
  const variable    = getDropdownValue("summary-variable");
  const strip       = getDropdownValue("summary-strip");
  const granularity = getDropdownValue("summary-granularity");

  const depthLabel =
    document.getElementById("summary-depth")?.selectedOptions[0]?.textContent || "";

  const unit = window.unitSystem || "us";

  // Resolve variable label safely (supports string OR {us,metric} mapping)
  const rawLabelEntry = options?.labelNameMapping?.[variable];
  let variableLabel = variable;

  if (rawLabelEntry && typeof rawLabelEntry === "object") {
    variableLabel = rawLabelEntry[unit] || rawLabelEntry.us || rawLabelEntry.metric || variable;
  } else if (typeof rawLabelEntry === "string") {
    variableLabel = rawLabelEntry;
  }

  // Title
  const stripPart = strip ? `, Strip ${strip}` : "";
  const mainTitle =
    data?.title ||
    `${capitalizeFirst(granularity)} Summary for ${variableLabel}${stripPart}, ${depthLabel}, ${year}`;

  console.log("✅ variable label:", variableLabel);
  console.log("✅ main title:", mainTitle);

  const titleEl = document.getElementById("summary-title");
  if (titleEl) titleEl.textContent = mainTitle;

  const isTempVariable = ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"].includes(variable);

  // 🌱 Growing-season layout uses the accordion renderer
  if (granularity === "gseason") {
    console.log("🌱 Detected growing season granularity. Building accordion layout…");

    const seasonStats = data?.gseason_stats || {};
    const accordionHTML = generateSeasonalSummaryAccordion(seasonStats, variable);

    const container = document.getElementById("summary-table-container");
    if (container) container.innerHTML = accordionHTML;
    return;
  }

  // 📅 Default (non-gseason) layout
  const container = document.getElementById("summary-table-container");
  if (!container) return;

  // IMPORTANT: If you're using innerHTML, all tables must be HTML strings
  const rawTableHTML = generateSummaryTable(data?.raw_statistics, variable, { returnType: "html" });

  const s1s2 = {};
  const s3s4 = {};
  for (const [key, value] of Object.entries(data?.ratio_statistics || {})) {
    if (key.includes("S1_S2")) s1s2[key] = value;
    else if (key.includes("S3_S4")) s3s4[key] = value;
  }

  const s1s2HTML =
    Object.keys(s1s2).length > 0
      ? generateSummaryTable(s1s2, variable, { returnType: "html" })
      : isTempVariable
        ? `<p class="text-muted">Soil temperature ratios are not shown because they are not meaningful.</p>`
        : `<p class="text-danger">No summary statistics available.</p>`;

  const s3s4HTML =
    Object.keys(s3s4).length > 0
      ? generateSummaryTable(s3s4, variable, { returnType: "html" })
      : isTempVariable
        ? `<p class="text-muted">Soil temperature ratios are not shown because they are not meaningful.</p>`
        : `<p class="text-danger">No summary statistics available.</p>`;

  const ratioHTML = `
    <h5 class="mt-4">S1/S2 Ratio (Top/Mid/Bottom Logger)</h5>
    ${s1s2HTML}
    <h5 class="mt-4">S3/S4 Ratio (Top/Mid/Bottom Logger)</h5>
    ${s3s4HTML}
  `;

  container.innerHTML = `
    <h5>Raw Values (Top/Mid/Bottom Logger)</h5>
    ${rawTableHTML}
    ${ratioHTML}
  `;
}

/**
 * Helper: format the accordion header title
 * e.g. "Winter (Nov–Apr) — Volumetric Water Content (%)"
 */
export function formatGseasonLabel(code, spec, prettyVarWithContext) {
  const label = (spec && spec.label) || code.replace(/_/g, " ");

  const monthNames = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
  ];

  function fmtMonth(md) {
    if (!md) return "";
    const [m] = md.split("-");
    const idx = parseInt(m, 10) - 1;
    return monthNames[idx] || md;
  }

  const startPart = spec?.start ? fmtMonth(spec.start) : "";
  const endPart   = spec?.end   ? fmtMonth(spec.end)   : "";

  const range =
    startPart && endPart ? ` (${startPart}–${endPart})` : "";

  return prettyVarWithContext
    ? `${label}${range} — ${prettyVarWithContext}`
    : `${label}${range}`;
}

/**
 * Build the seasonal (gseason) accordion HTML from pre-grouped stats.
 *
 * @param {Object} stats
 *   Object keyed by season code (Q1_Winter, …) → {
 *       raw_statistics: { … },
 *       ratio_statistics: { … }
 *   }
 * @param {string} variable
 *   Variable name (VWC, EC, T, SWC, …)
 */
export function generateSeasonalSummaryAccordion(stats, variable) {
  const periods  = window.gseasonPeriods   || {};
  const labelMap = window.labelNameMapping || {};
  const unit     = window.unitSystem || "us";

  // Detect Bootstrap major version by presence of window.bootstrap (v5) vs jQuery collapse (v4)
  const isBs5 = typeof window.bootstrap !== "undefined" && typeof window.bootstrap.Collapse !== "undefined";
  const isBs4 = !isBs5 && typeof window.$ !== "undefined" && typeof window.$.fn !== "undefined" && typeof window.$.fn.collapse !== "undefined";

  // Resolve variable label (unit-aware)
  const rawEntry = labelMap[variable];
  let prettyVar = variable;
  if (rawEntry && typeof rawEntry === "object") {
    prettyVar = rawEntry[unit] || rawEntry.us || rawEntry.metric || variable;
  } else if (typeof rawEntry === "string") {
    prettyVar = rawEntry;
  }

  // Add depth + year context for the seasonal headers
  const yearEl  = document.getElementById("summary-year");
  const depthEl = document.getElementById("summary-depth");

  const year      = yearEl ? yearEl.value : "";
  const depthText = depthEl?.selectedOptions[0]?.textContent?.trim() || "";

  const extras = [];
  if (depthText) extras.push(depthText);
  if (year) extras.push(year);

  const prettyVarWithContext = extras.length
    ? `${prettyVar} (${extras.join(", ")})`
    : prettyVar;

  const periodEntries = Object.entries(periods);
  if (!periodEntries.length) {
    return `<p class="text-muted">No seasonal periods are defined.</p>`;
  }

  const idBase = "gseason-accordion";
  let html = `<div class="accordion" id="${idBase}">`;

  periodEntries.forEach(([code, spec], idx) => {
    const headingId  = `${idBase}-${code}-heading`;
    const collapseId = `${idBase}-${code}`;

    const seasonStats = stats?.[code] || {};
    const raw   = seasonStats.raw_statistics   || {};
    const ratio = seasonStats.ratio_statistics || {};

    const rawHTML = Object.keys(raw).length
      ? generateSummaryTable(raw, prettyVar, { returnType: "html" })
      : `<p class="text-muted mb-0">No raw data available for this period.</p>`;

    // Split ratio stats into S1/S2 vs S3/S4
    const s1s2 = {};
    const s3s4 = {};
    for (const [trace, val] of Object.entries(ratio)) {
      if (trace.includes("S1_S2")) s1s2[trace] = val;
      else if (trace.includes("S3_S4")) s3s4[trace] = val;
    }

    const s1s2HTML = Object.keys(s1s2).length
      ? generateSummaryTable(s1s2, prettyVar, { returnType: "html" })
      : `<p class="text-muted mb-0">No summary statistics available.</p>`;

    const s3s4HTML = Object.keys(s3s4).length
      ? generateSummaryTable(s3s4, prettyVar, { returnType: "html" })
      : `<p class="text-muted mb-0">No summary statistics available.</p>`;

    const title = formatGseasonLabel(code, spec, prettyVarWithContext);

    const isFirst = idx === 0;

    // Bootstrap attribute compatibility
    const toggleAttr = isBs5 ? "data-bs-toggle" : "data-toggle";
    const targetAttr = isBs5 ? "data-bs-target" : "data-target";

    const btnClasses  = `accordion-button${isFirst ? "" : " collapsed"}`;
    const bodyClasses = `accordion-collapse collapse${isFirst ? " show" : ""}`;

    html += `
      <div class="accordion-item">
        <h2 class="accordion-header" id="${headingId}">
          <button
            class="${btnClasses}"
            type="button"
            ${toggleAttr}="collapse"
            ${targetAttr}="#${collapseId}"
            aria-expanded="${isFirst ? "true" : "false"}"
            aria-controls="${collapseId}">
            ${title}
          </button>
        </h2>
        <div
          id="${collapseId}"
          class="${bodyClasses}"
          aria-labelledby="${headingId}"
          ${isBs5 ? `data-bs-parent="#${idBase}"` : (isBs4 ? `data-parent="#${idBase}"` : "")}>
          <div class="accordion-body">
            <h6>Raw values</h6>
            ${rawHTML}

            <h6 class="mt-3">S1/S2 Ratio</h6>
            ${s1s2HTML}

            <h6 class="mt-3">S3/S4 Ratio</h6>
            ${s3s4HTML}
          </div>
        </div>
      </div>
    `;
  });

  html += "</div>";

  // If neither BS4 nor BS5 is detected, warn visibly
  if (!isBs5 && !isBs4) {
    html =
      `<div class="alert alert-warning">
         Accordion rendered, but Bootstrap JS was not detected. Expand/collapse will not work.
       </div>` + html;
  }

  return html;
}
// api_requests.js
export async function fetchJson(url, init = {}) {
  // Normalize/merge headers (case-insensitive safe)
  const headers = new Headers(init.headers || {});
  if (!headers.has("Accept")) headers.set("Accept", "application/json");

  // If we're sending a body and caller didn't specify content-type, default to JSON
  const hasBody =
    init.body !== undefined && init.body !== null && String(init.body).length > 0;

  if (hasBody && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const res = await fetch(url, { ...init, headers });

  // Handle non-OK with readable error
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`${url} failed: ${res.status} ${res.statusText} — ${txt}`);
  }

  // Some endpoints might return empty body (204) — avoid JSON parse blowups
  const contentType = res.headers.get("Content-Type") || "";
  if (!contentType.includes("application/json")) {
    const txt = await res.text();
    throw new Error(`${url} did not return JSON — ${txt.slice(0, 200)}`);
  }

  return await res.json();
}

export function generateSummaryTable(stats, variable, opts = {}) {
  const {
    decimals = null,                // number | null
    rowHeader = "Row",
    emptyMessage = "No summary statistics available.",
    returnType = "node",             // "node" | "html"
  } = opts;

  // Always build into a wrapper so return is predictable
  const wrapper = document.createElement("div");

  const finish = () => {
    if (returnType === "html") {
      return wrapper.innerHTML;
    }
    return wrapper;
  };

  // ------------------------------------------------------------------
  // Guards
  // ------------------------------------------------------------------
  if (!stats || typeof stats !== "object") {
    wrapper.className = "alert alert-warning";
    wrapper.textContent = emptyMessage;
    return finish();
  }

  const isPlainObject = (v) =>
    v !== null && typeof v === "object" && !Array.isArray(v);

  const displayValue = (v) => {
    if (v === null || v === undefined) return "";
    if (typeof v === "string") return v;        // ← preserves "ALL PREY"
    if (typeof v === "number") {
      if (!Number.isFinite(v)) return "";
      if (typeof decimals === "number") return v.toFixed(decimals);
      return String(v);
    }
    return String(v);
  };

  // ------------------------------------------------------------------
  // CASE 1: Array of objects
  // ------------------------------------------------------------------
  if (Array.isArray(stats)) {
    if (stats.length === 0) {
      wrapper.className = "alert alert-warning";
      wrapper.textContent = emptyMessage;
      return finish();
    }

    const table = document.createElement("table");
    table.className = "table table-sm table-striped table-bordered";

    const cols = Array.from(
      stats.reduce((set, row) => {
        if (isPlainObject(row)) {
          Object.keys(row).forEach((k) => set.add(k));
        }
        return set;
      }, new Set())
    );

    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    cols.forEach((c) => {
      const th = document.createElement("th");
      th.textContent = c;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    stats.forEach((row) => {
      const tr = document.createElement("tr");
      cols.forEach((c) => {
        const td = document.createElement("td");
        td.textContent = isPlainObject(row) ? displayValue(row[c]) : "";
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    wrapper.appendChild(table);
    return finish();
  }

  // ------------------------------------------------------------------
  // CASE 2: Standard table payload
  // { periods, rows, rowLabels?, data }
  // ------------------------------------------------------------------
  if (
    Array.isArray(stats.periods) &&
    Array.isArray(stats.rows) &&
    isPlainObject(stats.data)
  ) {
    const { periods, rows } = stats;
    const rowLabels = isPlainObject(stats.rowLabels) ? stats.rowLabels : {};
    const data = stats.data;

    let varKey = variable;
    if (!isPlainObject(data[varKey])) {
      const keys = Object.keys(data);
      if (keys.length) varKey = keys[0];
    }

    const varBlock = data[varKey];
    if (!isPlainObject(varBlock)) {
      wrapper.className = "alert alert-warning";
      wrapper.textContent = emptyMessage;
      return finish();
    }

    const table = document.createElement("table");
    table.className = "table table-sm table-striped table-bordered";

    const thead = document.createElement("thead");
    const trh = document.createElement("tr");

    const th0 = document.createElement("th");
    th0.textContent = rowHeader;
    trh.appendChild(th0);

    periods.forEach((p) => {
      const th = document.createElement("th");
      th.textContent = p?.label || p?.key || "";
      trh.appendChild(th);
    });

    thead.appendChild(trh);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach((rowKey) => {
      const tr = document.createElement("tr");

      const td0 = document.createElement("td");
      td0.textContent = rowLabels[rowKey] || rowKey;
      tr.appendChild(td0);

      const rowMap = varBlock[rowKey];
      periods.forEach((p) => {
        const pk = p?.key;
        const td = document.createElement("td");
        const v = isPlainObject(rowMap) && pk ? rowMap[pk] : null;
        td.textContent = displayValue(v);
        tr.appendChild(td);
      });

      tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    wrapper.appendChild(table);
    return finish();
  }

  // ------------------------------------------------------------------
  // CASE 3: Summary dict
  // { "STRIP 1": {min, mean, max, std, n}, ... }
  // ------------------------------------------------------------------
  const rowKeys = Object.keys(stats);
  if (rowKeys.length === 0) {
    wrapper.className = "alert alert-warning";
    wrapper.textContent = emptyMessage;
    return finish();
  }

  const firstRow = rowKeys.map((k) => stats[k]).find(isPlainObject);
  const metricKeys = firstRow ? Object.keys(firstRow) : [];

  if (metricKeys.length === 0) {
    wrapper.className = "alert alert-warning";
    wrapper.textContent = emptyMessage;
    return finish();
  }

  const table = document.createElement("table");
  table.className = "table table-sm table-striped table-bordered";

  const thead = document.createElement("thead");
  const trh = document.createElement("tr");

  const th0 = document.createElement("th");
  th0.textContent = rowHeader;
  trh.appendChild(th0);

  metricKeys.forEach((m) => {
    const th = document.createElement("th");
    th.textContent = m;
    trh.appendChild(th);
  });

  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rowKeys.forEach((rk) => {
    const tr = document.createElement("tr");

    const td0 = document.createElement("td");
    td0.textContent = rk;
    tr.appendChild(td0);

    const rowObj = stats[rk];
    metricKeys.forEach((m) => {
      const td = document.createElement("td");
      td.textContent = isPlainObject(rowObj)
        ? displayValue(rowObj[m])
        : "";
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  wrapper.appendChild(table);
  return finish();
}

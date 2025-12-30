// api_requests.js
import { getDropdownValue } from "./ui_utils.js";
import { generateSummaryTable } from "./tables.js";

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
  const depthLabel  =
    document.getElementById("summary-depth").selectedOptions[0]?.textContent ||
    "";

  const variableLabel =
    options?.labelNameMapping?.[variable] || variable;

  const mainTitle =
    data.title ||
    `${capitalizeFirst(granularity)} Summary for ${variableLabel}, Strip ${strip}, ${depthLabel}, ${year}`;

  console.log("✅ variable label:", variableLabel);
  console.log("✅ main title:", mainTitle);
  const titleEl = document.getElementById("summary-title");
  if (titleEl) {
    titleEl.textContent = mainTitle;
  }

  const isTempVariable = ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]
    .includes(variable);

  // 🌱 Growing-season layout uses the accordion renderer
  if (granularity === "gseason") {
    console.log("🌱 Detected growing season granularity. Building accordion layout…");

    const seasonStats = data.gseason_stats || {};
    const accordionHTML =
      generateSeasonalSummaryAccordion(seasonStats, variable);

    const container = document.getElementById("summary-table-container");
    if (container) {
      container.innerHTML = accordionHTML;
    }
    return;
  }

  // 📅 Default (non-gseason) layout
  const rawTableHTML = generateSummaryTable(data.raw_statistics, variable);

  const s1s2 = {};
  const s3s4 = {};
  for (const [key, value] of Object.entries(data.ratio_statistics || {})) {
    if (key.includes("S1_S2"))      s1s2[key] = value;
    else if (key.includes("S3_S4")) s3s4[key] = value;
  }

  const s1s2HTML = Object.keys(s1s2).length > 0
    ? generateSummaryTable(s1s2, variable)
    : isTempVariable
      ? `<p class="text-muted">Soil temperature ratios are not shown because they are not meaningful.</p>`
      : `<p class="text-danger">No summary statistics available.</p>`;

  const s3s4HTML = Object.keys(s3s4).length > 0
    ? generateSummaryTable(s3s4, variable)
    : isTempVariable
      ? `<p class="text-muted">Soil temperature ratios are not shown because they are not meaningful.</p>`
      : `<p class="text-danger">No summary statistics available.</p>`;

  const ratioHTML = `
    <h5 class="mt-4">S1/S2 Ratio (Top/Mid/Bottom Logger)</h5>
    ${s1s2HTML}
    <h5 class="mt-4">S3/S4 Ratio (Top/Mid/Bottom Logger)</h5>
    ${s3s4HTML}
  `;

  const container = document.getElementById("summary-table-container");
  if (container) {
    container.innerHTML = `
      <h5>Raw Values (Top/Mid/Bottom Logger)</h5>
      ${rawTableHTML}
      ${ratioHTML}
    `;
  }
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

  // Human-readable variable label (unit-aware if mapping is an object)
  const rawEntry = labelMap[variable];
  let prettyVar;

  if (rawEntry && typeof rawEntry === "object") {
    prettyVar =
      rawEntry[unit] ||
      rawEntry.us ||
      rawEntry.metric ||
      variable;
  } else {
    prettyVar = rawEntry || variable;
  }

  // Add depth + year context for the seasonal headers
  const yearEl  = document.getElementById("summary-year");
  const depthEl = document.getElementById("summary-depth");

  const year      = yearEl ? yearEl.value : "";
  const depthText =
    depthEl?.selectedOptions[0]?.textContent?.trim() || "";

  const extras = [];
  if (depthText) extras.push(depthText);
  if (year)      extras.push(year);

  let prettyVarWithContext = prettyVar;
  if (extras.length) {
    prettyVarWithContext = `${prettyVar} (${extras.join(", ")})`;
  }

  const periodEntries = Object.entries(periods);
  if (!periodEntries.length) {
    return `<p class="text-muted">No seasonal periods are defined.</p>`;
  }

  const idBase = "gseason-accordion";
  let html = `<div class="accordion" id="${idBase}">`;

  periodEntries.forEach(([code, spec], idx) => {
    const headingId  = `${idBase}-${code}-heading`;
    const collapseId = `${idBase}-${code}`;

    const seasonStats = stats[code] || {};
    const raw   = seasonStats.raw_statistics   || {};
    const ratio = seasonStats.ratio_statistics || {};

    // Raw table
    const rawHTML = Object.keys(raw).length
      ? generateSummaryTable(raw, prettyVar)
      : `<p class="text-muted mb-0">No raw data available for this period.</p>`;

    // Split ratio stats into S1/S2 vs S3/S4
    const s1s2 = {};
    const s3s4 = {};
    for (const [trace, val] of Object.entries(ratio)) {
      if (trace.includes("S1_S2"))      s1s2[trace] = val;
      else if (trace.includes("S3_S4")) s3s4[trace] = val;
    }

    const s1s2HTML = Object.keys(s1s2).length
      ? generateSummaryTable(s1s2, prettyVar)
      : `<p class="text-muted mb-0">No summary statistics available.</p>`;

    const s3s4HTML = Object.keys(s3s4).length
      ? generateSummaryTable(s3s4, prettyVar)
      : `<p class="text-muted mb-0">No summary statistics available.</p>`;

    const title = formatGseasonLabel(code, spec, prettyVarWithContext);

    const isFirst     = idx === 0;
    const btnClasses  = `accordion-button${isFirst ? "" : " collapsed"}`;
    const bodyClasses = `accordion-collapse collapse${isFirst ? " show" : ""}`;

    html += `
      <div class="accordion-item">
        <h2 class="accordion-header" id="${headingId}">
          <button
            class="${btnClasses}"
            type="button"
            data-bs-toggle="collapse"
            data-bs-target="#${collapseId}"
            aria-expanded="${isFirst ? "true" : "false"}"
            aria-controls="${collapseId}">
            ${title}
          </button>
        </h2>
        <div
          id="${collapseId}"
          class="${bodyClasses}"
          aria-labelledby="${headingId}"
          data-bs-parent="#${idBase}">
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
  return html;
}
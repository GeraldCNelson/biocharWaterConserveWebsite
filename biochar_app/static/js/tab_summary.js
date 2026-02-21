// static/js/tab_summary.js
//
// Summary Statistics tab controller:
// - Fetch /api/get_summary_stats
// - Render raw + ratio tables using generateSummaryTable()
// - Store window.latestSummaryStats for downloads, etc.

import { fetchJson, generateSummaryTable, formatGseasonLabel } from "./api_requests.js";
import { getDropdownValue } from "./ui_utils.js";
import { showLoadingOverlay, hideLoadingOverlay, startLoadingDots, stopLoadingDots } from "./ui_loading.js";

function capitalizeFirst(str) {
  return str ? str.charAt(0).toUpperCase() + str.slice(1) : "";
}

/**
 * Strict unit-aware label resolver.
 *
 * Accepts:
 *  - string
 *  - object like { us: "...", metric: "..." }
 *
 * Rules:
 *  - if object form is used, BOTH keys must exist
 *  - NO fallback to the other unit system (prevents silent wrong labels)
 *  - throws on invalid shape so we catch bugs early
 */

function isTemperatureVariable(variableKey) {
  return String(variableKey || "").trim().toUpperCase() === "T";
}
function resolveUnitLabelStrict(labelEntry, unitSystem, fallback) {
  if (!labelEntry) {
    throw new Error(
      `Missing label entry for unitSystem=${unitSystem}. fallback=${String(fallback)}`
    );
  }

  if (typeof labelEntry === "string") return labelEntry;

  if (typeof labelEntry === "object") {
    const hasUS = Object.prototype.hasOwnProperty.call(labelEntry, "us");
    const hasMetric = Object.prototype.hasOwnProperty.call(labelEntry, "metric");

    if (!hasUS || !hasMetric) {
      throw new Error(
        `Invalid label entry: expected keys {us, metric}. Got keys: ${Object.keys(labelEntry).join(", ")}`
      );
    }

    if (!Object.prototype.hasOwnProperty.call(labelEntry, unitSystem)) {
      throw new Error(
        `Label entry missing unitSystem=${unitSystem}. Keys: ${Object.keys(labelEntry).join(", ")}`
      );
    }

    const value = labelEntry[unitSystem];
    if (typeof value !== "string" || !value.trim()) {
      throw new Error(
        `Label for unitSystem=${unitSystem} must be a non-empty string. Got: ${String(value)}`
      );
    }

    return value;
  }

  throw new Error(`Invalid label entry type: ${typeof labelEntry}`);
}

function getUnitSystemForSummary() {
  const toggle = document.getElementById("units-toggle_summary");
  return toggle && toggle.checked ? "metric" : "us";
}

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Convert depth dropdown value to a display label:
 * - Assumes dropdown value is inches as a string like "6", "12", "18"
 * - Uses the dropdown selected text if parsing fails
 */
function getDepthDisplayLabel(unitSystem) {
  const depthEl = document.getElementById("summary-depth");
  if (!depthEl) return "";

  const rawVal = depthEl.value; // expected "6" / "12" / "18" (inches)
  const fallbackText = depthEl.selectedOptions?.[0]?.textContent?.trim() || "";

  const inches = parseFloat(rawVal);
  if (!Number.isFinite(inches)) {
    return fallbackText;
  }

  if (unitSystem === "metric") {
    const cm = inches * 2.54;
    const cmText = Math.abs(cm - Math.round(cm)) < 1e-9
      ? String(Math.round(cm))
      : cm.toFixed(1);
    return `${cmText} cm`;
  }

  return `${inches} inches`;
}

/**
 * Make a clean title WITHOUT using backend data.title.
 * This prevents the "{metric:..., us:...}" stringification problem.
 */
function buildSummaryTitle({ year, variable, strip, granularity, unitSystem }) {
  const labelMap = window.labelNameMapping || {};
  let prettyVar = variable;

  try {
    prettyVar = resolveUnitLabelStrict(labelMap[variable], unitSystem, variable);
  } catch (e) {
    console.error("❌ buildSummaryTitle label resolution failed:", e);
    // Fail loudly to catch mapping bugs early:
    throw e;
  }

  const depthLabel = getDepthDisplayLabel(unitSystem);
  const stripPart = strip ? `, Strip ${strip}` : "";

  const granLabel = capitalizeFirst(granularity);
  return `${granLabel} Summary for ${prettyVar}${stripPart}, ${depthLabel}, ${year}`;
}

/**
 * Make stats keys human-readable (Top/Mid/Bottom, strip, ratio groups).
 * Compatibility layer: transforms { key: {min, mean, ...}, ... } only.
 */
function prettifyStatsKeys(stats, variable, unitSystem) {
  if (!isPlainObject(stats)) return stats;

  const keys = Object.keys(stats);
  if (!keys.length) return stats;

  const firstVal = stats[keys[0]];
  if (!isPlainObject(firstVal)) return stats;

  const labelMap = window.labelNameMapping || {};
  let prettyVar = variable;

  try {
    prettyVar = resolveUnitLabelStrict(labelMap[variable], unitSystem, variable);
  } catch (e) {
    console.error("❌ prettifyStatsKeys label resolution failed:", e);
    throw e;
  }

  const loggerMap = { T: "Top", M: "Mid", B: "Bottom" };

  const out = {};
  keys.forEach((k) => {
    const val = stats[k];

    const suffixMatch = String(k).match(/_(T|M|B)$/);
    const loggerSuffix = suffixMatch ? suffixMatch[1] : null;
    const loggerLabel = loggerSuffix ? (loggerMap[loggerSuffix] || loggerSuffix) : null;

    const stripMatch = String(k).match(/_S([1-4])_/);
    const stripLabel = stripMatch ? `Strip ${stripMatch[1]}` : null;

    const ratioGroup =
      String(k).includes("S1_S2") ? "S1/S2" :
        (String(k).includes("S3_S4") ? "S3/S4" : null);

    let displayKey = prettyVar;

    if (ratioGroup && loggerLabel) {
      displayKey = `${prettyVar} (${ratioGroup}, ${loggerLabel})`;
    } else if (ratioGroup) {
      displayKey = `${prettyVar} (${ratioGroup})`;
    } else if (stripLabel && loggerLabel) {
      displayKey = `${prettyVar} (${stripLabel}, ${loggerLabel})`;
    } else if (stripLabel) {
      displayKey = `${prettyVar} (${stripLabel})`;
    } else if (loggerLabel) {
      displayKey = `${prettyVar} (${loggerLabel})`;
    }

    if (out[displayKey]) out[`${displayKey} • ${k}`] = val;
    else out[displayKey] = val;
  });

  return out;
}

/**
 * Build Bootstrap accordion HTML for gseason payload.
 */
function buildGseasonAccordionHTML(gseasonStats, variable, unitSystem) {
  const periods = window.gseasonPeriods || {};
  const idBase = "gseasonAccordion";

  const seasonEntries = Object.entries(periods);
  if (!seasonEntries.length) {
    return `<p class="text-muted">No seasonal periods are defined.</p>`;
  }

  let html = `<div class="accordion" id="${idBase}">`;

  seasonEntries.forEach(([code, spec], idx) => {
    const headingId = `${idBase}-heading-${code}`;
    const collapseId = `${idBase}-collapse-${code}`;

    const block = (gseasonStats && gseasonStats[code]) ? gseasonStats[code] : {};
    const rawStats = block?.raw_statistics || {};
    const ratioStats = block?.ratio_statistics || {};

    const rawPretty = prettifyStatsKeys(rawStats, variable, unitSystem);

    const s1s2 = {};
    const s3s4 = {};
    Object.entries(ratioStats || {}).forEach(([k, v]) => {
      if (String(k).includes("S1_S2")) s1s2[k] = v;
      else if (String(k).includes("S3_S4")) s3s4[k] = v;
    });

    const s1s2Pretty = prettifyStatsKeys(s1s2, variable, unitSystem);
    const s3s4Pretty = prettifyStatsKeys(s3s4, variable, unitSystem);

    const rawHTML = Object.keys(rawPretty || {}).length
      ? generateSummaryTable(rawPretty, variable, { returnType: "html" })
      : `<p class="text-muted mb-0">No raw data available for this period.</p>`;

    const s1s2HTML = Object.keys(s1s2Pretty || {}).length
      ? generateSummaryTable(s1s2Pretty, variable, { returnType: "html" })
      : `<p class="text-muted mb-0">No S1/S2 ratio summary available.</p>`;

    const s3s4HTML = Object.keys(s3s4Pretty || {}).length
      ? generateSummaryTable(s3s4Pretty, variable, { returnType: "html" })
      : `<p class="text-muted mb-0">No S3/S4 ratio summary available.</p>`;

    const title = (typeof formatGseasonLabel === "function")
      ? formatGseasonLabel(code, spec, null)
      : (spec?.label || code);

    const isFirst = idx === 0;

    html += `
      <div class="accordion-item">
        <h2 class="accordion-header" id="${headingId}">
          <button
            class="accordion-button${isFirst ? "" : " collapsed"}"
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
          class="accordion-collapse collapse${isFirst ? " show" : ""}"
          aria-labelledby="${headingId}"
          data-bs-parent="#${idBase}">
          <div class="accordion-body">
            <h6>Raw Summary</h6>
            ${rawHTML}

            <h6 class="mt-4">S1/S2 Ratio Summary</h6>
            ${s1s2HTML}

            <h6 class="mt-4">S3/S4 Ratio Summary</h6>
            ${s3s4HTML}
          </div>
        </div>
      </div>
    `;
  });

  html += "</div>";
  return html;
}

export async function updateSummaryStatistics() {
  console.log("📊 updateSummaryStatistics: Updating summary statistics...");

  startLoadingDots("summary-status", "Loading summary tables");

  const container = document.getElementById("summary-table-container");
  if (!container) {
    console.warn("❌ #summary-table-container not found.");
    stopLoadingDots("summary-status", "Summary container missing.");
    return;
  }

  showLoadingOverlay(container, "Loading summary tables");
  container.innerHTML = "";

  try {
    const year = parseInt(getDropdownValue("summary-year"), 10);
    const variable = getDropdownValue("summary-variable"); // backend variable key (e.g., VWC, T, EC, SWC)
    const stripRaw = getDropdownValue("summary-strip");
    const strip = stripRaw ? stripRaw : null;
    const granularity = getDropdownValue("summary-granularity");
    const depthRaw = getDropdownValue("summary-depth");
    const depth = depthRaw ? depthRaw : null;
    const unitSystem = getUnitSystemForSummary();

    console.log("🔍 Selected Summary Filters:", {
      year,
      variable,
      strip,
      granularity,
      depth,
      unitSystem,
    });

    if (!Number.isFinite(year) || !variable || !granularity || !depth) {
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent =
        "Please select Year, Variable, Time averages, and Depth before updating the summary.";
      container.appendChild(warn);

      stopLoadingDots("summary-status", "Missing required selections.");
      return;
    }

    const data = await fetchJson("/api/get_summary_stats", {
      method: "POST",
      body: JSON.stringify({ year, variable, strip, granularity, depth, unitSystem }),
    });

    console.log("✅ Received summary stats response:", data);

    window.latestSummaryStats = {
      raw: data?.raw_statistics ?? null,
      ratio: data?.ratio_statistics ?? null,
      gseason: data?.gseason_stats ?? null,
      meta: { year, variable, strip, granularity, depth, unitSystem },
    };

    window.__lastSummaryData = data;

    const titleEl = document.getElementById("summary-title");
    if (titleEl) {
      titleEl.textContent =
        data?.title || buildSummaryTitle({ year, variable, strip, granularity, unitSystem });
    }

    container.innerHTML = "";

    // ----------------------------
    // Growing season special render
    // ----------------------------
    if (granularity === "gseason") {
      container.innerHTML = buildGseasonAccordionHTML(data?.gseason_stats || {}, variable, unitSystem);
      console.log("✅ Seasonal accordion rendered.");
      stopLoadingDots("summary-status", "");
      return;
    }

    // ----------------------------
    // Raw block
    // ----------------------------
    const rawHeader = document.createElement("h5");
    rawHeader.textContent = "Raw Data";
    container.appendChild(rawHeader);

    const rawPretty = prettifyStatsKeys(data?.raw_statistics, variable, unitSystem);
    const rawTableEl = generateSummaryTable(rawPretty, variable);
    if (rawTableEl instanceof Node) {
      container.appendChild(rawTableEl);
    } else {
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent = "Raw summary table could not be rendered (unexpected return type).";
      container.appendChild(warn);
    }

    // ----------------------------
    // Ratio block (ALWAYS render header)
    // Then:
    //  - If temperature: show blue info and DO NOT show yellow warning
    //  - Else: show table if present, otherwise show yellow warning
    // ----------------------------
    const ratioHeader = document.createElement("h5");
    ratioHeader.className = "mt-4";
    ratioHeader.textContent = "Ratio Data";
    container.appendChild(ratioHeader);

    // IMPORTANT: Use the selected variable key (same as 'variable')
    const variableKey = variable;

    if (typeof isTemperatureVariable === "function" && isTemperatureVariable(variableKey)) {
      const info = document.createElement("div");
      info.className = "alert alert-info";
      info.innerHTML =
        "Ratios are not shown for <strong>Soil Temperature</strong> because temperature ratios are not meaningful in this dashboard " +
        "(unlike VWC/EC/SWC). See the <strong>Technical Details</strong> tab for an explanation.";
      container.appendChild(info);

      console.log("ℹ️ Temperature variable selected; ratio stats intentionally suppressed.");
      stopLoadingDots("summary-status", "");
      return; // stop here so we never show the generic yellow warning
    }

    const ratioStats = data?.ratio_statistics;
    const hasRatioStats =
      ratioStats && typeof ratioStats === "object" && Object.keys(ratioStats).length > 0;

    if (hasRatioStats) {
      const ratioPretty = prettifyStatsKeys(ratioStats, variable, unitSystem);
      const ratioTableEl = generateSummaryTable(ratioPretty, variable);

      if (ratioTableEl instanceof Node) {
        container.appendChild(ratioTableEl);
      } else {
        const warn = document.createElement("div");
        warn.className = "alert alert-warning";
        warn.textContent = "Ratio summary table could not be rendered (unexpected return type).";
        container.appendChild(warn);
      }
    } else {
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent = "No summary statistics available.";
      container.appendChild(warn);
    }

    console.log("✅ Summary statistics tables updated.");
    stopLoadingDots("summary-status", "");
  } catch (error) {
    console.error("❌ Unexpected error in updateSummaryStatistics:", error);

    container.innerHTML = "";
    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent =
      "Failed to load summary statistics. Check server logs and browser console.";
    container.appendChild(div);

    stopLoadingDots("summary-status", "Failed to load summary.");
  } finally {
    hideLoadingOverlay(container);
  }
}

export function initSummaryTab() {
  const btn = document.getElementById("update-summary");
  if (!btn) {
    console.warn("⚠️ #update-summary button not found; summary tab not wired.");
    return;
  }

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    updateSummaryStatistics();
  });

  // Auto-load when the Summary tab is opened the first time (non-blocking)
  const summaryTab = document.getElementById("summary-tab"); // adjust if your id differs
  const container = document.getElementById("summary-table-container");
  if (summaryTab && container && container.dataset.autoloaded !== "true") {
    summaryTab.addEventListener("shown.bs.tab", () => {
      if (container.dataset.autoloaded === "true") return;
      container.dataset.autoloaded = "true";
      updateSummaryStatistics(); // no await
    });
  }

  console.debug("✅ Summary tab wired (update button)");
}
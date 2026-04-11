// @ts-check
// static/js/tab_summary.js
//
// Summary Statistics tab controller:
// - Fetch /api/get_summary_stats
// - Render raw + ratio tables using generateSummaryTable()
// - Store window.latestSummaryStats for downloads, etc.

import { fetchJson, generateSummaryTable, formatGseasonLabel } from "./api_requests.js";
import { getDropdownValue } from "./ui_utils.js";
import { showLoadingOverlay, hideLoadingOverlay, startLoadingDots, stopLoadingDots } from "./ui_loading.js";

/**
 * @typedef {Window & {
 *   labelNameMapping?: Record<string, any>,
 *   gseasonPeriods?: Record<string, any>,
 *   latestSummaryStats?: any,
 *   __lastSummaryData?: any
 * }} SummaryWindow
 */

/** @type {SummaryWindow} */
const summaryWindow = /** @type {SummaryWindow} */ (window);

/**
 * @param {string} str
 * @returns {string}
 */
function capitalizeFirst(str) {
  return str ? str.charAt(0).toUpperCase() + str.slice(1) : "";
}
void capitalizeFirst;

/**
 * @param {string} variableKey
 * @returns {boolean}
 */
function isTemperatureVariable(variableKey) {
  return String(variableKey || "").trim().toUpperCase() === "T";
}

/**
 * @param {any} labelEntry
 * @param {"us" | "metric"} unitSystem
 * @param {string} fallback
 * @returns {string}
 */
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

/**
 * @returns {"us" | "metric"}
 */
function getUnitSystemForSummary() {
  const toggle = /** @type {HTMLInputElement | null} */ (
    document.getElementById("units-toggle_summary")
  );
  return toggle && toggle.checked ? "metric" : "us";
}

/**
 * @param {any} v
 * @returns {boolean}
 */
function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Convert depth dropdown value to a display label:
 * - Assumes dropdown value is inches as a string like "6", "12", "18"
 * - Uses the dropdown selected text if parsing fails
 *
 * @param {"us" | "metric"} unitSystem
 * @returns {string}
 */
function getDepthDisplayLabel(unitSystem) {
  const depthEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("summary-depth")
  );
  if (!depthEl) return "";

  const rawVal = depthEl.value;
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
 * @param {{
 *   year: number,
 *   variable: string,
 *   strip: string | null,
 *   granularity: string,
 *   unitSystem: "us" | "metric"
 * }} args
 * @returns {string}
 */
function buildSummaryTitle({ year, variable, strip, granularity, unitSystem }) {
  const labelMap = summaryWindow.labelNameMapping || {};
  let prettyVar = variable;

  try {
    prettyVar = resolveUnitLabelStrict(labelMap[variable], unitSystem, variable);
  } catch (e) {
    console.error("❌ buildSummaryTitle label resolution failed:", e);
    throw e;
  }

  const depthLabel = getDepthDisplayLabel(unitSystem);
  const stripPart = strip ? `, Strip ${strip}` : "";

  const granLabel = capitalizeFirst(granularity);
  return `${granLabel} Summary for ${prettyVar}${stripPart}, ${depthLabel}, ${year}`;
}

/**
 * @param {any} stats
 * @param {string} variable
 * @param {"us" | "metric"} unitSystem
 * @returns {any}
 */
function prettifyStatsKeys(stats, variable, unitSystem) {
  if (!isPlainObject(stats)) return stats;

  const keys = Object.keys(stats);
  if (!keys.length) return stats;

  const firstVal = stats[keys[0]];
  if (!isPlainObject(firstVal)) return stats;

  const labelMap = summaryWindow.labelNameMapping || {};
  let prettyVar = variable;

  try {
    prettyVar = resolveUnitLabelStrict(labelMap[variable], unitSystem, variable);
  } catch (e) {
    console.error("❌ prettifyStatsKeys label resolution failed:", e);
    throw e;
  }

  /** @type {Record<string, string>} */
  const loggerMap = { T: "Top", M: "Mid", B: "Bottom" };

  /** @type {Record<string, any>} */
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
 * @param {any} gseasonStats
 * @param {string} variable
 * @param {"us" | "metric"} unitSystem
 * @returns {string}
 */
function buildGseasonAccordionHTML(gseasonStats, variable, unitSystem) {
  const periods = summaryWindow.gseasonPeriods || {};
  const idBase = "gseasonAccordion";

  const seasonEntries = Object.entries(periods);
  if (!seasonEntries.length) {
    return `<p class="text-muted">No seasonal periods are defined.</p>`;
  }

  /**
   * @param {any} stats
   * @returns {Record<string, any>}
   */
  function normalizeFlatGseasonStats(stats) {
    /** @type {Record<string, any>} */
    const grouped = {};

    if (!Array.isArray(stats)) {
      return grouped;
    }

    stats.forEach((row) => {
      if (!row || typeof row !== "object") return;

      const periodCode = row.period_code;
      if (!periodCode) return;

      if (!grouped[periodCode]) {
        grouped[periodCode] = {
          raw_statistics: {},
          ratio_statistics: {},
        };
      }

      const strip = String(row.strip || "").trim();
      const depth = String(row.depth || "").trim();
      const loggerLocation = String(row.logger_location || "").trim();

      const rawKeyParts = [strip];
      if (depth) rawKeyParts.push(`D${depth}`);
      if (loggerLocation) rawKeyParts.push(loggerLocation);
      const rawKey = rawKeyParts.join("_");

      const hasRawStats =
        row.raw_min != null ||
        row.raw_mean != null ||
        row.raw_max != null ||
        row.raw_std != null;

      if (hasRawStats && rawKey) {
        grouped[periodCode].raw_statistics[rawKey] = {
          min: row.raw_min,
          mean: row.raw_mean,
          max: row.raw_max,
          std: row.raw_std,
        };
      }

      const ratioStrip = String(row.ratio_group || row.ratio_strip || row.strip_ratio || row.strip || "").trim();
      const ratioKeyParts = [ratioStrip];
      if (depth) ratioKeyParts.push(`D${depth}`);
      if (loggerLocation) ratioKeyParts.push(loggerLocation);
      const ratioKey = ratioKeyParts.join("_");

      const hasRatioStats =
        row.ratio_min != null ||
        row.ratio_mean != null ||
        row.ratio_max != null ||
        row.ratio_std != null;

      const looksLikeRatioGroup =
        ratioStrip.includes("S1/S2") ||
        ratioStrip.includes("S3/S4") ||
        ratioStrip.includes("S1_S2") ||
        ratioStrip.includes("S3_S4");

      if (hasRatioStats && ratioKey && looksLikeRatioGroup) {
        grouped[periodCode].ratio_statistics[ratioKey] = {
          min: row.ratio_min,
          mean: row.ratio_mean,
          max: row.ratio_max,
          std: row.ratio_std,
        };
      }
    });

    return grouped;
  }

  const groupedStats = Array.isArray(gseasonStats)
    ? normalizeFlatGseasonStats(gseasonStats)
    : (gseasonStats && typeof gseasonStats === "object" ? gseasonStats : {});

  let html = `<div class="accordion" id="${idBase}">`;

  seasonEntries.forEach(([code, spec], idx) => {
    const headingId = `${idBase}-heading-${code}`;
    const collapseId = `${idBase}-collapse-${code}`;

    const block = groupedStats[code] || {};
    const rawStats = block.raw_statistics || {};
    const ratioStats = block.ratio_statistics || {};

    const rawPretty = prettifyStatsKeys(rawStats, variable, unitSystem);

    /** @type {Record<string, any>} */
    const s1s2 = {};
    /** @type {Record<string, any>} */
    const s3s4 = {};

    Object.entries(ratioStats).forEach(([k, v]) => {
      const key = String(k);
      if (key.includes("S1/S2") || key.includes("S1_S2")) {
        s1s2[k] = v;
      } else if (key.includes("S3/S4") || key.includes("S3_S4")) {
        s3s4[k] = v;
      }
    });

    const s1s2Pretty = prettifyStatsKeys(s1s2, variable, unitSystem);
    const s3s4Pretty = prettifyStatsKeys(s3s4, variable, unitSystem);

    const rawHTML = Object.keys(rawPretty).length
      ? generateSummaryTable(rawPretty, variable, { returnType: "html" })
      : `<p class="text-muted mb-0">No raw data available for this period.</p>`;

    const s1s2HTML = Object.keys(s1s2Pretty).length
      ? generateSummaryTable(s1s2Pretty, variable, { returnType: "html" })
      : `<p class="text-muted mb-0">No S1/S2 ratio summary available.</p>`;

    const s3s4HTML = Object.keys(s3s4Pretty).length
      ? generateSummaryTable(s3s4Pretty, variable, { returnType: "html" })
      : `<p class="text-muted mb-0">No S3/S4 ratio summary available.</p>`;

    const title = (typeof formatGseasonLabel === "function")
      ? formatGseasonLabel(code, spec, "")
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

/**
 * @param {string} [text=""]
 * @returns {void}
 */
function showSummaryStatus(text = "") {
  const el = document.getElementById("summary-status");
  if (!el) return;
  el.style.display = "block";
  if (typeof text === "string") el.textContent = text;
}

/**
 * @returns {void}
 */
function hideSummaryStatus() {
  const el = document.getElementById("summary-status");
  if (!el) return;
  el.textContent = "";
  el.style.display = "none";
}

/**
 * @returns {Promise<void>}
 */
export async function updateSummaryStatistics() {
  console.log("📊 updateSummaryStatistics: Updating summary statistics...");

  showSummaryStatus("");
  startLoadingDots("summary-status", "Loading summary tables..");

  const container = document.getElementById("summary-table-container");
  if (!container) {
    console.warn("❌ #summary-table-container not found.");
    stopLoadingDots("summary-status", "Summary container missing.");
    showSummaryStatus("Summary container missing.");
    return;
  }

  showLoadingOverlay(container, "Loading summary tables..");
  container.innerHTML = "";

  try {
    const yearVal = getDropdownValue("summary-year");
    const year = parseInt(String(yearVal ?? ""), 10);
    const variable = /** @type {string} */ (getDropdownValue("summary-variable") || "");
    const stripRaw = /** @type {string | null} */ (getDropdownValue("summary-strip"));
    const strip = stripRaw ? stripRaw : null;
    const granularity = /** @type {string} */ (getDropdownValue("summary-granularity") || "");
    const depthRaw = /** @type {string | null} */ (getDropdownValue("summary-depth"));
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
      showSummaryStatus("Missing required selections.");
      return;
    }

    const data = await fetchJson("/api/get_summary_stats", {
      method: "POST",
      body: JSON.stringify({ year, variable, strip, granularity, depth, unitSystem }),
    });

    console.log("✅ Received summary stats response:", data);

    summaryWindow.latestSummaryStats = {
      raw: data?.raw_statistics ?? null,
      ratio: data?.ratio_statistics ?? null,
      gseason: data?.gseason_stats ?? null,
      meta: { year, variable, strip, granularity, depth, unitSystem },
    };

    summaryWindow.__lastSummaryData = data;

    const titleEl = document.getElementById("summary-title");
    if (titleEl) {
      titleEl.textContent =
        data?.title || buildSummaryTitle({ year, variable, strip, granularity, unitSystem });
    }

    container.innerHTML = "";

    if (granularity === "gseason") {
      container.innerHTML = buildGseasonAccordionHTML(data?.gseason_stats || {}, variable, unitSystem);
      console.log("✅ Seasonal accordion rendered.");

      stopLoadingDots("summary-status", "");
      hideSummaryStatus();
      return;
    }

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

    const ratioHeader = document.createElement("h5");
    ratioHeader.className = "mt-4";
    ratioHeader.textContent = "Ratio Data";
    container.appendChild(ratioHeader);

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
      hideSummaryStatus();
      return;
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
    hideSummaryStatus();
  } catch (error) {
    console.error("❌ Unexpected error in updateSummaryStatistics:", error);

    container.innerHTML = "";
    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent =
      "Failed to load summary statistics. Check server logs and browser console.";
    container.appendChild(div);

    stopLoadingDots("summary-status", "Failed to load summary.");
    showSummaryStatus("Failed to load summary.");
  } finally {
    hideLoadingOverlay(container);
  }
}

/**
 * @returns {void}
 */
export function initSummaryTab() {
  const btn = document.getElementById("update-summary");
  if (!btn) {
    console.warn("⚠️ #update-summary button not found; summary tab not wired.");
    return;
  }

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    void updateSummaryStatistics();
  });

  const summaryTab = document.getElementById("summary-tab");
  const container = document.getElementById("summary-table-container");
  if (summaryTab && container && container.dataset.autoloaded !== "true") {
    summaryTab.addEventListener("shown.bs.tab", () => {
      if (container.dataset.autoloaded === "true") return;
      container.dataset.autoloaded = "true";
      void updateSummaryStatistics();
    });
  }

  console.debug("✅ Summary tab wired (update button)");
}
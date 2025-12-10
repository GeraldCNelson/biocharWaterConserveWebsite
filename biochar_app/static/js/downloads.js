// static/js/downloads.js

/* global Plotly */

import { getDropdownValue, getElementByIdSafe, showAlert } from "./ui_utils.js";

// Ensure the global summary stats object exists (filled elsewhere in the app)
window.latestSummaryStats = window.latestSummaryStats || {};

// Fixed-size export for high-resolution images (pixels)
const FIXED_EXPORT_WIDTH = 2000;
const FIXED_EXPORT_HEIGHT = 1200;

// ----------------------------------------------------
//  Main data download buttons (raw / ratio / all)
// ----------------------------------------------------
function downloadTraceData(type) {
  console.log(`📥 Downloading ${type} data...`);

  const year = getDropdownValue("main-year");
  const granularity = getDropdownValue("main-granularity");
  const variable = getDropdownValue("main-variable");
  const strip = getDropdownValue("main-strip");
  const depth = getDropdownValue("main-depth");
  const loggerLocation = getDropdownValue("main-loggerLocation");

  const params = new URLSearchParams({
    year,
    granularity,
    variable,
    strip,
    depth,
    loggerLocation,
  }).toString();

  let route = "";
  if (type === "raw") {
    route = "/download_raw_data";
  } else if (type === "ratio") {
    route = "/download_ratio_data";
  } else if (type === "all") {
    route = "/download_all_data";
  } else {
    console.error("❌ Invalid download type");
    return;
  }

  // This hands control to the browser's download mechanism
  window.location.href = `${route}?${params}`;
}

// ----------------------------------------------------
//  Plot image download (raw / ratio, png / jpeg)
//     mode = "screen" (match browser size)
//          or "fixed" (static high-resolution)
// ----------------------------------------------------
function downloadPlot(plotType, format, mode = "screen") {
  console.log(`📡 Downloading ${plotType} plot as ${format} (${mode})...`);

  const year = getDropdownValue("main-year") || "unknown";
  const variable = getDropdownValue("main-variable") || "unknown";
  const strip = getDropdownValue("main-strip") || "unknown";
  const loggerLocation =
    getDropdownValue("main-loggerLocation") || "unknown";
  const depth = (getDropdownValue("main-depth") || "unknown").replace(" ", "");

  const filename = `${plotType}_plot_${year}_${variable}_${strip}_${loggerLocation}_${depth}_${mode}`;
  console.log(`📂 Generated filename: ${filename}.${format}`);

  const plotElement = getElementByIdSafe(
    plotType === "raw" ? "plot-1" : "plot-2",
  );
  if (!plotElement) {
    console.error(`❌ Plot container not found for type: ${plotType}`);
    return;
  }

  let exportWidth;
  let exportHeight;
  let scale = 2; // keep text crisp

  if (mode === "fixed") {
    // Static high-resolution export
    exportWidth = FIXED_EXPORT_WIDTH;
    exportHeight = FIXED_EXPORT_HEIGHT;
    scale = 2;
  } else {
    // Match current browser size (WYSIWYG)
    const bounds = plotElement.getBoundingClientRect();
    exportWidth = Math.max(800, Math.round(bounds.width));
    exportHeight = Math.max(400, Math.round(bounds.height));
  }

  // noinspection JSUnresolvedFunction
  Plotly.downloadImage(plotElement, {
    format,
    filename,
    width: exportWidth,
    height: exportHeight,
    scale,
  });
}

// ----------------------------------------------------
//  Summary tab downloads (already using fetch + blob)
// ----------------------------------------------------
async function downloadSummaryData(type) {
  const year = getDropdownValue("summary-year");
  const variable = getDropdownValue("summary-variable");
  const strip = getDropdownValue("summary-strip");
  const depth = getDropdownValue("summary-depth");
  const granularity = getDropdownValue("summary-granularity");

  const payload = {
    year,
    variable,
    strip,
    depth,
    granularity,
    type, // "raw", "ratio", or "all"
  };

  const stats = window.latestSummaryStats || {};
  const isSeason = granularity === "gseason";

  if (isSeason) {
    if (!stats.gseason_stats || Object.keys(stats.gseason_stats).length === 0) {
      return showAlert("No seasonal summary statistics available.");
    }
  } else {
    const hasRaw = stats.raw && Object.keys(stats.raw).length > 0;
    const hasRatio = stats.ratio && Object.keys(stats.ratio).length > 0;

    if (
      (type === "raw" && !hasRaw) ||
      (type === "ratio" && !hasRatio) ||
      (type === "all" && !hasRaw && !hasRatio)
    ) {
      return showAlert("No summary statistics available for download.");
    }
  }

  try {
    payload.summaryStats = isSeason
      ? stats.gseason_stats
      : type === "raw"
      ? stats.raw
      : type === "ratio"
      ? stats.ratio
      : { ...stats.raw, ...stats.ratio };

    const response = await fetch("/api/download_summary_data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Download failed: ${text}`);
    }

    const blob = await response.blob();
    const url = URL.createObjectURL(blob);

    const fileName = `summary_${granularity}_${type}.zip`;
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    a.click();
    URL.revokeObjectURL(url);
  } catch (err) {
    console.error("❌ Download error:", err);
    showAlert("Failed to download summary data.");
  }
}

// --- Bulk download tab helpers -----------------------------------------

async function fetchBulkDownloadOptions() {
  const resp = await fetch("/bulk_download/options");
  if (!resp.ok) {
    console.error("Failed to fetch bulk download options:", resp.status);
    return {};
  }
  const data = await resp.json();
  return data.available || {};
}

/**
 * Initialize the Bulk Downloads tab.
 * - Populates the year dropdown from /bulk_download/options
 * - Enables/disables buttons depending on which ZIPs exist
 * - Hooks click handlers to trigger the downloads
 */
export async function initBulkDownloadTab() {
  const yearSelect = document.getElementById("bulk-year");
  const loggersBtn = document.getElementById("bulk-download-loggers");
  const weatherBtn = document.getElementById("bulk-download-weather");

  if (!yearSelect || !loggersBtn || !weatherBtn) {
    console.warn("Bulk download elements not found in DOM.");
    return;
  }

  const availableByYear = await fetchBulkDownloadOptions();
  const years = Object.keys(availableByYear).sort();

  if (!years.length) {
    yearSelect.innerHTML = `<option value="">No years available</option>`;
    loggersBtn.disabled = true;
    weatherBtn.disabled = true;
    return;
  }

  // Populate the select
  yearSelect.innerHTML = "";
  years.forEach((y) => {
    const opt = document.createElement("option");
    opt.value = y;
    opt.textContent = y;
    yearSelect.appendChild(opt);
  });

  /**
   * Enable/disable buttons based on selected year availability.
   */
  function updateButtons() {
    const y = yearSelect.value;
    const avail = availableByYear[y] || {};
    loggersBtn.disabled = !Boolean(avail["loggers"]);
    weatherBtn.disabled = !Boolean(avail["weather"]);
  }

  yearSelect.addEventListener("change", updateButtons);

  // Click handlers: just hit the ZIP endpoints directly
  loggersBtn.addEventListener("click", () => {
    const y = yearSelect.value;
    if (!y) return;
    window.location.href = `/bulk_download/loggers/${y}`;
  });

  weatherBtn.addEventListener("click", () => {
    const y = yearSelect.value;
    if (!y) return;
    window.location.href = `/bulk_download/weather/${y}`;
  });

  // Initialize state for the default selection
  updateButtons();
}

// ----------------------------------------------------
//  ES module exports (for other JS files)
// ----------------------------------------------------
export { downloadTraceData, downloadPlot, downloadSummaryData };
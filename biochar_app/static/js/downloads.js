// static/js/downloads.js

/* global Plotly */

import { getDropdownValue, getElementByIdSafe, showAlert } from "./ui_utils.js";

// Ensure the global summary stats object exists (filled elsewhere in the app)
window.latestSummaryStats = window.latestSummaryStats || {};

// Fixed-size export for high-resolution images (pixels)
const FIXED_EXPORT_WIDTH  = 2000;
const FIXED_EXPORT_HEIGHT = 1200;

// ----------------------------------------------------
//  Main data download buttons (raw / ratio / all)
// ----------------------------------------------------
function downloadTraceData(type) {
  console.log(`📥 Downloading ${type} data...`);

  const year           = getDropdownValue("main-year");
  const granularity    = getDropdownValue("main-granularity");
  const variable       = getDropdownValue("main-variable");
  const strip          = getDropdownValue("main-strip");
  const depth          = getDropdownValue("main-depth");
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

  window.location.href = `${route}?${params}`;
}

/**
 * Download a ZIP bundle for the main data tab
 * (CSV(s) + README) via /download_all_data_zip.
 */
function downloadTraceBundleZip() {
  console.log("📥 Downloading ZIP bundle for main data...");

  const year           = getDropdownValue("main-year");
  const granularity    = getDropdownValue("main-granularity");
  const variable       = getDropdownValue("main-variable");
  const strip          = getDropdownValue("main-strip");
  const depth          = getDropdownValue("main-depth");
  const loggerLocation = getDropdownValue("main-loggerLocation");

  const params = new URLSearchParams({
    year,
    granularity,
    variable,
    strip,
    depth,
    loggerLocation,
  }).toString();

  window.location.href = `/download_all_data_zip?${params}`;
}

// ----------------------------------------------------
//  Plot image download (raw / ratio, png / jpeg)
// ----------------------------------------------------
function downloadPlot(plotType, format, mode = "screen") {
  console.log(`📡 Downloading ${plotType} plot as ${format} (${mode})...`);

  const year           = getDropdownValue("main-year")           || "unknown";
  const variable       = getDropdownValue("main-variable")       || "unknown";
  const strip          = getDropdownValue("main-strip")          || "unknown";
  const loggerLocation = getDropdownValue("main-loggerLocation") || "unknown";
  const depth          = (getDropdownValue("main-depth") || "unknown").replace(" ", "");

  const filename = `${plotType}_plot_${year}_${variable}_${strip}_${loggerLocation}_${depth}_${mode}`;
  console.log(`📂 Generated filename: ${filename}.${format}`);

  const plotElement = getElementByIdSafe(plotType === "raw" ? "plot-1" : "plot-2");
  if (!plotElement) {
    console.error(`❌ Plot container not found for type: ${plotType}`);
    return;
  }

  let exportWidth;
  let exportHeight;
  let scale = 2;

  if (mode === "fixed") {
    exportWidth  = FIXED_EXPORT_WIDTH;
    exportHeight = FIXED_EXPORT_HEIGHT;
    scale        = 2;
  } else {
    const bounds = plotElement.getBoundingClientRect();
    exportWidth  = Math.max(800, Math.round(bounds.width));
    exportHeight = Math.max(400, Math.round(bounds.height));
  }

  Plotly.downloadImage(plotElement, {
    format,
    filename,
    width:  exportWidth,
    height: exportHeight,
    scale,
  });
}

// ----------------------------------------------------
//  Summary tab downloads (via /api/download_summary_data)
// ----------------------------------------------------
async function downloadSummaryData(type) {
  console.log(`📥 Downloading summary data (${type})...`);

  const year        = getDropdownValue("summary-year");
  const variable    = getDropdownValue("summary-variable");
  const strip       = getDropdownValue("summary-strip");
  const depth       = getDropdownValue("summary-depth");
  const granularity = getDropdownValue("summary-granularity");

  const payload = { year, variable, strip, depth, granularity, type };

  const stats    = window.latestSummaryStats || {};
  const isSeason = granularity === "gseason";

  if (isSeason) {
    if (!stats.gseason_stats || Object.keys(stats.gseason_stats).length === 0) {
      return showAlert("No seasonal summary statistics available.");
    }
  } else {
    const hasRaw   = stats.raw && Object.keys(stats.raw).length > 0;
    const hasRatio = stats.ratio && Object.keys(stats.ratio).length > 0;

    if (
      (type === "raw"   && !hasRaw) ||
      (type === "ratio" && !hasRatio) ||
      (type === "all"   && !hasRaw && !hasRatio)
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
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Download failed: ${text}`);
    }

    const blob = await response.blob();
    const url  = URL.createObjectURL(blob);

    const fileName = `summary_${granularity}_${type}.zip`;
    const a        = document.createElement("a");
    a.href         = url;
    a.download     = fileName;
    a.click();
    URL.revokeObjectURL(url);
  } catch (err) {
    console.error("❌ Download error:", err);
    showAlert("Failed to download summary data.");
  }
}

// ----------------------------------------------------
//  Bulk downloads (Option A)
//  - One Year dropdown
//  - A stack of obvious "Download ____" buttons
//  - Enabled/disabled based on /bulk_download/options
// ----------------------------------------------------

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
 * Some backends name routes differently. If needed, override here.
 * Key = data-dataset attribute from the button.
 * Value = function(year)->url OR string template.
 */
const DATASET_ROUTE_OVERRIDES = {
  // If your backend already matches /bulk_download/<key>/<year>, you can leave this empty.
  // Example if needed:
  // weather: (year) => `/bulk_download/weather/${year}`,
  // loggers: (year) => `/bulk_download/loggers/${year}`,
};

function buildBulkDownloadUrl(datasetKey, year) {
  const override = DATASET_ROUTE_OVERRIDES[datasetKey];
  if (typeof override === "function") return override(year);
  if (typeof override === "string") return override.replace("{year}", year);

  // Default convention:
  return `/bulk_download/${datasetKey}/${year}`;
}

/**
 * Initialize the Bulk Downloads tab (Option A).
 *
 * Expects:
 *  - #bulk-year select exists
 *  - buttons have class "bulk-download-btn" and data-dataset="..."
 *  - /bulk_download/options returns:
 *      { available: { "2023": { loggers: true, weather: true, irrigation: true, ... }, ... } }
 */
export async function initBulkDownloadTab() {
  const yearSelect = document.getElementById("bulk-year");
  if (!yearSelect) {
    console.warn("Bulk year select (#bulk-year) not found.");
    return;
  }

  const buttons = Array.from(document.querySelectorAll("button.bulk-download-btn"));
  if (!buttons.length) {
    console.warn("No bulk download buttons found (button.bulk-download-btn).");
    return;
  }

  const availableByYear = await fetchBulkDownloadOptions();
  const years = Object.keys(availableByYear).sort();

  if (!years.length) {
    yearSelect.innerHTML = `<option value="">No years available</option>`;
    buttons.forEach((b) => { b.disabled = true; });
    return;
  }

  // Populate dropdown
  yearSelect.innerHTML = "";
  years.forEach((y) => {
    const opt = document.createElement("option");
    opt.value = y;
    opt.textContent = y;
    yearSelect.appendChild(opt);
  });

  function updateButtons() {
    const y = yearSelect.value;
    const avail = availableByYear[y] || {};

    buttons.forEach((btn) => {
      const key = btn.getAttribute("data-dataset");
      if (!key) return;
      btn.disabled = !Boolean(avail[key]);
    });
  }

  yearSelect.addEventListener("change", updateButtons);

  // Wire all buttons
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const y = yearSelect.value;
      const key = btn.getAttribute("data-dataset");
      if (!y || !key) return;

      const url = buildBulkDownloadUrl(key, y);
      window.location.href = url;
    });
  });

  updateButtons();
}

// ----------------------------------------------------
//  ES module exports (for other JS files)
// ----------------------------------------------------
export {
  downloadTraceData,
  downloadTraceBundleZip,
  downloadPlot,
  downloadSummaryData,
};
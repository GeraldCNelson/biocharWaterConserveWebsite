// static/js/downloads.js

/* global Plotly */

import { getDropdownValue, getElementByIdSafe, showAlert } from "./ui_utils.js";

// Fixed-size export for high-resolution images (pixels)
const FIXED_EXPORT_WIDTH  = 2000;
const FIXED_EXPORT_HEIGHT = 1200;

// ----------------------------------------------------
//  Main data download buttons (raw / ratio / all)
// ----------------------------------------------------
function downloadTraceData(type) {
  console.log(`📥 Downloading ${type} data...`);

  const year            = getDropdownValue("main-year");
  const granularity     = getDropdownValue("main-granularity");
  const variable        = getDropdownValue("main-variable");
  const strip           = getDropdownValue("main-strip");
  const depth           = getDropdownValue("main-depth");
  const loggerLocation  = getDropdownValue("main-loggerLocation");

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
  let scale = 2; // keep text crisp

  if (mode === "fixed") {
    // Static high-resolution export
    exportWidth  = FIXED_EXPORT_WIDTH;
    exportHeight = FIXED_EXPORT_HEIGHT;
    scale        = 2;
  } else {
    // Match current browser size (WYSIWYG)
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
//  Summary tab downloads (already using fetch + blob)
// ----------------------------------------------------
async function downloadSummaryData(type) {
  const year        = getDropdownValue("summary-year");
  const variable    = getDropdownValue("summary-variable");
  const strip       = getDropdownValue("summary-strip");
  const depth       = getDropdownValue("summary-depth");
  const granularity = getDropdownValue("summary-granularity");

  const payload = {
    year,
    variable,
    strip,
    depth,
    granularity,
    type, // "raw", "ratio", or "all"
  };

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
//  ES module exports (for other JS files)
// ----------------------------------------------------
export { downloadTraceData, downloadPlot, downloadSummaryData };
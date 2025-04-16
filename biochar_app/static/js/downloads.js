import { getDropdownValue, getElementByIdSafe } from "./ui_utils.js";

function showAlert(msg) {
  alert(msg); // replace with modal or toast if desired
}

function downloadPlot(plotType, format) {
    console.log(`📡 Downloading ${plotType} plot as ${format}...`);

    // ✅ Retrieve selected values safely
    const year = getDropdownValue("main-year") || "unknown";
    const variable = getDropdownValue("main-variable") || "unknown";
    const strip = getDropdownValue("main-strip") || "unknown";
    const loggerLocation = getDropdownValue("main-loggerLocation") || "unknown";
    const depth = (getDropdownValue("main-depth") || "unknown").replace(" ", "");

    const filename = `${plotType}_plot_${year}_${variable}_${strip}_${loggerLocation}_${depth}`;
    console.log(`📂 Generated filename: ${filename}.${format}`);

    const plotElement = getElementByIdSafe(`${plotType}-plot`);
    if (!plotElement) {
        console.error(`❌ Plot container not found: ${plotType}-plot`);
        return;
    }

    Plotly.downloadImage(plotElement, {
        format,
        filename,
        height: 500,
        width: 800,
    });
}


function downloadTraceData(type) {
    console.log(`📥 Downloading ${type} data...`);

    const year = getDropdownValue("main-year");
    const granularity = getDropdownValue("main-granularity");
    const variable = getDropdownValue("main-variable");
    const strip = getDropdownValue("main-strip");
    const depth = getDropdownValue("main-depth");
    const loggerLocation = getDropdownValue("main-loggerLocation");

    const params = new URLSearchParams({
        year, granularity, variable, strip, depth, loggerLocation
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
 * Downloads summary statistics as a CSV file.
 * Relies on data previously fetched and displayed in summary-table-container.
 */

export async function downloadSummaryData(type) {
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
    type  // "raw", "ratio", or "all"
  };

  // 🧠 Check that we have stats available before trying to download
  const stats = window.latestSummaryStats || {};
  const isGseason = granularity === "gseason";

  if (isGseason) {
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
      payload.summaryStats = isGseason ? stats.gseason_stats : (
      type === "raw" ? stats.raw :
      type === "ratio" ? stats.ratio :
      { ...stats.raw, ...stats.ratio }
    );
    const response = await fetch("/download_summary_data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
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


document.addEventListener("DOMContentLoaded", () => {
  // ✅ Main Data Display
  window.downloadPlot = downloadPlot;
  window.downloadTraceData = downloadTraceData;
  window.downloadSummaryData = downloadSummaryData;
});

document.addEventListener("DOMContentLoaded", () => {
  // ✅ Main Data Display
  window.downloadPlot = downloadPlot;
  window.downloadTraceData = downloadTraceData;
  window.downloadSummaryData = downloadSummaryData;

  // ✅ Attach event listeners for Summary downloads
  const rawBtn = document.getElementById("download-summary-raw");
  const ratioBtn = document.getElementById("download-summary-ratio");
  const allBtn = document.getElementById("download-summary-all");

  if (rawBtn) rawBtn.addEventListener("click", (e) => {
    e.preventDefault();
    downloadSummaryData("raw");
  });

  if (ratioBtn) ratioBtn.addEventListener("click", (e) => {
    e.preventDefault();
    downloadSummaryData("ratio");
  });

  if (allBtn) allBtn.addEventListener("click", (e) => {
    e.preventDefault();
    downloadSummaryData("all");
  });
});

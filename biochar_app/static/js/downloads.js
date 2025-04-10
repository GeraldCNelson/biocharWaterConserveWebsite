import { getDropdownValue, getElementByIdSafe } from "./ui_utils.js";

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

function downloadSummaryData(type) {
  const rawStats = window.latestSummaryStats?.raw;
  const ratioStats = window.latestSummaryStats?.ratio;

  if (!rawStats && !ratioStats) {
    alert("⚠️ No summary statistics available for download.");
    return;
  }

  let statsToDownload = {};
  let suffix = type;

  if (type === "raw") {
    statsToDownload = rawStats;
  } else if (type === "ratio") {
    statsToDownload = ratioStats;
  } else if (type === "all") {
    statsToDownload = {
      ...Object.fromEntries(Object.entries(rawStats || {}).map(([k, v]) => [`Raw - ${k}`, v])),
      ...Object.fromEntries(Object.entries(ratioStats || {}).map(([k, v]) => [`Ratio - ${k}`, v]))
    };
  }

  const year = getDropdownValue("summary-year");
  const variable = getDropdownValue("summary-variable");
  const strip = getDropdownValue("summary-strip");
  const granularity = getDropdownValue("summary-granularity");

  const payload = {
    summaryStats: statsToDownload,
    year,
    variable,
    strip,
    granularity
  };

  fetch("/download_summary_data", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  })
    .then(response => {
      if (!response.ok) throw new Error("❌ Download request failed.");
      return response.blob();
    })
    .then(blob => {
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `summary_${suffix}_${variable}_${strip}_${year}_${granularity}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
    })
    .catch(error => {
      console.error("❌ Error downloading summary:", error);
      alert("⚠️ Error downloading summary data.");
    });
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

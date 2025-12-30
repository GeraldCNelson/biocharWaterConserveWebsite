// static/js/downloads.js

/**
 * Helper: build a filename-safe slug from pieces.
 */
function buildFilename(parts) {
  return parts
    .filter(Boolean)
    .join("_")
    .replace(/\s+/g, "-")
    .replace(/[^\w\-]+/g, "");
}

/**
 * Generic helper to POST JSON and download the returned blob.
 */
async function postAndDownload(url, payload, filename) {
  console.log("⬇️ postAndDownload →", url, payload);

  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const txt = await resp.text();
    console.error("❌ Download failed:", resp.status, txt);
    throw new Error(`Download failed with status ${resp.status}`);
  }

  const blob   = await resp.blob();
  const objUrl = window.URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = objUrl;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();

  window.URL.revokeObjectURL(objUrl);
  console.log("✅ Download triggered:", filename);
}

/* -------------------------------------------------------------------------
 *  MAIN DATA DOWNLOADS (Raw / Ratio / All)
 * ---------------------------------------------------------------------- */

/**
 * Download trace data (raw / ratio / all) for the Main Data Display tab.
 *
 * This is exposed on window as `downloadTraceData` and is typically called
 * from the "Download Data" dropdown on the main tab.
 *
 * @param {string} kind - "raw" | "ratio" | "all"
 */
export async function downloadTraceData(kind = "all") {
  try {
    const yearEl     = document.getElementById("main-year");
    const variableEl = document.getElementById("main-variable");
    const stripEl    = document.getElementById("main-strip");
    const granEl     = document.getElementById("main-granularity");
    const depthEl    = document.getElementById("main-depth");

    if (!yearEl || !variableEl || !stripEl || !granEl || !depthEl) {
      alert("⚠️ Cannot download data: one or more controls are missing.");
      return;
    }

    const year        = parseInt(yearEl.value, 10);
    const variable    = variableEl.value;
    const strip       = stripEl.value;
    const granularity = granEl.value;
    const depth       = depthEl.value;
    const unitSystem  = window.unitSystem || "us";

    const payload = {
      year,
      variable,
      strip,
      granularity,
      depth,
      unitSystem,
      downloadType: kind,
    };

    const fname =
      buildFilename([
        "data",
        kind,
        variable,
        strip,
        `${depth}depth`,
        granularity,
        year,
      ]) + ".zip";

    // NOTE: this assumes your FastAPI route is mounted at /api/download_data.
    await postAndDownload("/api/download_data", payload, fname);
  } catch (err) {
    console.error("❌ Error in downloadTraceData:", err);
    alert("Unable to download data. Please check the console for details.");
  }
}

/* -------------------------------------------------------------------------
 *  SUMMARY STATISTICS DOWNLOAD (Summary Statistics tab)
 * ---------------------------------------------------------------------- */

/**
 * Download summary statistics (raw / ratio / all / zip) for the Summary tab.
 *
 * @param {string} mode - "raw" | "ratio" | "all" | "zip"
 */
export async function downloadSummaryData(mode = "all") {
  const yearEl        = document.getElementById("summary-year");
  const variableEl    = document.getElementById("summary-variable");
  const stripEl       = document.getElementById("summary-strip");
  const granularityEl = document.getElementById("summary-granularity");
  const depthEl       = document.getElementById("summary-depth");

  if (!yearEl || !variableEl || !stripEl || !granularityEl || !depthEl) {
    console.error("❌ downloadSummaryData: summary controls not found in DOM");
    alert("Internal error: summary controls are missing.");
    return;
  }

  const year        = parseInt(yearEl.value, 10);
  const variable    = variableEl.value;
  const strip       = stripEl.value;
  const granularity = granularityEl.value;
  const depth       = depthEl.value;
  const unitSystem  = window.unitSystem || "us";

  if (Number.isNaN(year)) {
    alert("Please choose a valid year before downloading.");
    return;
  }

  // If user selected "zip" but this isn’t gseason, treat it as "all"
  // so the filename matches the actual CSV content.
  let effectiveMode = mode;
  if (granularity !== "gseason" && mode === "zip") {
    effectiveMode = "all";
  }

  // Use the last summary response we already fetched in tables.js
  const summaryStats = window.__lastSummaryData || null;

  const payload = {
    year,
    variable,
    strip,
    granularity,
    depth,
    unitSystem,
    mode,          // backend can use or ignore this
    summaryStats,
  };

  console.log("⬇️ downloadSummaryData payload:", payload);

  try {
    const resp = await fetch("/api/download_summary_data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!resp.ok) {
      const txt = await resp.text();
      console.error("❌ Summary download failed:", resp.status, txt);
      alert("Unable to download summary data.");
      return;
    }

    const blob = await resp.blob();
    const url  = window.URL.createObjectURL(blob);

    // Extension is driven by granularity: gseason → ZIP, others → CSV
    const ext = granularity === "gseason" ? "zip" : "csv";

    // Build filename
    let baseName = `summary_${granularity}_${variable}_${year}`;

    // For gseason+zip we omit the mode suffix:
    //   summary_gseason_VWC_2025.zip
    // For everything else we append effectiveMode:
    //   summary_daily_VWC_2025_raw.csv
    //   summary_daily_VWC_2025_all.csv
    if (!(granularity === "gseason" && ext === "zip")) {
      baseName += `_${effectiveMode}`;
    }

    const a = document.createElement("a");
    a.href = url;
    a.download = `${baseName}.${ext}`;
    document.body.appendChild(a);
    a.click();
    a.remove();

    window.URL.revokeObjectURL(url);
    console.log("✅ Summary data download triggered:", `${baseName}.${ext}`);
  } catch (err) {
    console.error("❌ Error in downloadSummaryData:", err);
    alert("An error occurred while downloading summary data.");
  }
}

/**
 * Wire the Summary tab's dropdown menu to the downloadSummaryData() helper.
 *
 * Expected HTML IDs:
 *   - #download-summary-raw
 *   - #download-summary-ratio
 *   - #download-summary-all
 *   - #download-summary-zip   (optional: All Summary (ZIP) menu item)
 */
export function initSummaryDownloadMenu() {
  const rawBtn   = document.getElementById("download-summary-raw");
  const ratioBtn = document.getElementById("download-summary-ratio");
  const allBtn   = document.getElementById("download-summary-all");
  const zipBtn   = document.getElementById("download-summary-zip");

  if (!rawBtn && !ratioBtn && !allBtn && !zipBtn) {
    console.warn("⚠️ Summary download menu not found in DOM.");
    return;
  }

  if (rawBtn) {
    rawBtn.addEventListener("click", (evt) => {
      evt.preventDefault();
      // Explicitly ignore the Promise to avoid “Promise returned … is ignored” warnings
      void downloadSummaryData("raw");
    });
  }

  if (ratioBtn) {
    ratioBtn.addEventListener("click", (evt) => {
      evt.preventDefault();
      void downloadSummaryData("ratio");
    });
  }

  if (allBtn) {
    allBtn.addEventListener("click", (evt) => {
      evt.preventDefault();
      void downloadSummaryData("all");
    });
  }

  if (zipBtn) {
    zipBtn.addEventListener("click", (evt) => {
      evt.preventDefault();
      void downloadSummaryData("zip");
    });
  }

  console.log("✅ Summary download menu initialized.");
}

/* -------------------------------------------------------------------------
 *  PLOT IMAGE DOWNLOADS (JPEG/PNG of Plotly figures)
 * ---------------------------------------------------------------------- */

/**
 * Download a Plotly figure as an image.
 *
 * This is exposed on window as `downloadPlot`. It accepts either:
 *   - the ID of a plot div (e.g., "raw-plot"), or
 *   - a DOM element reference.
 */
export async function downloadPlot(target = "raw-plot") {
  try {
    // noinspection JSUnresolvedVariable
    const Plotly = window.Plotly;
    if (!Plotly) {
      alert("Plotly is not available; cannot download plot image.");
      return;
    }

    const el =
      typeof target === "string" ? document.getElementById(target) : target;

    if (!el) {
      console.error("❌ downloadPlot: plot element not found:", target);
      alert("Unable to find the plot container to download.");
      return;
    }

    const filenameBase = el.id || "plot";
    const filename = `${filenameBase}.png`;

    console.log("⬇️ Downloading plot image for:", filenameBase);

    // noinspection JSUnresolvedFunction
    const dataUrl = await Plotly.toImage(el, {
      format: "png",
      height: 600,
      width: 1000,
    });

    const a = document.createElement("a");
    a.href = dataUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();

    console.log("✅ Plot image download triggered:", filename);
  } catch (err) {
    console.error("❌ Error in downloadPlot:", err);
    alert("An error occurred while downloading the plot image.");
  }
}

/* -------------------------------------------------------------------------
 *  BULK DOWNLOAD TAB INITIALIZATION
 * ---------------------------------------------------------------------- */

/**
 * Initialize any handlers needed for the Bulk Downloads tab.
 *
 * If your Bulk Downloads tab is mostly simple links that hit static routes,
 * this can safely be a no-op with some debugging logs.
 */
export async function initBulkDownloadTab() {
  // Placeholder for any future wiring logic.
  console.log("📦 initBulkDownloadTab: nothing to wire at the moment.");
}
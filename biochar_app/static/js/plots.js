// plots.js (ES6 Module Version)

import { getSelectedFilters } from "./ui_controls.js";
import { DEBUG } from "./config.js";
import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";

// --- Zoom / pan syncing between raw (plot-1) and ratio (plot-2) ---
let isSyncingZoom = false;

function syncZoom(sourceDiv, targetDiv, eventData) {
  if (!targetDiv || isSyncingZoom) return;

  const hasXRange =
    "xaxis.range[0]" in eventData && "xaxis.range[1]" in eventData;
  if (!hasXRange) return;

  const newRange = [
    eventData["xaxis.range[0]"],
    eventData["xaxis.range[1]"],
  ];

  isSyncingZoom = true;
  Plotly.relayout(targetDiv, { "xaxis.range": newRange })
    .catch((err) => console.error("❌ Error syncing zoom:", err))
    .finally(() => {
      isSyncingZoom = false;
    });
}

export function debugLog(...args) {
  if (DEBUG) console.log(...args);
}

export function debugGroup(title, callback) {
  if (DEBUG) {
    console.groupCollapsed(title);
    try {
      callback();
    } finally {
      console.groupEnd();
    }
  } else {
    callback();
  }
}

const API_BASE = "/api";

/**
 * Ensure plots resize when the window size changes
 */
/**
 * Ensure plots resize when the window size changes
 * - Recomputes right gutter + legend placement
 * - Forces plot-1 and plot-2 to share the same right gutter so x-axes align
 */
let resizeHookInstalled = false;


function installResizeHandler() {
  if (resizeHookInstalled) return;
  resizeHookInstalled = true;

  let timer = null;

  function relayoutOne(el, plotType, forcedMargins = null) {
    if (!el || !el.layout) return null;

    const w = el.clientWidth || el.parentElement?.clientWidth || 1200;

    // Decide gutter from the *rendered* graph div (fullLayout/fullData)
    let rightGutter = computeRightGutterPx(el, plotType);
    let leftMargin = (el.layout?.margin?.l ?? 60);

    // Force margins if requested (to align domains)
    if (forcedMargins) {
      if (typeof forcedMargins.r === "number") rightGutter = forcedMargins.r;
      if (typeof forcedMargins.l === "number") leftMargin = forcedMargins.l;
    }

    // Build relayout payload
    const update = {
      width: w,
      "margin.l": leftMargin,
      "margin.r": rightGutter,
      "yaxis.automargin": false,
    };
    if (el.layout?.yaxis2) update["yaxis2.automargin"] = false;

    // Legend placement must match gutter choice
    const tmp = { legend: el.layout.legend || {} };
    applyResponsiveLegend(tmp, rightGutter);
    update.legend = tmp.legend;

    Plotly.relayout(el, update);
    Plotly.Plots.resize(el);

    return { l: leftMargin, r: rightGutter };
  }

  window.addEventListener("resize", () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      const p1 = document.getElementById("plot-1");
      const p2 = document.getElementById("plot-2");

      // 1) Relayout raw first, capture its margins
      const margins = relayoutOne(p1, "raw");

      // 2) Force ratio to use the exact same margins (align x-axis domains)
      if (margins) relayoutOne(p2, "ratio", margins);
      else relayoutOne(p2, "ratio");
    }, 120);
  });
}

/**
 * 📊 updatePlot
 * @param {string} plotType  "raw" | "ratio"
 * @param {string} plotDivId "plot-1" | "plot-2"
 */

export async function updatePlot(plotType, plotDiv) {
  const plotEl = document.getElementById(plotDiv);
  if (!plotEl) {
    console.error(`❌ updatePlot: plot container not found: ${plotDiv}`);
    return;
  }

  // Status line (optional but helpful)
  const statusEl = document.getElementById("plots-status");
  const setStatus = (msg, show = true) => {
    if (!statusEl) return;
    statusEl.textContent = msg || "";
    statusEl.style.display = show ? "block" : "none";
  };

  debugLog(`📡 Fetching ${plotType} plot data...`);

  // ✅ Show overlay + status
  const overlayMsg = (plotType === "raw") ? "Loading top plot" : "Loading bottom plot";
  showLoadingOverlay(plotEl, overlayMsg);
  setStatus("Loading plots…", true);

  try {
    // 1) Grab all dropdowns + inputs
    const requestData = getSelectedFilters("main");
    const { startDate, endDate, granularity, variable } = requestData;

    // 2) Validate dates
    if (!startDate || !endDate) {
      console.error(
        `❌ Cannot update plots: ${
          !startDate && !endDate
            ? "both Start Date and End Date are required."
            : !startDate
            ? "Start Date is missing."
            : "End Date is missing."
        }`
      );
      return;
    }

    // 3) Build query string
    const params = new URLSearchParams({
      year: String(requestData.year ?? ""),
      granularity: String(granularity ?? ""),
      startDate: String(startDate ?? ""),
      endDate: String(endDate ?? ""),
      variable: String(variable ?? ""),
      depth: String(requestData.depth ?? ""),
      strip: String(requestData.strip ?? ""),
      logger: String(requestData.logger ?? ""),
      // If you later add unitSystem to plot routes, add it here too:
      // unitSystem: window.unitSystem || "us",
    });

    // 4) Conditionally include weather overlays
    if (granularity !== "gseason") {
      if (variable === "T") params.set("includeTemperature", "true");
      if (variable === "VWC") params.set("includeRainfall", "true");
    }

    // 5) Construct GET URL
    const url = `${API_BASE}/plot/${plotType}?${params.toString()}`;
    debugLog("GET", url);

    // 6) Fetch
    const response = await fetch(url, { method: "GET" });
    if (!response.ok) {
      const errText = await response.text();
      throw new Error(`❌ Server error: ${response.status} – ${errText}`);
    }

    const plotlyJSON = await response.json();
    debugLog(`✅ Received ${plotType} JSON:`, plotlyJSON);

    // 7) Force responsive layout on the client, even if backend sent fixed width/height
    const layout = plotlyJSON.layout || {};
    layout.autosize = true;
    delete layout.width;
    delete layout.height;

    // 8) Render (responsive config!)
    await Plotly.react(plotEl, plotlyJSON.data, layout, { responsive: true });

    // 9) Resize (helps after initial render)
    Plotly.Plots.resize(plotEl);
    debugLog(`🔄 Resized ${plotDiv}`);

  } catch (err) {
    console.error(`❌ Error updating ${plotType} plot:`, err);
  } finally {
    // ✅ Always hide overlay
    hideLoadingOverlay(plotEl);

    // ✅ If BOTH plots are done, hide the status line.
    // Simple approach: if neither plot-1 nor plot-2 currently has a visible overlay, hide status.
    const p1 = document.getElementById("plot-1");
    const p2 = document.getElementById("plot-2");
    const p1Overlay = p1?.querySelector?.(":scope > .loading-overlay");
    const p2Overlay = p2?.querySelector?.(":scope > .loading-overlay");
    const p1Busy = p1Overlay && p1Overlay.style.display !== "none";
    const p2Busy = p2Overlay && p2Overlay.style.display !== "none";

    if (!p1Busy && !p2Busy) setStatus("", false);
  }
}

/**
 * Wire zoom syncing once plots exist
 */
export function wireMainPlotZoomSync() {
  const p1 = document.getElementById("plot-1");
  const p2 = document.getElementById("plot-2");
  if (!p1 || !p2) return;

  if (p1.dataset.zoomSync === "1") return;
  p1.dataset.zoomSync = "1";

  p1.on("plotly_relayout", (ev) => syncZoom(p1, p2, ev));
  p2.on("plotly_relayout", (ev) => syncZoom(p2, p1, ev));
}

/**
 * Utility
 */
export function capitalize(s) {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
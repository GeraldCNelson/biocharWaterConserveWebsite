// plot_utils.js

import { getSelectedFilters } from "./ui_controls.js";
import { isMobileDevice } from "./ui_utils.js";

/**
 * Pause until each of the given dropdown IDs exists in the DOM
 * and has been populated with at least one <option>.
 * @param {string[]} ids – array of element IDs, e.g. ["main-year", "main-variable", …]
 */
export async function waitForAllDropdowns(ids) {
  const delay = (ms) => new Promise((res) => setTimeout(res, ms));
  while (true) {
    const missing = ids.filter((id) => {
      const el = document.getElementById(id);
      return !(el && el.options && el.options.length > 0);
    });
    if (missing.length === 0) break;
    await delay(50);
  }
}

/* ------------------------------------------------------------------ */
/* Zoom / pan sync state                                              */
/* ------------------------------------------------------------------ */

// These will hold the actual Plotly graph divs once rendered.
let rawPlotDiv = null;    // corresponds to #plot-1
let ratioPlotDiv = null;  // corresponds to #plot-2
let zoomHandlersAttached = false;
let isSyncingZoom = false;

/**
 * Apply the x-axis range from one plot to another.
 */
function syncZoom(sourceDiv, targetDiv, eventData) {
  if (!targetDiv || isSyncingZoom || !eventData) return;

  const hasXRange =
    "xaxis.range[0]" in eventData && "xaxis.range[1]" in eventData;

  if (!hasXRange) {
    return;
  }

  const newRange = [
    eventData["xaxis.range[0]"],
    eventData["xaxis.range[1]"],
  ];

  console.log("🔁 syncing x-range →", newRange);

  isSyncingZoom = true;
  Plotly.relayout(targetDiv, { "xaxis.range": newRange })
    .catch((err) => {
      console.error("❌ Error syncing zoom:", err);
    })
    .finally(() => {
      isSyncingZoom = false;
    });
}

/**
 * Once both plots exist, attach relayout handlers in *both* directions.
 */
function maybeAttachZoomSyncHandlers() {
  if (zoomHandlersAttached) return;
  if (!rawPlotDiv || !ratioPlotDiv) return;

  const makeHandler = (source, target, label) => (ev) => {
    // Plotly's .on passes payload directly; no .detail.
    const payload = ev;
    console.log(`📐 ${label} relayout →`, payload);
    syncZoom(source, target, payload);
  };

  rawPlotDiv.on("plotly_relayout", makeHandler(rawPlotDiv, ratioPlotDiv, "raw"));
  ratioPlotDiv.on(
    "plotly_relayout",
    makeHandler(ratioPlotDiv, rawPlotDiv, "ratio")
  );

  zoomHandlersAttached = true;
  console.log("✅ Zoom sync handlers attached (raw ↔ ratio)");
}

/* ------------------------------------------------------------------ */
/* Fetch + render helper                                              */
/* ------------------------------------------------------------------ */

/**
 * Fetch plot data from the server and render it into a Plotly div.
 * @param {string} plotType – e.g. "raw" or "ratio"
 * @param {string} [plotDivId] – the DOM id of the target <div>; defaults to `plot-${plotType}`
 */
export async function fetchAndRenderPlot(plotType, plotDivId) {
  const targetId = plotDivId || `plot-${plotType}`;
  const label   = `🔧 fetchAndRenderPlot("${plotType}", "#${targetId}")`;
  console.group(label);

  try {
    // 1) Gather filters from the DOM
    const filters = getSelectedFilters("main");

    // 2) Inject unitSystem (FastAPI requires it)
    console.log("🌐 window.unitSystem =", window.unitSystem);
    console.log("🌐 filters =", filters);
    filters.unitSystem = window.unitSystem || "us";

    // 3) Add kind
    filters.kind = plotType;

    // 4) Debug assembled filters
    console.log("🔍 Filters (JS object) →", filters);
    console.log("📤 Payload JSON →", JSON.stringify(filters));

    // 5) Endpoint
    const url = `/api/plot_${plotType}`;
    console.log("🌐 Fetching →", url);

    // 6) Send request
    const resp = await fetch(url, {
      method:      "POST",
      headers:     { "Content-Type": "application/json" },
      credentials: "same-origin",
      body:        JSON.stringify(filters),
    });

    // 7) Read and log response
    const text = await resp.text();
    console.log("⏳ Response →", resp.status, resp.statusText);
    const snippet =
      text.length > 200
        ? text.slice(0, 200).replace(/\s+/g, " ") + "…"
        : text.replace(/\s+/g, " ");
    console.log("📄 Body snippet →", snippet);

    if (!resp.ok) {
      console.error(`❌ Server error ${resp.status}:`, text);
      return;
    }

    // 8) Parse JSON payload
    const plotData = JSON.parse(text);
    console.log("❓ payload for", plotType, plotData);

    // --- NEW: synchronize INITIAL x-axis range between raw & ratio ---
    // We only care about the Main Data Display pair: #plot-1 (raw) and #plot-2 (ratio).
    if (plotType === "raw" && targetId === "plot-1") {
      // Save whatever x-axis range the backend gave the raw plot
      const rawRange = plotData?.layout?.xaxis?.range || null;
      window._initialXRange = rawRange;
      console.log("📌 Saved raw x-range for sync:", window._initialXRange);
    }

    if (plotType === "ratio" && targetId === "plot-2" && window._initialXRange) {
      // Force ratio plot to use the same initial x-range as the raw plot
      plotData.layout = plotData.layout || {};
      plotData.layout.xaxis = plotData.layout.xaxis || {};
      plotData.layout.xaxis.range = window._initialXRange;
      console.log("📌 Applied raw x-range to ratio plot:", window._initialXRange);
    }
    // --- end NEW section ---

    console.log("🧱 shapes →", plotData.layout?.shapes);

    // 9) Debug trace info
    console.log("🔢 total traces →", plotData.data.length);
    console.log("🔖 trace names →", plotData.data.map(t => t.name));
    console.log("🔧 trace types →", plotData.data.map(t => t.type));
    console.log("📊 final data →", plotData.data);
    console.log("📊 final layout →", plotData.layout);

    // 10) Validate & find container
    if (!Array.isArray(plotData.data)) {
      console.error("❌ `data` is not an array:", plotData.data);
      return;
    }

    const container = document.getElementById(targetId);
    if (!container) {
      console.error(`❌ Container "#${targetId}" not found`);
      return;
    }
    console.log(`📦 Rendering into → #${targetId}`, container);

    // 11) Render with Plotly, then attach zoom handlers if applicable
    await new Promise(r => requestAnimationFrame(r));
    Plotly.purge(container);

    const parentWidth = container.clientWidth;
    const layout = {
      ...plotData.layout,
      autosize: false,
      width:    parentWidth,
      height:   500,
      margin: {
        l: plotData.layout?.margin?.l ?? 60,
        r: 20,
        t: plotData.layout?.margin?.t ?? 50,
        b: plotData.layout?.margin?.b ?? 50,
      },
    };

    const plotConfig = {
      displayModeBar: false,
      displaylogo:    false,
      responsive:     false,
    };

    const gd = await Plotly.newPlot(container, plotData.data, layout, plotConfig);
    Plotly.Plots.resize(container);

    // Track which plot we just rendered and try attaching sync handlers
    if (targetId === "plot-1") {
      rawPlotDiv = gd;
      console.log("📌 rawPlotDiv ready");
    } else if (targetId === "plot-2") {
      ratioPlotDiv = gd;
      console.log("📌 ratioPlotDiv ready");
    }

    maybeAttachZoomSyncHandlers();
  } catch (err) {
    console.error(`❌ fetchAndRenderPlot(${plotType}) uncaught:`, err);
  } finally {
    console.groupEnd();
  }
}
/* ------------------------------------------------------------------ */
/* Public helper to render both main plots                             */
/* ------------------------------------------------------------------ */

/**
 * Kick off both of your main‐tab plots in sequence.
 */
export async function renderMainPlots() {
  console.group("▶️ Rendering Main Data Display plots…");
  try {
    // reset in case we re-render everything
    zoomHandlersAttached = false;
    rawPlotDiv = null;
    ratioPlotDiv = null;

    await fetchAndRenderPlot("raw", "plot-1");
    await fetchAndRenderPlot("ratio", "plot-2");
    // handlers will be attached automatically once both plots exist
  } catch (err) {
    console.error("❌ renderMainPlots uncaught:", err);
  } finally {
    console.groupEnd();
  }
}
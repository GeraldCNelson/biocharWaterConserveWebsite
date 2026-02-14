// plot_utils.js

import { getSelectedFilters } from "./ui_controls.js";
import { isMobileDevice } from "./ui_utils.js";
import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";

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
 * Decide how much right margin ("gutter") we need *right now*.
 *
 * Supports:
 *  - initial render: pass (containerEl, plotType, plotLayout, plotDataArray)
 *  - after render:   pass (gd, plotType)  where gd is the Plotly graph div
 */
export function computeRightGutterPx(containerOrGd, plotType, plotLayout = null, plotData = null) {
  const el = containerOrGd;
  const w = el?.clientWidth || 1200;

  // Prefer "after render" truth when available
  const fullLayout = el?._fullLayout || null;
  const fullData = el?._fullData || el?.data || null;

  // Fall back to what we were passed during first render
  const layout = fullLayout || plotLayout || {};
  const data = fullData || plotData || [];

  // Does the layout define yaxis2?
  const layoutHasY2 = !!layout?.yaxis2;

  // Do any traces use y2?
  const dataUsesY2 = Array.isArray(data) && data.some((t) => (t?.yaxis || "") === "y2");

  // Raw plots typically need the extra space (precip bars + legend outside).
  // This stays safe if you later add y2 to ratio plots.
  const hasY2 = layoutHasY2 || dataUsesY2;

  if (!hasY2) return 20;

  // Responsive gutter — scales down as window narrows
  if (w >= 1200) return 240;
  if (w >= 1000) return 200;
  if (w >= 850) return 160;

  // Small screens: stop reserving a big gutter; legend should move below
  return 20;
}

/**
 * Update legend placement based on gutter choice.
 * Returns an object with:
 *  - legend: the legend config to apply
 *  - extraBottom: recommended extra bottom margin when legend is below
 */
function applyResponsiveLegend(layout, rightGutterPx) {
  if (rightGutterPx >= 160) {
    const base = layout.legend || {};
    layout.legend = {
      ...base,
      x: 1.02,
      xanchor: "left",
      y: 1.0,
      yanchor: "top",
      orientation: "v",
    };
    return { extraBottom: 0 };
  } else {
    const base = layout.legend || {};
    layout.legend = {
      ...base,
      x: 0,
      xanchor: "left",
      y: -0.25,
      yanchor: "top",
      orientation: "h",
    };
    // give space for the legend rows below the plot
    return { extraBottom: 80 };
  }
}

/**
 * Fetch plot data from the server and render it into a Plotly div.
 * @param {string} plotType – e.g. "raw" or "ratio"
 * @param {string} [plotDivId] – the DOM id of the target <div>; defaults to `plot-${plotType}`
 */

export async function fetchAndRenderPlot(plotType, plotDivId) {
  const targetId = plotDivId || `plot-${plotType}`;
  console.group(`🔧 fetchAndRenderPlot("${plotType}", "#${targetId}")`);

  const container = document.getElementById(targetId);
  if (!container) {
    console.error(`❌ Container "#${targetId}" not found`);
    console.groupEnd();
    return;
  }

  const overlayHost = container.parentElement || container;
  const statusEl = document.getElementById("plots-status");

  // --- helpers ---------------------------------------------------------

  // Use the SAME pixel width for both plots (prevents drift)
  function getSharedPlotWidth() {
    const p1 = document.getElementById("plot-1");
    const p2 = document.getElementById("plot-2");

    // Prefer wrapper widths (more stable than plot div widths)
    const w1 = p1?.parentElement?.clientWidth || p1?.clientWidth || 0;
    const w2 = p2?.parentElement?.clientWidth || p2?.clientWidth || 0;

    if (w1 && w2) return Math.max(320, Math.min(w1, w2));
    const w = overlayHost?.clientWidth || container.clientWidth || 1200;
    return Math.max(320, w);
  }

  function enforceNoAutoMargins(layout) {
    layout.yaxis = layout.yaxis || {};
    layout.yaxis.automargin = false;
    if (layout.yaxis2) {
      layout.yaxis2.automargin = false;
    }
  }

  // After BOTH plots exist: force same width/margins + force ratio x-domain to match raw
  async function syncPlotGeometry() {
    const p1 = document.getElementById("plot-1");
    const p2 = document.getElementById("plot-2");
    if (!p1?.layout || !p2?.layout) return;

    const sharedW = getSharedPlotWidth();

    // Compute gutters using “after-render truth” when available
    const g1 = computeRightGutterPx(p1, "raw", p1.layout, p1.data);
    const g2 = computeRightGutterPx(p2, "ratio", p2.layout, p2.data);
    const sharedRight = Math.max(g1, g2);

    const left = window._plotLeftMargin ?? 60;
    window._plotLeftMargin = left;

    // Legend placement depends on gutter (keep whatever behavior you already want)
    const legend1 = { ...(p1.layout.legend || {}) };
    const tmp1 = { legend: legend1 };
    applyResponsiveLegend(tmp1, sharedRight);

    const legend2 = { ...(p2.layout.legend || {}) };
    const tmp2 = { legend: legend2 };
    applyResponsiveLegend(tmp2, sharedRight);

    // Apply same width + margins to BOTH
    await Plotly.relayout(p1, {
      width: sharedW,
      "margin.l": left,
      "margin.r": sharedRight,
      "yaxis.automargin": false,
      ...(p1.layout.yaxis2 ? { "yaxis2.automargin": false } : {}),
      legend: tmp1.legend,
    });

    await Plotly.relayout(p2, {
      width: sharedW,
      "margin.l": left,
      "margin.r": sharedRight,
      "yaxis.automargin": false,
      ...(p2.layout.yaxis2 ? { "yaxis2.automargin": false } : {}),
      legend: tmp2.legend,
    });

    // Now force the *plot-area* to match: xaxis.domain
    const dom = p1?._fullLayout?.xaxis?.domain || p1.layout?.xaxis?.domain || null;
    if (Array.isArray(dom) && dom.length === 2) {
      await Plotly.relayout(p2, { "xaxis.domain": dom });
    }

    Plotly.Plots.resize(p1);
    Plotly.Plots.resize(p2);
  }

  // --- main ------------------------------------------------------------

  try {
    showLoadingOverlay(overlayHost, plotType === "raw" ? "Loading plots" : "Loading plot");

    if (statusEl) {
      statusEl.textContent = "Loading plots…";
      statusEl.style.display = "";
    }

    const filters = getSelectedFilters("main");
    filters.unitSystem = window.unitSystem || "us";
    filters.kind = plotType;

    const resp = await fetch(`/api/plot_${plotType}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify(filters),
    });

    const text = await resp.text();
    if (!resp.ok) {
      console.error(`❌ Server error ${resp.status}:`, text);
      return;
    }

    const plotData = JSON.parse(text);

    // --- Sync initial x-range between raw & ratio (your existing logic) ---
    if (plotType === "raw" && targetId === "plot-1") {
      window._initialXRange = plotData?.layout?.xaxis?.range || null;
    }
    if (plotType === "ratio" && targetId === "plot-2" && window._initialXRange) {
      plotData.layout = plotData.layout || {};
      plotData.layout.xaxis = plotData.layout.xaxis || {};
      plotData.layout.xaxis.range = window._initialXRange;
    }
    // --- end sync section ---

    if (!Array.isArray(plotData.data)) {
      console.error("❌ `data` is not an array:", plotData.data);
      return;
    }

    container.innerHTML = "";
    await new Promise((r) => requestAnimationFrame(r));
    Plotly.purge(container);

    // Use a shared width even on first draw
    const sharedW = getSharedPlotWidth();
    const leftMargin = window._plotLeftMargin ?? 60;
    window._plotLeftMargin = leftMargin;

    let rightGutter = computeRightGutterPx(container, plotType, plotData.layout, plotData.data);

    const layout = {
      ...plotData.layout,
      autosize: false,
      width: sharedW,
      height: 500,
      margin: {
        l: leftMargin,
        r: rightGutter,
        t: plotData.layout?.margin?.t ?? 50,
        b: plotData.layout?.margin?.b ?? 50,
      },
    };

    enforceNoAutoMargins(layout);
    applyResponsiveLegend(layout, rightGutter);

    const gd = await Plotly.newPlot(container, plotData.data, layout, {
      displayModeBar: false,
      displaylogo: false,
      responsive: false,
    });

    if (targetId === "plot-1") rawPlotDiv = gd;
    if (targetId === "plot-2") ratioPlotDiv = gd;

    // ✅ After each render attempt, try to sync both plots (no-ops until both exist)
    await syncPlotGeometry();

    // ✅ Install ONE resize handler that re-syncs geometry (not just Plotly.resize)
    if (!window._biocharResizePlotsInstalled) {
      window._biocharResizePlotsInstalled = true;

      let t = null;
      window.addEventListener("resize", () => {
        if (t) window.clearTimeout(t);
        t = window.setTimeout(() => {
          syncPlotGeometry().catch((e) => console.warn("syncPlotGeometry failed:", e));
        }, 150);
      });
    }

    maybeAttachZoomSyncHandlers();
  } catch (err) {
    console.error(`❌ fetchAndRenderPlot(${plotType}) uncaught:`, err);
  } finally {
    hideLoadingOverlay(overlayHost);
    console.groupEnd();
  }
}

/* Public helper to render both main plots                             */
/* ------------------------------------------------------------------ */

/**
 * Kick off both of your main‐tab plots in sequence.
 */
export async function renderMainPlots() {
  console.group("▶️ Rendering Interactive Plots…");

  const statusEl = document.getElementById("plots-status");
  try {
    if (statusEl) {
      statusEl.textContent = "Loading plots…";
      statusEl.style.display = "";
    }

    // reset in case we re-render everything
    zoomHandlersAttached = false;
    rawPlotDiv = null;
    ratioPlotDiv = null;

    await fetchAndRenderPlot("raw", "plot-1");
    await fetchAndRenderPlot("ratio", "plot-2");
  } catch (err) {
    console.error("❌ renderMainPlots uncaught:", err);
  } finally {
    if (statusEl) statusEl.style.display = "none";
    console.groupEnd();
  }
}
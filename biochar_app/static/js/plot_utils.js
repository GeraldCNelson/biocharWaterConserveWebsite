// plot_utils.js
//
// Restored from the working Lightsail/test-site layout logic, with small
// updates for the current POST plot routes.
//
// Design goals
// ------------
// 1. Keep raw and ratio plots aligned horizontally.
// 2. Use shared left/right margins across both plots.
// 3. Sync x-axis domains after render and after resize.
// 4. Keep legend on the right when there is room; move below only when needed.

import { getSelectedFilters } from "./ui_controls.js";
import { isMobileDevice } from "./ui_utils.js";
import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";

/**
 * Pause until each of the given dropdown IDs exists in the DOM
 * and has been populated with at least one <option>.
 * @param {string[]} ids – array of element IDs
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

  if (!hasXRange) return;

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
 * Once both plots exist, attach relayout handlers in both directions.
 */
function maybeAttachZoomSyncHandlers() {
  if (zoomHandlersAttached) return;
  if (!rawPlotDiv || !ratioPlotDiv) return;

  const makeHandler = (source, target, label) => (ev) => {
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
/* Responsive gutter / legend logic                                   */
/* ------------------------------------------------------------------ */

/**
 * Decide how much right margin ("gutter") we need right now.
 *
 * Supports:
 *  - initial render: pass (containerEl, plotType, plotLayout, plotDataArray)
 *  - after render:   pass (gd, plotType) where gd is the Plotly graph div
 */
export function computeRightGutterPx(containerOrGd, plotType, plotLayout = null, plotData = null) {
  const el = containerOrGd;
  const w = el?.clientWidth || 1200;

  // Prefer after-render truth when available
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
  const hasY2 = layoutHasY2 || dataUsesY2;

  if (!hasY2) return 20;

  // On mobile, don't try to keep a big right gutter.
  if (isMobileDevice()) return 20;

  // Desktop / tablet widths:
  if (w >= 1400) return 160;
  if (w >= 1150) return 140;
  if (w >= 950) return 120;
  if (w >= 800) return 100;

  // Narrow screens: stop reserving a large right gutter; legend goes below.
  return 20;
}

/**
 * Update legend placement based on gutter choice.
 */
function applyResponsiveLegend(layout, rightGutterPx) {
  const base = layout.legend || {};

  if (rightGutterPx >= 100) {
    layout.legend = {
      ...base,
      x: 1.01,
      xanchor: "left",
      y: 1.0,
      yanchor: "top",
      orientation: "v",
    };
    return { extraBottom: 0, minRight: 0 };
  } else {
    layout.legend = {
      ...base,
      x: 0,
      xanchor: "left",
      y: -0.16,
      yanchor: "top",
      orientation: "h",
    };
    return { extraBottom: 45, minRight: 70 };
  }
}

/* ------------------------------------------------------------------ */
/* Fetch + render helper                                              */
/* ------------------------------------------------------------------ */

export async function fetchAndRenderPlot(plotType, plotDivId) {
  const targetId = plotDivId || `plot-${plotType}`;
  const label = `🔧 fetchAndRenderPlot("${plotType}", "#${targetId}")`;
  console.group(label);

  const container = document.getElementById(targetId);
  if (!container) {
    console.error(`❌ Container "#${targetId}" not found`);
    console.groupEnd();
    return;
  }

  const overlayHost = container.parentElement || container;
  const statusEl = document.getElementById("plots-status");

  // ------------------------------------------------------------
  // Force plot-2 to use plot-1's xaxis.domain once both exist.
  // ------------------------------------------------------------
  const syncXAxisDomains = () => {
    const p1 = document.getElementById("plot-1");
    const p2 = document.getElementById("plot-2");
    if (!p1?._fullLayout?.xaxis || !p2?._fullLayout?.xaxis) return;

    const d = p1._fullLayout.xaxis.domain;
    if (!Array.isArray(d) || d.length !== 2) return;

    const d2 = p2._fullLayout.xaxis.domain;
    const same =
      Array.isArray(d2) &&
      d2.length === 2 &&
      Math.abs(d2[0] - d[0]) < 1e-6 &&
      Math.abs(d2[1] - d[1]) < 1e-6;

    if (!same) {
      Plotly.relayout(p2, { "xaxis.domain": d });
    }
  };

  try {
    const msg = plotType === "raw" ? "Loading plots" : "Loading plot";
    showLoadingOverlay(overlayHost, msg);

    if (statusEl) {
      statusEl.textContent = "Loading plots…";
      statusEl.style.display = "";
    }

    const filters = getSelectedFilters("main");
    filters.unitSystem = window.unitSystem || "us";
    filters.kind = plotType;

    const url = `/api/plot_${plotType}`;
    console.log(`📦 ${plotType} payload:`, filters);

    const resp = await fetch(url, {
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

    // --- Sync initial x-range between raw & ratio ---
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

    const parentWidth = container.clientWidth || 1200;

    // ------------------------------------
    // Consistent margins (key to x-axis alignment)
    // ------------------------------------
    const leftMargin = 60;
    window._plotLeftMargin = leftMargin;

    // Initial gutter guess
    let rightGutter = computeRightGutterPx(container, plotType, plotData.layout, plotData.data);

    // If ratio plot, force raw's gutter if known
    if (targetId === "plot-1") window._plotRightGutter = rightGutter;
    if (targetId === "plot-2" && typeof window._plotRightGutter === "number") {
      rightGutter = window._plotRightGutter;
    }

    const legendAdjust = { legend: plotData.layout?.legend || {} };
    const legendResponse = applyResponsiveLegend(legendAdjust, rightGutter);

    const effectiveRight = Math.max(
      rightGutter,
      legendResponse.minRight ?? 0
    );

    const layout = {
      ...plotData.layout,
      autosize: false,
      width: parentWidth,
      height: 500,
      margin: {
        l: leftMargin,
        r: effectiveRight,
        t: plotData.layout?.margin?.t ?? 50,
        b: Math.max(plotData.layout?.margin?.b ?? 50, 50 + legendResponse.extraBottom),
      },
      legend: legendAdjust.legend,
    };

    // Prevent Plotly from silently changing margins per plot
    layout.yaxis = layout.yaxis || {};
    layout.yaxis.automargin = false;
    if (layout.yaxis2) {
      layout.yaxis2.automargin = false;
    }

    const plotConfig = {
      displayModeBar: false,
      displaylogo: false,
      responsive: false, // we do our own resize handler
    };

    const gd = await Plotly.newPlot(container, plotData.data, layout, plotConfig);

    // Track rendered plot divs
    if (targetId === "plot-1") rawPlotDiv = gd;
    if (targetId === "plot-2") ratioPlotDiv = gd;

    // ------------------------------------
    // Refine gutter after render using _fullLayout/_fullData
    // ------------------------------------
    const refineGutter = () => {
      if (!gd) return null;

      let g = computeRightGutterPx(gd, plotType, null, null);

      // Save raw gutter; force ratio to match
      if (targetId === "plot-1") window._plotRightGutter = g;
      if (targetId === "plot-2" && typeof window._plotRightGutter === "number") {
        g = window._plotRightGutter;
      }

      const tmp = { legend: gd.layout?.legend || {} };
      const legendResult = applyResponsiveLegend(tmp, g);

      const effectiveRightAfterRender = Math.max(
        g,
        legendResult.minRight ?? 0
      );

      const update = {
        width: parentWidth,
        "margin.l": window._plotLeftMargin ?? 60,
        "margin.r": effectiveRightAfterRender,
        "margin.b": Math.max(gd.layout?.margin?.b ?? 50, 50 + legendResult.extraBottom),
        "yaxis.automargin": false,
        legend: tmp.legend,
      };
      if (gd.layout?.yaxis2) update["yaxis2.automargin"] = false;

      Plotly.relayout(gd, update);
      Plotly.Plots.resize(gd);

      return g;
    };

    // One refinement pass right after first draw
    refineGutter();

    // After rendering plot-2, force domain alignment to plot-1
    if (targetId === "plot-2") {
      await new Promise((r) => requestAnimationFrame(r));
      syncXAxisDomains();
    }

    // ------------------------------------
    // Install ONE resize handler
    // ------------------------------------
    if (!window._biocharResizePlotsInstalled) {
      window._biocharResizePlotsInstalled = true;

      let t = null;

      const relayoutOne = (el, kind, forceGutter = null) => {
        if (!el || !el.layout) return null;

        const w = el.clientWidth || el.parentElement?.clientWidth || 1200;

        let gutter =
          typeof forceGutter === "number"
            ? forceGutter
            : computeRightGutterPx(el, kind, el.layout, el.data);

        if (el.id === "plot-1") window._plotRightGutter = gutter;
        if (el.id === "plot-2" && typeof window._plotRightGutter === "number") {
          gutter = window._plotRightGutter;
        }

        const tmp = { legend: el.layout?.legend || {} };
        const legendResult = applyResponsiveLegend(tmp, gutter);

        const effectiveRightOnResize = Math.max(
          gutter,
          legendResult.minRight ?? 0
        );

        const update = {
          width: w,
          "margin.l": window._plotLeftMargin ?? 60,
          "margin.r": effectiveRightOnResize,
          "margin.b": Math.max(el.layout?.margin?.b ?? 50, 50 + legendResult.extraBottom),
          "yaxis.automargin": false,
          legend: tmp.legend,
        };
        if (el.layout?.yaxis2) update["yaxis2.automargin"] = false;

        Plotly.relayout(el, update);
        Plotly.Plots.resize(el);

        return gutter;
      };

      window.addEventListener("resize", () => {
        if (t) window.clearTimeout(t);
        t = window.setTimeout(() => {
          const p1 = document.getElementById("plot-1");
          const p2 = document.getElementById("plot-2");

          // Compute from raw first, then force ratio to match
          const gutter = relayoutOne(p1, "raw");
          relayoutOne(p2, "ratio", gutter);

          // Force exact x-axis domain match after relayout/resize
          syncXAxisDomains();
        }, 90);
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

/* ------------------------------------------------------------------ */
/* Public helper to render both main plots                            */
/* ------------------------------------------------------------------ */

export async function renderMainPlots() {
  console.group("▶️ Rendering Interactive Plots…");

  const statusEl = document.getElementById("plots-status");
  try {
    if (statusEl) {
      statusEl.textContent = "Loading plots…";
      statusEl.style.display = "";
    }

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

/**
 * Optional explicit hook for existing callers.
 * The handlers are also attached automatically after both plots render.
 */
export function wireMainPlotZoomSync() {
  maybeAttachZoomSyncHandlers();
}
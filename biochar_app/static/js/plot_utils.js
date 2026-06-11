// @ts-check
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
// 5. Use plot-1 as the master width and apply the same width to plot-2.
// 6. Choose ONE legend mode from plot-1 and force plot-2 to match it.
// 7. Reserve enough right-side space for the wider ratio legend when legend mode is "right".

import { getSelectedFilters } from "./ui_controls.js";
import { isMobileDevice } from "./ui_utils.js";
import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";

/**
 * Return Plotly from the global window object.
 * Kept as a helper so TypeScript stops complaining about the bare global.
 * @returns {any}
 */
function getPlotly() {
  return /** @type {any} */ (window).Plotly;
}

/**
 * @typedef {HTMLElement & {
 *   _fullLayout?: any,
 *   _fullData?: any,
 *   data?: any,
 *   layout?: any,
 *   on?: (eventName: string, handler: (ev: any) => void) => void
 * }} PlotlyGraphDiv
 */

/**
 * @typedef {Window & {
 *   _plotRenderWidth?: number | null,
 *   _plotRightGutter?: number | null,
 *   _plotLeftMargin?: number | null,
 *   _plotLegendMode?: "right" | "below" | null,
 *   _initialXRange?: any,
 *   _biocharResizePlotsInstalled?: boolean
 * }} PlotWindow
 */

/** @type {PlotWindow} */
const plotWindow = /** @type {PlotWindow} */ (window);

/**
 * Pause until each of the given dropdown IDs exists in the DOM
 * and has been populated with at least one <option>.
 *
 * @param {string[]} ids Array of element IDs.
 * @returns {Promise<void>}
 */
export async function waitForAllDropdowns(ids) {
  /**
   * @param {number} ms
   * @returns {Promise<void>}
   */
  const delay = (ms) => new Promise((res) => setTimeout(res, ms));

  while (true) {
    const missing = ids.filter((id) => {
      const el = /** @type {HTMLSelectElement | null} */ (document.getElementById(id));
      return !(el && el.options && el.options.length > 0);
    });

    if (missing.length === 0) break;
    await delay(50);
  }
}

/* ------------------------------------------------------------------ */
/* Zoom / pan sync state                                              */
/* ------------------------------------------------------------------ */

/** @type {PlotlyGraphDiv | null} */
let rawPlotDiv = null;
/** @type {PlotlyGraphDiv | null} */
let ratioPlotDiv = null;
let zoomHandlersAttached = false;
let isSyncingZoom = false;

/**
 * Apply the x-axis range from one plot to another.
 *
 * @param {PlotlyGraphDiv | null} sourceDiv
 * @param {PlotlyGraphDiv | null} targetDiv
 * @param {Record<string, any> | null | undefined} eventData
 * @returns {void}
 */
function syncZoom(sourceDiv, targetDiv, eventData) {
  void sourceDiv;
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

  /** @type {any} */
  const plotly = getPlotly();

  plotly
    .relayout(targetDiv, { "xaxis.range": newRange })
    .catch(
      /**
       * @param {unknown} err
       */
      (err) => {
        console.error("❌ Error syncing zoom:", err);
      }
    )
    .finally(() => {
      isSyncingZoom = false;
    });
}

/**
 * Once both plots exist, attach relayout handlers in both directions.
 *
 * @returns {void}
 */
function maybeAttachZoomSyncHandlers() {
  if (zoomHandlersAttached) return;
  if (!rawPlotDiv || !ratioPlotDiv) return;
  if (typeof rawPlotDiv.on !== "function" || typeof ratioPlotDiv.on !== "function") return;

  /**
   * @param {PlotlyGraphDiv} source
   * @param {PlotlyGraphDiv} target
   * @param {string} label
   * @returns {(ev: Record<string, any>) => void}
   */
  const makeHandler = (source, target, label) =>
    /**
     * @param {Record<string, any>} ev
     */
    (ev) => {
      const payload = ev;
      console.log(`📐 ${label} relayout →`, payload);
      syncZoom(source, target, payload);
    };

  rawPlotDiv.on("plotly_relayout", makeHandler(rawPlotDiv, ratioPlotDiv, "raw"));
  ratioPlotDiv.on("plotly_relayout", makeHandler(ratioPlotDiv, rawPlotDiv, "ratio"));

  zoomHandlersAttached = true;
  console.log("✅ Zoom sync handlers attached (raw ↔ ratio)");
}

/* ------------------------------------------------------------------ */
/* Shared width helpers                                               */
/* ------------------------------------------------------------------ */

/**
 * @param {HTMLElement | null} el
 * @returns {number}
 */
function measurePlotWidth(el) {
  if (!el) return 1200;
  return (
    el.clientWidth ||
    el.parentElement?.clientWidth ||
    el.getBoundingClientRect?.().width ||
    1200
  );
}

/**
 * @param {string} targetId
 * @param {HTMLElement | null} container
 * @returns {number}
 */
function getSharedPlotWidth(targetId, container) {
  const measured = Math.round(measurePlotWidth(container));

  if (targetId === "plot-1") {
    plotWindow._plotRenderWidth = measured;
    return measured;
  }

  if (targetId === "plot-2" && typeof plotWindow._plotRenderWidth === "number") {
    return plotWindow._plotRenderWidth;
  }

  return measured;
}

/* ------------------------------------------------------------------ */
/* Responsive gutter / legend logic                                   */
/* ------------------------------------------------------------------ */

/**
 * @param {HTMLElement | PlotlyGraphDiv | null} containerOrGd
 * @param {string} plotType
 * @param {any} [plotLayout=null]
 * @param {any[] | null} [plotData=null]
 * @returns {number}
 */
/**
 * @param {HTMLElement | PlotlyGraphDiv | null} containerOrGd
 * @param {string} plotType
 * @param {any} [plotLayout=null]
 * @param {any[] | null} [plotData=null]
 * @returns {number}
 */
export function computeRightGutterPx(containerOrGd, plotType, plotLayout = null, plotData = null) {
  void plotType;

  const el = /** @type {PlotlyGraphDiv | null} */ (containerOrGd);
  const w = el?.clientWidth || 1200;

  const fullLayout = el?._fullLayout || null;
  const fullData = el?._fullData || el?.data || null;

  const layout = fullLayout || plotLayout || {};
  const data = Array.isArray(fullData)
    ? fullData
    : (Array.isArray(plotData) ? plotData : []);

  const layoutAny = /** @type {any} */ (layout);
  const dataAny = /** @type {any[]} */ (data);

  const layoutHasY2 = !!layoutAny?.yaxis2;
  const dataUsesY2 = dataAny.some((t) => (t?.yaxis || "") === "y2");
  const hasY2 = layoutHasY2 || dataUsesY2;

  if (!hasY2) return 20;
  if (isMobileDevice()) return 20;

  if (w >= 1400) return 160;
  if (w >= 1150) return 140;
  if (w >= 950) return 120;
  if (w >= 800) return 100;

  return 20;
}
/**
 * Choose a single legend mode for the pair.
 * plot-1 decides, plot-2 follows.
 *
 * @param {string} targetId
 * @param {number} rightGutterPx
 * @returns {"right" | "below"}
 */
function chooseSharedLegendMode(targetId, rightGutterPx) {
  const el = document.getElementById(targetId);
  const plotWidth = el ? el.clientWidth : 0;

  /** @type {"right" | "below"} */
  const mode = (plotWidth >= 900 && rightGutterPx >= 70) ? "right" : "below";

  if (targetId === "plot-1") {
    plotWindow._plotLegendMode = mode;
    return mode;
  }

  const sharedMode = plotWindow._plotLegendMode;
  if (targetId === "plot-2" && sharedMode) {
    return sharedMode;
  }

  return mode;
}

/**
 * Update legend placement based on gutter choice and shared pair mode.
 *
 * @param {any} layout
 * @param {number} rightGutterPx
 * @param {string} targetId
 * @returns {{ extraBottom: number, minRight: number }}
 */
function applyResponsiveLegend(layout, rightGutterPx, targetId) {
  const base = layout.legend || {};
  const legendMode = chooseSharedLegendMode(targetId, rightGutterPx);

  if (legendMode === "right") {
    layout.legend = {
      ...base,
      x: 1.01,
      xanchor: "left",
      y: 1.0,
      yanchor: "top",
      orientation: "v",
    };
    return { extraBottom: 0, minRight: 160 };
  }

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

/* ------------------------------------------------------------------ */
/* Pair geometry sync                                                 */
/* ------------------------------------------------------------------ */

/**
 * @returns {Promise<void>}
 */
async function syncPairGeometryFromRaw() {
  const p1 = /** @type {PlotlyGraphDiv | null} */ (document.getElementById("plot-1"));
  const p2 = /** @type {PlotlyGraphDiv | null} */ (document.getElementById("plot-2"));

  if (!p1?._fullLayout || !p2?._fullLayout) return;

  const srcX = p1._fullLayout.xaxis;
  if (!srcX?.domain || !Array.isArray(srcX.domain) || srcX.domain.length !== 2) return;

  const srcMargin = p1._fullLayout.margin || {};
  const srcWidth =
    p1._fullLayout.width ||
    plotWindow._plotRenderWidth ||
    Math.round(measurePlotWidth(p1));

  const legendMode = plotWindow._plotLegendMode || "below";
  const syncedRightMargin =
    legendMode === "right"
      ? Math.max(srcMargin.r ?? 20, 160)
      : (srcMargin.r ?? 20);

  /** @type {Record<string, any>} */
  const update = {
    width: srcWidth,
    "margin.l": srcMargin.l ?? 60,
    "margin.r": syncedRightMargin,
    "margin.b": srcMargin.b ?? 50,
    "xaxis.domain": srcX.domain,
    "yaxis.automargin": false,
  };

  if (p2._fullLayout?.yaxis2) {
    update["yaxis2.automargin"] = false;
  }

  console.log("📏 syncing pair geometry from raw → ratio", update);

  /** @type {any} */
  const plotly = getPlotly();
  await plotly.relayout(p2, update);
  plotly.Plots.resize(p2);
}

/* ------------------------------------------------------------------ */
/* Fetch + render helper                                              */
/* ------------------------------------------------------------------ */

/**
 * @param {"raw" | "ratio"} plotType
 * @param {string} plotDivId
 * @returns {Promise<void>}
 */
export async function fetchAndRenderPlot(plotType, plotDivId) {
  const targetId = plotDivId || `plot-${plotType}`;
  const label = `🔧 fetchAndRenderPlot("${plotType}", "#${targetId}")`;
  console.group(label);

  const container = /** @type {PlotlyGraphDiv | null} */ (document.getElementById(targetId));
  if (!container) {
    console.error(`❌ Container "#${targetId}" not found`);
    console.groupEnd();
    return;
  }

  const overlayHost = container.parentElement || container;
  const statusEl = document.getElementById("plots-status");

  try {
    const msg = plotType === "raw" ? "Loading plots" : "Loading plot";
    showLoadingOverlay(overlayHost, msg);

    if (statusEl) {
      statusEl.textContent = "Loading plots…";
      statusEl.style.display = "";
    }

    /** @type {{ kind?: string } & Record<string, any>} */
    const filters = getSelectedFilters("main") || {};
    filters.kind = plotType;
    console.log("plot filters being sent:", filters);

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

    /** @type {{ data?: any[], layout?: any }} */
    const plotData = JSON.parse(text);

    if (plotType === "raw" && targetId === "plot-1") {
      plotWindow._initialXRange = plotData?.layout?.xaxis?.range || null;
    }
    if (plotType === "ratio" && targetId === "plot-2" && plotWindow._initialXRange) {
      plotData.layout = plotData.layout || {};
      plotData.layout.xaxis = plotData.layout.xaxis || {};
      plotData.layout.xaxis.range = plotWindow._initialXRange;
    }

    if (!Array.isArray(plotData.data)) {
      console.error("❌ `data` is not an array:", plotData.data);
      return;
    }

    container.innerHTML = "";
    await new Promise(
      /**
       * @param {FrameRequestCallback} r
       */
      (r) => requestAnimationFrame(r)
    );

    /** @type {any} */
    const plotly = getPlotly();
    plotly.purge(container);

    const renderWidth = getSharedPlotWidth(targetId, container);

    const leftMargin = 60;
    plotWindow._plotLeftMargin = leftMargin;

    let rightGutter = computeRightGutterPx(container, plotType, plotData.layout, plotData.data);

    if (targetId === "plot-1") plotWindow._plotRightGutter = rightGutter;
    if (targetId === "plot-2" && typeof plotWindow._plotRightGutter === "number") {
      rightGutter = plotWindow._plotRightGutter;
    }

    const legendAdjust = { legend: plotData.layout?.legend || {} };
    const legendResponse = applyResponsiveLegend(legendAdjust, rightGutter, targetId);

    const effectiveRight = Math.max(
      rightGutter,
      legendResponse.minRight ?? 160
    );

    const layout = {
      ...plotData.layout,
      autosize: false,
      width: renderWidth,
      height: 500,
      margin: {
        l: leftMargin,
        r: effectiveRight,
        t: plotData.layout?.margin?.t ?? 50,
        b: Math.max(plotData.layout?.margin?.b ?? 50, 50 + legendResponse.extraBottom),
      },
      legend: legendAdjust.legend,
    };

    layout.yaxis = layout.yaxis || {};
    layout.yaxis.automargin = false;
    if (layout.yaxis2) {
      layout.yaxis2.automargin = false;
    }

    const plotConfig = {
      displayModeBar: false,
      displaylogo: false,
      responsive: false,
    };

    const gd = /** @type {PlotlyGraphDiv} */ (
      await plotly.newPlot(container, plotData.data, layout, plotConfig)
    );

    if (targetId === "plot-1") rawPlotDiv = gd;
    if (targetId === "plot-2") ratioPlotDiv = gd;

    /**
     * @returns {Promise<number | null>}
     */
    const refineGutter = async () => {
      if (!gd) return null;

      let g = computeRightGutterPx(gd, plotType, null, null);

      if (targetId === "plot-1") plotWindow._plotRightGutter = g;
      if (targetId === "plot-2" && typeof plotWindow._plotRightGutter === "number") {
        g = plotWindow._plotRightGutter;
      }

      const tmp = { legend: gd.layout?.legend || {} };
      const legendResult = applyResponsiveLegend(tmp, g, targetId);

      const effectiveRightAfterRender = Math.max(
        g,
        legendResult.minRight ?? 160
      );

      const sharedWidth =
        targetId === "plot-2" && typeof plotWindow._plotRenderWidth === "number"
          ? plotWindow._plotRenderWidth
          : renderWidth;

      /** @type {Record<string, any>} */
      const update = {
        width: sharedWidth,
        "margin.l": plotWindow._plotLeftMargin ?? 60,
        "margin.r": effectiveRightAfterRender,
        "margin.b": Math.max(gd.layout?.margin?.b ?? 50, 50 + legendResult.extraBottom),
        "yaxis.automargin": false,
        legend: tmp.legend,
      };
      if (gd.layout?.yaxis2) {
        update["yaxis2.automargin"] = false;
      }

      await plotly.relayout(gd, update);
      plotly.Plots.resize(gd);

      return g;
    };

    await refineGutter();

    if (targetId === "plot-2") {
      await new Promise(
        /**
         * @param {FrameRequestCallback} r
         */
        (r) => requestAnimationFrame(r)
      );
      await syncPairGeometryFromRaw();
    }

    if (!plotWindow._biocharResizePlotsInstalled) {
      plotWindow._biocharResizePlotsInstalled = true;

      /** @type {number | null} */
      let t = null;

      /**
       * @param {PlotlyGraphDiv | null} el
       * @param {"raw" | "ratio"} kind
       * @param {string} targetIdForLegend
       * @param {number | null} [forceGutter=null]
       * @param {number | null} [forceWidth=null]
       * @returns {Promise<number | null>}
       */
      const relayoutOne = async (el, kind, targetIdForLegend, forceGutter = null, forceWidth = null) => {
        if (!el || !el.layout) return null;

        const w =
          typeof forceWidth === "number"
            ? forceWidth
            : Math.round(measurePlotWidth(el));

        let gutter =
          typeof forceGutter === "number"
            ? forceGutter
            : computeRightGutterPx(el, kind, el.layout, el.data);

        if (el.id === "plot-1") {
          plotWindow._plotRightGutter = gutter;
          plotWindow._plotRenderWidth = w;
        }

        if (el.id === "plot-2" && typeof plotWindow._plotRightGutter === "number") {
          gutter = plotWindow._plotRightGutter;
        }

        const tmp = { legend: el.layout?.legend || {} };
        const legendResult = applyResponsiveLegend(tmp, gutter, targetIdForLegend);

        const effectiveRightOnResize = Math.max(
          gutter,
          legendResult.minRight ?? 160
        );

        /** @type {Record<string, any>} */
        const update = {
          width: w,
          "margin.l": plotWindow._plotLeftMargin ?? 60,
          "margin.r": effectiveRightOnResize,
          "margin.b": Math.max(el.layout?.margin?.b ?? 50, 50 + legendResult.extraBottom),
          "yaxis.automargin": false,
          legend: tmp.legend,
        };
        if (el.layout?.yaxis2) {
          update["yaxis2.automargin"] = false;
        }

        await plotly.relayout(el, update);
        plotly.Plots.resize(el);

        return gutter;
      };

      window.addEventListener(
        "resize",
        /**
         * @returns {void}
         */
        () => {
          if (t) window.clearTimeout(t);
          t = window.setTimeout(async () => {
            const p1 = /** @type {PlotlyGraphDiv | null} */ (document.getElementById("plot-1"));
            const p2 = /** @type {PlotlyGraphDiv | null} */ (document.getElementById("plot-2"));

            const masterWidth = Math.round(measurePlotWidth(p1));
            const gutter = await relayoutOne(p1, "raw", "plot-1", null, masterWidth);
            await relayoutOne(p2, "ratio", "plot-2", gutter, masterWidth);

            await syncPairGeometryFromRaw();
          }, 90);
        }
      );
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

/**
 * @returns {Promise<void>}
 */
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

    plotWindow._plotRenderWidth = null;
    plotWindow._plotRightGutter = null;
    plotWindow._plotLeftMargin = 60;
    plotWindow._plotLegendMode = null;
    plotWindow._initialXRange = null;

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
 *
 * @returns {void}
 */
export function wireMainPlotZoomSync() {
  maybeAttachZoomSyncHandlers();
}
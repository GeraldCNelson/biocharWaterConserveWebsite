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
  // ✅ Domain sync helper (this is the missing piece)
  // Plotly can keep margins equal yet still compute slightly
  // different xaxis.domain values due to tick/title/layout nuances.
  // We force plot-2 to use plot-1's xaxis.domain once both exist.
  // ------------------------------------------------------------
  const syncXAxisDomains = () => {
    const p1 = document.getElementById("plot-1");
    const p2 = document.getElementById("plot-2");
    if (!p1?._fullLayout?.xaxis || !p2?._fullLayout?.xaxis) return;

    const d = p1._fullLayout.xaxis.domain;
    if (!Array.isArray(d) || d.length !== 2) return;

    // Only relayout if different (avoid relayout loops)
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
    // ✅ Consistent margins (key to x-axis alignment)
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

    const layout = {
      ...plotData.layout,
      autosize: false,
      width: parentWidth,
      height: 500,
      margin: {
        l: leftMargin,
        r: rightGutter,
        t: plotData.layout?.margin?.t ?? 50,
        b: plotData.layout?.margin?.b ?? 50,
      },
    };

    // Prevent Plotly from silently changing margins per plot
    layout.yaxis = layout.yaxis || {};
    layout.yaxis.automargin = false;
    if (layout.yaxis2) {
      layout.yaxis2.automargin = false;
    }

    // Legend placement depends on gutter
    applyResponsiveLegend(layout, rightGutter);

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
    // ✅ Refine gutter *after render* using _fullLayout/_fullData
    // ------------------------------------
    const refineGutter = () => {
      if (!gd) return null;

      let g = computeRightGutterPx(gd, plotType, null, null);

      // Save raw gutter; force ratio to match
      if (targetId === "plot-1") window._plotRightGutter = g;
      if (targetId === "plot-2" && typeof window._plotRightGutter === "number") {
        g = window._plotRightGutter;
      }

      const update = {
        "margin.l": window._plotLeftMargin ?? 60,
        "margin.r": g,
        "yaxis.automargin": false,
      };
      if (gd.layout?.yaxis2) update["yaxis2.automargin"] = false;

      // Legend update (replace legend object)
      const tmp = { legend: gd.layout?.legend || {} };
      applyResponsiveLegend(tmp, g);
      update.legend = tmp.legend;

      Plotly.relayout(gd, update);
      Plotly.Plots.resize(gd);

      return g;
    };

    // One refinement pass right after first draw
    refineGutter();

    // After rendering plot-2, force domain alignment to plot-1
    if (targetId === "plot-2") {
      // Wait a frame so _fullLayout is fully settled
      await new Promise((r) => requestAnimationFrame(r));
      syncXAxisDomains();
    }

    // ------------------------------------
    // ✅ Install ONE resize handler (relayout + resize + domain sync)
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

        const update = {
          width: w,
          "margin.l": window._plotLeftMargin ?? 60,
          "margin.r": gutter,
          "yaxis.automargin": false,
        };
        if (el.layout?.yaxis2) update["yaxis2.automargin"] = false;

        // Legend update
        const tmp = { legend: el.layout?.legend || {} };
        applyResponsiveLegend(tmp, gutter);
        update.legend = tmp.legend;

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
// plot_utils.js
// @ts-nocheck
import { getDropdownValue, getInputValue } from "./ui_utils.js";
import { getSelectedFilters } from "./ui_controls.js";
import { DEBUG } from "./config.js";

function debugLog(...args) {
  if (DEBUG) console.log(...args);
}
function debugGroup(title, callback) {
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

/* global Plotly */

function isMobileDevice() {
  return /Mobi|Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
}

/**
 * Fetch and render the main plots (raw & ratio) into their containers.
 */
export const renderMainPlots = async () => {
  console.log("▶️ Rendering Main Data Display plots…");
  await fetchAndRenderPlot("raw", "plot-1");
  await fetchAndRenderPlot("ratio", "plot-2");
  // ensure proper sizing
  Plotly.Plots.resize(document.getElementById("plot-1"));
  Plotly.Plots.resize(document.getElementById("plot-2"));
};

/**
 * Fetch & render one Plotly chart (raw or ratio) into a specified div.
 * @param {"raw"|"ratio"} plotType
 * @param {string} [plotDivId]  target div id (defaults to `plot-${plotType}`)
 */
export async function fetchAndRenderPlot(plotType, plotDivId) {
  console.group(`🔧 fetchAndRenderPlot("${plotType}", "#${plotDivId || `plot-${plotType}`}")`);
  try {
    // derive container ID
    const targetId = plotDivId || `plot-${plotType}`;

    // gather filters
    const filters = getSelectedFilters("main");
    filters.kind = plotType;
    console.log("🔍 Filters →", filters);

    // pick endpoint
    const isGseason = filters.granularity === "gseason";
    const url = isGseason
      ? `/api/plot_${plotType}_gseason`
      : `/api/plot_${plotType}`;
    console.log("🌐 Fetching →", url);

    // fetch
    const resp = await fetch(url, {
      method:      "POST",
      headers:     { "Content-Type": "application/json" },
      credentials: "same-origin",
      body:        JSON.stringify(filters),
    });

    const text = await resp.text();
    console.log("⏳ Response →", resp.status, resp.statusText);
    const snippet = text.length > 200
      ? text.slice(0, 200).replace(/\s+/g, ' ') + '…'
      : text.replace(/\s+/g, ' ');
    console.log("📄 Body snippet →", snippet);

    if (!resp.ok) {
      console.error(`❌ Server error ${resp.status}:`, text);
      console.groupEnd();
      return;
    }

    // parse JSON
    let plotData;
    try {
      plotData = JSON.parse(text);

      // **NEW**: inspect entire payload
      console.log("❓ payload for", plotType, plotData);
     // debugger;

      console.log("🧱 shapes →", plotData.layout?.shapes);
      // debugger;
    }
    catch (err) {
      console.error("❌ JSON parse error:", err);
      console.groupEnd();
      return;
    }

    // debug trace info
    console.log("🔢 total traces →", plotData.data.length);
    console.log("🔖 trace names →", plotData.data.map(t => t.name));
    console.log("🔧 trace types →", plotData.data.map(t => t.type));

    // 2) (optional) drop the irrigation‐volume trace entirely,
    //    since we only want it as vertical shapes, not as a data series
    plotData.data = plotData.data.filter(
      t => t.name !== "Irrigation Volume (000 gal)"
    );

    // **NEW**: inspect data & layout right before rendering
    console.log("📊 final data →", plotData.data);
    console.log("📊 final layout →", plotData.layout);
    // debugger;

    // validate
    if (!Array.isArray(plotData.data)) {
      console.error("❌ `data` is not an array:", plotData.data);
      console.groupEnd();
      return;
    }

    // find container
    const container = document.getElementById(targetId);
    if (!container) {
      console.error(`❌ Container "#${targetId}" not found`);
      console.groupEnd();
      return;
    }
    console.log(`📦 Rendering into → #${targetId}`, container);

    // wait a frame for layout
    await new Promise(r => requestAnimationFrame(r));

    // purge old plot
    Plotly.purge(container);

    // measure & set size
    const parentWidth = container.clientWidth;
    const fixedHeight = 500;
    const layout = {
      ...plotData.layout,
      autosize: false,
      width:    parentWidth,
      height:   fixedHeight,
      margin: {
        l: plotData.layout?.margin?.l ?? 60,
        r: 20,
        t: plotData.layout?.margin?.t ?? 50,
        b: plotData.layout?.margin?.b ?? 50,
      }
    };

    // config for Plotly (must be declared before use)
    const plotConfig = {
      displayModeBar: !isMobileDevice(),
      responsive:     false,
    };

    // draw & resize
    await Plotly.newPlot(container, plotData.data, layout, plotConfig);
    Plotly.Plots.resize(container);
  }
  catch (err) {
    console.error(`❌ fetchAndRenderPlot(${plotType}) uncaught:`, err);
  }
  finally {
    console.groupEnd();
  }
}

// resize when Bootstrap tabs show
document.querySelectorAll('a[data-toggle="tab"]').forEach(tab => {
  tab.addEventListener("shown.bs.tab", () => {
    ["plot-1","plot-2"].forEach(id => {
      const gd = document.getElementById(id);
      if (gd && gd.data) Plotly.Plots.resize(gd);
    });
  });
});

/**
 * Wait for dropdown elements to populate.
 */
export async function waitForAllDropdowns(dropdownIds, timeout = 7000, postDelay = 200) {
  console.log("⏳ Waiting for dropdowns to be available...");
  return new Promise((resolve, reject) => {
    let elapsed = 0;
    const interval = 150;
    const check = setInterval(() => {
      const missing = dropdownIds.filter(id => {
        const el = document.getElementById(id);
        return !el || el.options.length === 0;
      });
      if (missing.length === 0) {
        clearInterval(check);
        setTimeout(resolve, postDelay);
      } else if (elapsed >= timeout) {
        clearInterval(check);
        reject(new Error(`Timeout waiting for dropdowns: ${missing.join(", ")}`));
      }
      elapsed += interval;
    }, interval);
  });
}

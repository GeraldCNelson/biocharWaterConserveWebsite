// plot_utils.js

import { getSelectedFilters } from "./ui_controls.js";
import { isMobileDevice } from "./ui_utils.js";

/**
 * Pause until each of the given dropdown IDs exists in the DOM
 * and has been populated with at least one <option>.
 * @param {string[]} ids – array of element IDs, e.g. ["main-year", "main-variable", …]
 */
export async function waitForAllDropdowns(ids) {
  const delay = ms => new Promise(res => setTimeout(res, ms));
  while (true) {
    const missing = ids.filter(id => {
      const el = document.getElementById(id);
      return !(el && el.options && el.options.length > 0);
    });
    if (missing.length === 0) break;
    await delay(50);
  }
}

/**
 * Fetch plot data from the server and render it into a Plotly div.
 * @param {string} plotType – e.g. "raw" or "ratio"
 * @param {string} [plotDivId] – the DOM id of the target <div>; defaults to `plot-${plotType}`
 */
export async function fetchAndRenderPlot(plotType, plotDivId) {
  console.group(`🔧 fetchAndRenderPlot("${plotType}", "${plotDivId || `plot-${plotType}`}")`);
  try {
    // 1) Determine container ID
    const targetId = plotDivId || `plot-${plotType}`;

    // 2) Gather filters from the DOM
    const filters = getSelectedFilters("main");

    // 3) Debug: current unitSystem
    console.log("🌐 window.unitSystem =", window.unitSystem);
    console.log("🌐 filters =", filters);

    // 4) Inject unitSystem (FastAPI requires it)
    filters.unitSystem = window.unitSystem || "us";

    // 5) Add kind
    filters.kind = plotType;

    // 6) Debug assembled filters
    console.log("🔍 Filters (JS object) →", filters);
    console.log("📤 Payload JSON →", JSON.stringify(filters));

    // 7) Choose endpoint
    const isGseason = filters.granularity === "gseason";
    const url = `/api/plot_${plotType}`;
    console.log("🌐 Fetching →", url);

    // 8) Send request
    const resp = await fetch(url, {
      method:      "POST",
      headers:     { "Content-Type": "application/json" },
      credentials: "same-origin",
      body:        JSON.stringify(filters),
    });

    // 9) Read and log response
    const text = await resp.text();
    console.log("⏳ Response →", resp.status, resp.statusText);
    const snippet = text.length > 200
      ? text.slice(0, 200).replace(/\s+/g, " ") + "…"
      : text.replace(/\s+/g, " ");
    console.log("📄 Body snippet →", snippet);

    if (!resp.ok) {
      console.error(`❌ Server error ${resp.status}:`, text);
      return console.groupEnd();
    }

    // 10) Parse JSON payload
    const plotData = JSON.parse(text);
    console.log("❓ payload for", plotType, plotData);
    console.log("🧱 shapes →", plotData.layout?.shapes);

    // 11) Debug trace info
    console.log("🔢 total traces →", plotData.data.length);
    console.log("🔖 trace names →", plotData.data.map(t => t.name));
    console.log("🔧 trace types →", plotData.data.map(t => t.type));


    console.log("📊 final data →", plotData.data);
    console.log("📊 final layout →", plotData.layout);

    // 13) Validate data & find container
    if (!Array.isArray(plotData.data)) {
      console.error("❌ `data` is not an array:", plotData.data);
      return console.groupEnd();
    }
    const container = document.getElementById(targetId);
    if (!container) {
      console.error(`❌ Container "#${targetId}" not found`);
      return console.groupEnd();
    }
    console.log(`📦 Rendering into → #${targetId}`, container);

    // 14) Render with Plotly
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
      }
    };
        const plotConfig = {
          displayModeBar: false,   // hide the toolbar entirely
          displaylogo:   false,    // remove Plotly logo
          responsive:     false,
        };
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

/**
 * Kick off both of your main‐tab plots in sequence.
 */
export async function renderMainPlots() {
  console.group("▶️ Rendering Main Data Display plots…");
  try {
    await fetchAndRenderPlot("raw",   "plot-1");
    await fetchAndRenderPlot("ratio", "plot-2");
  }
  catch (err) {
    console.error("❌ renderMainPlots uncaught:", err);
  }
  finally {
    console.groupEnd();
  }
}

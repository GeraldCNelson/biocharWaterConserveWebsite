// plots.js (ES6 Module Version)
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

const API_BASE = "/api";

/**
 * 📊 updatePlot - Fetches and updates a Plotly chart dynamically.
 * @param {string} plotType - "raw" or "ratio"
 * @param {string} plotDiv  - the DOM id of the div to render into
 */
export async function updatePlot(plotType, plotDiv) {
  debugLog(`📡 Fetching ${plotType} plot data...`);

  try {
    // 1) Grab all your dropdowns + inputs
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
      year: requestData.year,            // if you have a year field
      granularity,                       // "raw", "monthly", or "gseason"
      startDate,                         // ISO yyyy-mm-dd
      endDate,                           // ISO yyyy-mm-dd
      variable,                         // e.g. "VWC" or "T"
      depth: requestData.depth,          // sensor depth code
      strip: requestData.strip,          // e.g. "S1"
      logger: requestData.logger,        // e.g. "M" or "B"
    });

    // 4) Conditionally include weather overlays
    if (granularity !== "gseason") {
      if (variable === "T")   params.set("includeTemperature", "true");
      if (variable === "VWC") params.set("includeRainfall",  "true");
    }

    // 5) Construct the GET URL
    const url = `${API_BASE}/plot/${plotType}?${params.toString()}`;
    debugLog("GET", url);

    // 6) Fetch & render
    const response = await fetch(url, { method: "GET" });

    if (!response.ok) {
      const errText = await response.text();
      throw new Error(`❌ Server error: ${response.status} – ${errText}`);
    }

    const plotlyJSON = await response.json();
    debugLog(`✅ Received ${plotType} JSON:`, plotlyJSON);

    // 7) Plotly.react for fast updates
    Plotly.react(plotDiv, plotlyJSON.data, plotlyJSON.layout).then(() => {
      Plotly.Plots.resize(document.getElementById(plotDiv));
      debugLog(`🔄 Resized ${plotDiv}`);
    });
  } catch (err) {
    console.error(`❌ Error updating ${plotType} plot:`, err);
  }
}

/**
 * Helper to capitalize strings if you need it elsewhere.
 */
export function capitalize(s) {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
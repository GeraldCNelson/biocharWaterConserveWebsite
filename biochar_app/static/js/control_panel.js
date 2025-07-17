// control_panel.js – Shared logic for control panel setup (Main & Summary)
// Added initialization log for debugging
console.log(`🚀 control_panel.js loaded at ${new Date().toISOString()}`);

let dateDebounceTimer;

import { populateDropdown } from "./ui_controls.js";
import { updateSummaryStatistics } from "./tables.js";
import { fetchAndRenderPlot, renderMainPlots } from "./plot_utils.js";

export function initializeUpdateButtons() {
  console.log("🔘 Initializing update buttons");
  document.getElementById("update-plots")?.addEventListener("click", () => {
    console.log("🔄 Update plots button clicked");
    fetchAndRenderPlot("raw",   "plot-1");
    fetchAndRenderPlot("ratio", "plot-2");
  });

  ["start-date", "end-date"].forEach(id => {
    const el = document.getElementById(`main-${id}`);
    if (!el) return;
    el.addEventListener("input", () => {
      clearTimeout(dateDebounceTimer);
      dateDebounceTimer = setTimeout(() => {
        console.log(`⏱ Debounced date input (${id}), triggering update`);
        document.getElementById("update-plots")?.click();
      }, 500);
    });
  });
}

export const dropdownConfigs = {
  main: [
    { id: "year",           source: "years" },
    { id: "variable",       source: "variables" },
    { id: "strip",          source: "strips" },
    { id: "granularity",    source: "granularities" },
    { id: "loggerLocation", source: "loggerLocations" },
    { id: "depth",          source: "depths" },
    { id: "traceOption",    source: "traceOptions" },
  ],
  summary: [
    { id: "year",        source: "years" },
    { id: "variable",    source: "variables" },
    { id: "strip",       source: "strips" },
    { id: "granularity", source: "granularities" },
    { id: "depth",       source: "depths" },
  ]
};

// ─── populateAllDropdowns: handles both primitive arrays and {value,label} arrays ───
export function populateAllDropdowns(options, unitSystem) {
  console.log("🔑 Populating dropdowns; sources =", Object.keys(options));

  ["main", "summary"].forEach((tab) => {
    dropdownConfigs[tab].forEach(({ id, source }) => {
      const selectId = `${tab}-${id}`;
      const list     = options[source];

      if (!Array.isArray(list)) {
        console.warn(`⚠️ Skipping '${source}', not an array:`, list);
        return;
      }

      let values, labels;

      // object style: [{value,label}, …]
      if (list[0] != null && typeof list[0] === "object" && "value" in list[0]) {
        values = list.map(item => item.value);
        labels = list.map(item => item.label);
      } else {
        // primitive style: [2023,2024,2025] or ["a","b"]
        values = list;
        labels = list.map(item => String(item));
      }

      console.log(`[${tab}] ${id}:`, values, labels);

      populateDropdown(
        selectId,
        values,
        options.defaults[id],
        labels
      );
    });
  });
}

export function setupUnitToggleHandlers(options) {
  console.log("🔄 Setting up unit toggle handlers");
  const mainToggle    = document.getElementById("units-toggle_main");
  const summaryToggle = document.getElementById("units-toggle_summary");

  function mirrorToggles(isMetric) {
    if (mainToggle)    mainToggle.checked    = isMetric;
    if (summaryToggle) summaryToggle.checked = isMetric;
  }

  async function onToggleChange() {
    const isMetric = mainToggle?.checked ?? false;
    window.unitSystem = isMetric ? "metric" : "us";
    console.log(`🌡 Unit system changed to ${window.unitSystem}`);
    // updateDepthLabels if needed
    await renderMainPlots();
    updateSummaryStatistics();
    mirrorToggles(isMetric);
  }

  if (mainToggle)    mainToggle.addEventListener("change", onToggleChange);
  if (summaryToggle) summaryToggle.addEventListener("change", onToggleChange);

  mirrorToggles(options.defaults.unitSystem === "metric");
}

export function triggerUpdates() {
  console.log("🔁 triggerUpdates called");
  document.getElementById("update-plots")?.click();
  document.getElementById("update-summary")?.click();
}

export function getAllDropdownIds() {
  return [
    "main-year",
    "main-variable",
    "main-strip",
    "main-loggerLocation",
    "main-depth",
    "main-granularity",
    "main-traceOption",
    "summary-year",
    "summary-variable",
    "summary-strip",
    "summary-granularity",
    "summary-depth",
  ];
}

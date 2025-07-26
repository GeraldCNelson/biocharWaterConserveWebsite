// control_panel.js – Shared logic for control panel setup (Main & Summary)
// Added initialization log for debugging
console.log(`🚀 control_panel.js loaded at ${new Date().toISOString()}`);

let dateDebounceTimer;

import { populateDropdown } from "./ui_controls.js";
import { updateSummaryStatistics } from "./tables.js";
import { renderMainPlots } from "./plot_utils.js";

export function initializeUpdateButtons() {
  console.log("🔘 Initializing update buttons");
  document
    .getElementById("update-plots")
    ?.addEventListener("click", async () => {
      console.log("🔄 Update plots button clicked");
      await renderMainPlots();
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

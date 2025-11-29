// control_panel.js – Shared logic for control panel setup (Main & Summary)
console.log(`🚀 control_panel.js loaded at ${new Date().toISOString()}`);

import { updateDepthLabels } from "./ui_controls.js";
import { updateSummaryStatistics } from "./tables.js";
import { renderMainPlots } from "./plot_utils.js";

let dateDebounceTimer;

/**
 * Wire the "Update Plots" and "Update Summary" buttons.
 */
export function initializeUpdateButtons() {
  console.log("🔘 Initializing update buttons");

  document
    .getElementById("update-plots")
    ?.addEventListener("click", async () => {
      console.log("🔄 Update plots button clicked");
      await renderMainPlots();
    });

  document
    .getElementById("update-summary")
    ?.addEventListener("click", () => {
      console.log("📊 Update summary button clicked");
      updateSummaryStatistics();
    });
}

/**
 * Configure the US/Metric toggle on both Main + Summary.
 * Keeps them in sync and updates:
 *  - window.unitSystem
 *  - depth dropdown labels
 *  - plots
 *  - summary tables
 *
 * @param {"us"|"metric"} initialUnitSystem
 */
export function setupUnitToggleHandlers(initialUnitSystem) {
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

    // Update depth dropdown labels on BOTH tabs
    updateDepthLabels(window.unitSystem);

    // Re-render plots and summary in the new unit system
    await renderMainPlots();
    updateSummaryStatistics();

    // Keep the two toggles visually in sync
    mirrorToggles(isMetric);
  }

  if (mainToggle) {
    mainToggle.addEventListener("change", onToggleChange);
  }
  if (summaryToggle) {
    summaryToggle.addEventListener("change", onToggleChange);
  }

  // Initialize toggles & labels based on initial unit system
  const isMetricDefault = initialUnitSystem === "metric";
  mirrorToggles(isMetricDefault);
  updateDepthLabels(initialUnitSystem);
}

/**
 * Convenience helper used elsewhere to trigger both updates.
 */
export function triggerUpdates() {
  console.log("🔁 triggerUpdates called");
  document.getElementById("update-plots")?.click();
  document.getElementById("update-summary")?.click();
}

/**
 * The IDs we wait for before doing certain operations (like in waitForAllDropdowns).
 */
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
// control_panel.js – Shared logic for control panel setup (Main & Summary)
console.log(`🚀 control_panel.js loaded at ${new Date().toISOString()}`);

import { updateDepthLabels } from "./ui_controls.js";
import { updateSummaryStatistics } from "./tab_summary.js";
import { renderMainPlots } from "./plots.js";

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

  /**
   * Keep both toggles visually in sync.
   */
  function mirrorToggles(isMetric) {
    if (mainToggle) {
      mainToggle.checked = isMetric;
    }
    if (summaryToggle) {
      summaryToggle.checked = isMetric;
    }
  }

  /**
   * Handle either toggle being changed.
   * We *only* trust event.target.checked and then mirror the other toggle.
   */
  async function onToggleChange(event) {
    const src = event?.target;
    if (!src) return;

    const isMetric   = !!src.checked;
    const sourceId   = src.id || "(unknown-toggle)";
    const newSystem  = isMetric ? "metric" : "us";

    // 1) Update global unit system
    window.unitSystem = newSystem;
    console.log(
      `🌡 Unit system changed to ${window.unitSystem} ` +
      `(source: ${sourceId}, checked=${isMetric})`
    );

    // 2) Mirror to both toggles
    mirrorToggles(isMetric);

    // 3) Update depth dropdown labels on BOTH tabs
    //    ui_controls.js uses the mapping set earlier from the backend
    updateDepthLabels(window.unitSystem, window.depthMapping || {});

    // 4) Re-render plots and summary in the new unit system
    await renderMainPlots();
    updateSummaryStatistics();
  }

  // Attach listeners (both toggles share the same handler)
  if (mainToggle) {
    mainToggle.addEventListener("change", onToggleChange);
  }
  if (summaryToggle) {
    summaryToggle.addEventListener("change", onToggleChange);
  }

  // Initialize global unit system & UI based on initial setting
  window.unitSystem = initialUnitSystem === "metric" ? "metric" : "us";
  const isMetricDefault = window.unitSystem === "metric";
  console.log("🌡 Initial unitSystem =", window.unitSystem);

  mirrorToggles(isMetricDefault);
  updateDepthLabels(window.unitSystem, window.depthMapping || {});
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
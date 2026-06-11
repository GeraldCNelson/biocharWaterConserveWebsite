// @ts-check
// control_panel.js – Shared logic for control panel setup (Main & Summary)
console.log(`🚀 control_panel.js loaded at ${new Date().toISOString()}`);

import { updateDepthLabels, getSelectedFilters } from "./ui_controls.js";
import { updateSummaryStatistics } from "./tab_summary.js";
import { renderMainPlots } from "./plots.js";

/**
 * @typedef {Window & {
 *   Plotly?: {
 *     purge: (el: Element) => void
 *   },
 *   unitSystem?: "us" | "metric"
 * }} ControlPanelWindow
 */

/** @type {ControlPanelWindow} */
const controlWindow = /** @type {ControlPanelWindow} */ (window);

/**
 * Keep both top-plot controls available.
 *
 * Current UI behavior:
 * - "Top Plot grouped by" affects only how the TOP plot is grouped
 * - Depth and Logger Location are both still meaningful selections overall
 *   because the ratio plot uses both selections
 *
 * So we no longer disable either control here.
 *
 * @param {string} traceOption
 * @returns {void}
 */
function updateMainTraceControlState(traceOption) {
  const depthEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("main-depth")
  );
  const loggerLocEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("main-loggerLocation")
  );

  if (!depthEl || !loggerLocEl) return;

  const mode = String(traceOption || "").trim();
  console.log("🔀 updateMainTraceControlState:", mode);

  depthEl.disabled = false;
  depthEl.classList.remove("disabled");

  loggerLocEl.disabled = false;
  loggerLocEl.classList.remove("disabled");
}

/**
 * Clear stale plot output when an invalid date/filter cancels an update.
 *
 * @returns {void}
 */
function clearMainPlots() {
  const p1 = document.getElementById("plot-1");
  const p2 = document.getElementById("plot-2");

  try {
    if (controlWindow.Plotly) {
      if (p1) controlWindow.Plotly.purge(p1);
      if (p2) controlWindow.Plotly.purge(p2);
    }
  } catch (err) {
    console.warn("⚠️ Plotly purge failed while clearing plots:", err);
  }

  if (p1) p1.innerHTML = "";
  if (p2) p2.innerHTML = "";

  const statusEl = document.getElementById("plots-status");
  if (statusEl) {
    statusEl.textContent = "Please correct the date range and try again.";
    statusEl.style.display = "";
  }
}

/**
 * Wire the main trace-option dropdown.
 *
 * We still react to changes so helper text / future logic can stay synced,
 * but we no longer disable Depth or Logger Location.
 *
 * @returns {void}
 */
export function initializeTraceOptionControls() {
  const traceEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("main-traceOption")
  );
  if (!traceEl) {
    console.warn("⚠️ #main-traceOption not found; trace-option control state not wired.");
    return;
  }

  traceEl.addEventListener("change", (e) => {
    const target = /** @type {HTMLSelectElement | null} */ (e.target);
    const value = target?.value || "";
    console.log("🔀 main trace option changed:", value);
    updateMainTraceControlState(value);
  });

  updateMainTraceControlState(traceEl.value);
}

/**
 * Wire the "Update Plots" and "Update Summary" buttons.
 *
 * @returns {void}
 */
export function initializeUpdateButtons() {
  console.log("🔘 Initializing update buttons");

  document
    .getElementById("update-plots")
    ?.addEventListener("click", async () => {
      console.log("🔄 Update plots button clicked");

      const filters = getSelectedFilters("main");
      if (!filters) {
        console.warn("⚠️ Update plots cancelled due to invalid filters.");
        clearMainPlots();
        return;
      }

      const statusEl = document.getElementById("plots-status");
      if (statusEl) {
        statusEl.textContent = "";
        statusEl.style.display = "none";
      }

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
 * @returns {void}
 */
export function setupUnitToggleHandlers(initialUnitSystem) {
  console.log("🔄 Setting up unit toggle handlers");

  const mainToggle = /** @type {HTMLInputElement | null} */ (
    document.getElementById("units-toggle_main")
  );
  const summaryToggle = /** @type {HTMLInputElement | null} */ (
    document.getElementById("units-toggle_summary")
  );

  /**
   * @param {boolean} isMetric
   * @returns {void}
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
   * @param {Event} event
   * @returns {Promise<void>}
   */
  async function onToggleChange(event) {
    const src = /** @type {HTMLInputElement | null} */ (event.target);
    if (!src) return;

    const isMetric = !!src.checked;
    const sourceId = src.id || "(unknown-toggle)";
    /** @type {"us" | "metric"} */
    const newSystem = isMetric ? "metric" : "us";

    if (controlWindow.unitSystem === newSystem) {
      mirrorToggles(isMetric);
      updateDepthLabels(controlWindow.unitSystem);
      return;
    }

    controlWindow.unitSystem = newSystem;
    console.log(
      `🌡 Unit system changed to ${controlWindow.unitSystem} ` +
      `(source: ${sourceId}, checked=${isMetric})`
    );

    mirrorToggles(isMetric);
    updateDepthLabels(controlWindow.unitSystem);

    await renderMainPlots();
    updateSummaryStatistics();
  }

  if (mainToggle && !mainToggle.dataset.unitToggleBound) {
    mainToggle.addEventListener("change", onToggleChange);
    mainToggle.dataset.unitToggleBound = "true";
  }

  if (summaryToggle && !summaryToggle.dataset.unitToggleBound) {
    summaryToggle.addEventListener("change", onToggleChange);
    summaryToggle.dataset.unitToggleBound = "true";
  }

  if (!controlWindow.unitSystem) {
    controlWindow.unitSystem = initialUnitSystem === "metric" ? "metric" : "us";
    console.log("🌡 Initial unitSystem =", controlWindow.unitSystem);
  } else {
    console.log("🌡 Preserving existing unitSystem =", controlWindow.unitSystem);
  }

  const isMetricCurrent = controlWindow.unitSystem === "metric";
  mirrorToggles(isMetricCurrent);
  updateDepthLabels(controlWindow.unitSystem);
}

/**
 * Convenience helper used elsewhere to trigger both updates.
 *
 * @returns {void}
 */
export function triggerUpdates() {
  console.log("🔁 triggerUpdates called");
  document.getElementById("update-plots")?.click();
  document.getElementById("update-summary")?.click();
}

/**
 * The IDs we wait for before doing certain operations (like in waitForAllDropdowns).
 * @returns {string[]}
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
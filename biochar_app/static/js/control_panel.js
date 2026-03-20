// control_panel.js – Shared logic for control panel setup (Main & Summary)
console.log(`🚀 control_panel.js loaded at ${new Date().toISOString()}`);

import { updateDepthLabels, getSelectedFilters } from "./ui_controls.js";
import { updateSummaryStatistics } from "./tab_summary.js";
import { renderMainPlots } from "./plots.js";

let dateDebounceTimer;

/**
 * Enable/disable the main-tab fixed selector based on trace grouping.
 *
 * If traces are grouped by depth, logger location is the fixed selector,
 * so the depth dropdown is not used and should be disabled.
 *
 * If traces are grouped by logger location, depth is the fixed selector,
 * so the logger-location dropdown is not used and should be disabled.
 */
function updateMainTraceControlState(traceOption) {
  const depthEl = document.getElementById("main-depth");
  const loggerLocEl = document.getElementById("main-loggerLocation");

  if (!depthEl || !loggerLocEl) return;

  const mode = String(traceOption || "").trim();

  if (mode === "depth") {
    depthEl.disabled = true;
    depthEl.classList.add("disabled");

    loggerLocEl.disabled = false;
    loggerLocEl.classList.remove("disabled");
    return;
  }

  if (mode === "loggerLocation") {
    depthEl.disabled = false;
    depthEl.classList.remove("disabled");

    loggerLocEl.disabled = true;
    loggerLocEl.classList.add("disabled");
    return;
  }

  // Fallback: if options are not populated yet, leave both enabled.
  depthEl.disabled = false;
  depthEl.classList.remove("disabled");

  loggerLocEl.disabled = false;
  loggerLocEl.classList.remove("disabled");
}

/**
 * Clear stale plot output when an invalid date/filter cancels an update.
 */
function clearMainPlots() {
  const p1 = document.getElementById("plot-1");
  const p2 = document.getElementById("plot-2");

  try {
    if (window.Plotly) {
      if (p1) window.Plotly.purge(p1);
      if (p2) window.Plotly.purge(p2);
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
 * Wire the main trace-option dropdown so the inactive fixed selector
 * is disabled immediately and whenever the grouping changes.
 */
export function initializeTraceOptionControls() {
  const traceEl = document.getElementById("main-traceOption");
  if (!traceEl) {
    console.warn("⚠️ #main-traceOption not found; trace-option control state not wired.");
    return;
  }

  traceEl.addEventListener("change", (e) => {
    const value = e?.target?.value || "";
    console.log("🔀 main trace option changed:", value);
    updateMainTraceControlState(value);
  });

  // Initialize immediately using current value
  updateMainTraceControlState(traceEl.value);
}

/**
 * Wire the "Update Plots" and "Update Summary" buttons.
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
 */
export function setupUnitToggleHandlers(initialUnitSystem) {
  console.log("🔄 Setting up unit toggle handlers");

  const mainToggle = document.getElementById("units-toggle_main");
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

    const isMetric = !!src.checked;
    const sourceId = src.id || "(unknown-toggle)";
    const newSystem = isMetric ? "metric" : "us";

    // 1) Update global unit system
    window.unitSystem = newSystem;
    console.log(
      `🌡 Unit system changed to ${window.unitSystem} ` +
      `(source: ${sourceId}, checked=${isMetric})`
    );

    // 2) Mirror to both toggles
    mirrorToggles(isMetric);

    // 3) Update depth dropdown labels on BOTH tabs
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
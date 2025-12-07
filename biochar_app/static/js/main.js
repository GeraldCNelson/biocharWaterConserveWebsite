// static/js/main.js

import { FALLBACK_UNIT_SYSTEM, fetchMarkdownFiles } from "./config.js";

import {
  downloadTraceData,
  downloadPlot,
  downloadSummaryData,
} from "./downloads.js";

// ✅ Expose download helpers for inline onclick handlers in index.html
window.downloadTraceData   = downloadTraceData;
window.downloadPlot        = downloadPlot;
window.downloadSummaryData = downloadSummaryData;

// 1) debugging & logging
import { debugLog, debugGroup } from "./plots.js";

// 2) markdown loader
import { loadMarkdownContent } from "./markdown.js";

// 3) all your UI controls (dropdowns, handlers, etc.)
import {
  initializeUpdateButtons,
  setupUnitToggleHandlers,
  getAllDropdownIds,
} from "./control_panel.js";

import {
  fetchDefaultsAndOptions,
  populateAllDropdowns,
  initializeMainDatepickers,
  updateDepthLabels,
  updateStartAndEndDatesFromYear,
} from "./ui_controls.js";

// 5) main plotting routines
import {
  renderMainPlots,
  waitForAllDropdowns,
} from "./plot_utils.js";

// 6) summary‐table updater
import { updateSummaryStatistics } from "./tables.js";

// 7) custom‐season setup
import { initCustomGseason } from "./custom_gseason.js";


document.addEventListener("DOMContentLoaded", async () => {
  // ----------------------------------------------------
  // 1) Core app initialization
  // ----------------------------------------------------
  debugLog("🌐 Initializing application...");

  // Fetch defaults & options from the server
  const options = await fetchDefaultsAndOptions();
  if (!options) return;

  // Make options globally available if needed elsewhere
  window.dropdownOptions = options;

  // Normalize defaults into a local object
  const defaults = options.defaults || {};

  // Decide initial unit system (backend → fallback to config)
  window.unitSystem = defaults.unitSystem || FALLBACK_UNIT_SYSTEM;

  // Populate dropdowns and wire up control-panel buttons
  populateAllDropdowns(options);
  setupUnitToggleHandlers(options);
  initializeUpdateButtons();

  // Wait until all dropdowns exist & are initialized
  await waitForAllDropdowns(getAllDropdownIds());
  await new Promise(requestAnimationFrame);

  // Seed defaults into the main & summary inputs
  for (const [key, value] of Object.entries(defaults)) {
    const mainEl    = document.getElementById(`main-${key}`);
    const summaryEl = document.getElementById(`summary-${key}`);
    if (mainEl)    mainEl.value    = value;
    if (summaryEl) summaryEl.value = value;
  }

  // Initialize the date-pickers on the main tab
  initializeMainDatepickers();

  // Reset date range when the year changes
  document
    .getElementById("main-year")
    ?.addEventListener("change", (e) =>
      updateStartAndEndDatesFromYear(e.target.value)
    );

  // Make sure the depth labels match the current unit system
  updateDepthLabels(window.unitSystem);

  // Debug summary of defaults & depth mapping
  debugGroup("🎛️ Dropdown defaults & mappings", () => {
    console.table(defaults);
    if (window.depthMapping) {
      console.table(
        Object.entries(window.depthMapping).map(([depth, map]) => ({
          Depth: depth,
          US: map.us,
          Metric: map.metric,
        }))
      );
    }
  });

  // If “Main Data Display” is already active on load, render immediately
  const mainTabLink = document.querySelector('a[href="#main"]');
  if (mainTabLink?.classList.contains("active")) {
    await renderMainPlots();
  }

  // Re-render Main plots whenever that tab is shown
  mainTabLink?.addEventListener("shown.bs.tab", renderMainPlots);

  // Kick off the summary statistics table (async)
  await updateSummaryStatistics();

  // ----------------------------------------------------
  // 2) Load markdown snippets (from backend mapping)
  // ----------------------------------------------------
  debugLog("📖 Loading markdown mapping from backend…");
  let markdownFiles = {};
  try {
    markdownFiles = await fetchMarkdownFiles();
    debugLog("📄 Markdown mapping:", markdownFiles);
  } catch (err) {
    console.error("❌ Failed to fetch markdown mapping:", err);
  }

  if (markdownFiles && Object.keys(markdownFiles).length > 0) {
    debugLog("📖 Loading markdown snippets…");
    await Promise.all(
      Object.entries(markdownFiles).map(([id, path]) =>
        loadMarkdownContent(id, path)
      )
    );
  } else {
    console.warn("⚠️ No markdown mapping returned; skipping markdown load.");
  }

  debugLog("✅ Application initialized.");

  // ----------------------------------------------------
  // 3) Initialize the Custom Season editor (if present)
  // ----------------------------------------------------
  const gseasonContent = document.getElementById("gseason-content");
if (gseasonContent && window.CUSTOM_GSEASON_CONFIG) {
  initCustomGseason(window.CUSTOM_GSEASON_CONFIG);
}
});
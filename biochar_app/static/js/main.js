// static/js/main.js

import { FALLBACK_UNIT_SYSTEM } from "./config.js";

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

import{
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

// mapping of markdown‐injection points to files
const MARKDOWN_FILES = {
  "intro-content": "/markdown/intro.md",
  "experiment-content": "/markdown/experimentDesign.md",
  "tech-content": "/markdown/techDetails.md",
  "modal-main-help": "/markdown/help_main.md",
  "modal-summary-help": "/markdown/help_summary.md",
};

document.addEventListener("DOMContentLoaded", async () => {
  debugLog("🌐 Initializing application...");

  // 1) Fetch defaults & options from the server
  const options = await fetchDefaultsAndOptions();
  if (!options) return;

  // ✅ use backend default, fall back to US if missing
  window.dropdownOptions = options;

// Decide initial unit system from backend (falls back to "us")
const backendUnitDefault = options.defaults?.unitSystem || "us";
window.unitSystem = backendUnitDefault;

// 2) Populate all the dropdowns and wire up control-panel buttons
populateAllDropdowns(options);
setupUnitToggleHandlers(options);   // pass the full options object
initializeUpdateButtons();

  // 3) Wait until every dropdown is in the DOM & initialized by Bootstrap
  await waitForAllDropdowns(getAllDropdownIds());
  await new Promise(requestAnimationFrame);

  // 4) Seed defaults into the main & summary inputs
  if (options.defaults) {
    for (const [key, value] of Object.entries(options.defaults)) {
      const mainEl    = document.getElementById(`main-${key}`);
      const summaryEl = document.getElementById(`summary-${key}`);
      if (mainEl)    mainEl.value    = value;
      if (summaryEl) summaryEl.value = value;
    }
  }

  // 5) Initialize the date-pickers on the main tab
  initializeMainDatepickers();

  // 6) Reset date range when the year changes
  document
    .getElementById("main-year")
    ?.addEventListener("change", (e) =>
      updateStartAndEndDatesFromYear(e.target.value)
    );

  // 7) Make sure the depth labels match the current unit system
  updateDepthLabels(window.unitSystem);

  debugGroup("🎛️ Dropdown defaults & mappings", () => {
    console.table(options.defaults || {});
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

  // 8) If “Main Data Display” is already active on load, render immediately
  const mainTabLink = document.querySelector('a[href="#main"]');
  if (mainTabLink?.classList.contains("active")) {
    await renderMainPlots();
  }

  // 9) Re-render Main plots whenever that tab is shown
  mainTabLink?.addEventListener("shown.bs.tab", renderMainPlots);

  // 10) Kick off the summary statistics table
  updateSummaryStatistics();

  // 11) Load all markdown snippets into their containers/modals
  debugLog("📖 Loading markdown snippets…");
  await Promise.all(
    Object.entries(MARKDOWN_FILES).map(([id, path]) =>
      loadMarkdownContent(id, path)
    )
  );
  debugLog("✅ Application initialized.");

  // 12) Initialize the Custom Season editor (if present)
  const gseasonContent = document.getElementById("gseason-content");
  if (gseasonContent && window.CUSTOM_GSEASON_CONFIG) {
    initCustomGseason(window.CUSTOM_GSEASON_CONFIG);
  }
});
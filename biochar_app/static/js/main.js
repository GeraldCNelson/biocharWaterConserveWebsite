// static/js/main.js

// 1) debugging & logging
import { debugLog, debugGroup } from "./plots.js";

// 2) markdown loader
import { loadMarkdownContent } from "./markdown.js";

// 3) all your UI controls (dropdowns, handlers, etc.)
import {
  fetchDefaultsAndOptions,
  updateDepthLabels,
  handleTraceOptionChange,
  populateAllDropdowns,
  initializeMainDatepickers,
  updateStartAndEndDatesFromYear,
} from "./ui_controls.js";

// 4) datepicker & date‐range helpers
import {
  initializeUpdateButtons,
  setupUnitToggleHandlers,
  getAllDropdownIds,
} from "./control_panel.js";

// 5) main plotting routines
import {
  renderMainPlots,
  waitForAllDropdowns
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
  window.unitSystem      = options.defaults.unitSystem || "us";
  window.dropdownOptions = options;

  // 2) Populate all the dropdowns and wire up control-panel buttons
  populateAllDropdowns(options, window.unitSystem);
  setupUnitToggleHandlers(options);
  initializeUpdateButtons();

  // 3) Wait until every dropdown is in the DOM & initialized by Bootstrap
  await waitForAllDropdowns(getAllDropdownIds());
  await new Promise(requestAnimationFrame);

  // 4) Seed defaults into the main & summary inputs
  for (const [key, value] of Object.entries(options.defaults)) {
    const mainEl    = document.getElementById(`main-${key}`);
    const summaryEl = document.getElementById(`summary-${key}`);
    if (mainEl)    mainEl.value    = value;
    if (summaryEl) summaryEl.value = value;
  }

  // 5) Initialize the date-pickers on the main tab
  initializeMainDatepickers();

  // 6) Reset date range when the year changes
  document
    .getElementById("main-year")
    ?.addEventListener("change", (e) =>
      updateStartAndEndDatesFromYear(e.target.value)
    );

  // 7) Swap unit labels if the US/Metric toggle is flipped
  updateDepthLabels(window.unitSystem);

  debugGroup("🎛️ Dropdown defaults & mappings", () => {
    console.table(options.defaults);
    console.table(
      Object.entries(window.depthMapping).map(([depth, map]) => ({
        Depth: depth,
        US: map.us,
        Metric: map.metric,
      }))
    );
  });

  // 8) Wire up trace-option switch (depth vs logger location vs strip)
  document
    .getElementById("main-traceOption")
    ?.addEventListener("change", handleTraceOptionChange);
  debugLog("🛠️ Trace-Option handler attached.");

  // 9) If “Main Data Display” is already active on load, render immediately
  const mainTabLink = document.querySelector('a[href="#main"]');
  if (mainTabLink?.classList.contains("active")) {
    await renderMainPlots();
  }
  // 10) Re-render Main plots whenever that tab is shown
  mainTabLink?.addEventListener("shown.bs.tab", renderMainPlots);

  // 11) Kick off the summary statistics table
  updateSummaryStatistics();

  // 12) Load all markdown snippets into their modals
  debugLog("📖 Loading markdown snippets…");
  await Promise.all(
    Object.entries(MARKDOWN_FILES).map(([id, path]) =>
      loadMarkdownContent(id, path)
    )
  );
  debugLog("✅ Application initialized.");

// 13) Initialize the Custom Season editor (partial is already in the DOM)
const gseasonContent = document.getElementById("gseason-content");
if (gseasonContent) {
  // grab the config blob you rendered into window.CUSTOM_GSEASON_CONFIG
  const cfg = window.CUSTOM_GSEASON_CONFIG;
  // now call it with a single object:
  initCustomGseason(cfg);
}
});
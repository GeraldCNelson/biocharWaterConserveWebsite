// main.js – Handles page load, toggles, and UI event wiring

import { DEBUG, MARKDOWN_FILES } from "./config.js";
import {
  initializeMainDatepickers,
  updateStartAndEndDatesFromYear,
  fetchDefaultsAndOptions,
  populateSelect,
  updateDepthLabels,
  handleTraceOptionChange
} from "./ui_controls.js";
import { waitForAllDropdowns, fetchAndRenderPlot } from "./plot_utils.js";
import { updateSummaryStatistics } from "./tables.js";
import {
  populateAllDropdowns,
  setupUnitToggleHandlers,
  initializeUpdateButtons,
  getAllDropdownIds
} from "./control_panel.js";
import { loadMarkdownContent } from "./markdown.js";
import { renderMainPlots } from "./plot_utils.js";

function debugLog(...args) {
  if (DEBUG) console.log(...args);
}
function debugGroup(title, callback) {
  if (DEBUG) {
    console.groupCollapsed(title);
    try { callback(); }
    finally { console.groupEnd(); }
  } else {
    callback();
  }
}

// ─── wire the year‐dropdown change to call the imported helper ────────────────
document
  .getElementById("main-year")
  ?.addEventListener("change", () => updateStartAndEndDatesFromYear("main"));

document
  .getElementById("summary-year")
  ?.addEventListener("change", () => updateStartAndEndDatesFromYear("summary"));

// ─── on load, seed any existing <select>s into the date‐fields ───────────────
updateStartAndEndDatesFromYear("main");
updateStartAndEndDatesFromYear("summary");

// ─── DOMContentLoaded: the rest of your initialization ────────────────────────
document.addEventListener("DOMContentLoaded", async function () {
  debugLog("🌐 Initializing application...");

  // 1) Fetch defaults & options from the server
  const options = await fetchDefaultsAndOptions();
  if (!options) return;
  window.unitSystem      = options.defaults.unitSystem || "us";
  window.dropdownOptions = options;

  // 2) Build out all dropdowns & wire the unit-toggle & "Update Plots" button
  populateAllDropdowns(options, window.unitSystem);
  setupUnitToggleHandlers(options);
    initializeMainDatepickers(
      options.years,
      options.defaults.startDate,
      options.defaults.endDate
    );
  initializeUpdateButtons();

  // 3) Wait until every dropdown is populated (and let Bootstrap finish layout)
  await waitForAllDropdowns(getAllDropdownIds());
  await new Promise(requestAnimationFrame);

  // 4) Seed defaults into all <select> and <input> controls
  for (const [key, value] of Object.entries(options.defaults)) {
    const mainEl    = document.getElementById(`main-${key}`);
    const summaryEl = document.getElementById(`summary-${key}`);
    if (mainEl)    mainEl.value    = value;
    if (summaryEl) summaryEl.value = value;
  }

  // 5) Initialize the Main date-pickers (range allowed + default start/end)
  //    signature is: initializeMainDatepickers(years, defaultStart, defaultEnd)
  initializeMainDatepickers(
    options.years,
    options.defaults.startDate,
    options.defaults.endDate
  );

  // 6) Whenever the user changes the year → update both start & end via your fetch‐backed helper
  document
    .getElementById("main-year")
    ?.addEventListener("change", () => updateStartAndEndDatesFromYear("main"));

  document
    .getElementById("summary-year")
    ?.addEventListener("change", () => updateStartAndEndDatesFromYear("summary"));

  // 7) Apply depth labels (for both main & summary depth selects)
  updateDepthLabels(options.depthMapping, window.unitSystem);

  debugGroup("🎛️ Dropdown defaults & mappings", () => {
    console.table(options.defaults);
    console.table(
      Object.entries(options.depthMapping).map(([key, d]) => ({
        DepthNumber: key,
        US: d.us,
        Metric: d.metric,
      }))
    );
  });

  // 8) Attach trace‐option change handler
  const traceOptionEl = document.getElementById("main-traceOption");
  if (traceOptionEl) {
    traceOptionEl.addEventListener("change", handleTraceOptionChange);
    debugLog("🛠️ Trace-Option handler attached to main-traceOption.");
  }

  // 9) Initial render if Main tab is already active
  const mainTabLink = document.querySelector('a[href="#main"]');
  if (mainTabLink?.classList.contains("active")) {
    await renderMainPlots();
  }

  // 10) Re-render whenever Main tab is shown
  mainTabLink?.addEventListener("shown.bs.tab", renderMainPlots);

  // 11) Initial summary-statistics render
  updateSummaryStatistics();

  // 12) Load all markdown snippets into their modals
  debugLog("📖 Loading markdown snippets…");
  await Promise.all(
    Object.entries(MARKDOWN_FILES).map(([id, path]) =>
      loadMarkdownContent(id, path)
    )
  );

  debugLog("✅ Application initialized.");
});
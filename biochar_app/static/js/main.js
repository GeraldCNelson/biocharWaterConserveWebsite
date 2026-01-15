// static/js/main.js

// 1) Config / constants
import { FALLBACK_UNIT_SYSTEM, fetchMarkdownFiles } from "./config.js";
import { renderNirSet1Table } from "./nir_tab.js";

// 2) Downloads (data, plots, summary CSVs, bulk tab)
import {
  downloadTraceData,
  downloadPlot,
  downloadSummaryData,
  initBulkDownloadTab,
  initSummaryDownloadMenu,
} from "./downloads.js";

// 3) Debugging & logging
import { debugLog, debugGroup } from "./plots.js";

// 4) Markdown loader
import { loadMarkdownContent } from "./markdown.js";

// 5) UI controls (dropdowns, unit toggle, etc.)
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
  // keep this import only if something else still uses it
  updateStartAndEndDatesFromYear,
  // ✅ add these:
  applyDateRangeFromDefaults,
  wireMainDateRangeListeners,
} from "./ui_controls.js";

// 6) Main plotting routines
import { renderMainPlots, waitForAllDropdowns } from "./plot_utils.js";

// 7) Summary-table updater
import { updateSummaryStatistics } from "./tables.js";

// 8) Custom-season setup
import { initCustomGseason } from "./custom_gseason.js";

// ----------------------------------------------------
// Expose download helpers for inline onclick handlers
// ----------------------------------------------------
window.downloadTraceData = downloadTraceData;
window.downloadPlot = downloadPlot;
window.downloadSummaryData = downloadSummaryData;

// ----------------------------------------------------
// Main app bootstrap
// ----------------------------------------------------
document.addEventListener("DOMContentLoaded", async () => {
  debugLog("🌐 Initializing application...");

  // Fetch defaults & options from the server
  const options = await fetchDefaultsAndOptions();
  if (!options) return;

  // Make options globally available if needed elsewhere
  window.dropdownOptions = options;

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

  // ----------------------------------------------------
  // Seed defaults into inputs
  // IMPORTANT: do NOT overwrite startDate/endDate here
  // because those must come from DATE_RANGES.
  // ----------------------------------------------------
  const DO_NOT_SEED = new Set(["startDate", "endDate", "dateRanges"]);

  for (const [key, value] of Object.entries(defaults)) {
    if (DO_NOT_SEED.has(key)) continue;

    const mainEl = document.getElementById(`main-${key}`);
    const summaryEl = document.getElementById(`summary-${key}`);
    if (mainEl) mainEl.value = value;
    if (summaryEl) summaryEl.value = value;
  }

  // ----------------------------------------------------
  // Initialize the date inputs, then apply DATE_RANGES
  // ----------------------------------------------------
  initializeMainDatepickers();

  // Apply correct range immediately based on current selections
  const yearEl = document.getElementById("main-year");
  const granEl = document.getElementById("main-granularity");

  const selectedYear = yearEl ? yearEl.value : String(defaults.year);
  const selectedGran = granEl ? granEl.value : defaults.granularity;

  // Use global dateRanges (set by fetchDefaultsAndOptions / ui_controls.js)
  applyDateRangeFromDefaults(selectedYear, selectedGran, window.dateRanges || {});

  // Wire listeners so year/granularity changes update start/end automatically
  wireMainDateRangeListeners();

  // ❌ Remove this old handler — it can fight with DATE_RANGES logic
  // document
  //   .getElementById("main-year")
  //   ?.addEventListener("change", (e) =>
  //     updateStartAndEndDatesFromYear(e.target.value)
  //   );

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
    if (window.dateRanges) {
      console.log("🗓️ window.dateRanges keys:", Object.keys(window.dateRanges));
    }
  });

  // ----------------------------------------------------
  // Tab wiring
  // ----------------------------------------------------

  // If “Main Data Display” is already active on load, render immediately
  const mainTabLink = document.querySelector('a[href="#main"]');
  if (mainTabLink?.classList.contains("active")) {
    await renderMainPlots();
  }

  // Re-render Main plots whenever that tab is shown
  mainTabLink?.addEventListener("shown.bs.tab", renderMainPlots);

  // ✅ NIR: render the Set 1 table whenever the NIR tab becomes active
  // This requires:
  // - the NIR tab link has href="#nir"
  // - the pane has id="nir"
  // - index.html includes <div id="nir-set1-table-container"></div> inside that pane
  const nirTabLink = document.querySelector('a[href="#nir"]');
  nirTabLink?.addEventListener("shown.bs.tab", () => {
    // Render once (nir_tab.js can internally cache if you implemented it that way)
    renderNirSet1Table();
  });

  // Optional: if the page loads with NIR already active, render immediately
  if (nirTabLink?.classList.contains("active")) {
    renderNirSet1Table();
  }

  // Kick off the summary statistics table (async)
  await updateSummaryStatistics();

  // ----------------------------------------------------
  // Load markdown snippets (from backend mapping)
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
  // Initialize the Custom Season editor (if present)
  // ----------------------------------------------------
  const gseasonContent = document.getElementById("gseason-content");
  if (gseasonContent && window.CUSTOM_GSEASON_CONFIG) {
    initCustomGseason(window.CUSTOM_GSEASON_CONFIG);
  }

  // Bulk downloads tab
  try {
    await initBulkDownloadTab();
  } catch (err) {
    console.error("Failed to initialize Bulk Downloads tab:", err);
  }

  // Summary Statistics dropdown (Raw / Ratio / All)
  try {
    initSummaryDownloadMenu();
  } catch (err) {
    console.error("Failed to initialize Summary Summary dropdown:", err);
  }
});
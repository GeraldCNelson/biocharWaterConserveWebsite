// static/js/main.js

// 1) Config / constants
import { FALLBACK_UNIT_SYSTEM, fetchMarkdownFiles } from "./config.js";
import { renderNirTables } from "./nir_tab.js";
import { renderSoilChemTable, renderSoilBioTable } from "./soil_tab.js";
import { renderBiomassFieldTables } from "./biomass_field_tab.js";

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

  // ----------------------------------------------------
  // Tab wiring helper (adds console breadcrumbs + safe error handling)
  // ----------------------------------------------------
  function wireTabRender({
    tabId = null,                 // e.g. "biomass-field-tab"
    href = null,                  // e.g. "#biomass-field"
    paneId = null,                // e.g. "biomass-field"
    renderFn = null,              // e.g. renderBiomassFieldTables
    label = "tab",
    renderOnLoadIfActive = true,
  }) {
    // Find the tab link
    let tabEl = null;
    if (tabId) tabEl = document.getElementById(tabId);
    if (!tabEl && href) tabEl = document.querySelector(`a[href="${href}"]`);

    if (!tabEl) {
      console.warn(`❌ [wireTabRender] Missing tab link for ${label} (tabId=${tabId}, href=${href})`);
      return;
    }

    // Sanity checks (helpful when Network shows "nothing")
    if (paneId && !document.getElementById(paneId)) {
      console.warn(`⚠️ [wireTabRender] Pane #${paneId} not found for ${label}`);
    }
    if (typeof renderFn !== "function") {
      console.error(`❌ [wireTabRender] renderFn is not a function for ${label}. Got:`, renderFn);
      // Still wire the tab event so you see "shown" logs even if render fn is missing
    }

    async function runRender(reason) {
      console.log(`✅ [wireTabRender] ${label} render triggered (${reason})`);
      try {
        if (typeof renderFn === "function") {
          await renderFn();
        } else {
          console.error(`❌ [wireTabRender] No valid renderFn for ${label}; nothing to run.`);
        }
      } catch (err) {
        console.error(`❌ [wireTabRender] Render failed for ${label}:`, err);
      }
    }

    tabEl.addEventListener("shown.bs.tab", () => runRender("shown.bs.tab"));

    if (renderOnLoadIfActive && tabEl.classList.contains("active")) {
      // don't block the rest of init; render after the current call stack
      Promise.resolve().then(() => runRender("active on load"));
    }
  }

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

  // Main plots
  wireTabRender({
    href: "#main",
    paneId: "main",
    renderFn: renderMainPlots,
    label: "Interactive Plots",
  });

  // NIR
  wireTabRender({
    href: "#nir",
    paneId: "nir",
    renderFn: renderNirTables,
    label: "Pasture Quality Metrics (NIR)",
  });

  // Soil Chemistry
  wireTabRender({
    href: "#soilchem",
    paneId: "soilchem",
    renderFn: renderSoilChemTable,
    label: "Soil Chemistry",
  });

  // Soil Biology
  wireTabRender({
    href: "#soilbio",
    paneId: "soilbio",
    renderFn: renderSoilBioTable,
    label: "Soil Biological Health",
  });

  // ✅ Biomass (Field Samples)
  // Requires:
  // - tab link: <a id="biomass-field-tab" ... href="#biomass-field">
  // - pane:     <div id="biomass-field" ...>
  // - render fn exists in scope (imported into main.js):
  //     renderBiomassFieldTables()
  wireTabRender({
    tabId: "biomass-field-tab",
    href: "#biomass-field",     // fallback if tabId ever changes
    paneId: "biomass-field",
    renderFn: (typeof renderBiomassFieldTables === "function")
      ? renderBiomassFieldTables
      : null,
    label: "Biomass (Field Samples)",
  });

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

  // Extra: quick visibility check so you *know* the Biomass elements exist
  // (This is the #1 reason Network shows "nothing": event never fired.)
  const bioTab = document.getElementById("biomass-field-tab");
  const bioPane = document.getElementById("biomass-field");
  console.log("🔎 Biomass wiring sanity:", {
    biomassTabFound: !!bioTab,
    biomassPaneFound: !!bioPane,
    biomassTabIsActive: !!bioTab && bioTab.classList.contains("active"),
    biomassRendererType: typeof renderBiomassFieldTables,
  });
});
// static/js/main.js

// 1) Config / constants
import { fetchMarkdownFiles } from "./config.js";
import { renderNirTables } from "./tab_nir.js";
import { renderSoilChemTable, renderSoilBioTable } from "./tab_soil.js";
import { renderBiomassFieldTables } from "./tab_biomass_field.js";
import { initSummaryTab } from "./tab_summary.js";

// 2) Downloads (data, plots, summary CSVs, bulk tab)
import {
  downloadTraceData,
  downloadPlot,
  downloadSummaryData,
  initBulkDownloadTab,
  initSummaryDownloadMenu,
} from "./downloads.js";

// 3) Debugging & logging
import { renderMainPlots, waitForAllDropdowns } from "./plots.js";
import { debugLog, debugGroup } from "./debug_utils.js";

// 4) Markdown loader
import { loadMarkdownContent } from "./markdown.js";

// 5) UI controls (dropdowns, unit toggle, etc.)
import {
  initializeUpdateButtons,
  setupUnitToggleHandlers,
  getAllDropdownIds,
  initializeTraceOptionControls,
} from "./control_panel.js";

import {
  fetchDefaultsAndOptions,
  populateAllDropdowns,
  initializeMainDatepickers,
  updateDepthLabels,
  applyDateRangeFromDefaults,
  wireMainDateRangeListeners,
} from "./ui_controls.js";

// 7) Custom-season setup
import { initCustomGseason } from "./custom_gseason.js";

// 8) Boot loader helper
import { hideBootLoading } from "./ui_loading.js";

// ----------------------------------------------------
// Expose download helpers for inline onclick handlers
// ----------------------------------------------------
window.downloadTraceData = downloadTraceData;
window.downloadPlot = downloadPlot;
window.downloadSummaryData = downloadSummaryData;

// ----------------------------------------------------
// Tab wiring helper
// ----------------------------------------------------
function wireTabRender({ href, tabId, paneId, renderFn, label }) {
  // Prefer explicit tabId if provided.
  // Fallback supports both old anchor tabs (href="#pane")
  // and new button tabs (data-bs-target="#pane").
  const tabLink =
    (tabId ? document.getElementById(tabId) : null) ||
    (href
      ? document.querySelector(`[data-bs-target="${href}"], a[href="${href}"]`)
      : null);

  if (!tabLink) {
    console.warn("[wireTabRender] Tab control not found", {
      href,
      tabId,
      paneId,
      label,
    });
    return;
  }

  if (typeof renderFn !== "function") {
    console.error("[wireTabRender] renderFn is not a function", {
      href,
      tabId,
      paneId,
      label,
      renderFnType: typeof renderFn,
    });
    return;
  }

  tabLink.addEventListener("shown.bs.tab", () => {
    console.debug(`[wireTabRender] ${label || href || tabId} render triggered`);
    renderFn();
  });

  // Render immediately if already active
  if (tabLink.classList.contains("active")) {
    console.debug(
      `[wireTabRender] ${label || href || tabId} already active — rendering`
    );
    renderFn();
  }
}

// ----------------------------------------------------
// Main app bootstrap
// ----------------------------------------------------
document.addEventListener("DOMContentLoaded", async () => {
  debugLog("🌐 Initializing application...");

  try {
    // Fetch defaults & options from the server
    const options = await fetchDefaultsAndOptions();
    if (!options) {
      console.error("❌ fetchDefaultsAndOptions returned null/undefined.");
      return;
    }

    // Make options globally available if needed elsewhere
    window.dropdownOptions = options;

    const defaults = options.defaults || {};


if (!defaults.unitSystem) {
  console.error("❌ Missing defaults.unitSystem from backend", defaults);
  throw new Error("unitSystem missing from API defaults");
}

window.unitSystem = defaults.unitSystem;
console.log("✅ unitSystem initialized from backend:", window.unitSystem);

    // Populate dropdowns and wire up control-panel buttons
    populateAllDropdowns(options);
    setupUnitToggleHandlers(window.unitSystem);
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

    const yearEl = document.getElementById("main-year");
    const granEl = document.getElementById("main-granularity");

    const selectedYear = yearEl ? yearEl.value : String(defaults.year || "");
    const selectedGran = granEl ? granEl.value : defaults.granularity;

    applyDateRangeFromDefaults(
      selectedYear,
      selectedGran,
      window.dateRanges || {}
    );
    wireMainDateRangeListeners();

    // Make sure the depth labels match the current unit system
    updateDepthLabels(window.unitSystem);

    // Wire the trace-option dependent control enable/disable behavior
    initializeTraceOptionControls();

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
    wireTabRender({
      href: "#main",
      tabId: "main-tab",
      paneId: "main",
      renderFn: renderMainPlots,
      label: "Interactive Plots",
    });

    wireTabRender({
      href: "#nir",
      tabId: "nir-tab",
      paneId: "nir",
      renderFn: renderNirTables,
      label: "Pasture Quality Metrics (NIR)",
    });

    wireTabRender({
      href: "#soilchem",
      tabId: "soilchem-tab",
      paneId: "soilchem",
      renderFn: renderSoilChemTable,
      label: "Soil Chemistry",
    });

    wireTabRender({
      href: "#soilbio",
      tabId: "soilbio-tab",
      paneId: "soilbio",
      renderFn: renderSoilBioTable,
      label: "Soil Biological Health",
    });

    wireTabRender({
      href: "#biomass-field",
      tabId: "biomass-field-tab",
      paneId: "biomass-field",
      renderFn: renderBiomassFieldTables,
      label: "Biomass (Field Samples)",
    });

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

    // ----------------------------------------------------
    // Initialize the Custom Season editor (if present)
    // ----------------------------------------------------
    const gseasonContent = document.getElementById("gseason-content");
    if (gseasonContent && window.CUSTOM_GSEASON_CONFIG) {
      initCustomGseason(window.CUSTOM_GSEASON_CONFIG);
    }

    initSummaryTab();

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
    const bioTab = document.getElementById("biomass-field-tab");
    const bioPane = document.getElementById("biomass-field");
    console.log("🔎 Biomass wiring sanity:", {
      biomassTabFound: !!bioTab,
      biomassPaneFound: !!bioPane,
      biomassTabIsActive: !!bioTab && bioTab.classList.contains("active"),
      biomassRendererType: typeof renderBiomassFieldTables,
    });

    debugLog("✅ Application initialized.");
  } catch (err) {
    console.error("❌ Fatal error during app initialization:", err);
  } finally {
    // ✅ CRITICAL: remove the full-screen boot overlay so it can’t hide the UI
    hideBootLoading();
  }
});
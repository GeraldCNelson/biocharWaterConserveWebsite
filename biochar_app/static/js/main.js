// @ts-check
// static/js/main.js

// 1) Config / constants
import { fetchMarkdownFiles } from "./config.js";
import { renderNirTables } from "./tab_nir.js";
import { renderSoilChemTable, renderSoilBioTable } from "./tab_soil.js";
import { renderBiomassFieldTables } from "./tab_biomass_field.js";
import { initSummaryTab } from "./tab_summary.js";
import { renderGlossary } from "./glossary.js";

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

/**
 * @typedef {Window & {
 *   downloadTraceData?: typeof downloadTraceData,
 *   downloadPlot?: typeof downloadPlot,
 *   downloadSummaryData?: typeof downloadSummaryData,
 *   dropdownOptions?: Record<string, any>,
 *   unitSystem?: string,
 *   dateRanges?: Record<string, any>,
 *   depthMapping?: Record<string, { us?: string, metric?: string }>,
 *   CUSTOM_GSEASON_CONFIG?: any,
 *   bootstrap?: {
 *     Dropdown?: {
 *       getOrCreateInstance: (el: Element) => { hide: () => void }
 *     }
 *   }
 * }} MainWindow
 */

/** @type {MainWindow} */
const mainWindow = /** @type {MainWindow} */ (window);

/**
 * Hide the initial boot/loading overlay if present.
 * Kept local so main.js does not depend on a missing export.
 *
 * @returns {void}
 */
function hideBootLoading() {
  const boot = document.getElementById("boot-loading");
  if (boot) {
    boot.style.display = "none";
  }
}

/**
 * @typedef {{
 *   href?: string,
 *   tabId?: string,
 *   paneId?: string,
 *   renderFn: () => void | Promise<void>,
 *   label?: string
 * }} WireTabRenderArgs
 */

// ----------------------------------------------------
// Expose download helpers for inline onclick handlers
// ----------------------------------------------------
mainWindow.downloadTraceData = downloadTraceData;
mainWindow.downloadPlot = downloadPlot;
mainWindow.downloadSummaryData = downloadSummaryData;

/**
 * @param {WireTabRenderArgs} args
 * @returns {void}
 */
function wireTabRender({ href, tabId, paneId, renderFn, label }) {
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
    void renderFn();
  });

  if (tabLink.classList.contains("active")) {
    console.debug(
      `[wireTabRender] ${label || href || tabId} already active — rendering`
    );
    void renderFn();
  }
}

// ----------------------------------------------------
// Main app bootstrap
// ----------------------------------------------------
document.addEventListener("DOMContentLoaded", async () => {
  debugLog("🌐 Initializing application...");

  try {
    const options = await fetchDefaultsAndOptions();
    if (!options) {
      console.error("❌ fetchDefaultsAndOptions returned null/undefined.");
      return;
    }

    mainWindow.dropdownOptions = options;

    const defaults = options.defaults || {};

    if (!defaults.unitSystem) {
      console.error("❌ Missing defaults.unitSystem from backend", defaults);
      throw new Error("unitSystem missing from API defaults");
    }

    mainWindow.unitSystem = defaults.unitSystem;
    console.log("✅ unitSystem initialized from backend:", mainWindow.unitSystem);

    populateAllDropdowns(options);
    setupUnitToggleHandlers(mainWindow.unitSystem === "metric" ? "metric" : "us");
    initializeUpdateButtons();

    await waitForAllDropdowns(getAllDropdownIds());
    await new Promise(
      /**
       * @param {FrameRequestCallback} resolve
       */
      (resolve) => requestAnimationFrame(resolve)
    );

    const DO_NOT_SEED = new Set(["startDate", "endDate", "dateRanges"]);
    for (const [key, value] of Object.entries(defaults)) {
      if (DO_NOT_SEED.has(key)) continue;

      const mainEl = /** @type {HTMLInputElement | HTMLSelectElement | null} */ (
        document.getElementById(`main-${key}`)
      );
      const summaryEl = /** @type {HTMLInputElement | HTMLSelectElement | null} */ (
        document.getElementById(`summary-${key}`)
      );

      if (mainEl) mainEl.value = String(value);
      if (summaryEl) summaryEl.value = String(value);
    }

    initializeMainDatepickers();

    const yearEl = /** @type {HTMLSelectElement | null} */ (
      document.getElementById("main-year")
    );
    const granEl = /** @type {HTMLSelectElement | null} */ (
      document.getElementById("main-granularity")
    );

    const selectedYear = yearEl ? yearEl.value : String(defaults.year || "");
    const selectedGran = granEl ? granEl.value : defaults.granularity;

    applyDateRangeFromDefaults(
      selectedYear,
      selectedGran,
      mainWindow.dateRanges || {}
    );
    wireMainDateRangeListeners();

    updateDepthLabels(mainWindow.unitSystem || "us");

    initializeTraceOptionControls();

    debugGroup("🎛️ Dropdown defaults & mappings", () => {
      console.table(defaults);

      if (mainWindow.depthMapping) {
        console.table(
          Object.entries(mainWindow.depthMapping).map(([depth, map]) => ({
            Depth: depth,
            US: map.us,
            Metric: map.metric,
          }))
        );
      }

      if (mainWindow.dateRanges) {
        console.log("🗓️ window.dateRanges keys:", Object.keys(mainWindow.dateRanges));
      }
    });

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

    wireTabRender({
      href: "#glossary",
      tabId: "glossary-tab",
      paneId: "glossary",
      renderFn: renderGlossary,
      label: "Glossary",
    });

    debugLog("📖 Loading markdown mapping from backend…");

    /** @type {Record<string, string>} */
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

    const gseasonContent = document.getElementById("gseason-content");
    if (gseasonContent && mainWindow.CUSTOM_GSEASON_CONFIG) {
      initCustomGseason(mainWindow.CUSTOM_GSEASON_CONFIG);
    }

    initSummaryTab();

    try {
      await initBulkDownloadTab();
    } catch (err) {
      console.error("Failed to initialize Bulk Downloads tab:", err);
    }

    try {
      initSummaryDownloadMenu();
    } catch (err) {
      console.error("Failed to initialize Summary Summary dropdown:", err);
    }

    const bioTab = document.getElementById("biomass-field-tab");
    const bioPane = document.getElementById("biomass-field");
    console.log("🔎 Biomass wiring sanity:", {
      biomassTabFound: !!bioTab,
      biomassPaneFound: !!bioPane,
      biomassTabIsActive: !!bioTab && bioTab.classList.contains("active"),
      biomassRendererType: typeof renderBiomassFieldTables,
    });

    document.addEventListener("click", function (event) {
      const target = event.target;
      if (!(target instanceof Element)) return;

      const link = target.closest(".tab-link");
      if (!link) return;

      event.preventDefault();

      const tabId = link.getAttribute("data-tab");
      if (!tabId) return;

      const tabButton = document.getElementById(tabId);
      if (!(tabButton instanceof HTMLElement)) return;

      tabButton.click();

      const dropdownMenu = tabButton.closest(".dropdown-menu");
      if (dropdownMenu && mainWindow.bootstrap?.Dropdown) {
        const toggle = dropdownMenu.previousElementSibling;
        if (toggle instanceof Element) {
          const dropdownInstance = mainWindow.bootstrap.Dropdown.getOrCreateInstance(toggle);
          dropdownInstance.hide();
        }
      }
    });

    debugLog("✅ Application initialized.");
  } catch (err) {
    console.error("❌ Fatal error during app initialization:", err);
  } finally {
    hideBootLoading();
  }
});
// @ts-check
// static/js/downloads.js

/**
 * @typedef {Window & {
 *   unitSystem?: string,
 *   depthMapping?: Record<string, Record<string, string>>,
 *   __lastSummaryData?: any,
 *   __bulkDownloadManifest?: any,
 *   Plotly?: any
 * }} DownloadsWindow
 */

/** @type {DownloadsWindow} */
const downloadsWindow = /** @type {DownloadsWindow} */ (window);

/**
 * @param {Array<string | number | null | undefined | false>} parts
 * @returns {string}
 */
function buildFilename(parts) {
  return parts
      .filter(Boolean)
      .join("_")
      .replace(/\s+/g, "-")
      .replace(/[^\w\-]+/g, "");
}

/**
 * @param {string | null} cd
 * @returns {string}
 */
function getFilenameFromContentDisposition(cd) {
  if (!cd) return "";

  const starMatch = cd.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
  if (starMatch && starMatch[1]) {
    try {
      return decodeURIComponent(starMatch[1].trim().replace(/^"(.*)"$/, "$1"));
    } catch {
      return starMatch[1].trim().replace(/^"(.*)"$/, "$1");
    }
  }

  const match = cd.match(/filename\s*=\s*("?)([^";]+)\1/i);
  if (match && match[2]) return match[2].trim();

  return "";
}

/**
 * @param {string | null} ct
 * @returns {string}
 */
function extFromContentType(ct) {
  if (!ct) return "";
  const c = ct.toLowerCase();

  if (c.includes("application/zip")) return "zip";
  if (c.includes("text/csv")) return "csv";
  if (c.includes("application/pdf")) return "pdf";
  if (c.includes("application/json")) return "json";
  if (c.includes("image/png")) return "png";
  if (c.includes("image/jpeg")) return "jpg";
  if (c.includes("image/svg")) return "svg";
  if (c.includes("image/webp")) return "webp";

  return "";
}

/**
 * @param {string} url
 * @param {any} payload
 * @param {string} fallbackFilename
 * @returns {Promise<void>}
 */
async function postAndDownload(url, payload, fallbackFilename) {
  console.log("⬇️ postAndDownload →", url, payload);

  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const txt = await resp.text();
    console.error("❌ Download failed:", resp.status, txt);
    throw new Error(`Download failed with status ${resp.status}`);
  }

  const blob = await resp.blob();
  const objUrl = window.URL.createObjectURL(blob);

  const cd = resp.headers.get("content-disposition");
  const ct = resp.headers.get("content-type");
  let filename = getFilenameFromContentDisposition(cd);

  if (!filename) {
    const ext = extFromContentType(ct);
    if (ext && fallbackFilename) {
      filename = fallbackFilename.replace(/\.[A-Za-z0-9]+$/, `.${ext}`);
      if (!/\.[A-Za-z0-9]+$/.test(filename)) filename = `${filename}.${ext}`;
    } else {
      filename = fallbackFilename || "download";
    }
  }

  const a = document.createElement("a");
  a.href = objUrl;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();

  window.URL.revokeObjectURL(objUrl);
  console.log("✅ Download triggered:", filename, { contentType: ct, contentDisposition: cd });
}

/**
 * @param {"raw"|"ratio"|"all"} [kind="all"]
 * @returns {Promise<void>}
 */
export async function downloadTraceData(kind = "all") {
  try {
    const yearEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-year"));
    const variableEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-variable"));
    const stripEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-strip"));
    const granEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-granularity"));
    const depthEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-depth"));

    if (!yearEl || !variableEl || !stripEl || !granEl || !depthEl) {
      alert("⚠️ Cannot download data: one or more controls are missing.");
      return;
    }

    const year = parseInt(yearEl.value, 10);
    const variable = variableEl.value;
    const strip = stripEl.value;
    const granularity = granEl.value;
    const depth = depthEl.value;
    const unitSystem = downloadsWindow.unitSystem || "us";

    const payload = {
      year,
      variable,
      strip,
      granularity,
      depth,
      unitSystem,
      downloadType: kind,
    };

    const fname =
        buildFilename([
          "data",
          kind,
          variable,
          strip,
          `${depth}depth`,
          granularity,
          year,
        ]) + ".zip";

    await postAndDownload("/api/download_data", payload, fname);
  } catch (err) {
    console.error("❌ Error in downloadTraceData:", err);
    alert("Unable to download data. Please check the console for details.");
  }
}

/**
 * @param {"raw"|"ratio"|"all"|"zip"} [mode="all"]
 * @returns {Promise<void>}
 */
export async function downloadSummaryData(mode = "all") {
  const yearEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("summary-year"));
  const variableEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("summary-variable"));
  const stripEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("summary-strip"));
  const granularityEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("summary-granularity"));
  const depthEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("summary-depth"));

  if (!yearEl || !variableEl || !stripEl || !granularityEl || !depthEl) {
    console.error("❌ downloadSummaryData: summary controls not found in DOM");
    alert("Internal error: summary controls are missing.");
    return;
  }

  const year = parseInt(yearEl.value, 10);
  const variable = variableEl.value;
  const strip = stripEl.value;
  const granularity = granularityEl.value;
  const depth = depthEl.value;
  const unitSystem = downloadsWindow.unitSystem || "us";

  if (Number.isNaN(year)) {
    alert("Please choose a valid year before downloading.");
    return;
  }

  const summaryStats = downloadsWindow.__lastSummaryData || null;

  const payload = {
    year,
    variable,
    strip,
    granularity,
    depth,
    unitSystem,
    mode,
    summaryStats,
  };

  console.log("⬇️ downloadSummaryData payload:", payload);

  try {
    const wantZip = mode === "zip";
    const ext = wantZip ? "zip" : "csv";

    let baseName = `summary_${granularity}_${variable}_${year}`;
    if (!wantZip) baseName += `_${mode}`;

    const fallbackName = `${baseName}.${ext}`;
    await postAndDownload("/api/download_summary_data", payload, fallbackName);
  } catch (err) {
    console.error("❌ Error in downloadSummaryData:", err);
    alert("An error occurred while downloading summary data.");
  }
}

/**
 * @returns {void}
 */
export function initSummaryDownloadMenu() {
  const rawBtn = document.getElementById("download-summary-raw");
  const ratioBtn = document.getElementById("download-summary-ratio");
  const allBtn = document.getElementById("download-summary-all");
  const zipBtn = document.getElementById("download-summary-zip");

  if (!rawBtn && !ratioBtn && !allBtn && !zipBtn) {
    console.warn("⚠️ Summary download menu not found in DOM.");
    return;
  }

  if (rawBtn) {
    rawBtn.addEventListener("click", (e) => {
      e.preventDefault();
      void downloadSummaryData("raw");
    });
  }

  if (ratioBtn) {
    ratioBtn.addEventListener("click", (e) => {
      e.preventDefault();
      void downloadSummaryData("ratio");
    });
  }

  if (allBtn) {
    allBtn.addEventListener("click", (e) => {
      e.preventDefault();
      void downloadSummaryData("all");
    });
  }

  if (zipBtn) {
    zipBtn.addEventListener("click", (e) => {
      e.preventDefault();
      void downloadSummaryData("zip");
    });
  }

  console.log("✅ Summary download menu initialized.");
}

/**
 * @param {{ plotType: string, format: string }} param0
 * @returns {string}
 */
function buildPlotFilename({ plotType, format }) {
  /**
   * @param {string} id
   * @param {string} [fallback=""]
   * @returns {string}
   */
  const getVal = (id, fallback = "") => {
    const el = /** @type {HTMLInputElement | HTMLSelectElement | null} */ (document.getElementById(id));
    return el?.value || fallback;
  };

  const variable = getVal("main-variable");
  const strip = getVal("main-strip");
  const logger = getVal("main-loggerLocation");
  const depthIndex = getVal("main-depth");
  const granularity = getVal("main-granularity");
  const year = getVal("main-year");
  const unitSystem = downloadsWindow.unitSystem || "us";

  let depthPart = "";
  if (depthIndex && downloadsWindow.depthMapping?.[depthIndex]) {
    const depthLabel = downloadsWindow.depthMapping[depthIndex][unitSystem];
    depthPart = String(depthLabel)
        .replace(/\s+/g, "")
        .replace("inches", "in")
        .replace("inch", "in")
        .replace("centimeters", "cm")
        .replace("centimeter", "cm");
  }

  const ext = format === "jpeg" ? "jpg" : format;

  const parts = [
    "biochar",
    plotType,
    variable,
    strip,
    logger,
    depthPart,
    granularity,
    year,
  ].filter(Boolean);

  const safe = parts.join("_").replace(/\s+/g, "_").replace(/[^A-Za-z0-9._-]/g, "");
  return `${safe}.${ext}`;
}

/**
 * @param {string|HTMLElement} [target="raw"]
 * @param {string} [format="png"]
 * @param {string} [sizeMode="screen"]
 * @returns {Promise<void>}
 */
export async function downloadPlot(target = "raw", format = "png", sizeMode = "screen") {
  try {
    /** @type {any} */
    const Plotly = downloadsWindow.Plotly;
    if (!Plotly) {
      alert("Plotly is not available; cannot download plot image.");
      return;
    }

    console.log("🎯 downloadPlot called with:", { target, format, sizeMode });

    let plotlyFormat = (format || "png").toLowerCase().trim();
    if (plotlyFormat === "jpg") plotlyFormat = "jpeg";
    if (!["png", "jpeg", "webp", "svg"].includes(plotlyFormat)) {
      console.warn("⚠️ Unknown format requested; falling back to png:", plotlyFormat);
      plotlyFormat = "png";
    }

    /** @type {HTMLElement | null} */
    let el = null;

    if (typeof target === "object" && target !== null) {
      el = /** @type {HTMLElement} */ (target);
    } else {
      const t = String(target).toLowerCase().trim();
      /** @type {string[]} */
      const idCandidates = [];

      if (t === "raw") idCandidates.push("plot-1");
      else if (t === "ratio") idCandidates.push("plot-2");
      else idCandidates.push(String(target));

      for (const id of idCandidates) {
        const candidate = document.getElementById(id);
        if (candidate) {
          el = candidate;
          break;
        }
      }

      if (!el) {
        const plotDivs = Array.from(document.querySelectorAll(".js-plotly-plot"));
        if (plotDivs.length > 0) {
          el = /** @type {HTMLElement} */ (t === "ratio" ? plotDivs[1] || plotDivs[0] : plotDivs[0]);
        }
      }
    }

    if (!el) {
      console.error("❌ downloadPlot: No plot found. Target =", target);
      alert("Unable to find the plot container to download.");
      return;
    }

    let width;
    let height;
    let scale;

    if (String(sizeMode).toLowerCase().trim() === "fixed") {
      width = 1600;
      height = 900;
      scale = 2;
    } else {
      const rect = el.getBoundingClientRect();
      width = Math.max(600, Math.round(rect.width));
      height = Math.max(450, Math.round(rect.height));
      scale = 1;
    }

    const plotType = String(target).toLowerCase().trim() === "ratio" ? "ratio" : "raw";
    const filename = buildPlotFilename({ plotType, format: plotlyFormat });
    const dataUrl = await Plotly.toImage(el, { format: plotlyFormat, width, height, scale });

    const a = document.createElement("a");
    a.href = dataUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();

    console.log("✅ Plot image download triggered:", filename);
  } catch (err) {
    console.error("❌ Error in downloadPlot:", err);
    alert("An error occurred while downloading the plot image.");
  }
}

const ALL_YEARS_DATASETS = new Set([
  "irrigation",
  "fertilizer",
  "soil_chem_all",
  "soil_bio_all",
  "hay_all",
]);

/** @type {Record<string, string>} */
const ALL_YEARS_UI_ALIASES = {
  soil_chem: "soil_chem_all",
  soil_chemistry: "soil_chem_all",
  soil_bio: "soil_bio_all",
  soil_biology: "soil_bio_all",
  biomass: "hay_all",
  biomass_hay: "hay_all",
  hay: "hay_all",
  hay_nir: "hay_all",
};

/** @type {Record<string, string>} */
const ALL_YEARS_KEY_BY_DATASET = {
  irrigation: "irrigation_all",
  fertilizer: "fertilizer_all",
  soil_chem_all: "soil_chem_all",
  soil_bio_all: "soil_bio_all",
  hay_all: "hay_all",
};

/** @type {Record<string, string>} */
const BULK_DATASET_TOKEN = {
  loggers: "loggers",
  weather: "weather",
  irrigation: "irrigation",
  fertilizer: "fertilizer",
  biomass: "biomass",
};

/**
 * @param {string} uiDatasetRaw
 * @returns {string}
 */
function normalizeBulkDataset(uiDatasetRaw) {
  const ui = String(uiDatasetRaw || "").trim().toLowerCase();

  if (ui === "fertilizing") return "fertilizer";

  return ALL_YEARS_UI_ALIASES[ui] || ui;
}

/**
 * @param {string} normalizedDataset
 * @returns {boolean}
 */
function isAllYearsDataset(normalizedDataset) {
  return ALL_YEARS_DATASETS.has(normalizedDataset);
}

/**
 * @param {string} normalizedDataset
 * @returns {string}
 */
function allYearsManifestKey(normalizedDataset) {
  return ALL_YEARS_KEY_BY_DATASET[normalizedDataset] || normalizedDataset;
}

/**
 * @param {string | null | undefined} raw
 * @returns {string | null}
 */
function normalizeResolution(raw) {
  if (raw == null) return null;

  let g = String(raw).trim().toLowerCase();
  if (!g) return null;

  const map = {
    "15-min": "15min",
    "15 min": "15min",
    "15m": "15min",
    "15 minutes": "15min",
    hour: "hourly",
    hours: "hourly",
    hourly: "hourly",
    day: "daily",
    days: "daily",
    daily: "daily",
    month: "monthly",
    months: "monthly",
    monthly: "monthly",
    "g-season": "gseason",
    "g season": "gseason",
    g_season: "gseason",
    gseason: "gseason",
  };

  if (Object.prototype.hasOwnProperty.call(map, g)) {
    g = /** @type {Record<string, string>} */ (map)[g];
  }

  return g;
}

/**
 * @param {any} manifest
 * @returns {any[] | null}
 */
function getManifestEntries(manifest) {
  if (!manifest) return null;
  if (Array.isArray(manifest)) return manifest;
  if (Array.isArray(manifest.entries)) return manifest.entries;
  return null;
}

/**
 * @param {any} manifest
 * @param {string} key
 * @returns {any | null}
 */
function findEntryByKey(manifest, key) {
  const entries = getManifestEntries(manifest);
  if (!Array.isArray(entries)) return null;
  return entries.find((e) => e && typeof e === "object" && e.key === key) || null;
}

/**
 * @param {any} manifest
 * @param {{ dataset: string, year: number | null, granularity: string | null }} args
 * @returns {string | null}
 */
function findManifestKey(manifest, { dataset, year, granularity }) {
  if (!manifest) return null;

  if (isAllYearsDataset(dataset)) {
    const key = allYearsManifestKey(dataset);
    return findEntryByKey(manifest, key)?.key || null;
  }

  const token = BULK_DATASET_TOKEN[dataset] || dataset;
  const y = year == null ? null : parseInt(String(year), 10);
  const g = normalizeResolution(granularity);
  const entries = getManifestEntries(manifest);

  if (!Array.isArray(entries)) return null;

  return (
      entries.find((e) => {
        if (!e || typeof e !== "object") return false;
        if (String(e.dataset || "").trim().toLowerCase() !== String(token).trim().toLowerCase()) return false;
        if (y != null && parseInt(e.year, 10) !== y) return false;

        const res = normalizeResolution(e.resolution ?? e.granularity ?? null);
        if (g && res !== g) return false;

        return Boolean(e.key);
      })?.key || null
  );
}

/**
 * @param {HTMLSelectElement | null} el
 * @returns {boolean}
 */
function selectHasRealOptions(el) {
  if (!el) return false;
  const values = Array.from(el.options || []).map((opt) => String(opt.value || "").trim());
  return values.some((v) => v !== "");
}

/**
 * @param {any} manifest
 * @param {string} dataset
 * @returns {boolean}
 */
function hasDatasetFamily(manifest, dataset) {
  const token = BULK_DATASET_TOKEN[dataset] || dataset;
  const entries = getManifestEntries(manifest);

  if (!Array.isArray(entries)) return false;

  return entries.some(
      (e) =>
          e &&
          typeof e === "object" &&
          String(e.dataset || "").trim().toLowerCase() === String(token).trim().toLowerCase()
  );
}

/**
 * @returns {"us" | "metric"}
 */
function selectedBulkUnitSystem() {
  const bulkToggle = /** @type {HTMLInputElement | null} */ (
      document.getElementById("units-toggle_bulk")
  );

  if (bulkToggle) {
    return bulkToggle.checked ? "metric" : "us";
  }

  return downloadsWindow.unitSystem === "metric" ? "metric" : "us";
}
/**
 * @returns {Promise<void>}
 */
export async function initBulkDownloadTab() {
  const yearEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("bulk-year") ||
    document.querySelector('[data-role="bulk-year"]')
  );

  const granEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("bulk-granularity") ||
    document.querySelector('[data-role="bulk-granularity"]')
  );

  const buttons = /** @type {HTMLButtonElement[]} */ (
    Array.from(document.querySelectorAll(".bulk-download-btn, [data-dataset]"))
  );

  if (!buttons.length) {
    console.warn("⚠️ Bulk download UI not found (need buttons with data-dataset).", {
      hasYear: !!yearEl,
      hasGranularity: !!granEl,
      hasButtons: false,
    });
    return;
  }

  function selectedYear() {
    if (!yearEl) return null;
    const raw = String(yearEl.value || "").trim();
    const y = parseInt(raw, 10);
    return Number.isFinite(y) ? y : null;
  }

  function selectedGranularity() {
    if (!granEl) return null;
    return normalizeResolution(granEl.value);
  }

  function setButtonState(btn, { visualEnabled, hardDisable = false }) {
    btn.disabled = hardDisable;
    btn.classList.toggle("disabled", !visualEnabled);
    btn.setAttribute("aria-disabled", String(!visualEnabled));
  }

  /** @type {any} */
  let manifest = null;

  try {
    const resp = await fetch("/api/bulk_download_manifest");
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

    manifest = await resp.json();
    downloadsWindow.__bulkDownloadManifest = manifest;

    console.log("🧾 Bulk manifest loaded.", {
      entries: getManifestEntries(manifest)?.length || 0,
      hasIrrigation: Boolean(findEntryByKey(manifest, "irrigation_all")),
      hasFertilizer: Boolean(findEntryByKey(manifest, "fertilizer_all")),
    });
  } catch (e) {
    console.warn("⚠️ Bulk manifest fetch failed:", e);

    for (const b of buttons) {
      setButtonState(b, { visualEnabled: false, hardDisable: true });
    }

    return;
  }

  const years = Array.isArray(manifest?.years) ? manifest.years : [];
  const rawGranularities = Array.isArray(manifest?.granularities)
    ? manifest.granularities
    : [];

  const granularityOptions = rawGranularities
    .map((g) => {
      if (typeof g === "string") {
        return { value: g, label: g };
      }

      return {
        value: String(g?.value || ""),
        label: String(g?.label || g?.value || ""),
      };
    })
    .filter((g) => g.value);

  if (yearEl && !selectHasRealOptions(yearEl)) {
    const opts = (years.length ? years : [2023, 2024, 2025, 2026])
      .map((y) => `<option value="${y}">${y}</option>`)
      .join("");

    yearEl.innerHTML = `<option value="">Select year...</option>${opts}`;
  }

  if (granEl && !selectHasRealOptions(granEl)) {
    granEl.innerHTML =
      `<option value="">Select granularity...</option>` +
      granularityOptions
        .map((g) => `<option value="${g.value}">${g.label}</option>`)
        .join("");
  }

  function refreshEnabledState() {
    const y = selectedYear();
    const g = selectedGranularity();
    const entries = getManifestEntries(manifest);
    const hasEntries = Array.isArray(entries) && entries.length > 0;

    for (const btn of buttons) {
      const uiDsRaw = btn.dataset.dataset || btn.getAttribute("data-dataset") || "";
      const ds = normalizeBulkDataset(uiDsRaw);

      if (!hasEntries) {
        setButtonState(btn, {
          visualEnabled: isAllYearsDataset(ds) || Boolean(y),
          hardDisable: false,
        });
        continue;
      }

      if (isAllYearsDataset(ds)) {
        const key = allYearsManifestKey(ds);
        const entry = findEntryByKey(manifest, key);

        setButtonState(btn, {
          visualEnabled: Boolean(entry?.key),
          hardDisable: !Boolean(entry?.key),
        });

        continue;
      }

      if (ds === "loggers" || ds === "weather") {
        const familyExists = hasDatasetFamily(manifest, ds);

        if (!familyExists) {
          setButtonState(btn, { visualEnabled: false, hardDisable: true });
          continue;
        }

        if (!y || !g) {
          setButtonState(btn, { visualEnabled: false, hardDisable: false });
          continue;
        }

        const key = findManifestKey(manifest, {
          dataset: ds,
          year: y,
          granularity: g,
        });

        setButtonState(btn, {
          visualEnabled: Boolean(key),
          hardDisable: !Boolean(key),
        });

        continue;
      }

      const familyExists = hasDatasetFamily(manifest, ds);

      if (!familyExists) {
        setButtonState(btn, { visualEnabled: false, hardDisable: true });
        continue;
      }

      if (!y) {
        setButtonState(btn, { visualEnabled: false, hardDisable: false });
        continue;
      }

      const key = findManifestKey(manifest, {
        dataset: ds,
        year: y,
        granularity: null,
      });

      setButtonState(btn, {
        visualEnabled: Boolean(key),
        hardDisable: !Boolean(key),
      });
    }
  }

  if (yearEl) yearEl.addEventListener("change", refreshEnabledState);
  if (granEl) granEl.addEventListener("change", refreshEnabledState);

  const bulkUnitToggle = /** @type {HTMLInputElement | null} */ (
    document.getElementById("units-toggle_bulk")
  );

  if (bulkUnitToggle) {
    bulkUnitToggle.checked = downloadsWindow.unitSystem === "metric";
  }

  for (const btn of buttons) {
    btn.addEventListener("click", async (evt) => {
      evt.preventDefault();

      const uiDatasetRaw = btn.dataset.dataset || btn.getAttribute("data-dataset") || "";
      const uiDataset = String(uiDatasetRaw || "").trim();
      const ds = normalizeBulkDataset(uiDataset);
      const y = selectedYear();
      const g = selectedGranularity();
      const allYears = isAllYearsDataset(ds);

      if ((ds === "loggers" || ds === "weather") && (!y || !g)) {
        alert("Please select both a year and a granularity.");

        if (!y) document.getElementById("bulk-year")?.focus();
        else document.getElementById("bulk-granularity")?.focus();

        return;
      }

      if (!allYears && !y) {
        alert("Please select a year.");
        document.getElementById("bulk-year")?.focus();
        return;
      }

      const key = findManifestKey(manifest, {
        dataset: ds,
        year: ds === "loggers" || ds === "weather" ? y : allYears ? null : y,
        granularity: ds === "loggers" || ds === "weather" ? g : null,
      });

      if (!key) {
        console.error("❌ No manifest key found for selection:", {
          uiDataset,
          normalized: ds,
          year: y,
          granularity: g,
          manifest,
        });

        alert("That dataset/year (and granularity, if applicable) is not available.");
        return;
      }

      const unitSystem = selectedBulkUnitSystem();
      const payload = { keys: [key], unitSystem };
      console.log("🧾 Bulk download request:", payload);

      let suffix = "";
      if (ds === "loggers" || ds === "weather") suffix = `${y}_${g}`;
      else if (allYears) suffix = "all_years";
      else suffix = String(y);

      const fallbackZipName = buildFilename(["biochar", key, suffix]) + ".zip";

      try {
        await postAndDownload("/api/bulk_download", payload, fallbackZipName);
      } catch (err) {
        console.error("❌ Bulk download failed:", err);
        alert("Unable to download the selected dataset. Please check the console.");
      }
    });
  }

  refreshEnabledState();

  console.log("✅ Bulk download tab initialized.", {
    years: years.length,
    granularities: granularityOptions.length,
    buttons: buttons.length,
    hasEntries: Array.isArray(getManifestEntries(manifest)),
  });
}
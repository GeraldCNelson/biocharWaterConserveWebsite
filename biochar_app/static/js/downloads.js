// static/js/downloads.js

/**
 * Helper: build a filename-safe slug from pieces.
 */
function buildFilename(parts) {
  return parts
    .filter(Boolean)
    .join("_")
    .replace(/\s+/g, "-")
    .replace(/[^\w\-]+/g, "");
}

/**
 * Parse Content-Disposition header for filename.
 * Supports:
 *   - filename="..."
 *   - filename*=UTF-8''...
 */
function getFilenameFromContentDisposition(cd) {
  if (!cd) return "";

  // filename*=UTF-8''encoded
  const starMatch = cd.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
  if (starMatch && starMatch[1]) {
    try {
      return decodeURIComponent(starMatch[1].trim().replace(/^"(.*)"$/, "$1"));
    } catch {
      return starMatch[1].trim().replace(/^"(.*)"$/, "$1");
    }
  }

  // filename="plain"
  const match = cd.match(/filename\s*=\s*("?)([^";]+)\1/i);
  if (match && match[2]) return match[2].trim();

  return "";
}

/**
 * Map content-type to a reasonable extension.
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
 * Generic helper to POST JSON and download the returned blob.
 * - Uses server-provided filename if present (Content-Disposition)
 * - Otherwise uses fallbackFilename, but adjusts extension to match Content-Type if possible
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

/* -------------------------------------------------------------------------
 *  MAIN DATA DOWNLOADS (Raw / Ratio / All)
 * ---------------------------------------------------------------------- */

export async function downloadTraceData(kind = "all") {
  try {
    const yearEl     = document.getElementById("main-year");
    const variableEl = document.getElementById("main-variable");
    const stripEl    = document.getElementById("main-strip");
    const granEl     = document.getElementById("main-granularity");
    const depthEl    = document.getElementById("main-depth");

    if (!yearEl || !variableEl || !stripEl || !granEl || !depthEl) {
      alert("⚠️ Cannot download data: one or more controls are missing.");
      return;
    }

    const year        = parseInt(yearEl.value, 10);
    const variable    = variableEl.value;
    const strip       = stripEl.value;
    const granularity = granEl.value;
    const depth       = depthEl.value;
    const unitSystem  = window.unitSystem || "us";

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

/* -------------------------------------------------------------------------
 *  SUMMARY STATISTICS DOWNLOAD (Summary Statistics tab)
 * ---------------------------------------------------------------------- */

export async function downloadSummaryData(mode = "all") {
  const yearEl        = document.getElementById("summary-year");
  const variableEl    = document.getElementById("summary-variable");
  const stripEl       = document.getElementById("summary-strip");
  const granularityEl = document.getElementById("summary-granularity");
  const depthEl       = document.getElementById("summary-depth");

  if (!yearEl || !variableEl || !stripEl || !granularityEl || !depthEl) {
    console.error("❌ downloadSummaryData: summary controls not found in DOM");
    alert("Internal error: summary controls are missing.");
    return;
  }

  const year        = parseInt(yearEl.value, 10);
  const variable    = variableEl.value;
  const strip       = stripEl.value;
  const granularity = granularityEl.value;
  const depth       = depthEl.value;
  const unitSystem  = window.unitSystem || "us";

  if (Number.isNaN(year)) {
    alert("Please choose a valid year before downloading.");
    return;
  }

  const summaryStats = window.__lastSummaryData || null;

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

export function initSummaryDownloadMenu() {
  const rawBtn   = document.getElementById("download-summary-raw");
  const ratioBtn = document.getElementById("download-summary-ratio");
  const allBtn   = document.getElementById("download-summary-all");
  const zipBtn   = document.getElementById("download-summary-zip");

  if (!rawBtn && !ratioBtn && !allBtn && !zipBtn) {
    console.warn("⚠️ Summary download menu not found in DOM.");
    return;
  }

  if (rawBtn) rawBtn.addEventListener("click", (e) => { e.preventDefault(); void downloadSummaryData("raw"); });
  if (ratioBtn) ratioBtn.addEventListener("click", (e) => { e.preventDefault(); void downloadSummaryData("ratio"); });
  if (allBtn) allBtn.addEventListener("click", (e) => { e.preventDefault(); void downloadSummaryData("all"); });
  if (zipBtn) zipBtn.addEventListener("click", (e) => { e.preventDefault(); void downloadSummaryData("zip"); });

  console.log("✅ Summary download menu initialized.");
}

/* -------------------------------------------------------------------------
 *  PLOT IMAGE DOWNLOADS (JPEG/PNG of Plotly figures)
 * ---------------------------------------------------------------------- */

function buildPlotFilename({ plotType, format }) {
  const getVal = (id, fallback = "") => document.getElementById(id)?.value || fallback;

  const variable = getVal("main-variable");
  const strip = getVal("main-strip");
  const logger = getVal("main-loggerLocation");
  const depthIndex = getVal("main-depth");
  const granularity = getVal("main-granularity");
  const year = getVal("main-year");
  const unitSystem = window.unitSystem || "us";

  let depthPart = "";
  if (depthIndex && window.depthMapping?.[depthIndex]) {
    const depthLabel = window.depthMapping[depthIndex][unitSystem];
    depthPart = depthLabel
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

export async function downloadPlot(target = "raw", format = "png", sizeMode = "screen") {
  try {
    // noinspection JSUnresolvedVariable
    const Plotly = window.Plotly;
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

    let el = null;
    if (typeof target === "object" && target !== null) {
      el = target;
    } else {
      const t = String(target).toLowerCase().trim();
      const idCandidates = [];
      if (t === "raw") idCandidates.push("plot-1");
      else if (t === "ratio") idCandidates.push("plot-2");
      else idCandidates.push(target);

      for (const id of idCandidates) {
        const candidate = document.getElementById(id);
        if (candidate) { el = candidate; break; }
      }

      if (!el) {
        const plotDivs = Array.from(document.querySelectorAll(".js-plotly-plot"));
        if (plotDivs.length > 0) el = (t === "ratio") ? (plotDivs[1] || plotDivs[0]) : plotDivs[0];
      }
    }

    if (!el) {
      console.error("❌ downloadPlot: No plot found. Target =", target);
      alert("Unable to find the plot container to download.");
      return;
    }

    let width, height, scale;
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

/* -------------------------------------------------------------------------
 *  BULK DOWNLOAD TAB INITIALIZATION
 * ---------------------------------------------------------------------- */

const ALL_YEARS_MANIFEST_KEYS = new Set([
  "soil_chem_all",
  "soil_bio_all",
  "hay_all",
]);

const ALL_YEARS_UI_ALIASES = {
  soil_chem: "soil_chem_all",
  soil_chemistry: "soil_chem_all",
  soil_bio: "soil_bio_all",
  soil_biology: "soil_bio_all",
  biomass: "hay_all",
  biomass_hay: "hay_all",
  hay: "hay_all",
};

const BULK_DATASET_TOKEN = {
  loggers: "loggers",
  weather: "weather",
  irrigation: "irrigation",
  fertilizing: "fertilizing",
  biomass: "biomass",
  biomass_hay: "biomass",
};

function normalizeBulkDataset(uiDatasetRaw) {
  const ui = String(uiDatasetRaw || "").trim();
  return ALL_YEARS_UI_ALIASES[ui] || ui;
}

function isAllYearsDataset(normalizedDataset) {
  return ALL_YEARS_MANIFEST_KEYS.has(normalizedDataset);
}

function normalizeResolution(raw) {
  if (raw == null) return null;
  let g = String(raw).trim().toLowerCase();
  if (!g) return null;

  // Common UI variants → manifest/backend tokens
  const map = {
    "15-min": "15min",
    "15 min": "15min",
    "15m": "15min",
    "15 minutes": "15min",
    "hour": "hourly",
    "hours": "hourly",
    "day": "daily",
    "days": "daily",
    "month": "monthly",
    "months": "monthly",
    "g-season": "gseason",
    "g season": "gseason",
    "g_season": "gseason",
  };
  if (map[g]) g = map[g];

  return g;
}

function getManifestEntries(manifest) {
  if (!manifest) return null;
  if (Array.isArray(manifest)) return manifest;
  if (Array.isArray(manifest.entries)) return manifest.entries;
  return null;
}

function findEntryByKey(manifest, key) {
  const entries = getManifestEntries(manifest);
  if (!Array.isArray(entries)) return null;
  return entries.find((e) => e && typeof e === "object" && e.key === key) || null;
}

function findManifestKey(manifest, { dataset, year, granularity }) {
  if (!manifest) return null;

  // If dataset is already a manifest key, return it if present.
  if (isAllYearsDataset(dataset)) {
    return findEntryByKey(manifest, dataset)?.key || null;
  }

  const token = BULK_DATASET_TOKEN[dataset] || dataset;
  const y = year == null ? null : parseInt(year, 10);
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

export async function initBulkDownloadTab() {
  const yearEl =
    document.getElementById("bulk-year") ||
    document.querySelector('[data-role="bulk-year"]') ||
    null;

  const granEl =
    document.getElementById("bulk-granularity") ||
    document.querySelector('[data-role="bulk-granularity"]') ||
    null;

  const buttons = Array.from(document.querySelectorAll(".bulk-download-btn, [data-dataset]"));
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

  function setButtonEnabled(btn, enabled) {
    btn.disabled = !enabled;
    btn.classList.toggle("disabled", !enabled);
    btn.setAttribute("aria-disabled", String(!enabled));
  }

  // Fetch manifest
  let manifest = null;
  try {
    const resp = await fetch("/api/bulk_download_manifest");
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    manifest = await resp.json();
    window.__bulkDownloadManifest = manifest;
  } catch (e) {
    console.warn("⚠️ Bulk manifest fetch failed:", e);
    for (const b of buttons) setButtonEnabled(b, false);
    return;
  }

  const years = Array.isArray(manifest?.years) ? manifest.years : [];
  const granularities = Array.isArray(manifest?.granularities) ? manifest.granularities : [];

  if (yearEl && yearEl.options.length === 0) {
    const opts = (years.length ? years : [2023, 2024, 2025, 2026])
      .map((y) => `<option value="${y}">${y}</option>`)
      .join("");
    yearEl.innerHTML = `<option value="">Select year…</option>${opts}`;
  }

  if (granEl && granEl.options.length === 0) {
    const gList = granularities.length ? granularities : ["15min", "hourly", "daily", "monthly", "gseason"];
    granEl.innerHTML =
      `<option value="">Select granularity…</option>` +
      gList.map((g) => `<option value="${g}">${g}</option>`).join("");
  }

function normalizeGranularityToken(v) {
  const s = String(v || "").trim().toLowerCase();
  if (!s) return "";

  // Normalize common variants to manifest tokens
  // e.g. "15-min", "15 min", "15_min" -> "15min"
  const compact = s.replace(/\s+/g, "").replace(/_/g, "-");
  if (compact === "15-min" || compact === "15min") return "15min";

  // pass-through for known tokens
  if (["hourly", "daily", "monthly", "gseason"].includes(compact)) return compact;

  // last resort: strip hyphens
  return compact.replace(/-/g, "");
}

function normalizeDatasetToken(v) {
  return String(v || "").trim().toLowerCase();
}

function refreshEnabledState() {
  const y = selectedYear();
  const gRaw = selectedGranularity();
  const g = normalizeGranularityToken(gRaw);

  const entries = getManifestEntries(manifest);
  const hasEntries = Array.isArray(entries) && entries.length > 0;

  for (const btn of buttons) {
    const uiDsRaw = btn.dataset.dataset || btn.getAttribute("data-dataset") || "";
    const ds = normalizeBulkDataset(uiDsRaw);

    // If we have an entry-based manifest, be strict: only enable if we can resolve a manifest key.
    if (hasEntries) {
      // All-years (file) datasets: soil_chem_all, soil_bio_all, hay_all
      if (isAllYearsDataset(ds)) {
        const entry = findEntryByKey(manifest, ds);
        setButtonEnabled(btn, Boolean(entry?.key));
        continue;
      }

      // Logger / Weather require year + granularity
      if (ds === "loggers" || ds === "weather") {
        if (!y || !g) {
          setButtonEnabled(btn, false);
          continue;
        }

        // Make sure findManifestKey sees normalized dataset + granularity
        const key = findManifestKey(manifest, {
          dataset: normalizeDatasetToken(ds),
          year: y,
          granularity: g,
        });

        setButtonEnabled(btn, Boolean(key));
        continue;
      }

      // Year-only datasets (irrigation/fertilizing/biomass)
      if (!y) {
        setButtonEnabled(btn, false);
        continue;
      }

      const key = findManifestKey(manifest, {
        dataset: normalizeDatasetToken(ds),
        year: y,
        granularity: null,
      });

      setButtonEnabled(btn, Boolean(key));
      continue;
    }

    // Fallback mode (old manifest shape)
    if (isAllYearsDataset(ds)) {
      setButtonEnabled(btn, true);
      continue;
    }

    if (ds === "loggers" || ds === "weather") {
      if (!y || !g) {
        setButtonEnabled(btn, false);
        continue;
      }
      setButtonEnabled(btn, isParquetAvailable(ds, y, g));
      continue;
    }

    if (!y) {
      setButtonEnabled(btn, false);
      continue;
    }

    if (ds === "irrigation" || ds === "fertilizing" || ds === "biomass") {
      setButtonEnabled(btn, isWorkbookAvailable(ds, y));
      continue;
    }

    setButtonEnabled(btn, true);
  }
}

  if (yearEl) yearEl.addEventListener("change", refreshEnabledState);
  if (granEl) granEl.addEventListener("change", refreshEnabledState);

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
        return;
      }
      if (!allYears && !y) {
        alert("Please select a year.");
        return;
      }

      const key = findManifestKey(manifest, {
        dataset: ds,
        year: (ds === "loggers" || ds === "weather") ? y : (allYears ? null : y),
        granularity: (ds === "loggers" || ds === "weather") ? g : null,
      });

      if (!key) {
        console.error("❌ No manifest key found for selection:", { uiDataset, normalized: ds, year: y, granularity: g, manifest });
        alert("That dataset/year (and granularity, if applicable) is not available.");
        return;
      }

      const payload = { keys: [key] };
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
    granularities: granularities.length,
    buttons: buttons.length,
    hasEntries: Array.isArray(getManifestEntries(manifest)),
  });
}
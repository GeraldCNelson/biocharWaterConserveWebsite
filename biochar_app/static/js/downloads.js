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

/**
 * Bulk download behavior (important):
 * - Do NOT invent keys like "irrigation:2024" etc.
 * - Always send manifest keys verbatim (e.g. "irrigation_2024", "loggers_2024_monthly", ...)
 *
 * The backend expects req.keys: string[] and validates them against its own key format/dataset list.
 * Your manifest already contains the canonical keys; use those.
 */

/**
 * Normalize dataset tokens so UI data-dataset values map to manifest dataset names.
 * (Manifest shows e.g. "biomass" not "biomass_hay".)
 */
const BULK_DATASET_TOKEN = {
  loggers: "loggers",
  weather: "weather",
  irrigation: "irrigation",
  fertilizing: "fertilizing",
  biomass: "biomass",
  biomass_hay: "biomass",

  // keep these for later; they may not exist in the manifest yet
  soil_chem: "soil_chem",
  soil_bio: "soil_bio",
};

/**
 * Find the correct manifest key for a requested dataset selection.
 *
 * Manifest can be either:
 *  A) { years:[], granularities:[], availability:{...}, entries:[...] }
 *  B) a direct array of entries: [{key,dataset,year,resolution,label,...}, ...]
 *
 * We support both and return entry.key.
 */
function findManifestKey(manifest, { dataset, year, granularity }) {
  if (!manifest) return null;

  const token = BULK_DATASET_TOKEN[dataset] || dataset;
  const y = year == null ? null : parseInt(year, 10);
  const g = granularity ? String(granularity).trim() : null;

  const entries = Array.isArray(manifest)
    ? manifest
    : (Array.isArray(manifest.entries) ? manifest.entries : null);

  if (!Array.isArray(entries)) return null;

  return (
    entries.find((e) => {
      if (!e || typeof e !== "object") return false;
      if (e.dataset !== token) return false;
      if (y != null && parseInt(e.year, 10) !== y) return false;

      // Some manifests use `resolution`, some might use `granularity`
      // For parquet-ish datasets, we require a match when provided.
      const res = e.resolution ?? e.granularity ?? null;
      if (g && (res == null || String(res).trim() !== g)) return false;

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
    const g = String(granEl.value || "").trim();
    return g || null;
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

  // Support both possible shapes
  const years = Array.isArray(manifest?.years) ? manifest.years : [];
  const granularities = Array.isArray(manifest?.granularities) ? manifest.granularities : [];

  // Populate dropdowns
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

  // Availability helpers (work with the old manifest style if it exists)
  function isParquetAvailable(dataset, y, g) {
    const avail = manifest?.availability?.[dataset];
    if (!avail || !y || !g) return false;
    const arr = avail[String(y)];
    return Array.isArray(arr) ? arr.includes(g) : false;
  }

  function isWorkbookAvailable(dataset, y) {
    const avail = manifest?.availability?.[dataset];
    if (!avail || !y) return true; // if not provided, don’t block
    const flag = avail[String(y)];
    if (typeof flag === "boolean") return flag;
    return true;
  }

  function refreshEnabledState() {
    const y = selectedYear();
    const g = selectedGranularity();

    for (const btn of buttons) {
      const ds = btn.dataset.dataset || btn.getAttribute("data-dataset") || "";

      // Parquet datasets require year + granularity
      if (ds === "loggers" || ds === "weather") {
        if (!y || !g) {
          setButtonEnabled(btn, false);
          continue;
        }

        // Prefer manifest entries if present; fallback to availability map
        const key = findManifestKey(manifest, { dataset: ds, year: y, granularity: g });
        if (key) {
          setButtonEnabled(btn, true);
        } else {
          setButtonEnabled(btn, isParquetAvailable(ds, y, g));
        }
        continue;
      }

      // Year-only datasets (irrigation/fertilizing/biomass + others)
      if (!y) {
        setButtonEnabled(btn, false);
        continue;
      }

      if (ds === "irrigation" || ds === "fertilizing") {
        // If availability exists and says false, disable
        setButtonEnabled(btn, isWorkbookAvailable(ds, y));
        continue;
      }

      // For everything else, enable only if a manifest key exists when the manifest is entry-based.
      const key = findManifestKey(manifest, { dataset: ds, year: y, granularity: null });

      // If we have entry-style manifest (array or manifest.entries), respect it strictly.
      const entries = Array.isArray(manifest) ? manifest : (Array.isArray(manifest?.entries) ? manifest.entries : null);
      if (Array.isArray(entries)) {
        setButtonEnabled(btn, Boolean(key));
      } else {
        // If we don't have entries, don't block here.
        setButtonEnabled(btn, true);
      }
    }
  }

  if (yearEl) yearEl.addEventListener("change", refreshEnabledState);
  if (granEl) granEl.addEventListener("change", refreshEnabledState);

  // Wire clicks
  for (const btn of buttons) {
    btn.addEventListener("click", async (evt) => {
      evt.preventDefault();

      const uiDataset = btn.dataset.dataset || btn.getAttribute("data-dataset") || "";
      const y = selectedYear();
      const g = selectedGranularity();

      // Validate based on dataset type
      if ((uiDataset === "loggers" || uiDataset === "weather") && (!y || !g)) {
        alert("Please select both a year and a granularity.");
        return;
      }
      if (!y) {
        alert("Please select a year.");
        return;
      }

      // Find the canonical manifest key and send it verbatim
      const key = findManifestKey(manifest, {
        dataset: uiDataset,
        year: y,
        granularity: (uiDataset === "loggers" || uiDataset === "weather") ? g : null,
      });

      if (!key) {
        console.error("❌ No manifest key found for selection:", { uiDataset, year: y, granularity: g, manifest });
        alert("That dataset/year (and granularity, if applicable) is not available.");
        return;
      }

      const payload = { keys: [key] };
      console.log("🧾 Bulk download request:", payload);

      // Fallback filename if server doesn't provide one
      const suffix =
        (uiDataset === "loggers" || uiDataset === "weather")
          ? `${y}_${g}`
          : String(y);

      const fallbackZipName = buildFilename(["biochar", "bulk", uiDataset, suffix]) + ".zip";

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
    hasEntries: Array.isArray(manifest) || Array.isArray(manifest?.entries),
  });
}
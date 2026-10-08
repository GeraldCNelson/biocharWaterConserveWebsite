// @ts-check
// static/js/downloads.js
import { getSelectedFilters } from "./ui_controls.js?v=20261007-apply-seasons";

/**
 * @typedef {Window & {
 *   unitSystem?: string,
 *   depthMapping?: Record<string, Record<string, string>>,
 *   __lastSummaryData?: any,
 *   __seasonalComparisonDownload?: any,
 *   __bulkDownloadManifest?: any,
 *   Plotly?: any
 * }} DownloadsWindow
 */

/** @type {DownloadsWindow} */
const downloadsWindow = /** @type {DownloadsWindow} */ (window);

function downloadBrowserBlob(blob, filename) {
  const objUrl = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objUrl;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(objUrl);
}

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
    const loggerEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-loggerLocation"));
    const granEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-granularity"));
    const depthEl = /** @type {HTMLSelectElement | null} */ (document.getElementById("main-depth"));
    const traceOptionEl= /** @type {HTMLSelectElement | null} */(document.getElementById("main-traceOption"));

   if (!yearEl || !variableEl || !stripEl || !loggerEl || !granEl || !depthEl || !traceOptionEl) {
    alert("⚠️ Cannot download data: one or more controls are missing.");
    return;
  }

    const year = parseInt(yearEl.value, 10);
    const variable = variableEl.value;
    const strip = stripEl.value;
    const loggerLocation = loggerEl.value;
    const granularity = granEl.value;
    const depth = depthEl.value;
    const traceOption = traceOptionEl.value;
    const unitSystem = downloadsWindow.unitSystem || "us";
    const seasonalFilters = granularity === "gseason" ? getSelectedFilters("main") : null;
    if (granularity === "gseason" && !seasonalFilters) return;

    const payload = {
      year,
      variable,
      strip,
      loggerLocation,
      granularity,
      depth,
      traceOption,
      unitSystem,
      downloadType: kind,
      ...(granularity === "gseason" ? {
        periods: seasonalFilters?.periods,
        periodsAnchorYear: seasonalFilters?.periodsAnchorYear,
      } : {}),
    };

    console.log("⬇️ downloadTraceData payload:", payload);

    const fname =
      buildFilename([
        "data",
        kind,
        variable,
        strip,
        `logger${loggerLocation}`,
        `${depth}depth`,
        granularity,
        year,
      ]) + ".zip";

    await postAndDownload("/api/download_plot_data", payload, fname);
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

    let baseName = `summary_${granularity}_${variable}_${strip}_depth_code_${depth}_${year}`;
    if (!wantZip) baseName += `_${mode}`;

    const fallbackName = `${baseName}.${ext}`;
    await postAndDownload("/api/download_summary_data", payload, fallbackName);
  } catch (err) {
    console.error("❌ Error in downloadSummaryData:", err);
    alert("An error occurred while downloading summary data.");
  }
}

function seasonalComparisonFilename(suffix, extension) {
  const comparison = downloadsWindow.__seasonalComparisonDownload;
  const years = Array.isArray(comparison?.years)
    ? comparison.years.map(Number).filter(Number.isFinite)
    : [];
  const yearRange = years.length ? `${Math.min(...years)}-${Math.max(...years)}` : "years";
  return `${buildFilename([
    "seasonal-comparison",
    comparison?.periodLabel,
    comparison?.variable,
    comparison?.strip,
    `depth-code-${comparison?.depth || "unknown"}`,
    yearRange,
    suffix,
  ])}.${extension}`;
}

function csvCell(value) {
  if (value == null) return "";
  const text = String(value);
  return /[",\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

// Small, dependency-free ZIP writer. Entries are stored without compression.
function comparisonZip(files) {
  const encoder = new TextEncoder();
  const localParts = [];
  const directoryParts = [];
  let offset = 0;
  for (const [filename, content] of files) {
    const name = encoder.encode(filename);
    const data = encoder.encode(content);
    let crc = 0xffffffff;
    for (const byte of data) {
      crc ^= byte;
      for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
    }
    crc = (crc ^ 0xffffffff) >>> 0;
    const local = new Uint8Array(30 + name.length);
    const lv = new DataView(local.buffer);
    lv.setUint32(0, 0x04034b50, true);
    lv.setUint16(4, 20, true);
    lv.setUint16(6, 0x0800, true); // UTF-8 names.
    lv.setUint16(12, 33, true); // DOS date: 1980-01-01.
    lv.setUint32(14, crc, true);
    lv.setUint32(18, data.length, true);
    lv.setUint32(22, data.length, true);
    lv.setUint16(26, name.length, true);
    local.set(name, 30);
    localParts.push(local, data);
    const central = new Uint8Array(46 + name.length);
    const cv = new DataView(central.buffer);
    cv.setUint32(0, 0x02014b50, true);
    cv.setUint16(4, 20, true);
    cv.setUint16(6, 20, true);
    cv.setUint16(8, 0x0800, true);
    cv.setUint16(14, 33, true);
    cv.setUint32(16, crc, true);
    cv.setUint32(20, data.length, true);
    cv.setUint32(24, data.length, true);
    cv.setUint16(28, name.length, true);
    cv.setUint32(42, offset, true);
    central.set(name, 46);
    directoryParts.push(central);
    offset += local.length + data.length;
  }
  const directorySize = directoryParts.reduce((size, entry) => size + entry.length, 0);
  const end = new Uint8Array(22);
  const ev = new DataView(end.buffer);
  ev.setUint32(0, 0x06054b50, true);
  ev.setUint16(8, files.length, true);
  ev.setUint16(10, files.length, true);
  ev.setUint32(12, directorySize, true);
  ev.setUint32(16, offset, true);
  return new Blob([...localParts, ...directoryParts, end], { type: "application/zip" });
}

function seasonalComparisonReadme(comparison, csvName) {
  return `SEASONAL COMPARISON DATA
CSV file: ${csvName}
Exported: ${new Date().toISOString()}
Selected period: ${comparison.periodLabel}
Selected variable: ${comparison.variable}
Selected raw strip: ${comparison.strip}
Selected depth code: ${comparison.depth}
Website unit system: ${downloadsWindow.unitSystem || "us"}

One row represents one anchor year and logger position for the selected seasonal period and depth.
Depth codes: 1 = 6 inches; 2 = 12 inches; 3 = 18 inches.
Top, Middle and Bottom refer to positions along a strip, not sensor depths.
Blank numeric cells mean unavailable data, not zero.
Coverage uses expected 15-minute observations from the period start through the elapsed portion of the period (not future dates), rounded to one decimal place and capped at 100%.
Coverage is valid observations divided by expected observations, multiplied by 100. Missing observations can reflect recording gaps or values excluded by quality checks; the coverage percentage alone does not identify the cause.
The chart note "Some observations missing" means at least one displayed logger has less than 100% reported raw coverage, even if the shortfall is small. "Season unfinished" instead identifies a seasonal window whose end date has not yet passed.
Coverage percentages are reported separately for each year, logger position and treatment pair. Missing observations can affect seasonal means, especially when gaps cluster during unusually wet or dry periods.

COLUMN DEFINITIONS
seasonal_period: Name and date boundaries of the selected seasonal period.
variable: Website variable code (for example VWC).
year: Anchor year used to label the seasonal period. A winter period can begin in the preceding calendar year.
status: Complete, Partial, No data, or Not started. Partial can indicate missing valid raw observations or an unfinished season. No data means no raw mean is available. Coverage percentages provide the quantitative detail.
logger_position: Top, Middle or Bottom location along each strip.
raw_strip: Strip selected for the raw-value summary (S1, S2, S3 or S4).
raw_depth_code: Depth code used for the raw-value summary.
raw_mean: Arithmetic mean of available processed observations for the selected strip, depth, position and seasonal period. It is not an individual instantaneous reading.
raw_coverage_pct: Available raw observations as a percentage of expected observations under the website's seasonal coverage calculation.
ratio_depth_code: Depth code used for both paired-strip ratios.
s1_s2_ratio_mean: For VWC, seasonal mean S1 divided by seasonal mean S2, using only matching timestamps with finite readings in both strips at the same depth and logger position. S1 is biochar-treated; S2 is untreated. Other variables retain their mean of individual ratios.
s1_s2_coverage_pct: Available S1/S2 ratio observations as a percentage of expected observations under the website's seasonal coverage calculation.
s3_s4_ratio_mean: For VWC, seasonal mean S3 divided by seasonal mean S4, using only matching timestamps with finite readings in both strips at the same depth and logger position. S3 is biochar-treated; S4 is untreated. Other variables retain their mean of individual ratios.
s3_s4_coverage_pct: Available S3/S4 ratio observations as a percentage of expected observations under the website's seasonal coverage calculation.

UNITS AND INTERPRETATION
VWC raw means are percent soil volume occupied by water. EC is in dS/m. Temperature and water-volume raw means use the selected website units (US or metric).
Ratios are dimensionless: 1 means equal values, greater than 1 means the numerator strip has a higher value, and less than 1 means it has a lower value.
S1/S2 are the approximately monthly irrigation comparison; S3/S4 are the approximately fortnightly comparison. Frequency is not irrigation volume or application rate.
The raw-strip selection affects raw_mean, not which two paired-strip ratios are included.
For VWC, ratio coverage counts matched finite readings. A nonpositive denominator mean produces an unavailable ratio. The displayed raw_mean may use more observations than the paired means used in the ratio.
Partial periods and unequal coverage can affect year-to-year comparisons. These descriptive ratios alone do not establish statistical significance, a causal treatment effect, or movement of biochar to deeper soil.
`;
}

export function downloadSeasonalComparisonData() {
  const comparison = downloadsWindow.__seasonalComparisonDownload;
  if (!comparison?.rows?.length) {
    alert("Load a Seasonal Periods comparison before downloading its data.");
    return;
  }
  const columns = [
    ["seasonal_period", () => comparison.periodLabel],
    ["variable", () => comparison.variable],
    ["year", (row) => row.year],
    ["status", (row) => row.status],
    ["logger_position", (row) => row.position],
    ["raw_strip", () => comparison.strip],
    ["raw_depth_code", () => comparison.depth],
    ["raw_mean", (row) => row.rawMean],
    ["raw_coverage_pct", (row) => row.rawCoverage],
    ["ratio_depth_code", () => comparison.depth],
    ["s1_s2_ratio_mean", (row) => row.s1s2Mean],
    ["s1_s2_coverage_pct", (row) => row.s1s2Coverage],
    ["s3_s4_ratio_mean", (row) => row.s3s4Mean],
    ["s3_s4_coverage_pct", (row) => row.s3s4Coverage],
  ];
  const csv = [
    columns.map(([name]) => name).join(","),
    ...comparison.rows.map((row) => columns.map(([, getter]) => csvCell(getter(row))).join(",")),
  ].join("\n");
  const csvName = seasonalComparisonFilename("data", "csv");
  downloadBrowserBlob(comparisonZip([
    [csvName, `${csv}\n`],
    ["README.txt", seasonalComparisonReadme(comparison, csvName)],
  ]), seasonalComparisonFilename("data", "zip"));
}

function comparisonExportHeading({ heading, context, note }, width = 1600) {
  const escape = (text) => String(text).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  const wrap = (text, size) => {
    const limit = Math.max(20, Math.floor((width - 110) / (size * 0.58)));
    const lines = [];
    let line = "";
    for (const word of String(text || "").split(/\s+/).filter(Boolean)) {
      if (line && line.length + word.length + 1 > limit) { lines.push(line); line = ""; }
      let rest = word;
      while (rest.length > limit) {
        if (line) { lines.push(line); line = ""; }
        lines.push(rest.slice(0, limit)); rest = rest.slice(limit);
      }
      line += (line ? " " : "") + rest;
    }
    if (line) lines.push(line);
    return lines;
  };
  const blocks = [
    { lines: wrap(heading, 30), size: 30, leading: 38 },
    { lines: wrap(context, 22), size: 22, leading: 28 },
    { lines: wrap(note, 16), size: 16, leading: 21 },
  ];
  const top = 24 + blocks.reduce((height, block) => height + block.lines.length * block.leading, 0) + 55;
  let offset = 24;
  const annotations = [];
  for (const block of blocks) {
    for (const line of block.lines) {
      annotations.push({ xref: "paper", yref: "paper", x: 0, y: 1,
        xanchor: "left", yanchor: "top", yshift: top - offset,
        text: escape(line), showarrow: false, align: "left",
        font: { size: block.size }, borderpad: 0 });
      offset += block.leading;
    }
  }
  return { top, annotations };
}

export async function downloadSeasonalComparisonPlot(chartType) {
  const comparison = downloadsWindow.__seasonalComparisonDownload;
  const plotly = downloadsWindow.Plotly;
  const chart = chartType === "raw" ? comparison?.rawChart : comparison?.ratioChart;
  if (!comparison?.rows?.length || !plotly || !chart) {
    alert("Load a Seasonal Periods comparison before downloading its plot.");
    return;
  }

  // The interactive comparison is roughly dashboard-sized. Exporting that
  // same layout at 1600 x 900 makes its screen-sized type look too small in
  // the resulting 3200 x 1800 PNG. Render a hidden export-only copy with
  // publication-sized type so the on-screen chart remains unchanged.
  const exportChart = document.createElement("div");
  exportChart.style.position = "fixed";
  exportChart.style.left = "-10000px";
  exportChart.style.top = "0";
  exportChart.style.width = "1600px";
  exportChart.style.height = "900px";
  document.body.appendChild(exportChart);

  const exportLayout = JSON.parse(JSON.stringify(chart.layout || {}));
  exportLayout.annotations = (exportLayout.annotations || []).map((annotation) => {
    if (!String(annotation.text || "").startsWith("Dark outline")) return annotation;
    return { ...annotation, text: "Dark outline indicates ratio below 1",
      x: 0, xshift: 225, yshift: 7, y: 1.01, xref: "paper", yref: "paper",
      xanchor: "left", yanchor: "bottom", borderwidth: 0, borderpad: 0,
      bgcolor: "rgba(0,0,0,0)", font: { ...(annotation.font || {}), size: 16 } };
  });
  exportLayout.autosize = false;
  exportLayout.width = 1600;
  exportLayout.height = 900;
  exportLayout.margin = {
    ...(exportLayout.margin || {}),
    b: 145,
    t: 165,
  };
  exportLayout.font = { ...(exportLayout.font || {}), size: 22 };
  const exportTitle = typeof exportLayout.title === "string"
    ? { text: exportLayout.title }
    : (exportLayout.title || {});
  exportLayout.title = {
    ...exportTitle,
    font: { ...(exportTitle.font || {}), size: 30 },
  };
  // Do not enlarge the interactive title's <sup> block: exported notes need
  // independent type size and line spacing, with room reserved above the legend.
  const heading = comparison.headings?.[chartType];
  if (heading) {
    const headingLayout = comparisonExportHeading(heading);
    exportLayout.title = { text: "" };
    exportLayout.margin.t = headingLayout.top;
    exportLayout.annotations = [
      ...(exportLayout.annotations || []), ...headingLayout.annotations,
    ];
  }
  exportLayout.legend = {
    ...(exportLayout.legend || {}),
    font: { ...(exportLayout.legend?.font || {}), size: 20 },
    x: 0,
    xanchor: "left",
    xref: "paper",
    y: 1.01,
    yanchor: "bottom",
    yref: "paper",
  };
  ["xaxis", "yaxis"].forEach((axisName) => {
    const axis = exportLayout[axisName] || {};
    const axisTitle = typeof axis.title === "string"
      ? { text: axis.title }
      : (axis.title || {});
    exportLayout[axisName] = {
      ...axis,
      tickfont: { ...(axis.tickfont || {}), size: 18 },
      title: {
        ...axisTitle,
        font: { ...(axisTitle.font || {}), size: 22 },
        ...(axisName === "xaxis" ? { standoff: 32 } : {}),
      },
    };
  });

  try {
    await plotly.newPlot(
      exportChart,
      JSON.parse(JSON.stringify(chart.data || [])),
      exportLayout,
      { staticPlot: true, displayModeBar: false }
    );
    await plotly.downloadImage(exportChart, {
      format: "png",
      filename: seasonalComparisonFilename(chartType, "png").replace(/\.png$/, ""),
      width: 1600,
      height: 900,
      scale: 2,
    });
  } finally {
    plotly.purge(exportChart);
    exportChart.remove();
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
  const comparisonDataBtn = document.getElementById("download-seasonal-comparison-data");
  const comparisonRawPlotBtn = document.getElementById("download-seasonal-comparison-raw-plot");
  const comparisonRatioPlotBtn = document.getElementById("download-seasonal-comparison-ratio-plot");

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

  comparisonDataBtn?.addEventListener("click", (e) => {
    e.preventDefault();
    downloadSeasonalComparisonData();
  });
  comparisonRawPlotBtn?.addEventListener("click", (e) => {
    e.preventDefault();
    void downloadSeasonalComparisonPlot("raw");
  });
  comparisonRatioPlotBtn?.addEventListener("click", (e) => {
    e.preventDefault();
    void downloadSeasonalComparisonPlot("ratio");
  });

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
async function runBulkDownloadWithFeedback(btn, download, refreshState) {
  if (btn.dataset.bulkDownloading === "true") return false;
  const label = btn.textContent;
  const statusId = `${btn.id}-status`;
  let status = document.getElementById(statusId);
  if (!status) {
    status = document.createElement("p");
    status.id = statusId;
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");
    status.setAttribute("aria-atomic", "true");
    btn.insertAdjacentElement("afterend", status);
    btn.setAttribute("aria-describedby", statusId);
  }
  btn.dataset.bulkDownloading = "true";
  btn.disabled = true;
  btn.setAttribute("aria-disabled", "true");
  btn.setAttribute("aria-busy", "true");
  btn.textContent = "Preparing download…";
  status.className = "small mt-1 mb-0 text-muted";
  status.textContent = "Preparing ZIP download. Large datasets may take a minute or longer.";
  try {
    await download();
    status.className = "small mt-1 mb-0 text-success";
    status.textContent = "Download started. Check your browser’s downloads.";
    return true;
  } catch (err) {
    console.error("❌ Bulk download failed:", err);
    status.className = "small mt-1 mb-0 text-danger";
    status.textContent = "Download failed. Please try again. If it keeps failing, contact us.";
    return false;
  } finally {
    delete btn.dataset.bulkDownloading;
    btn.textContent = label;
    btn.setAttribute("aria-busy", "false");
    refreshState();
  }
}

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
    const busy = btn.dataset.bulkDownloading === "true";
    btn.disabled = hardDisable || busy;
    btn.classList.toggle("disabled", !visualEnabled);
    btn.setAttribute("aria-disabled", String(hardDisable || busy || !visualEnabled));
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
      if (btn.dataset.bulkDownloading === "true") return;

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

      await runBulkDownloadWithFeedback(btn,
        () => postAndDownload("/api/bulk_download", payload, fallbackZipName),
        refreshEnabledState);
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

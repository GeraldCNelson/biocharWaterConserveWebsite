// @ts-check
// static/js/tab_summary.js
//
// Summary Statistics tab controller:
// - Fetch /api/get_summary_stats
// - Render raw + ratio tables using generateSummaryTable()
// - Store window.latestSummaryStats for downloads, etc.

import { fetchJson, generateSummaryTable, formatGseasonLabel } from "./api_requests.js";
import { getDropdownValue } from "./ui_utils.js";
import { getCustomSeasonPeriods } from "./ui_controls.js?v=20260919-custom-seasons-fix-3";
import { showLoadingOverlay, hideLoadingOverlay, startLoadingDots, stopLoadingDots } from "./ui_loading.js";

/**
 * @typedef {Window & {
 *   labelNameMapping?: Record<string, any>,
 *   depthMapping?: Record<string, Record<string, string>>,
 *   gseasonPeriods?: Record<string, any>,
 *   latestSummaryStats?: any,
 *   __lastSummaryData?: any,
 *   __seasonalComparisonDownload?: any,
 *   multiYearSummaryCache?: Map<string, any>,
 *   multiYearSummaryRequests?: Map<string, Promise<any>>
 * }} SummaryWindow
 */

/** @type {SummaryWindow} */
const summaryWindow = /** @type {SummaryWindow} */ (window);
summaryWindow.multiYearSummaryCache ||= new Map();
summaryWindow.multiYearSummaryRequests ||= new Map();

/**
 * @param {string} str
 * @returns {string}
 */
function capitalizeFirst(str) {
  return str ? str.charAt(0).toUpperCase() + str.slice(1) : "";
}
void capitalizeFirst;

/**
 * @param {string} variableKey
 * @returns {boolean}
 */
function isTemperatureVariable(variableKey) {
  return String(variableKey || "").trim().toUpperCase() === "T";
}

/**
 * @param {any} labelEntry
 * @param {"us" | "metric"} unitSystem
 * @param {string} fallback
 * @returns {string}
 */
function resolveUnitLabelStrict(labelEntry, unitSystem, fallback) {
  if (!labelEntry) {
    throw new Error(
      `Missing label entry for unitSystem=${unitSystem}. fallback=${String(fallback)}`
    );
  }

  if (typeof labelEntry === "string") return labelEntry;

  if (typeof labelEntry === "object") {
    const hasUS = Object.prototype.hasOwnProperty.call(labelEntry, "us");
    const hasMetric = Object.prototype.hasOwnProperty.call(labelEntry, "metric");

    if (!hasUS || !hasMetric) {
      throw new Error(
        `Invalid label entry: expected keys {us, metric}. Got keys: ${Object.keys(labelEntry).join(", ")}`
      );
    }

    if (!Object.prototype.hasOwnProperty.call(labelEntry, unitSystem)) {
      throw new Error(
        `Label entry missing unitSystem=${unitSystem}. Keys: ${Object.keys(labelEntry).join(", ")}`
      );
    }

    const value = labelEntry[unitSystem];
    if (typeof value !== "string" || !value.trim()) {
      throw new Error(
        `Label for unitSystem=${unitSystem} must be a non-empty string. Got: ${String(value)}`
      );
    }

    return value;
  }

  throw new Error(`Invalid label entry type: ${typeof labelEntry}`);
}

/**
 * @returns {"us" | "metric"}
 */
function getUnitSystemForSummary() {
  const toggle = /** @type {HTMLInputElement | null} */ (
    document.getElementById("units-toggle_summary")
  );
  return toggle && toggle.checked ? "metric" : "us";
}

/**
 * @param {any} v
 * @returns {boolean}
 */
function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Convert depth dropdown value to a display label:
 * - Assumes dropdown value is inches as a string like "6", "12", "18"
 * - Uses the dropdown selected text if parsing fails
 *
 * @param {"us" | "metric"} unitSystem
 * @returns {string}
 */
function getDepthDisplayLabel(unitSystem) {
  const depthEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("summary-depth")
  );
  if (!depthEl) return "";

  const fallbackText = depthEl.selectedOptions?.[0]?.textContent?.trim() || "";
  return fallbackText;
}

/**
 * @param {{
 *   year: number,
 *   variable: string,
 *   strip: string | null,
 *   granularity: string,
 *   unitSystem: "us" | "metric"
 * }} args
 * @returns {string}
 */
function buildSummaryTitle({ year, variable, strip, granularity, unitSystem }) {
  const labelMap = summaryWindow.labelNameMapping || {};
  let prettyVar = variable;

  try {
    prettyVar = resolveUnitLabelStrict(labelMap[variable], unitSystem, variable);
  } catch (e) {
    console.error("❌ buildSummaryTitle label resolution failed:", e);
    throw e;
  }

  const depthLabel = getDepthDisplayLabel(unitSystem);
  const stripPart = strip ? `, Strip ${strip}` : "";

  const granLabel = granularity === "gseason"
    ? "Seasonal Periods"
    : capitalizeFirst(granularity);
  return `${granLabel} Summary for ${prettyVar}${stripPart}, ${depthLabel}, ${year}`;
}

/**
 * @param {any} stats
 * @param {string} variable
 * @param {"us" | "metric"} unitSystem
 * @returns {any}
 */
function prettifyStatsKeys(stats, variable, unitSystem) {
  if (!isPlainObject(stats)) return stats;

  const keys = Object.keys(stats);
  if (!keys.length) return stats;

  const firstVal = stats[keys[0]];
  if (!isPlainObject(firstVal)) return stats;

  const labelMap = summaryWindow.labelNameMapping || {};
  let prettyVar = variable;

  try {
    prettyVar = resolveUnitLabelStrict(labelMap[variable], unitSystem, variable);
  } catch (e) {
    console.error("❌ prettifyStatsKeys label resolution failed:", e);
    throw e;
  }

  /** @type {Record<string, string>} */
  const loggerMap = { T: "Top", M: "Mid", B: "Bottom" };

  /** @type {Record<string, any>} */
  const out = {};
  keys.forEach((k) => {
    const val = stats[k];

    const suffixMatch = String(k).match(/_(T|M|B)$/);
    const loggerSuffix = suffixMatch ? suffixMatch[1] : null;
    const loggerLabel = loggerSuffix ? (loggerMap[loggerSuffix] || loggerSuffix) : null;

    const stripMatch = String(k).match(/_S([1-4])_/);
    const stripLabel = stripMatch ? `Strip ${stripMatch[1]}` : null;

    const ratioGroup =
      String(k).includes("S1_S2") ? "S1/S2" :
      (String(k).includes("S3_S4") ? "S3/S4" : null);

    let displayKey = prettyVar;

    if (ratioGroup && loggerLabel) {
      displayKey = `${prettyVar} (${ratioGroup}, ${loggerLabel})`;
    } else if (ratioGroup) {
      displayKey = `${prettyVar} (${ratioGroup})`;
    } else if (stripLabel && loggerLabel) {
      displayKey = `${prettyVar} (${stripLabel}, ${loggerLabel})`;
    } else if (stripLabel) {
      displayKey = `${prettyVar} (${stripLabel})`;
    } else if (loggerLabel) {
      displayKey = `${prettyVar} (${loggerLabel})`;
    }

    if (out[displayKey]) out[`${displayKey} • ${k}`] = val;
    else out[displayKey] = val;
  });

  return out;
}

/**
 * Return a user-facing notice when a configured seasonal period is not complete.
 * Seasonal periods that wrap across New Year are anchored to the year in which
 * they end, matching the Custom Seasons editor.
 *
 * @param {{start?: string, end?: string}} spec
 * @param {number} anchorYear
 * @param {Date} [today=new Date()]
 * @returns {string}
 */
export function getIncompletePeriodNotice(spec, anchorYear, today = new Date()) {
  const startText = String(spec?.start || "");
  const endText = String(spec?.end || "");
  const startMatch = startText.match(/^(?:(\d{4})-)?(\d{2})-(\d{2})$/);
  const endMatch = endText.match(/^(?:(\d{4})-)?(\d{2})-(\d{2})$/);
  if (!startMatch || !endMatch || !Number.isFinite(anchorYear)) return "";

  const startMonth = Number(startMatch[2]);
  const startDay = Number(startMatch[3]);
  const endMonth = Number(endMatch[2]);
  const endDay = Number(endMatch[3]);
  const wrapsYear = startMonth > endMonth;
  const startYear = startMatch[1] ? Number(startMatch[1]) : (wrapsYear ? anchorYear - 1 : anchorYear);
  const endYear = endMatch[1] ? Number(endMatch[1]) : anchorYear;
  const start = new Date(startYear, startMonth - 1, startDay);
  const end = new Date(endYear, endMonth - 1, endDay);
  const current = new Date(today.getFullYear(), today.getMonth(), today.getDate());

  if (
    start.getFullYear() !== startYear ||
    start.getMonth() !== startMonth - 1 ||
    start.getDate() !== startDay ||
    end.getFullYear() !== endYear ||
    end.getMonth() !== endMonth - 1 ||
    end.getDate() !== endDay ||
    current > end
  ) {
    return "";
  }

  const endLabel = end.toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });

  if (current < start) {
    return `This seasonal period has not started. It is scheduled to end ${endLabel}; no result shown here is a complete-period summary.`;
  }

  return `This seasonal period is still in progress and ends ${endLabel}. Results shown here are partial-period statistics through the latest available data.`;
}

/**
 * @param {any} gseasonStats
 * @param {string} variable
 * @param {"us" | "metric"} unitSystem
 * @param {number} anchorYear
 * @returns {string}
 */
function buildGseasonSummaryTableHTML(gseasonStats, variable, unitSystem, anchorYear, periodsRaw) {
  const periods = periodsRaw || summaryWindow.gseasonPeriods || {};

  const seasonEntries = Array.isArray(periods)
    ? periods.map((period) => [period.code, period])
    : Object.entries(periods);
  if (!seasonEntries.length) {
    return `<p class="text-muted">No seasonal periods are defined.</p>`;
  }

  /**
   * @param {any} stats
   * @returns {Record<string, any>}
   */
  function normalizeFlatGseasonStats(stats) {
    /** @type {Record<string, any>} */
    const grouped = {};

    if (!Array.isArray(stats)) {
      return grouped;
    }

    stats.forEach((row) => {
      if (!row || typeof row !== "object") return;

      const periodCode = row.period_code;
      if (!periodCode) return;

      if (!grouped[periodCode]) {
        grouped[periodCode] = {
          raw_statistics: {},
          ratio_statistics: {},
        };
      }

      const strip = String(row.strip || "").trim();
      const depth = String(row.depth || "").trim();
      const loggerLocation = String(row.logger_location || "").trim();

      const rawKeyParts = [strip];
      if (depth) rawKeyParts.push(`D${depth}`);
      if (loggerLocation) rawKeyParts.push(loggerLocation);
      const rawKey = rawKeyParts.join("_");

      const hasRawStats =
        row.raw_min != null ||
        row.raw_mean != null ||
        row.raw_max != null ||
        row.raw_std != null;

      if (hasRawStats && rawKey) {
        grouped[periodCode].raw_statistics[rawKey] = {
          min: row.raw_min,
          mean: row.raw_mean,
          max: row.raw_max,
          std: row.raw_std,
          n: row.raw_n,
          expected_n: row.raw_expected_n,
          coverage_pct: row.raw_coverage_pct,
        };
      }

      const ratioStrip = String(row.ratio_group || row.ratio_strip || row.strip_ratio || row.strip || "").trim();
      const ratioKeyParts = [ratioStrip];
      if (depth) ratioKeyParts.push(`D${depth}`);
      if (loggerLocation) ratioKeyParts.push(loggerLocation);
      const ratioKey = ratioKeyParts.join("_");

      const hasRatioStats =
        row.ratio_min != null ||
        row.ratio_mean != null ||
        row.ratio_max != null ||
        row.ratio_std != null;

      const looksLikeRatioGroup =
        ratioStrip.includes("S1/S2") ||
        ratioStrip.includes("S3/S4") ||
        ratioStrip.includes("S1_S2") ||
        ratioStrip.includes("S3_S4");

      if (hasRatioStats && ratioKey && looksLikeRatioGroup) {
        grouped[periodCode].ratio_statistics[ratioKey] = {
          min: row.ratio_min,
          mean: row.ratio_mean,
          max: row.ratio_max,
          std: row.ratio_std,
          n: row.ratio_n,
          expected_n: row.ratio_expected_n,
          coverage_pct: row.ratio_coverage_pct,
        };
      }
    });

    return grouped;
  }

  const groupedStats = Array.isArray(gseasonStats)
    ? normalizeFlatGseasonStats(gseasonStats)
    : (gseasonStats && typeof gseasonStats === "object" ? gseasonStats : {});

  const escapeHTML = (value) => String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");

  const formatNumber = (value) => {
    if (value == null || value === "" || !Number.isFinite(Number(value))) return "—";
    return String(Math.round(Number(value) * 10000) / 10000);
  };

  const positionForKey = (key) => {
    const suffix = String(key).split("_").pop()?.toUpperCase();
    return suffix === "T" ? "Top" : suffix === "M" ? "Middle" : suffix === "B" ? "Bottom" : key;
  };

  const positionOrder = { Top: 0, Middle: 1, Bottom: 2 };

  const renderRows = (stats) => Object.entries(stats)
    .map(([key, metrics]) => ({ key, position: positionForKey(key), metrics }))
    .sort((a, b) => (positionOrder[a.position] ?? 99) - (positionOrder[b.position] ?? 99))
    .map(({ position, metrics }) => {
      const count = metrics?.n == null ? "—" : Math.round(Number(metrics.n)).toLocaleString();
      const expected = metrics?.expected_n == null ? null : Math.round(Number(metrics.expected_n));
      const coverage = metrics?.coverage_pct == null ? "—" : `${formatNumber(metrics.coverage_pct)}%`;
      const coverageTitle = expected == null
        ? ""
        : ` title="${escapeHTML(`${count} valid observations out of ${expected.toLocaleString()} expected`)}"`;
      return `
        <tr>
          <th scope="row">${escapeHTML(position)}</th>
          <td>${formatNumber(metrics?.min)}</td>
          <td>${formatNumber(metrics?.mean)}</td>
          <td>${formatNumber(metrics?.max)}</td>
          <td>${formatNumber(metrics?.std)}</td>
          <td>${count}</td>
          <td${coverageTitle}>${coverage}</td>
        </tr>`;
    }).join("");

  let html = `
    <div class="table-responsive">
      <table class="table table-sm table-bordered align-middle mb-0">
        <caption class="caption-top text-muted pt-0">
          Coverage is the percentage of expected 15-minute observations with valid data, through the elapsed part of each period.
          Raw statistics summarize all valid 15-minute observations for the selected strip, depth, and logger position.
        </caption>
        <thead>
          <tr>
            <th scope="col">Position</th>
            <th scope="col">Min</th>
            <th scope="col">Mean</th>
            <th scope="col">Max</th>
            <th scope="col">SD</th>
            <th scope="col">Valid n</th>
            <th scope="col">Coverage</th>
          </tr>
        </thead>
        <tbody>`;

  seasonEntries.forEach(([code, spec]) => {
    const block = groupedStats[code] || {};
    const rawStats = block.raw_statistics || {};
    const ratioStats = block.ratio_statistics || {};

    /** @type {Record<string, any>} */
    const s1s2 = {};
    /** @type {Record<string, any>} */
    const s3s4 = {};

    Object.entries(ratioStats).forEach(([k, v]) => {
      const key = String(k);
      if (key.includes("S1/S2") || key.includes("S1_S2")) {
        s1s2[k] = v;
      } else if (key.includes("S3/S4") || key.includes("S3_S4")) {
        s3s4[k] = v;
      }
    });

    const title = (typeof formatGseasonLabel === "function")
      ? formatGseasonLabel(code, spec, "")
      : (spec?.label || code);
    const incompleteNotice = getIncompletePeriodNotice(spec, anchorYear);

    html += `
      <tr class="table-primary">
        <th colspan="7" class="py-2">${escapeHTML(title)}</th>
      </tr>`;

    if (incompleteNotice) {
      html += `
        <tr class="table-warning">
          <td colspan="7"><strong>Incomplete seasonal period:</strong> ${escapeHTML(incompleteNotice)}</td>
        </tr>`;
    }

    const sections = [
      ["Raw summary", rawStats, "No raw data available for this period."],
      ["S1/S2 ratio summary", s1s2, "No S1/S2 ratio summary available."],
      ["S3/S4 ratio summary", s3s4, "No S3/S4 ratio summary available."],
    ];

    sections.forEach(([label, stats, emptyMessage]) => {
      html += `<tr class="table-light"><th colspan="7">${label}</th></tr>`;
      html += Object.keys(stats).length
        ? renderRows(stats)
        : `<tr><td colspan="7" class="text-muted">${emptyMessage}</td></tr>`;
    });
  });

  html += "</tbody></table></div>";
  return html;
}

function escapeSummaryHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function comparisonRowsForPeriod(yearEntries, periodCode) {
  const positions = ["T", "M", "B"];
  const positionLabels = { T: "Top", M: "Middle", B: "Bottom" };
  const rows = [];

  yearEntries.forEach((entry) => {
    const year = Number(entry?.year);
    const period = (entry?.periods || []).find((item) => item.code === periodCode);
    if (!period || !Number.isFinite(year)) return;

    const notice = getIncompletePeriodNotice(period, year);
    const status = notice.includes("not started") ? "Not started" : (notice ? "Partial" : "Complete");
    const periodRows = (entry?.gseason_stats || []).filter((row) => row?.period_code === periodCode);
    positions.forEach((position) => {
      const raw = periodRows.find((row) => row?.logger_location === position && row?.raw_mean != null);
      const s1s2 = periodRows.find((row) => row?.logger_location === position && row?.ratio_group === "S1/S2");
      const s3s4 = periodRows.find((row) => row?.logger_location === position && row?.ratio_group === "S3/S4");
      rows.push({
        year,
        status,
        position: positionLabels[position],
        rawMean: raw?.raw_mean ?? null,
        rawCoverage: raw?.raw_coverage_pct ?? null,
        s1s2Mean: s1s2?.ratio_mean ?? null,
        s1s2Coverage: s1s2?.ratio_coverage_pct ?? null,
        s3s4Mean: s3s4?.ratio_mean ?? null,
        s3s4Coverage: s3s4?.ratio_coverage_pct ?? null,
      });
    });
  });

  return rows;
}

function setComparisonDownloadsAvailable(available, visible = true) {
  document.querySelectorAll(".seasonal-comparison-download-item").forEach((item) => {
    item.classList.toggle("d-none", !visible);
    item.querySelectorAll("button").forEach((button) => {
      button.disabled = !available;
    });
  });
}

async function renderMultiYearComparison(section, yearEntries, periods, variable, unitSystem, selectedCode, metadata = {}) {
  const selectedPeriod = periods.find((period) => period.code === selectedCode) || periods[0];
  if (!selectedPeriod) return;

  const rows = comparisonRowsForPeriod(yearEntries, selectedPeriod.code);
  const numberText = (value) => value == null || !Number.isFinite(Number(value))
    ? "—"
    : String(Math.round(Number(value) * 10000) / 10000);
  const coverageText = (value) => value == null || !Number.isFinite(Number(value))
    ? "—"
    : `${numberText(value)}%`;
  const statusClass = { Complete: "text-bg-success", Partial: "text-bg-warning", "Not started": "text-bg-secondary" };

  const tableRows = rows.map((row, index) => `
    <tr>
      ${index % 3 === 0 ? `<th scope="row" rowspan="3" class="align-middle">${row.year}</th>` : ""}
      ${index % 3 === 0 ? `<td rowspan="3" class="align-middle"><span class="badge ${statusClass[row.status] || "text-bg-secondary"}">${row.status}</span></td>` : ""}
      <th scope="row">${row.position}</th>
      <td>${numberText(row.rawMean)}</td>
      <td>${coverageText(row.rawCoverage)}</td>
      <td>${numberText(row.s1s2Mean)}</td>
      <td>${coverageText(row.s1s2Coverage)}</td>
      <td>${numberText(row.s3s4Mean)}</td>
      <td>${coverageText(row.s3s4Coverage)}</td>
    </tr>`).join("");

  section.querySelector(".multi-year-content").innerHTML = `
    <div class="table-responsive mb-4">
      <table class="table table-sm table-bordered align-middle">
        <thead>
          <tr>
            <th rowspan="2">Year</th><th rowspan="2">Status</th><th rowspan="2">Position</th>
            <th colspan="2">Raw summary</th><th colspan="2">S1/S2 ratio</th><th colspan="2">S3/S4 ratio</th>
          </tr>
          <tr>
            <th>Mean</th><th>Coverage</th><th>Mean</th><th>Coverage</th><th>Mean</th><th>Coverage</th>
          </tr>
        </thead>
        <tbody>${tableRows || `<tr><td colspan="9" class="text-muted">No comparison data are available.</td></tr>`}</tbody>
      </table>
    </div>
    <div id="multi-year-raw-chart" class="multi-year-chart"></div>
    <div id="multi-year-ratio-chart" class="multi-year-chart"></div>`;

  const plotly = window.Plotly;
  if (!rows.length) {
    summaryWindow.__seasonalComparisonDownload = null;
    setComparisonDownloadsAvailable(false, true);
    return;
  }

  const years = [...new Set(rows.map((row) => row.year))];
  summaryWindow.__seasonalComparisonDownload = {
    rows,
    periodCode: selectedPeriod.code,
    periodLabel: selectedPeriod.label,
    variable,
    strip: metadata.strip,
    depth: metadata.depth,
    years,
  };
  setComparisonDownloadsAvailable(false, true);
  if (!plotly) {
    setComparisonDownloadsAvailable(true);
    return;
  }

  const yearLabel = (year) => {
    const yearRows = rows.filter((row) => row.year === year);
    return yearRows.some((row) => row.status === "Partial") ? `${year}*` : String(year);
  };
  const positions = ["Top", "Middle", "Bottom"];
  const colors = { Top: "#3f8fc1", Middle: "#efb23f", Bottom: "#36aa8a" };
  const rawTraces = positions.map((position) => ({
    type: "bar",
    name: position,
    x: years.map(yearLabel),
    y: years.map((year) => rows.find((row) => row.year === year && row.position === position)?.rawMean ?? null),
    marker: { color: colors[position] },
    hovertemplate: "%{x}<br>%{fullData.name}: %{y:.4g}<extra></extra>",
  }));

  let prettyVariable = variable;
  try {
    prettyVariable = resolveUnitLabelStrict(summaryWindow.labelNameMapping?.[variable], unitSystem, variable);
  } catch (_) {
    prettyVariable = variable;
  }
  const depthLabel = summaryWindow.depthMapping?.[String(metadata.depth)]?.[unitSystem]
    || `depth code ${metadata.depth}`;
  const yearRange = years.length ? `${Math.min(...years)}–${Math.max(...years)}` : "available years";
  const rawContext = `Strip ${String(metadata.strip || "").replace(/^S/i, "")}, ${depthLabel}, anchor years ${yearRange}`;
  const ratioContext = `Strip ratios S1/S2 and S3/S4, ${depthLabel}, anchor years ${yearRange}`;
  const commonLayout = {
    autosize: true,
    height: 370,
    margin: { l: 70, r: 25, t: 80, b: 60 },
    paper_bgcolor: "white",
    plot_bgcolor: "white",
    barmode: "group",
    legend: { orientation: "h", y: 1.12 },
    xaxis: { title: "Anchor year" },
  };
  const hasPartialPeriod = rows.some((row) => row.status === "Partial");
  const partialAnnotations = hasPartialPeriod
    ? [{
        text: "* partial period (season is still in progress)",
        xref: "paper",
        yref: "paper",
        x: 1,
        y: -0.16,
        xanchor: "right",
        showarrow: false,
      }]
    : [];
  const rawRender = plotly.react("multi-year-raw-chart", rawTraces, {
    ...commonLayout,
    title: {
      text: `${selectedPeriod.label}: mean ${prettyVariable} by year<br><sup>${rawContext}</sup>`,
      font: { size: 18 },
    },
    yaxis: { title: `Mean ${prettyVariable}`, rangemode: "tozero" },
    xaxis: {
      title: "Anchor year",
      categoryorder: "array",
      categoryarray: years.map(yearLabel),
    },
    annotations: partialAnnotations,
  }, { responsive: true, displaylogo: false });

  const ratioX = years.flatMap((year) => positions.map((position) => `${yearLabel(year)} · ${position}`));
  const ratioTrace = (group, field, color) => ({
    type: "bar",
    name: group,
    x: ratioX,
    y: years.flatMap((year) => positions.map((position) => rows.find(
      (row) => row.year === year && row.position === position
    )?.[field] ?? null)),
    marker: { color },
    hovertemplate: "%{x}<br>%{fullData.name}: %{y:.4g}<extra></extra>",
  });
  const ratioRender = plotly.react("multi-year-ratio-chart", [
    ratioTrace("S1/S2", "s1s2Mean", "#3f8fc1"),
    ratioTrace("S3/S4", "s3s4Mean", "#df7f3f"),
  ], {
    ...commonLayout,
    title: {
      text: `${selectedPeriod.label}: treatment ratios by year<br><sup>${ratioContext}</sup>`,
      font: { size: 18 },
    },
    yaxis: { title: `${variable} ratio`, rangemode: "tozero" },
    xaxis: {
      title: "Anchor year and logger position",
      tickangle: -25,
      categoryorder: "array",
      categoryarray: ratioX,
    },
    annotations: partialAnnotations,
  }, { responsive: true, displaylogo: false });

  await Promise.all([Promise.resolve(rawRender), Promise.resolve(ratioRender)]);
  setComparisonDownloadsAvailable(true);

}

function appendMultiYearComparison(container, yearEntries, periods, variable, unitSystem, metadata = {}) {
  if (!Array.isArray(yearEntries) || !yearEntries.length || !Array.isArray(periods) || !periods.length) return;

  const defaultPeriod = periods.find((period) => /growing/i.test(period.label || "")) || periods[0];
  const section = document.createElement("section");
  section.className = "multi-year-summary mt-4";
  section.innerHTML = `
    <div class="d-flex flex-wrap align-items-end justify-content-between gap-2 mb-2">
      <div>
        <h5 class="mb-1">Comparison across all years</h5>
        <p class="text-muted mb-0">Means use the same seasonal definitions and filters as the detailed summary above.</p>
      </div>
      <div>
        <label for="multi-year-period" class="form-label mb-1">Seasonal period</label>
        <select id="multi-year-period" class="form-select form-select-sm">
          ${periods.map((period) => `<option value="${escapeSummaryHTML(period.code)}"${period.code === defaultPeriod.code ? " selected" : ""}>${escapeSummaryHTML(period.label)}</option>`).join("")}
        </select>
      </div>
    </div>
    <div class="multi-year-content"></div>`;
  container.appendChild(section);

  const select = section.querySelector("#multi-year-period");
  const render = () => {
    void renderMultiYearComparison(
      section, yearEntries, periods, variable, unitSystem, select?.value || defaultPeriod.code, metadata
    ).catch((error) => {
      console.error("Failed to render seasonal comparison plots:", error);
      setComparisonDownloadsAvailable(false, true);
    });
  };
  select?.addEventListener("change", render);
  render();
}

/**
 * @param {string} [text=""]
 * @returns {void}
 */
function showSummaryStatus(text = "") {
  const el = document.getElementById("summary-status");
  if (!el) return;
  el.style.display = "block";
  if (typeof text === "string") el.textContent = text;
}

/**
 * @returns {void}
 */
function hideSummaryStatus() {
  const el = document.getElementById("summary-status");
  if (!el) return;
  el.textContent = "";
  el.style.display = "none";
}

function getCustomPeriodsAnchorYear(fallbackYear) {
  const value = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("anchor-year")
  )?.value;
  const year = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(year) ? year : fallbackYear;
}

function multiYearComparisonCacheKey({ periods, periodsAnchorYear, variable, strip, depth, unitSystem }) {
  const relativePeriods = periods.map((period) => {
    const relativeDate = (value) => {
      const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!match) return String(value || "");
      return `${Number(match[1]) - periodsAnchorYear}:${match[2]}-${match[3]}`;
    };
    return {
      code: period.code,
      label: period.label,
      start: relativeDate(period.start),
      end: relativeDate(period.end),
    };
  });
  return JSON.stringify({ variable, strip, depth, unitSystem, periods: relativePeriods });
}

/**
 * @returns {Promise<void>}
 */
export async function updateSummaryStatistics() {
  console.log("📊 updateSummaryStatistics: Updating summary statistics...");

  showSummaryStatus("");
  startLoadingDots("summary-status", "Loading summary tables..");

  const container = document.getElementById("summary-table-container");
  if (!container) {
    console.warn("❌ #summary-table-container not found.");
    stopLoadingDots("summary-status", "Summary container missing.");
    showSummaryStatus("Summary container missing.");
    return;
  }

  showLoadingOverlay(container, "Loading summary tables..");
  container.innerHTML = "";

  try {
    const yearVal = getDropdownValue("summary-year");
    const year = parseInt(String(yearVal ?? ""), 10);
    const variable = /** @type {string} */ (getDropdownValue("summary-variable") || "");
    const stripRaw = /** @type {string | null} */ (getDropdownValue("summary-strip"));
    const strip = stripRaw ? stripRaw : null;
    const granularity = /** @type {string} */ (getDropdownValue("summary-granularity") || "");
    const depthRaw = /** @type {string | null} */ (getDropdownValue("summary-depth"));
    const depth = depthRaw ? depthRaw : null;
    const unitSystem = getUnitSystemForSummary();
    summaryWindow.__seasonalComparisonDownload = null;
    setComparisonDownloadsAvailable(false, granularity === "gseason");
    let periods = [];
    let periodsAnchorYear = year;
    if (granularity === "gseason") {
      try {
        periods = getCustomSeasonPeriods();
        periodsAnchorYear = getCustomPeriodsAnchorYear(year);
      } catch (error) {
        const warning = document.createElement("div");
        warning.className = "alert alert-warning";
        warning.textContent = error instanceof Error
          ? error.message
          : "The custom seasonal periods are invalid.";
        container.appendChild(warning);
        stopLoadingDots("summary-status", "Custom seasonal periods need attention.");
        showSummaryStatus("Custom seasonal periods need attention.");
        return;
      }
    }

    const titleEl = document.getElementById("summary-title");
    if (titleEl) {
      titleEl.textContent = buildSummaryTitle({ year, variable, strip, granularity, unitSystem });
    }

    console.log("🔍 Selected Summary Filters:", {
      year,
      variable,
      strip,
      granularity,
      depth,
      unitSystem,
    });

    if (!Number.isFinite(year) || !variable || !granularity || !depth) {
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent =
        "Please select Year, Variable, Time averages, and Depth before updating the summary.";
      container.appendChild(warn);

      stopLoadingDots("summary-status", "Missing required selections.");
      showSummaryStatus("Missing required selections.");
      return;
    }

    const comparisonKey = granularity === "gseason"
      ? multiYearComparisonCacheKey({
          periods,
          periodsAnchorYear,
          variable,
          strip,
          depth,
          unitSystem,
        })
      : null;

    let cachedComparison = comparisonKey
      ? summaryWindow.multiYearSummaryCache?.get(comparisonKey)
      : null;
    if (!cachedComparison && comparisonKey) {
      const pendingComparison = summaryWindow.multiYearSummaryRequests?.get(comparisonKey);
      if (pendingComparison) cachedComparison = await pendingComparison;
    }

    const cachedYearEntry = Array.isArray(cachedComparison)
      ? cachedComparison.find((entry) => Number(entry?.year) === year)
      : null;

    const data = cachedYearEntry
      ? {
          year,
          variable,
          strip,
          granularity,
          depth,
          gseason_stats: cachedYearEntry.gseason_stats || [],
          periods: cachedYearEntry.periods || [],
          multi_year_gseason: cachedComparison,
        }
      : await fetchJson("/api/get_summary_stats", {
          method: "POST",
          body: JSON.stringify({
            year,
            variable,
            strip,
            granularity,
            depth,
            unitSystem,
            periods,
            periodsAnchorYear,
            compareYears: false,
          }),
        });

    console.log("✅ Received summary stats response:", data);

    summaryWindow.latestSummaryStats = {
      raw: data?.raw_statistics ?? null,
      ratio: data?.ratio_statistics ?? null,
      gseason: data?.gseason_stats ?? null,
      meta: { year, variable, strip, granularity, depth, unitSystem },
    };

    summaryWindow.__lastSummaryData = data;

    if (titleEl) {
      titleEl.textContent =
        data?.title || buildSummaryTitle({ year, variable, strip, granularity, unitSystem });
    }

    container.innerHTML = "";

    if (granularity === "gseason") {
      const displayPeriods = data?.periods || periods;
      container.innerHTML = buildGseasonSummaryTableHTML(
        data?.gseason_stats || {},
        variable,
        unitSystem,
        year,
        displayPeriods
      );

      cachedComparison = comparisonKey
        ? summaryWindow.multiYearSummaryCache?.get(comparisonKey)
        : null;
      if (cachedComparison) {
        appendMultiYearComparison(
          container,
          cachedComparison,
          displayPeriods,
          variable,
          unitSystem,
          { strip, depth }
        );
      } else {
        const comparisonHost = document.createElement("div");
        comparisonHost.className = "multi-year-summary mt-4 text-muted";
        comparisonHost.textContent = "Loading comparison across all years…";
        container.appendChild(comparisonHost);

        let comparisonRequest = comparisonKey
          ? summaryWindow.multiYearSummaryRequests?.get(comparisonKey)
          : null;
        if (!comparisonRequest) {
          comparisonRequest = fetchJson("/api/get_summary_stats", {
            method: "POST",
            body: JSON.stringify({
              year,
              variable,
              strip,
              granularity,
              depth,
              unitSystem,
              periods,
              periodsAnchorYear,
              compareYears: true,
            }),
          }).then((comparisonData) => {
            const entries = comparisonData?.multi_year_gseason || [];
            if (comparisonKey) summaryWindow.multiYearSummaryCache?.set(comparisonKey, entries);
            return entries;
          }).finally(() => {
            if (comparisonKey) summaryWindow.multiYearSummaryRequests?.delete(comparisonKey);
          });
          if (comparisonKey) {
            summaryWindow.multiYearSummaryRequests?.set(comparisonKey, comparisonRequest);
          }
        }

        void comparisonRequest.then((entries) => {
          if (!comparisonHost.isConnected) return;
          comparisonHost.className = "";
          comparisonHost.textContent = "";
          appendMultiYearComparison(
            comparisonHost,
            entries,
            displayPeriods,
            variable,
            unitSystem,
            { strip, depth }
          );
        }).catch((error) => {
          console.error("❌ Failed to load multi-year comparison:", error);
          if (!comparisonHost.isConnected) return;
          comparisonHost.className = "alert alert-warning mt-4";
          comparisonHost.textContent = "The selected-year summary loaded, but the comparison across all years did not.";
        });
      }
      console.log("✅ Selected-year seasonal summary rendered.");

      stopLoadingDots("summary-status", "");
      hideSummaryStatus();
      return;
    }

    const rawHeader = document.createElement("h5");
    rawHeader.textContent = "Raw Data";
    container.appendChild(rawHeader);

    const rawPretty = prettifyStatsKeys(data?.raw_statistics, variable, unitSystem);
    const rawTableEl = generateSummaryTable(rawPretty, variable);
    if (rawTableEl instanceof Node) {
      container.appendChild(rawTableEl);
    } else {
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent = "Raw summary table could not be rendered (unexpected return type).";
      container.appendChild(warn);
    }

    const ratioHeader = document.createElement("h5");
    ratioHeader.className = "mt-4";
    ratioHeader.textContent = "Ratio Data";
    container.appendChild(ratioHeader);

    const variableKey = variable;

    if (typeof isTemperatureVariable === "function" && isTemperatureVariable(variableKey)) {
      const info = document.createElement("div");
      info.className = "alert alert-info";
      info.innerHTML =
        "Ratios are not shown for <strong>Soil Temperature</strong> because temperature ratios are not meaningful in this dashboard " +
        "(unlike VWC/EC/SWC). See the <strong>Technical Details</strong> tab for an explanation.";
      container.appendChild(info);

      console.log("ℹ️ Temperature variable selected; ratio stats intentionally suppressed.");

      stopLoadingDots("summary-status", "");
      hideSummaryStatus();
      return;
    }

    const ratioStats = data?.ratio_statistics;
    const hasRatioStats =
      ratioStats && typeof ratioStats === "object" && Object.keys(ratioStats).length > 0;

    if (hasRatioStats) {
      const ratioPretty = prettifyStatsKeys(ratioStats, variable, unitSystem);
      const ratioTableEl = generateSummaryTable(ratioPretty, variable);

      if (ratioTableEl instanceof Node) {
        container.appendChild(ratioTableEl);
      } else {
        const warn = document.createElement("div");
        warn.className = "alert alert-warning";
        warn.textContent = "Ratio summary table could not be rendered (unexpected return type).";
        container.appendChild(warn);
      }
    } else {
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent = "No summary statistics available.";
      container.appendChild(warn);
    }

    console.log("✅ Summary statistics tables updated.");

    stopLoadingDots("summary-status", "");
    hideSummaryStatus();
  } catch (error) {
    console.error("❌ Unexpected error in updateSummaryStatistics:", error);

    container.innerHTML = "";
    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent =
      "Failed to load summary statistics. Check server logs and browser console.";
    container.appendChild(div);

    stopLoadingDots("summary-status", "Failed to load summary.");
    showSummaryStatus("Failed to load summary.");
  } finally {
    hideLoadingOverlay(container);
  }
}

/**
 * @returns {void}
 */
export function initSummaryTab() {
  const btn = document.getElementById("update-summary");
  if (!btn) {
    console.warn("⚠️ #update-summary button not found; summary tab not wired.");
    return;
  }

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    void updateSummaryStatistics();
  });

  const summaryTab = document.getElementById("summary-tab");
  const container = document.getElementById("summary-table-container");
  if (summaryTab && container && container.dataset.autoloaded !== "true") {
    summaryTab.addEventListener("shown.bs.tab", () => {
      if (container.dataset.autoloaded === "true") return;
      container.dataset.autoloaded = "true";
      void updateSummaryStatistics();
    });
  }

  console.debug("✅ Summary tab wired (update button)");
}

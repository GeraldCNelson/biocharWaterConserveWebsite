// tables.js
import { formatValue } from "./ui_utils.js";

/**
 * Unit labels for each variable and unit system.
 * These are used in the summary subtitles, *not* for table headers.
 */
const VARIABLE_UNITS = {
  VWC:   { us: "%",    metric: "%" },
  T:     { us: "°F",   metric: "°C" },
  EC:    { us: "dS/m", metric: "dS/m" },
  SWC:   { us: "gal per sensor cylinder", metric: "L per sensor cylinder" },
};

/**
 * Resolve a nice display name for the variable, including SWC special-case.
 * Note: units are *not* included here; subtitles add them separately.
 */
function resolveDisplayName(variable, unitSystem) {
  const mapping = window.variableNameMapping || {};
  const base = mapping[variable] || variable;

  if (variable === "SWC") {
    // Explicit SWC label, without units (those come from VARIABLE_UNITS)
    return "Soil Water Content";
  }

  return base;
}

/**
 * Get the unit label for subtitle usage.
 * For ratio summaries we now keep subtitles/headers unit-free.
 */
function getUnitLabel(variable, unitSystem, isRatio = false) {
  if (isRatio) {
    // Ratios are shown without units in subtitles and headers.
    return "";
  }
  const varUnits = VARIABLE_UNITS[variable];
  if (!varUnits) return "";
  return varUnits[unitSystem] || "";
}

/**
 * Builds an HTML table for a flat set of summary stats.
 * `displayVar` is the human label; `unitLabel` is currently unused,
 * but kept in the signature for future flexibility.
 *
 * Table headers are intentionally unit-free:
 *   Variable (Logger) | Min | Mean | Max | Std Dev
 */
function generateSummaryTable(stats, displayVar, unitLabel) {
  if (!stats || Object.keys(stats).length === 0) {
    return `
      <p class="text-muted mb-0">
        No summary statistics are available for this selection.
      </p>
    `;
  }

  let table = `
    <table class="table table-striped table-bordered mb-3">
      <thead class="table-dark">
        <tr>
          <th>Variable (Logger)</th>
          <th>Min</th>
          <th>Mean</th>
          <th>Max</th>
          <th>Std Dev</th>
        </tr>
      </thead>
      <tbody>
  `;

  for (const [key, value] of Object.entries(stats)) {
    // Expect keys like VWC_1_raw_S1_T, EC_2_raw_S1_M, SWC_1_raw_S1_T, etc.
    const match = key.match(/_(B|M|T)$/);
    const logger = match ? match[1] : key;
    const displayName = `${displayVar} (${logger})`;
    const { min, mean, max, std } = value || {};

    table += `
      <tr>
        <td>${displayName}</td>
        <td>${formatValue(min)}</td>
        <td>${formatValue(mean)}</td>
        <td>${formatValue(max)}</td>
        <td>${formatValue(std)}</td>
      </tr>
    `;
  }

  table += "</tbody></table>";
  return table;
}

/**
 * Splits ratio stats into S1/S2 and S3/S4 sections.
 */
function renderSplitRatioTables(ratioStats, variable, unitSystem) {
  const s1s2 = {};
  const s3s4 = {};

  for (const [trace, values] of Object.entries(ratioStats || {})) {
    const key = trace.trim().toUpperCase();
    if (key.includes("S1_S2"))      s1s2[trace] = values;
    else if (key.includes("S3_S4")) s3s4[trace] = values;
  }

  const displayVar = resolveDisplayName(variable, unitSystem);
  const ratioUnit  = getUnitLabel(variable, unitSystem, true); // now "" for ratios

  const build = (label, group) => {
    if (!group || Object.keys(group).length === 0) {
      // Variable-aware explanation for missing ratios
      let reason;
      if (variable === "T") {
        reason =
          "Temperature-based ratio summaries are not shown for this variable.";
      } else if (variable === "SWC") {
        reason =
          "Soil Water Content (per-cylinder volume) ratio summaries are not shown in this version of the dashboard.";
      } else {
        reason =
          "No ratio summaries are available for this selection.";
      }

      return `
        <p class="text-muted">
          ${label} Ratio: ${reason}
          See the Technical Details tab for an extended explanation.
        </p>
      `;
    }

    const unitSuffix = ratioUnit ? ` (${ratioUnit})` : "";
    const subtitle = `${label} Ratio – ${displayVar}${unitSuffix} by logger location`;

    return `
      <h5 class="mt-3 mb-1">${subtitle}</h5>
      ${generateSummaryTable(group, displayVar, ratioUnit)}
    `;
  };

  return build("S1/S2", s1s2) + build("S3/S4", s3s4);
}

/**
 * 📊 Fetches & renders the summary statistics.
 */
async function updateSummaryStatistics() {
  console.log("📊 updateSummaryStatistics: starting…");

  // 1) Grab controls
  const yearEl     = document.getElementById("summary-year");
  const variableEl = document.getElementById("summary-variable");
  const stripEl    = document.getElementById("summary-strip");
  const granEl     = document.getElementById("summary-granularity");
  const depthEl    = document.getElementById("summary-depth");

  if (!yearEl || !variableEl || !stripEl || !granEl || !depthEl) {
    console.error("❌ Summary controls not found in DOM!");
    alert("⚠️ Internal error: summary controls missing.");
    return;
  }

  const year        = parseInt(yearEl.value, 10);
  const variable    = variableEl.value;
  const strip       = stripEl.value;
  const granularity = granEl.value;
  const depth       = depthEl.value;            // keep as string, e.g. "1"
  const unitSystem  = window.unitSystem || "us";

  console.log("🔍 Summary request:", {
    year,
    variable,
    strip,
    granularity,
    depth,
    unitSystem,
  });

  if (isNaN(year)) {
    alert("⚠️ Please select a valid year.");
    return;
  }

  // 2) Call backend
  const payload = { year, variable, strip, granularity, depth, unitSystem };
  const resp = await fetch("/api/get_summary_stats", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const txt = await resp.text();
    console.error("❌ Summary API error:", resp.status, txt);
    alert("⚠️ Error retrieving summary statistics.");
    return;
  }

  const data = await resp.json();
  console.log("✅ Summary stats received:", data);

  const container = document.getElementById("summary-table-container");
  if (!container) return;

  // 3) Figure out units and labels
  const effectiveUnitSystem = data.unitSystem || unitSystem; // "us" or "metric"
  const displayVar          = resolveDisplayName(variable, effectiveUnitSystem);
  const rawUnit             = getUnitLabel(variable, effectiveUnitSystem, false);

  // Depth label via mapping from backend (sensor_depth_mapping)
  const depthMapping = window.depthMapping || {};
  let depthLabel = depth;

  if (depth && depthMapping[depth]) {
    // Prefer the currently active unit system (us / metric)
    if (depthMapping[depth][effectiveUnitSystem]) {
      depthLabel = depthMapping[depth][effectiveUnitSystem];
    } else if (depthMapping[depth].us) {
      // Fallback to US label if present
      depthLabel = depthMapping[depth].us;
    } else {
      // Fallback to "first available" label
      const firstKey = Object.keys(depthMapping[depth])[0];
      if (firstKey) {
        depthLabel = depthMapping[depth][firstKey];
      }
    }
  }

  if (granularity !== "gseason") {
    const unitSuffix = rawUnit ? ` (${rawUnit})` : "";

    const rawSubtitle = (
      `Raw Values – ${displayVar}${unitSuffix} ` +
      `by logger location in strip ${strip}, depth = ${depthLabel}`
    );

    const rawHTML = generateSummaryTable(
      data.raw_statistics,
      displayVar,
      rawUnit
    );

    const ratioHTML = renderSplitRatioTables(
      data.ratio_statistics,
      variable,
      effectiveUnitSystem
    );

    container.innerHTML = `
      <h5 class="mb-1">${rawSubtitle}</h5>
      ${rawHTML}
      ${ratioHTML}
    `;
  } else {
    // Seasonal (gseason) summaries use the accordion renderer,
    // which already handles its own titles.
    if (typeof generateSeasonalSummaryAccordion === "function") {
      container.innerHTML = generateSeasonalSummaryAccordion(data, variable);
    } else {
      container.innerHTML = `
        <p class="text-muted">
          Seasonal summary renderer is not available.
        </p>
      `;
    }
  }

  console.log("✅ Summary tables updated.");
}

export { updateSummaryStatistics };
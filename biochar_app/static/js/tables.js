// tables.js
import { formatValue } from "./ui_utils.js";

/**
 * Builds an HTML table for a flat set of summary stats.
 */
function generateSummaryTable(stats, variable) {
    const options = window.variableNameMapping || {};
    const displayVar = options[variable] || variable;

    let table = `
      <table class="table table-striped table-bordered">
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
        const match = key.match(/_(B|M|T)$/);
        const logger = match ? match[1] : key;
        const displayName = `${displayVar} (${logger})`;
        const { min, mean, max, std } = value;

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
function renderSplitRatioTables(ratioStats, variable) {
    const s1s2 = {}, s3s4 = {};
    for (const [trace, values] of Object.entries(ratioStats || {})) {
        const key = trace.trim().toUpperCase();
        if (key.includes("S1_S2")) s1s2[trace] = values;
        else if (key.includes("S3_S4")) s3s4[trace] = values;
    }
    const build = (label, group) => {
      return Object.keys(group).length
        ? `<h5>${label} Ratio</h5>${generateSummaryTable(group, variable)}`
        : `<p class="text-muted">No ${label} ratio summary available.</p>`;
    };
    return build("S1/S2", s1s2) + build("S3/S4", s3s4);
}

/**
 * 📊 Fetches & renders the summary statistics.
 */
async function updateSummaryStatistics() {
    console.log("📊 updateSummaryStatistics: starting…");

    // 1) Read values straight from the Summary controls:
    const yearEl        = document.getElementById("summary-year");
    const variableEl    = document.getElementById("summary-variable");
    const stripEl       = document.getElementById("summary-strip");
    const granEl        = document.getElementById("summary-granularity");
    const depthEl       = document.getElementById("summary-depth");
    const unitSystem    = window.unitSystem || "us";

    if (!yearEl || !variableEl || !stripEl || !granEl || !depthEl) {
      console.error("❌ Summary controls not found in DOM!");
      alert("⚠️ Internal error: summary controls missing.");
      return;
    }

    const year        = parseInt(yearEl.value, 10);
    const variable    = variableEl.value;
    const strip       = stripEl.value;
    const granularity = granEl.value;
    const depth       = parseInt(depthEl.value, 10);

    console.log("🔍 Summary request:", { year, variable, strip, granularity, depth, unitSystem });

    // 2) Validate:
    if (isNaN(year)) {
      alert("⚠️ Please select a valid year.");
      return;
    }

    // 3) Build payload and fetch:
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

    // 4) Render:
    const container = document.getElementById("summary-table-container");
    if (!container) return;

    if (granularity !== "gseason") {
      const rawHTML   = generateSummaryTable(data.raw_statistics, variable);
      const ratioHTML = renderSplitRatioTables(data.ratio_statistics, variable);
      container.innerHTML = `
        <h5>Raw Values</h5>
        ${rawHTML}
        ${ratioHTML}
      `;
    } else {
      // If you have a seasonal accordion generator, call it here:
      container.innerHTML = generateSeasonalSummaryAccordion(data, variable);
    }

    console.log("✅ Summary tables updated.");
}

export { updateSummaryStatistics };
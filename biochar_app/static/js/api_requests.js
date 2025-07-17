import { getDropdownValue, getInputValue } from "./ui_utils.js";

function capitalizeFirst(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function formatGseasonLabel(code) {
    const period = window.gseasonPeriods?.[code];
    if (!period) return code.replace("_", " ");  // fallback

    const toMonth = str => new Date(`2000-${str}-01`).toLocaleString('default', { month: 'short' });
    const startMonth = toMonth(period.start.split("-")[0]);
    const endMonth = toMonth(period.end.split("-")[0]);
    return `${period.label} (${startMonth}–${endMonth})`;
}

export function updateMainDataDisplay(data, options) {
    console.log("📊 Updating Main Data Display...");

    const year = getDropdownValue("summary-year", true);
    const variable = getDropdownValue("summary-variable");
    const strip = getDropdownValue("summary-strip");
    const granularity = getDropdownValue("summary-granularity");
    const depthLabel = document.getElementById("summary-depth").selectedOptions[0]?.textContent || "";
    const variableLabel = options?.labelNameMapping?.[variable] || variable;
    const mainTitle = data.title || `${capitalizeFirst(granularity)} Summary for ${variableLabel}, Strip ${strip}, ${depthLabel}, ${year}`;

    console.log("✅ variable label:", variableLabel);
    console.log("✅ main title:", mainTitle);
    document.getElementById("summary-title").textContent = mainTitle;

    const isTempVariable = ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"].includes(variable);

    // 🌱 Gseason accordion layout
    if (granularity === "gseason") {
        console.log("🌱 Detected growing season granularity. Building accordion layout...");

        const seasonStats = data.gseason_stats || {};
        const accordionHTML = generateSeasonalSummaryAccordion(seasonStats, variable, options);
        document.getElementById("summary-table-container").innerHTML = accordionHTML;
        return;
    }

    // 📅 Default (non-gseason) layout
    const rawTableHTML = generateSummaryTable(data.raw_statistics, variable);

    const s1s2 = {}, s3s4 = {};
    for (const [key, value] of Object.entries(data.ratio_statistics || {})) {
        if (key.includes("S1_S2")) s1s2[key] = value;
        else if (key.includes("S3_S4")) s3s4[key] = value;
    }

    const s1s2HTML = Object.keys(s1s2).length > 0
        ? generateSummaryTable(s1s2, variable)
        : isTempVariable
            ? `<p class="text-muted">Soil temperature ratios are not shown because they are not meaningful.</p>`
            : `<p class="text-danger">No summary statistics available.</p>`;

    const s3s4HTML = Object.keys(s3s4).length > 0
        ? generateSummaryTable(s3s4, variable)
        : isTempVariable
            ? `<p class="text-muted">Soil temperature ratios are not shown because they are not meaningful.</p>`
            : `<p class="text-danger">No summary statistics available.</p>`;

    const ratioHTML = `
        <h5 class="mt-4">S1/S2 Ratio (Top/Mid/Bottom Logger)</h5>
        ${s1s2HTML}
        <h5 class="mt-4">S3/S4 Ratio (Top/Mid/Bottom Logger)</h5>
        ${s3s4HTML}
    `;

    document.getElementById("summary-table-container").innerHTML = `
        <h5>Raw Values (Top/Mid/Bottom Logger)</h5>
        ${rawTableHTML}
        ${ratioHTML}
    `;
}

function generateGseasonAccordion(stats, variable, options) {
    const labelMap = window.labelNameMapping || {};
    const prettyVar = labelMap[variable] || variable;

    const idBase = "gseason-accordion";
    let html = `<div class="accordion" id="${idBase}">`;

    for (const [key, label] of Object.entries(seasonLabels)) {
        const seasonStats = stats[key] || {};
        const raw = seasonStats.raw_statistics || {};
        const ratio = seasonStats.ratio_statistics || {};

        const rawHTML = Object.keys(raw).length > 0
            ? generateSummaryTable(raw, variable)
            : `<p class="text-muted">No raw data available for ${label.split(" ")[0]}</p>`;

        const s1s2 = {}, s3s4 = {};
        for (const [trace, val] of Object.entries(ratio)) {
            if (trace.includes("S1_S2")) s1s2[trace] = val;
            else if (trace.includes("S3_S4")) s3s4[trace] = val;
        }

        const s1s2HTML = Object.keys(s1s2).length > 0
            ? generateSummaryTable(s1s2, variable)
            : `<p class="text-muted">No summary statistics available</p>`;
        const s3s4HTML = Object.keys(s3s4).length > 0
            ? generateSummaryTable(s3s4, variable)
            : `<p class="text-muted">No summary statistics available</p>`;

        const collapseId = `${idBase}-${key}`;
        const headingId = `${collapseId}-heading`;

        html += `
        <div class="accordion-item">
            <h2 class="accordion-header" id="${headingId}">
                <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse"
                        data-bs-target="#${collapseId}" aria-expanded="false" aria-controls="${collapseId}">
                    ${formatGseasonLabel(seasonCode)}
                </button>
            </h2>
            <div id="${collapseId}" class="accordion-collapse collapse" aria-labelledby="${headingId}" data-bs-parent="#${idBase}">
                <div class="accordion-body">
                    <h6>Raw Values</h6>
                    ${rawHTML}
                    <h6 class="mt-3">S1/S2 Ratio</h6>
                    ${s1s2HTML}
                    <h6 class="mt-3">S3/S4 Ratio</h6>
                    ${s3s4HTML}
                </div>
            </div>
        </div>
        `;
    }

    html += "</div>";
    return html;
}


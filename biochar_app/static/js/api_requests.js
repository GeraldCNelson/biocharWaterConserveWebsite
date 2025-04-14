import { getDropdownValue, getInputValue } from "./ui_utils.js";

function formatValue(value) {
    return (value === null || isNaN(value)) ? "NA" : Number(value).toFixed(4);
}

function capitalizeFirst(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

export function generateSummaryTable(stats, variable) {
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
        const match = key.match(/_(B|M|T)$/);  // Match suffix
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

export function updateMainDataDisplay(data, options) {
    console.log("📊 Updating Main Data Display...");

    const year = getDropdownValue("summary-year", true);
    const variable = getDropdownValue("summary-variable");
    const strip = getDropdownValue("summary-strip");
    const granularity = getDropdownValue("summary-granularity");
    const depthLabel = document.getElementById("summary-depth").selectedOptions[0]?.textContent || "";
    const variableLabel = options?.labelNameMapping?.[variable] || variable;
    const mainTitle = `${capitalizeFirst(granularity)} Summary for ${variableLabel}, Strip ${strip}, ${depthLabel}, ${year}`;

    console.log("✅ variable label:", variableLabel);
    console.log("✅ main title:", mainTitle);
    document.getElementById("summary-title").textContent = mainTitle;

    const isTempVariable = ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"].includes(variable);

    // 🌱 Gseason accordion layout
    if (granularity === "gseason") {
        console.log("🌱 Detected growing season granularity. Building accordion layout...");

        const seasonStats = data.gseason_stats || {};
        const seasonLabels = {
            Q1_Winter: "Winter",
            Q2_Early_Growing: "Early Growing",
            Q3_Peak_Harvest: "Peak Growing"
        };

        let accordionHTML = '<div class="accordion" id="gseasonAccordion">';

        for (const [key, label] of Object.entries(seasonLabels)) {
            const stats = seasonStats[key] || {};
            const rawStats = stats.raw_statistics || {};
            const ratioStats = stats.ratio_statistics || {};

            const rawHTML = Object.keys(rawStats).length > 0
                ? generateSummaryTable(rawStats, variable)
                : `<p class="text-muted">No raw data available for ${label}</p>`;

            const s1s2 = {}, s3s4 = {};
            for (const [trace, values] of Object.entries(ratioStats)) {
                if (trace.includes("S1_S2")) s1s2[trace] = values;
                else if (trace.includes("S3_S4")) s3s4[trace] = values;
            }

            const s1s2HTML = Object.keys(s1s2).length > 0
                ? generateSummaryTable(s1s2, variable)
                : isTempVariable
                    ? `<p class="text-muted">Temperature ratios are not shown because they are not meaningful.</p>`
                    : `<p class="text-muted">No summary statistics available</p>`;

            const s3s4HTML = Object.keys(s3s4).length > 0
                ? generateSummaryTable(s3s4, variable)
                : isTempVariable
                    ? `<p class="text-muted">Temperature ratios are not shown because they are not meaningful.</p>`
                    : `<p class="text-muted">No summary statistics available</p>`;

            accordionHTML += `
                <div class="accordion-item">
                    <h2 class="accordion-header" id="heading-${key}">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapse-${key}" aria-expanded="false" aria-controls="collapse-${key}">
                            ${label} Summary
                        </button>
                    </h2>
                    <div id="collapse-${key}" class="accordion-collapse collapse" aria-labelledby="heading-${key}" data-bs-parent="#gseasonAccordion">
                        <div class="accordion-body">
                            <h6>Raw Values</h6>
                            ${rawHTML}
                            <h6 class="mt-3">S1/S2 Ratio</h6>
                            ${s1s2HTML}
                            <h6 class="mt-3">S3/S4 Ratio</h6>
                            ${s3s4HTML}
                        </div>
                    </div>
                </div>`;
        }

        accordionHTML += "</div>";
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
            ? `<p class="text-muted">Temperature ratios are not shown because they are not meaningful.</p>`
            : `<p class="text-danger">No summary statistics available.</p>`;

    const s3s4HTML = Object.keys(s3s4).length > 0
        ? generateSummaryTable(s3s4, variable)
        : isTempVariable
            ? `<p class="text-muted">Temperature ratios are not shown because they are not meaningful.</p>`
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

    const seasonLabels = {
        Q1_Winter: "Winter Summary",
        Q2_Early_Growing: "Early Growing Summary",
        Q3_Peak_Harvest: "Peak Growing Summary"
    };

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
                    ${label}
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


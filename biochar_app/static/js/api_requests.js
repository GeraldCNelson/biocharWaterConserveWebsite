
import { getDropdownValue, getInputValue } from "./ui_utils.js";
/**
 * ✅ Fetch summary statistics data from backend
 */
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

function formatValue(value) {
    return (value === null || isNaN(value)) ? "NA" : Number(value).toFixed(4);
}

async function updateMainDataDisplay() {
    console.log("📊 Updating Main Data Display...");

    const params = {
        year: getDropdownValue("main-year", true),
        startDate: getInputValue("start-date"),
        endDate: getInputValue("end-date"),
        strip: getDropdownValue("main-strip"),
        variable: getDropdownValue("main-variable"),
        depth: getDropdownValue("main-depth"),
        loggerLocation: getDropdownValue("main-loggerLocation"),
        granularity: getDropdownValue("main-granularity")
    };

    console.log("📋 Main Data Parameters:", params);

    if (!params.year || !params.startDate || !params.endDate || !params.strip || !params.variable) {
        console.error("❌ Error: Missing required parameters!");
        return;
    }

    // Placeholder for future use:
    // const response = await fetch("/get_main_data", { ... });
}

export { updateMainDataDisplay };

import { getDropdownValue, getInputValue } from "./ui_utils.js";
/**
 * ✅ Fetch summary statistics data from backend
 */
function generateSummaryTable(statistics, variable) {
    if (!statistics || Object.keys(statistics).length === 0) {
        return "<p class='text-danger'>No summary statistics available.</p>";
    }

    let tableHTML = `
        <table class="table table-bordered">
            <thead class="thead-dark">
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

    for (const [logger, stats] of Object.entries(statistics)) {
        if (stats) {
            tableHTML += `
                <tr>
                    <td>${variable} (${logger})</td>
                    <td>${stats.min.toFixed(4)}</td>
                    <td>${stats.mean.toFixed(4)}</td>
                    <td>${stats.max.toFixed(4)}</td>
                    <td>${stats.std.toFixed(4)}</td>
                </tr>
            `;
        }
    }

    tableHTML += `
            </tbody>
        </table>
    `;

    return tableHTML;
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

export { generateSummaryTable, updateMainDataDisplay };
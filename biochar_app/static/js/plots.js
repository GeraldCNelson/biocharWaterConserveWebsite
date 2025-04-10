// plots.js (ES6 Module Version)
import { getSelectedFilters } from "./ui_controls.js";
import { getDropdownValue, getInputValue } from "./ui_utils.js";
import { generateSummaryTable, updateMainDataDisplay } from "./api_requests.js";

/**
 * 📊 updatePlot - Fetches and updates a Plotly chart dynamically.
 * @param {string} plotType - The type of plot to fetch ("raw" or "ratio").
 * @param {string} plotDiv - The ID of the div where the plot will be rendered.
 */
async function updatePlot(plotType, plotDiv) {
    console.log(`📡 Fetching ${plotType} plot data...`);

    try {
        let requestData = getSelectedFilters("main");

        // requestData.traceOption = "depths";

        const isGseason = requestData.granularity === "gseason";

        // ✅ Conditionally include weather overlays
        if (!isGseason) {
            if (requestData.variable === "T") {
                requestData.includeTemperature = true;
            }
            if (requestData.variable === "VWC") {
                requestData.includeRainfall = true;
            }
        } else {
            // 🚫 Remove weather flags if present
            delete requestData.includeRainfall;
            delete requestData.includeTemperature;
        }

        const route = isGseason
            ? `/plot_${plotType}_gseason`
            : `/plot_${plotType}`;

        const response = await fetch(route, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(requestData),
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`❌ Server returned error: ${response.status} - ${errorText}`);
        }

        const plotlyJSON = await response.json();
        console.log(`✅ Received ${plotType} plot JSON:`, plotlyJSON);

        Plotly.react(plotDiv, plotlyJSON.data, plotlyJSON.layout)
            .then(() => {
                Plotly.Plots.resize(document.getElementById(plotDiv));
                console.log(`🔄 Forced resize on ${plotDiv}`);
            });

    } catch (error) {
        console.error(`❌ Error updating ${plotType} plot:`, error);
    }
}


async function updateSummaryStatistics() {
    console.log("📊 updateSummaryStatistics: Updating summary statistics...");

    try {
        const year = parseInt(getDropdownValue("summary-year"));
        const variable = getDropdownValue("summary-variable");
        const strip = getDropdownValue("summary-strip");
        const granularity = getDropdownValue("summary-granularity");

        console.log("🔍 Selected Summary Filters:", { year, variable, strip, granularity });

        const response = await fetch("/get_summary_stats", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ year, variable, strip, granularity })
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error("❌ Server error in updateSummaryStatistics:", errorText);
            alert("⚠️ Error retrieving summary statistics.");
            return;
        }

        const data = await response.json();
        console.log("✅ Received summary stats response:", data);
        window.latestSummaryStats = {
          raw: data.raw_statistics,
          ratio: data.ratio_statistics
        };
        const title = `${capitalize(granularity)} Summary for ${variable} in Strip ${strip}, ${year}`;
        document.getElementById("summary-title").textContent = title;

        const rawTable = generateSummaryTable(data.raw_statistics, variable);
        const ratioTable = generateSummaryTable(data.ratio_statistics, variable);

        const container = document.getElementById("summary-table-container");
        container.innerHTML = `
            <h5>Raw Data</h5>
            ${rawTable}
            <h5 class="mt-4">Ratio Data</h5>
            ${ratioTable}
        `;

        console.log("✅ Summary statistics tables updated.");
    } catch (error) {
        console.error("❌ Unexpected error in updateSummaryStatistics:", error);
        alert("⚠️ Unexpected error occurred while updating summary statistics.");
    }
}

function capitalize(s) {
    return s.charAt(0).toUpperCase() + s.slice(1);
}

// Hook up Update Plots button only after DOM is ready
if (document.readyState !== "loading") {
    document.getElementById("update-plots")?.addEventListener("click", () => {
        updatePlot("raw", "raw-plot");
        updatePlot("ratio", "ratio-plot");
    });
} else {
    document.addEventListener("DOMContentLoaded", () => {
        document.getElementById("update-plots")?.addEventListener("click", () => {
            updatePlot("raw", "raw-plot");
            updatePlot("ratio", "ratio-plot");
        });
    });
}

export { updatePlot, updateSummaryStatistics };

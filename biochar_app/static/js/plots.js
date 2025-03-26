/**
 * 📊 updatePlot - Fetches and updates a Plotly chart dynamically.
 *
 * This function is used to fetch Plotly JSON data from either `/plot_raw` or `/plot_ratio`
 * and render it inside the specified `plotDiv` container.
 *
 * ✅ Why Use This?
 * - Reduces redundant code by handling both raw and ratio plots.
 * - Ensures consistent error handling and logging.
 * - Easily extendable for additional plots in the future.
 *
 * @param {string} plotType - The type of plot to fetch ("raw" or "ratio").
 * @param {string} plotDiv - The ID of the div where the plot will be rendered.
 */
async function updatePlot(plotType, plotDiv) {
    console.log(`📡 Fetching ${plotType} plot data...`);

    try {
        let requestData = getSelectedFilters("main");

        // ✅ Add required traceOption (default to 'depth')
        requestData.traceOption = "depth";

        // ✅ Conditional overlays
        if (requestData.variable === "T") {
            requestData.includeTemperature = true;
        }
        if (requestData.variable === "VWC") {
            requestData.includeRainfall = true;
        }

        const response = await fetch(`/plot_${plotType}`, {
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

        Plotly.react(plotDiv, plotlyJSON.data, plotlyJSON.layout);
    } catch (error) {
        console.error(`❌ Error updating ${plotType} plot:`, error);
    }
}

// 🎯 Attach the function to the "Update Plots" button
document.getElementById("update-plots").addEventListener("click", function () {
    updatePlot("raw", "raw-plot");   // Fetch and update raw plot
    updatePlot("ratio", "ratio-plot"); // Fetch and update ratio plot
});

async function updateSummaryStatistics() {
    console.log("📊 updateSummaryStatistics: Updating summary statistics...");

    try {
        const year = document.getElementById("summary-year").value;
        const variable = document.getElementById("summary-variable").value;
        const strip = document.getElementById("summary-strip").value;
        const granularity = document.getElementById("summary-granularity").value;

        console.log("🔍 Selected Summary Filters (FINAL):", { year, variable, strip, granularity });

        const response = await fetch("/get_summary_stats", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ year, variable, strip, granularity })
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error("❌ updateSummaryStatistics: Server error:", errorText);
            alert("⚠️ Error retrieving summary statistics.");
            return;
        }

        const data = await response.json();
        console.log("✅ updateSummaryStatistics: Data received:", data);

        if (!data.statistics || Object.keys(data.statistics).length === 0) {
            console.warn("⚠️ No statistics found for selected variable.", data);
            document.getElementById("summary-table-container").innerHTML =
                "<p class='text-warning'>No statistics available for the selected variable.</p>";
            return;
        }

        // ✅ Generate and insert summary table
        console.log("🛠 Inserting summary table...");
        document.getElementById("summary-table-container").innerHTML = generateSummaryTable(data.statistics, variable);
        console.log("✅ Summary table successfully updated.");

    } catch (error) {
        console.error("❌ updateSummaryStatistics: Unexpected error:", error);
        alert("⚠️ Unexpected error occurred while updating summary statistics.");
    }
}
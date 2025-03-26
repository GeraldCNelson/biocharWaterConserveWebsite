
/**
 * ✅ Fetch summary statistics data from backend
 */
async function fetchSummaryStatistics(year, variable, strip, granularity) {
    try {
        const response = await fetch("/get_summary_stats", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ year, variable, strip, granularity })
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error("❌ updateSummaryStatistics: Server error:", errorText);
            alert("⚠️ Error: Could not retrieve summary statistics.");
            return;
        }

        const data = await response.json();
        console.log("✅ Summary Statistics Data Received:", data);

        // ✅ Update the UI with summary statistics (Modify this to suit your UI)
        document.getElementById("summary-content").innerHTML = generateSummaryTable(data);

    } catch (error) {
        console.error("❌ fetchSummaryStatistics: Unexpected error:", error);
        alert("⚠️ Unexpected error occurred while fetching summary statistics.");
    }
}

function generateSummaryTable(statistics, variable) {
    if (!statistics || Object.keys(statistics).length === 0) {
        return "<p class='text-danger'>No summary statistics available.</p>";
    }

    if (!statistics[variable]) {
        console.warn(`⚠️ No statistics found for variable: ${variable}`, statistics);
        return "<p class='text-warning'>No statistics available for the selected variable.</p>";
    }

    let stats = statistics[variable];  // ✅ Extract nested data

    let tableHTML = `
        <table class="table table-bordered">
            <thead class="thead-dark">
                <tr>
                    <th>Variable</th>
                    <th>Min</th>
                    <th>Mean</th>
                    <th>Max</th>
                    <th>Std Dev</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>${variable}</td>
                    <td>${stats.min.toFixed(4)}</td>
                    <td>${stats.mean.toFixed(4)}</td>
                    <td>${stats.max.toFixed(4)}</td>
                    <td>${stats.std.toFixed(4)}</td>
                </tr>
            </tbody>
        </table>
    `;

    return tableHTML;
}


async function updateMainDataDisplay() {
    console.log("📊 Updating Main Data Display...");

    // ✅ Get values from the Main Data Display tab
    const params = {
        year: document.getElementById("year").value,
        startDate: document.getElementById("start-date").value,
        endDate: document.getElementById("end-date").value,
        strip: document.getElementById("strip").value,
        variable: document.getElementById("variable").value,
        depth: document.getElementById("depth").value,
        loggerLocation: document.getElementById("loggerLocation").value,
        granularity: document.getElementById("granularity").value
    };

    console.log("📋 Main Data Parameters:", params);

    if (!params.year || !params.startDate || !params.endDate || !params.strip || !params.variable) {
        console.error("❌ Error: Missing required parameters!");
        return;
    }

    try {
        const response = await fetch("/get_main_data", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(params)
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(`HTTP ${response.status} - ${JSON.stringify(errorData)}`);
        }

        const data = await response.json();
        console.log("📊 Main Data Response:", data);

        // ✅ Update plots
//        updatePlots(data);
    } catch (error) {
        console.error("❌ Error fetching main data:", error);
    }
}
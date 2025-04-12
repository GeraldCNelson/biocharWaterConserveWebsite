import { fetchDefaultsAndOptions, populateDropdownsByTab } from "./ui_controls.js";
import { getInputValue, setInputValue } from "./ui_utils.js";
import { fetchAndRenderPlot, waitForAllDropdowns } from "./plot_utils.js";
import { loadMarkdownContent } from "./markdown.js";
import { generateSummaryTable } from "./api_requests.js";

/* global Plotly */

document.addEventListener("DOMContentLoaded", async function () {
    console.log("🌐 Initializing application...");

    try {
        console.log("🟡 Fetching defaults and options...");
        const options = await fetchDefaultsAndOptions();
        if (!options || !options.defaults) {
            throw new Error("❌ CRITICAL ERROR: Missing defaults or options!");
        }

        console.log("🔍 Checking fetched options:", options);
        console.log("🛠 Populating dropdowns...");
        populateDropdownsByTab(options);

        await waitForAllDropdowns([
            "main-year", "main-variable", "main-strip", "main-granularity", "main-loggerLocation", "main-depth",
            "summary-year", "summary-variable", "summary-strip", "summary-granularity", "summary-depth"
        ]);

        console.log("✅ Dropdowns successfully populated.");

        window.mainDataDisplayConfig = {
            year: options.defaults.year || options.years?.[0] || null,
            strip: options.defaults.strip || options.strips?.[0] || null,
            variable: options.defaults.variable || options.variables?.[0] || null,
            loggerLocation: options.defaults.loggerLocation || options.loggerLocations?.[0] || null,
            depth: options.defaults.depth || options.depths?.[0] || null,
            granularity: options.defaults.granularity || options.granularities?.[0] || null,
        };

        const missingFields = Object.entries(window.mainDataDisplayConfig)
            .filter(([_, value]) => value === null)
            .map(([key]) => key);

        if (missingFields.length > 0) {
            throw new Error(`❌ Missing required values: ${missingFields.join(", ")}`);
        }

        console.log("✅ Initialized mainDataDisplayConfig:", window.mainDataDisplayConfig);

        const defaultYear = window.mainDataDisplayConfig.year;
        setInputValue("start-date", `${defaultYear}-01-01`);
        setInputValue("end-date", options.defaults.endDate);

        document.getElementById("main-year").addEventListener("change", function () {
            const selectedYear = this.value;
            setInputValue("start-date", `${selectedYear}-01-01`);

            fetch(`/get_end_date?year=${selectedYear}`)
                .then(response => response.json())
                .then(data => {
                    if (data.endDate) {
                        setInputValue("end-date", data.endDate);
                    } else {
                        console.warn("⚠️ No endDate returned, falling back to Dec 31");
                        setInputValue("end-date", `${selectedYear}-12-31`);
                    }
                })
                .catch(error => {
                    console.error("❌ Error fetching end date:", error);
                    setInputValue("end-date", `${selectedYear}-12-31`);
                });

            console.log("✅ Start and End Dates Initialized.");
        });

        document.getElementById("main-granularity").addEventListener("change", function () {
            const selected = this.value;
            const startInput = document.getElementById("start-date");
            const endInput = document.getElementById("end-date");

            const isGseason = selected === "gseason";
            startInput.disabled = isGseason;
            endInput.disabled = isGseason;
            startInput.style.opacity = isGseason ? 0.5 : 1;
            endInput.style.opacity = isGseason ? 0.5 : 1;
        });

        document.getElementById("main-traceOption").addEventListener("change", () => {
            updatePlot("raw", "raw-plot");
            updatePlot("ratio", "ratio-plot");
        });

        document.getElementById("update-summary").addEventListener("click", async () => {
            const year = document.getElementById("summary-year").value;
            const granularity = document.getElementById("summary-granularity").value;
            const variable = document.getElementById("summary-variable").value;
            const strip = document.getElementById("summary-strip").value;
            const depth = document.getElementById("summary-depth").value;
            const startDate = document.getElementById("start-date")?.value || `${year}-01-01`;
            const endDate = document.getElementById("end-date")?.value || `${year}-12-31`;

            try {
                const response = await fetch("/get_summary_stats", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ year, granularity, variable, strip, depth, startDate, endDate })
                });

                const data = await response.json();

                if (data.error) {
                    document.getElementById("summary-table-container").innerHTML =
                        `<p class="text-danger">${data.error}</p>`;
                    return;
                }

                window.latestSummaryStats = {
                    raw: data.raw_statistics,
                    ratio: data.ratio_statistics
                };

                updateMainDataDisplay(data, options);

                console.log("✅ Summary tables updated successfully.");
            } catch (error) {
                console.error("❌ Error fetching summary statistics:", error);
                document.getElementById("summary-table-container").innerHTML =
                    `<p class="text-danger">Failed to load summary statistics</p>`;
            }
        });

        console.log("📊 Auto-loading plots and summary statistics...");
        setTimeout(() => {
            requestAnimationFrame(() => {
                fetchAndRenderPlot("/plot_raw", "raw-plot", { traceOption: "depths" });
                fetchAndRenderPlot("/plot_ratio", "ratio-plot");
                document.getElementById("update-summary").click();
            });
        }, 600);

        console.log("📖 Loading markdown files...");
        await Promise.all([
            loadMarkdownContent("intro-content", "/markdown/intro.md"),
            loadMarkdownContent("experiment-content", "/markdown/experimentDesign.md"),
            loadMarkdownContent("tech-content", "/markdown/techDetails.md")
        ]);

        const tabLink = document.querySelector('a[href="#main"]');
        if (tabLink) {
            tabLink.addEventListener("shown.bs.tab", function () {
                console.log("🧼 Tab is now visible: resizing plots...");
                Plotly.Plots.resize(document.getElementById("raw-plot"));
                Plotly.Plots.resize(document.getElementById("ratio-plot"));
            });
        } else {
            console.warn("⚠️ Main tab link not found: skipping tab resize hook.");
        }
        console.log("✅ Application successfully initialized.");

    } catch (error) {
        console.error("❌ ERROR: Application initialization failed:", error);
    }
});

function updateMainDataDisplay(data, options) {
    const year = document.getElementById("summary-year").value;
    const variable = document.getElementById("summary-variable").value;
    const variableLabel = options?.variableNameMapping?.[variable] || variable;
    const strip = document.getElementById("summary-strip").value;
    const granularity = document.getElementById("summary-granularity").value;
    const depthLabel = document.getElementById("summary-depth").selectedOptions[0]?.textContent || "";

    const mainTitle = `${capitalizeFirst(granularity)} Summary for ${variableLabel} in Strip ${strip}, ${year}`;
    document.getElementById("summary-title").textContent = mainTitle;

    const rawTableHTML = generateSummaryTable(data.raw_statistics, variable);

    const s1s2 = {};
    const s3s4 = {};

    for (const [key, value] of Object.entries(data.ratio_statistics || {})) {
        if (key.includes("S1_S2")) s1s2[key] = value;
        else if (key.includes("S3_S4")) s3s4[key] = value;
    }

const isTempVariable = ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"].includes(variable);

const s1s2HTML = Object.keys(s1s2).length > 0
    ? generateSummaryTable(s1s2, variable)
    : isTempVariable
        ? `<p class="text-muted">Temperature ratios are not shown because they are not meaningful.</p>`
        : `<p class="text-danger">No summary statistics available.</p>`;

const s3s4HTML = Object.keys(s3s4).length > 0
    ? generateSummaryTable(s1s2, variable)
    : isTempVariable
        ? `<p class="text-muted">Temperature ratios are not shown because they are not meaningful.</p>`
        : `<p class="text-danger">No summary statistics available.</p>`;

    const ratioTitleS1S2 = `${capitalizeFirst(granularity)} Summary for ${variableLabel} (S1/S2, ${depthLabel}, ${year})`;
    const ratioTitleS3S4 = `${capitalizeFirst(granularity)} Summary for ${variableLabel} (S3/S4, ${depthLabel}, ${year})`;

    const ratioHTML = `
        <h5 class="mt-4">${ratioTitleS1S2}</h5>
        ${s1s2HTML}
        <h5 class="mt-4">${ratioTitleS3S4}</h5>
        ${s3s4HTML}
    `;
  
    document.getElementById("summary-table-container").innerHTML = `
        <h5>Raw Data (${depthLabel})</h5>
        ${rawTableHTML}
        ${ratioHTML}
    `;
}

function capitalizeFirst(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

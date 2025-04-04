
import { fetchDefaultsAndOptions, populateSelect, getSelectedFilters, populateDropdownsByTab } from "./ui_controls.js";
import { getDropdownValue, getInputValue, setInputValue } from "./ui_utils.js";
import { updateSummaryStatistics, updatePlot } from "./plots.js";
import { fetchAndRenderPlot, waitForAllDropdowns } from "./plot_utils.js";
import { loadMarkdownContent } from "./markdown.js";
/* global Plotly */

// ✅ Main initialization
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
            "summary-year", "summary-variable", "summary-strip", "summary-granularity"
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

        console.log("📊 Auto-loading plots and summary statistics...");
        setTimeout(() => {
            requestAnimationFrame(() => {
                fetchAndRenderPlot("/plot_raw", "raw-plot", { traceOption: "depths" });
                fetchAndRenderPlot("/plot_ratio", "ratio-plot");
            });
            updateSummaryStatistics();
        }, 600);
        updateSummaryStatistics();

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

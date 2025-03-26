// ✅ Main initialization
document.addEventListener("DOMContentLoaded", async function () {
    console.log("🌐 Initializing application...");

    try {
        // ✅ Fetch default values and options
        console.log("🟡 Fetching defaults and options...");
        const options = await fetchDefaultsAndOptions();
        if (!options || !options.defaults) {
            throw new Error("❌ CRITICAL ERROR: Missing defaults or options!");
        }

        console.log("🔍 Checking fetched options:", options);

        // ✅ Populate dropdowns and wait for them to load
        console.log("🛠 Populating dropdowns...");
        populateDropdownsByTab(options);
        await waitForDropdowns([
            "main-year", "main-variable", "main-strip", "main-granularity", "main-loggerLocation", "main-depth",
            "summary-year", "summary-variable", "summary-strip", "summary-granularity"
        ]);
        console.log("✅ Dropdowns successfully populated.");

        // ✅ Initialize mainDataDisplayConfig (still used as fallback object)
        window.mainDataDisplayConfig = {
            year: options.defaults.year || options.years?.[0] || null,
            strip: options.defaults.strip || options.strips?.[0] || null,
            variable: options.defaults.variable || options.variables?.[0] || null,
            loggerLocation: options.defaults.loggerLocation || options.loggerLocations?.[0] || null,
            depth: options.defaults.depth || options.depths?.[0] || null,
            granularity: options.defaults.granularity || options.granularities?.[0] || null,
        };

        // ✅ Validate critical config fields
        const missingFields = Object.entries(window.mainDataDisplayConfig)
            .filter(([_, value]) => value === null)
            .map(([key]) => key);

        if (missingFields.length > 0) {
            throw new Error(`❌ Missing required values: ${missingFields.join(", ")}`);
        }

        console.log("✅ Initialized mainDataDisplayConfig:", window.mainDataDisplayConfig);

        // ✅ Set Start and End Dates
        const defaultYear = window.mainDataDisplayConfig.year;
        document.getElementById("start-date").value = `${defaultYear}-01-01`;
        document.getElementById("end-date").value = options.defaults.endDate;

        // ✅ Update End Date on Year Change
        document.getElementById("main-year").addEventListener("change", function () {
    const selectedYear = this.value;
    document.getElementById("start-date").value = `${selectedYear}-01-01`;

    // Dynamically fetch the correct end date for the selected year
    fetch(`/get_end_date?year=${selectedYear}`)
        .then(response => response.json())
        .then(data => {
            if (data.endDate) {
                document.getElementById("end-date").value = data.endDate;
            } else {
                console.warn("⚠️ No endDate returned, falling back to Dec 31");
                document.getElementById("end-date").value = `${selectedYear}-12-31`;
            }
        })
        .catch(error => {
            console.error("❌ Error fetching end date:", error);
            document.getElementById("end-date").value = `${selectedYear}-12-31`;
        });
});

        console.log("✅ Start and End Dates Initialized.");

        // ✅ Initial plot and summary loads
        console.log("📊 Auto-loading plots and summary statistics...");
        await Promise.all([
            fetchAndRenderPlot("/plot_raw", "raw-plot", { traceOption: "depth" }),
            fetchAndRenderPlot("/plot_ratio", "ratio-plot"),
            updateSummaryStatistics()
        ]);

        await waitForSummaryDropdowns();
        updateSummaryStatistics();

        // ✅ Load markdown content
        console.log("📖 Loading markdown files...");
        await Promise.all([
            loadMarkdownContent("intro-content", "/markdown/intro.md"),
            loadMarkdownContent("tech-content", "/markdown/techDetails.md")
        ]);

        console.log("✅ Application successfully initialized.");
    } catch (error) {
        console.error("❌ ERROR: Application initialization failed:", error);
    }
});

// ✅ Fetch and Render Plot (raw or ratio)
async function fetchAndRenderPlot(endpoint, plotContainerId, extraConfig = {}) {
    try {
        const config = {
            year: document.getElementById("main-year").value,
            variable: document.getElementById("main-variable").value,
            strip: document.getElementById("main-strip").value,
            granularity: document.getElementById("main-granularity").value,
            loggerLocation: document.getElementById("main-loggerLocation").value,
            depth: document.getElementById("main-depth").value,
            startDate: document.getElementById("start-date").value,
            endDate: document.getElementById("end-date").value,
            traceOption: document.getElementById("main-traceOption")?.value || extraConfig.traceOption || "depth"
        };

        console.log(`📡 Fetching plot from ${endpoint} with config:`, config);

        const response = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(config)
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`❌ Server returned error: ${response.status} - ${errorText}`);
        }

        const plotData = await response.json();
        console.log(`✅ Received plot data for ${plotContainerId}:`, plotData);

        if (!plotData.data || plotData.data.length === 0) {
            console.warn(`⚠️ No plot data received for ${plotContainerId}`);
            return;
        }

        Plotly.react(plotContainerId, plotData.data, plotData.layout);
        setTimeout(() => {
            Plotly.Plots.resize(document.getElementById(plotContainerId));
            console.log(`🔄 Forced resize on ${plotContainerId}`);
        }, 500);

        console.log(`✅ Plot rendered in #${plotContainerId}`);
    } catch (error) {
        console.error(`❌ Error fetching plot from ${endpoint}:`, error);
    }
}

// ✅ Wait until dropdowns are loaded
async function waitForDropdowns(dropdownIds, timeout = 7000) {
    console.log("⏳ Waiting for dropdowns to be available...");
    return new Promise((resolve, reject) => {
        let elapsed = 0;
        const interval = 150;

        const checkDropdowns = setInterval(() => {
            const missing = dropdownIds.filter(id => {
                const el = document.getElementById(id);
                return !el || el.options.length === 0;
            });

            if (missing.length === 0) {
                clearInterval(checkDropdowns);
                console.log("✅ All dropdowns are fully populated.");
                resolve();
            } else if (elapsed >= timeout) {
                clearInterval(checkDropdowns);
                console.error(`❌ Timeout: These dropdowns didn’t load in time:`, missing);
                reject(new Error("Dropdowns not populated in time"));
            }

            elapsed += interval;
        }, interval);
    });
}

function populateDropdownsByTab(options) {
    console.log("🔍 Full options object:", options);

    // Ensure mappings exist
    options.variableNameMapping = options.variableNameMapping || {};
    options.stripNameMapping = options.stripNameMapping || {};
    options.granularityNameMapping = options.granularityNameMapping || {};
    options.loggerLocationMapping = options.loggerLocationMapping || {};
    options.depthMapping = options.depthMapping || {};

    // ✅ Populate Main Data Dropdowns
    document.getElementById("main-year").innerHTML = options.years
        .map(year => `<option value="${year}" ${year == options.defaults.year ? 'selected' : ''}>${year}</option>`)
        .join("");

    document.getElementById("main-variable").innerHTML = Object.keys(options.variableNameMapping)
        .map(varKey => `<option value="${varKey}" ${varKey == options.defaults.variable ? 'selected' : ''}>${options.variableNameMapping[varKey]}</option>`)
        .join("");

    document.getElementById("main-strip").innerHTML = Object.keys(options.stripNameMapping)
        .map(stripKey => `<option value="${stripKey}" ${stripKey == options.defaults.strip ? 'selected' : ''}>${options.stripNameMapping[stripKey]}</option>`)
        .join("");

    document.getElementById("main-granularity").innerHTML = Object.keys(options.granularityNameMapping)
        .map(granKey => `<option value="${granKey}" ${granKey == options.defaults.granularity ? 'selected' : ''}>${options.granularityNameMapping[granKey]}</option>`)
        .join("");

    document.getElementById("main-loggerLocation").innerHTML = Object.keys(options.loggerLocationMapping)
        .map(logKey => `<option value="${logKey}" ${logKey == options.defaults.loggerLocation ? 'selected' : ''}>${options.loggerLocationMapping[logKey]}</option>`)
        .join("");

    document.getElementById("main-depth").innerHTML = Object.keys(options.depthMapping)
        .map(depthKey => `<option value="${depthKey}" ${depthKey == options.defaults.depth ? 'selected' : ''}>${options.depthMapping[depthKey]}</option>`)
        .join("");

    console.log("✅ Main Data Display dropdowns successfully populated.");

    // ✅ Populate Summary Data Dropdowns
    if (!document.getElementById("summary-year")) {
        console.error("❌ Summary-year dropdown not found in DOM.");
        return;
    }

    document.getElementById("summary-year").innerHTML = options.years
        .map(year => `<option value="${year}" ${year == options.defaults.year ? 'selected' : ''}>${year}</option>`)
        .join("");

    document.getElementById("summary-variable").innerHTML = Object.keys(options.variableNameMapping)
        .map(varKey => `<option value="${varKey}" ${varKey == options.defaults.variable ? 'selected' : ''}>${options.variableNameMapping[varKey]}</option>`)
        .join("");

    document.getElementById("summary-strip").innerHTML = Object.keys(options.stripNameMapping)
        .map(stripKey => `<option value="${stripKey}" ${stripKey == options.defaults.strip ? 'selected' : ''}>${options.stripNameMapping[stripKey]}</option>`)
        .join("");

    document.getElementById("summary-granularity").innerHTML = Object.keys(options.granularityNameMapping)
        .map(granKey => `<option value="${granKey}" ${granKey == options.defaults.granularity ? 'selected' : ''}>${options.granularityNameMapping[granKey]}</option>`)
        .join("");

    console.log("✅ Summary dropdowns successfully populated.");
}

// ✅ Wait for summary dropdowns only
async function waitForSummaryDropdowns() {
    console.log("📊 Ensuring summary dropdowns are populated...");
    return waitForDropdowns([
        "summary-year", "summary-variable", "summary-strip", "summary-granularity"
    ]);
}
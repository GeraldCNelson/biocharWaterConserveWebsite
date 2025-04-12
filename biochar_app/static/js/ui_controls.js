import { getDropdownValue, getInputValue } from "./ui_utils.js";
import { fetchAndRenderPlot, waitForAllDropdowns } from "./plot_utils.js";

// ✅ Ensure global state objects are only defined once
if (!window.mainDataDisplayConfig) {
    window.mainDataDisplayConfig = {
        year: null,
        strip: null,
        variable: null,
        loggerLocation: null,
        depth: null
    };
}

if (!window.summaryStatsDisplay) {
    window.summaryStatsDisplay = {
        year: null,
        variable: null,
        strip: null,
        granularity: null
    };
}

/**
 * ✅ Fetch defaults and options from the server
 */
async function fetchDefaultsAndOptions() {
    console.log("📡 Fetching default values and dropdown options...");

    try {
        const response = await fetch("/get_defaults_and_options");
        console.log("🔍 Raw response from /get_defaults_and_options:", response);

        if (!response.ok) {
            throw new Error(`Server error: ${response.status} - ${response.statusText}`);
        }

        const data = await response.json();
        console.log("✅ Parsed JSON response:", data);

        if (!data.defaults) {
            throw new Error("🚨 Defaults not found in response!");
        }

        return data;
    } catch (error) {
        console.error("❌ fetchDefaultsAndOptions: Failed to load options:", error);
        return null;
    }
}

/**
 * ✅ Generic Function to Populate Dropdowns
 */
function populateSelect(stateObject, stateKey, options, defaultValue, tabName) {
    console.log(`🔍 Calling populateSelect -> tabName: ${tabName}, stateKey: ${stateKey}`);

    if (!tabName || !stateKey) {
        console.error(`❌ populateSelect: Missing tabName or stateKey in populateSelect`);
        return;
    }

    const selectElement = document.querySelector(`.${stateKey}-dropdown[data-tab="${tabName}"]`);

    if (!selectElement) {
        console.error(`❌ populateSelect: Dropdown not found for ${tabName}: ${stateKey}`);
        return;
    }

    if (!Array.isArray(options)) {
        console.error(`❌ options is not an array for ${stateKey} in ${tabName}. Got:`, options);
        return;
    }

    selectElement.innerHTML = ""; // Clear existing options

    options.forEach(({ value, label }) => {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = label;
        selectElement.appendChild(option);
    });

    if (defaultValue) {
        selectElement.value = defaultValue;
    } else {
        console.warn(`⚠️ No default value found for ${stateKey} in ${tabName}.`);
    }

    stateObject[stateKey] = defaultValue;
    console.log(`✅ Successfully populated ${stateKey} dropdown for ${tabName}`);
}

/**
 * ✅ Returns selected filters based on the active tab
 */
function getSelectedFilters(tab) {
    console.log(`🔍 Getting selected filters for: ${tab}`);

    const prefix = tab === "main" ? "main" : "summary";
    const filters = {
        year: getDropdownValue(`${prefix}-year`, true),
        granularity: getDropdownValue(`${prefix}-granularity`),
        variable: getDropdownValue(`${prefix}-variable`),
        strip: getDropdownValue(`${prefix}-strip`)
    };

    if (tab === "main") {
        Object.assign(filters, {
            startDate: getInputValue("start-date"),
            endDate: getInputValue("end-date"),
            loggerLocation: getDropdownValue("main-loggerLocation"),
            depth: getDropdownValue("main-depth"),
            traceOption: getDropdownValue("main-traceOption"),
        });
    }

    console.log("✅ Selected Filters:", filters);
    return filters;
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
        .map(year => `<option value="${year}" ${year === options.defaults.year ? 'selected' : ''}>${year}</option>`)
        .join("");

    document.getElementById("main-variable").innerHTML = Object.keys(options.variableNameMapping)
        .map(varKey => `<option value="${varKey}" ${varKey === options.defaults.variable ? 'selected' : ''}>${options.variableNameMapping[varKey]}</option>`)
        .join("");

    document.getElementById("main-strip").innerHTML = Object.keys(options.stripNameMapping)
        .map(stripKey => `<option value="${stripKey}" ${stripKey === options.defaults.strip ? 'selected' : ''}>${options.stripNameMapping[stripKey]}</option>`)
        .join("");

    document.getElementById("main-granularity").innerHTML = Object.keys(options.granularityNameMapping)
        .map(granKey => `<option value="${granKey}" ${granKey === options.defaults.granularity ? 'selected' : ''}>${options.granularityNameMapping[granKey]}</option>`)
        .join("");

    document.getElementById("main-loggerLocation").innerHTML = Object.keys(options.loggerLocationMapping)
        .map(logKey => `<option value="${logKey}" ${logKey === options.defaults.loggerLocation ? 'selected' : ''}>${options.loggerLocationMapping[logKey]}</option>`)
        .join("");

    document.getElementById("main-depth").innerHTML = Object.keys(options.depthMapping)
        .map(depthKey => `<option value="${depthKey}" ${depthKey === options.defaults.depth ? 'selected' : ''}>${options.depthMapping[depthKey]}</option>`)
        .join("");

    console.log("✅ Main Data Display dropdowns successfully populated.");

    // ✅ Populate Summary Data Dropdowns
    if (!document.getElementById("summary-year")) {
        console.error("❌ Summary-year dropdown not found in DOM.");
        return;
    }

    document.getElementById("summary-year").innerHTML = options.years
        .map(year => `<option value="${year}" ${year === options.defaults.year ? 'selected' : ''}>${year}</option>`)
        .join("");

    document.getElementById("summary-variable").innerHTML = Object.keys(options.variableNameMapping)
        .map(varKey => `<option value="${varKey}" ${varKey === options.defaults.variable ? 'selected' : ''}>${options.variableNameMapping[varKey]}</option>`)
        .join("");

    document.getElementById("summary-strip").innerHTML = Object.keys(options.stripNameMapping)
        .map(stripKey => `<option value="${stripKey}" ${stripKey === options.defaults.strip ? 'selected' : ''}>${options.stripNameMapping[stripKey]}</option>`)
        .join("");

    document.getElementById("summary-granularity").innerHTML = Object.keys(options.granularityNameMapping)
        .map(granKey => `<option value="${granKey}" ${granKey === options.defaults.granularity ? 'selected' : ''}>${options.granularityNameMapping[granKey]}</option>`)
        .join("");

    document.getElementById("summary-depth").innerHTML = Object.keys(options.depthMapping)
        .map(depthKey => `<option value="${depthKey}" ${depthKey === options.defaults.depth ? 'selected' : ''}>${options.depthMapping[depthKey]}</option>`)
        .join("");

    // ✅ Initialize Tippy.js tooltips
        tippy('[data-tippy-content]');
    console.log("✅ Summary dropdowns successfully populated.");
}

export { fetchDefaultsAndOptions, populateSelect, getSelectedFilters, populateDropdownsByTab};
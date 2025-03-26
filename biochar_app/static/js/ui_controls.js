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

        console.log("🔍 Raw response from /get_defaults_and_options:", response); // ✅ Print raw response

        if (!response.ok) {
            throw new Error(`Server error: ${response.status} - ${response.statusText}`);
        }

        const data = await response.json();
        console.log("✅ Parsed JSON response:", data); // ✅ Print parsed JSON response

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
        console.error(`❌ populateSelect: Missing tabName or stateKey in populateSelect: tabName=${tabName}, stateKey=${stateKey}`);
        return;
    }

    const selectElement = document.querySelector(`.${stateKey}-dropdown[data-tab="${tabName}"]`);

    if (!selectElement) {
        console.error(`❌ populateSelect: Dropdown not found for ${tabName}: ${stateKey}`);
        return;
    }

    // ✅ Ensure options is an array before iterating
    if (!Array.isArray(options)) {
        console.error(`❌ options is not an array for ${stateKey} in ${tabName}. Got:`, options);
        return;
    }

    console.log(`🔍 Default value for ${stateKey} in ${tabName}:`, defaultValue);

    selectElement.innerHTML = ""; // Clear existing options

    options.forEach(({ value, label }) => {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = label;
        selectElement.appendChild(option);
    });

    if (defaultValue) {
        selectElement.value = defaultValue;  // ✅ Set the default value in the dropdown
    } else {
        console.warn(`⚠️ No default value found for ${stateKey} in ${tabName}.`);
    }

    stateObject[stateKey] = defaultValue;

    console.log(`✅ Successfully populated ${stateKey} dropdown for ${tabName}`);
}

// ✅ Returns selected filters based on the active tab
function getSelectedFilters(tab) {
    console.log(`🔍 Getting selected filters for: ${tab}`);

    let filters = {};

    if (tab === "main") {
        filters = {
            year: document.getElementById("main-year").value,
            startDate: document.getElementById("start-date").value,
            endDate: document.getElementById("end-date").value,
            granularity: document.getElementById("main-granularity").value,
            variable: document.getElementById("main-variable").value,
            strip: document.getElementById("main-strip").value,
            loggerLocation: document.getElementById("main-loggerLocation").value,
            depth: document.getElementById("main-depth").value,
        };
    } else if (tab === "summary") {
        filters = {
            year: document.getElementById("summary-year").value,
            granularity: document.getElementById("summary-granularity").value,
            variable: document.getElementById("summary-variable").value,
            strip: document.getElementById("summary-strip").value,
        };
    }

    console.log("✅ Selected Filters:", filters);
    return filters;
}

/**
 * ✅ Ensure event listeners are added after DOM is ready
 */
document.addEventListener("DOMContentLoaded", function () {
    console.log("🎯 Adding event listeners...");

    const updatePlotsButton = document.getElementById("update-plots");
    if (updatePlotsButton) {
        updatePlotsButton.addEventListener("click", updateMainDataDisplay);
    } else {
        console.error("❌ ui-controls: Button 'update-plots' not found in the DOM!");
    }

    const updateSummaryButton = document.getElementById("update-summary");
    if (updateSummaryButton) {
        updateSummaryButton.addEventListener("click", updateSummaryStatistics);
    } else {
        console.error("❌ ui-controls: Button 'update-summary' not found in the DOM!");
    }
});
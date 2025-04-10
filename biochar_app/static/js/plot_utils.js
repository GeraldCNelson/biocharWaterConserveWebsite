// plot_utils.js
import { getDropdownValue, getInputValue } from "./ui_utils.js";
import { getSelectedFilters } from "./ui_controls.js";

/* global Plotly */

/**
 * ✅ Fetch and Render Plot (raw or ratio)
 */
async function fetchAndRenderPlot(endpoint, plotContainerId, extraConfig = {}) {
    try {
        const config = {
            year: parseInt(getDropdownValue("main-year", true)),
            variable: getDropdownValue("main-variable"),
            strip: getDropdownValue("main-strip"),
            granularity: getDropdownValue("main-granularity"),
            loggerLocation: getDropdownValue("main-loggerLocation"),
            depth: getDropdownValue("main-depth"),
            startDate: getInputValue("start-date"),
            endDate: getInputValue("end-date"),
            traceOption: getDropdownValue("main-traceOption") || extraConfig.traceOption || "depths"  // ✅ FIXED HERE
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

        Plotly.react(plotContainerId, plotData.data, plotData.layout)
            .then(() => {
                Plotly.Plots.resize(document.getElementById(plotContainerId));
                console.log(`🔄 Forced resize on ${plotContainerId}`);
            });

        // ✅ Add window resize listener ONCE per plot container
        if (!window.__resizeListeners) {
            window.__resizeListeners = {};
        }
        if (!window.__resizeListeners[plotContainerId]) {
            window.addEventListener("resize", () => {
                Plotly.Plots.resize(document.getElementById(plotContainerId));
            });
            window.__resizeListeners[plotContainerId] = true;
        }

        console.log(`✅ Plot rendered in #${plotContainerId}`);
    } catch (error) {
        console.error(`❌ Error fetching plot from ${endpoint}:`, error);
    }
}

/**
 * ✅ Generic wait function for dropdowns
 */
async function waitForAllDropdowns(dropdownIds, timeout = 7000, postDelay = 200) {
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
                setTimeout(resolve, postDelay);
            } else if (elapsed >= timeout) {
                clearInterval(checkDropdowns);
                console.error(`❌ Timeout: These dropdowns didn’t load in time:`, missing);
                reject(new Error("Dropdowns not populated in time"));
            }

            elapsed += interval;
        }, interval);
    });
}


export { fetchAndRenderPlot, waitForAllDropdowns };
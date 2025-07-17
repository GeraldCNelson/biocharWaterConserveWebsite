// ui_controls.js
import { getDropdownValue, getInputValue } from "./ui_utils.js";
import { fetchAndRenderPlot, waitForAllDropdowns } from "./plot_utils.js";

// --- Global debug flag ---
const DEBUG = true;  // Set to false in production

/**
 * Initialize Bootstrap Datepickers on the Main Data Display tab.
 *
 * @param {[number,number]} years         – [minYear, maxYear]
 * @param {string} defaultStart           – ISO date "YYYY-MM-DD"
 * @param {string} defaultEnd             – ISO date "YYYY-MM-DD"
 */
export function initializeMainDatepickers(years, defaultStart, defaultEnd) {
  const [minYear, maxYear] = years;
  const $start = $('#main-startDate');
  const $end   = $('#main-endDate');

  $start.add($end).datepicker({
    format:     'yyyy-mm-dd',
    autoclose:  true,
    forceParse: false,
    startDate:  new Date(minYear, 0, 1),
    endDate:    new Date(maxYear, 11, 31),
  });

  // Seed initial values
  $start.datepicker('update', defaultStart);
  $end  .datepicker('update', defaultEnd);

  // When year changes, reset start & fetch new end
  $('#main-year').on('change', async function () {
    const y = parseInt(this.value, 10);
    if (isNaN(y)) return;

    const startIso = `${y}-01-01`;
    $start.datepicker('update', startIso);

    try {
      const resp = await fetch(`/api/get_end_date?year=${y}`);
      if (!resp.ok) throw new Error(`Status ${resp.status}`);
      const { endDate } = await resp.json();
      $end.datepicker('update', endDate);
    } catch (err) {
      console.error("Could not fetch end date, falling back to Dec 31:", err);
      $end.datepicker('update', `${y}-12-31`);
    }
  });
}

// Ensure global state objects exist
window.mainDataDisplayConfig ||= { year: null, strip: null, variable: null, loggerLocation: null, depth: null };
window.summaryStatsDisplay   ||= { year: null, variable: null, strip: null, granularity: null };

/**
 * Fetch defaults & dropdown options from the API.
 * @returns {Object|null} options or null on error
 */
export async function fetchDefaultsAndOptions() {
  console.log("📡 Fetching default values and dropdown options...");
  try {
    const response = await fetch("/api/get_defaults_and_options");
    if (!response.ok) {
      throw new Error(`Server error: ${response.status} - ${response.statusText}`);
    }
    const options = await response.json();
    console.log("✅ Parsed JSON response:", options);
    if (!options.defaults) {
      throw new Error("🚨 Defaults not found in response!");
    }
    // store mappings globally for other modules
    window.depthMapping          = options.depthMapping;
    window.loggerLocationMapping = options.loggerLocations.reduce((m, o) => ({ ...m, [o.value]: o.label }), {});
    window.variableNameMapping   = options.variableNameMapping || {};
    return options;
  } catch (error) {
    console.error("❌ fetchDefaultsAndOptions: Failed to load options:", error);
    return null;
  }
}

/**
 * Populate a <select> by CSS selector lookup.
 *
 * @param {Object} stateObject     – e.g. window.mainDataDisplayConfig
 * @param {string} stateKey        – the key in stateObject, e.g. "year"
 * @param {Array<{value,label}>} options
 * @param {string|number} defaultValue
 * @param {string} tabName         – "main" or "summary"
 */
export function populateSelect(stateObject, stateKey, options, defaultValue, tabName) {
  console.log(`🔍 populateSelect -> tab: ${tabName}, key: ${stateKey}`);
  const selectEl = document.querySelector(`.${stateKey}-dropdown[data-tab="${tabName}"]`);
  if (!selectEl) {
    console.error(`❌ populateSelect: Dropdown not found for ${tabName}.${stateKey}`);
    return;
  }
  selectEl.innerHTML = options.map(({ value, label }) =>
    `<option value="${value}">${label}</option>`
  ).join("");
  if (defaultValue != null) {
    selectEl.value = defaultValue;
  } else {
    console.warn(`⚠️ No default for ${tabName}.${stateKey}`);
  }
  stateObject[stateKey] = selectEl.value;
  console.log(`✅ Populated ${tabName}.${stateKey} = ${selectEl.value}`);
}

/**
 * Read all filters for "main" or "summary" tab.
 * @param {"main"|"summary"} tab
 */
export function getSelectedFilters(tab) {
  console.log(`🔍 Getting selected filters for: ${tab}`);
  const prefix = tab === "main" ? "main" : "summary";
  const filters = {
    year:        getDropdownValue(`${prefix}-year`, true),
    granularity: getDropdownValue(`${prefix}-granularity`),
    variable:    getDropdownValue(`${prefix}-variable`),
    strip:       getDropdownValue(`${prefix}-strip`)
  };
  if (tab === "main") {
    Object.assign(filters, {
      startDate:      getInputValue("main-startDate"),
      endDate:        getInputValue("main-endDate"),
      loggerLocation: getDropdownValue("main-loggerLocation"),
      depth:          getDropdownValue("main-depth"),
      traceOption:    getDropdownValue("main-traceOption"),
    });
  } else {
    filters.depth = getDropdownValue("summary-depth");
  }
  const metricToggle = document.getElementById("units-toggle_main");
  filters.unitSystem = metricToggle?.checked ? "metric" : "us";
  console.log("✅ Selected Filters:", filters);
  return filters;
}

/**
 * Update all depth‐dropdown labels and the "Plots Based On" label.
 */
export function updateDepthLabels(depthMapping, unitSystem) {
  console.log("👀 updateDepthLabels:", depthMapping, unitSystem);

  document.querySelectorAll(".depth-dropdown").forEach(select => {
    for (let option of select.options) {
      const key = option.value;
      if (depthMapping[key]) {
        option.text = depthMapping[key][unitSystem];
      }
    }
  });

  const traceLabel = document.querySelector('label[for="main-traceOption"]');
  if (traceLabel && window.depthMapping) {
    const nums = Object.values(window.depthMapping)
                       .map(m => m[unitSystem].replace(/[^\d]/g, ""));
    const unit = unitSystem === "metric" ? "cm" : "inches";
    traceLabel.textContent = `Plots Based On: ${nums.join(", ")} ${unit}`;
  }
}

/**
 * Populate a <select> element with sorted options, set a default, and apply labels.
 *
 * @param {string} elementId
 * @param {Array<string|number>} values
 * @param {string|number} defaultValue
 * @param {Object<string,string>|Array<string>} labelMapping
 */
export function populateAllDropdowns(options, unitSystem) {
  // collect all of the 'source' names from your two tabs
  const allSources = [
    ...dropdownConfigs.main.map(cfg => cfg.source),
    ...dropdownConfigs.summary.map(cfg => cfg.source),
  ];
  console.log("🔑 Populating dropdowns; sources =", allSources);
  console.log("📦 JSON options keys =", Object.keys(options));

  ["main", "summary"].forEach((tab) => {
    dropdownConfigs[tab].forEach(({ id, source, labelMap, formatter }) => {
      const selectId = `${tab}-${id}`;
      const list = options[source];

      if (!Array.isArray(list)) {
        console.warn(`⚠️ Skipping '${source}' because options['${source}'] is not an array:`, list);
        return;
      }

      // Determine whether this is an array of {value,label} objects or primitives
      let values, labels;
      if (
        list.length > 0 &&
        typeof list[0] === "object" &&
        list[0] !== null &&
        "value" in list[0] &&
        "label" in list[0]
      ) {
        // object‐based
        values = list.map(item => item.value);
        labels = list.map(item => item.label);
      } else {
        // primitive‐based
        values = list;
        labels = list.map(item => String(item));
      }

      console.log(`[${tab}] '${id}' values =`, values);
      console.log(`[${tab}] '${id}' labels =`, labels);

      const defaultValue = options.defaults[id];
      populateDropdown(selectId, values, defaultValue, labels);
    });
  });
}

/**
 * Populate a <select> with sorted options, pick a default, and apply labels.
 *
 * @param {string} elementId               – id of the <select> element
 * @param {Array<string|number>} values    – list of option values
 * @param {string|number} defaultValue     – which value to mark `selected`
 * @param {Array<string>|Object<string,string>} labelMapping
 *        – either:
 *           • an array of labels parallel to `values`, or
 *           • an object mapping value→label
 */
export function populateDropdown(elementId, values, defaultValue, labelMapping = {}) {
  const selectEl = document.getElementById(elementId);
  if (!selectEl) {
    console.warn(`⚠️ Dropdown not found: ${elementId}`);
    return;
  }

  // 1) Clone & sort
  const sorted = [...values];
  const allNum = sorted.every(v => typeof v === "number" || (!isNaN(v) && v !== ""));
  sorted.sort(
    allNum
      ? (a, b) => Number(a) - Number(b)
      : (a, b) => String(a).localeCompare(String(b))
  );

  // 2) Build option HTML
  const defStr = String(defaultValue);
  const html = sorted.map((v, idx) => {
    const val = String(v);
    let label;
    if (Array.isArray(labelMapping)) {
      label = labelMapping[idx] ?? val;
    } else {
      label = labelMapping[val] ?? val;
    }
    const sel = (val === defStr) ? " selected" : "";
    return `<option value="${val}"${sel}>${label}</option>`;
  }).join("");

  // 3) Inject into the DOM
  selectEl.innerHTML = html;
}

/**
 * Handle change of the "Plots Based On" radio to swap depth vs. loggerLocation.
 */
export function handleTraceOptionChange() {
  const mode = document.getElementById("main-traceOption")?.value;
  const mapping = (mode === "loggerLocation")
    ? window.loggerLocationMapping
    : window.depthMapping;
  const elementId = "main-depth";
  const values    = Object.keys(mapping);
  const labels    = Object.values(mapping).map(m => m.us);
  const defaultValue = mode === "loggerLocation"
    ? window.mainDataDisplayConfig.loggerLocation
    : window.mainDataDisplayConfig.depth;
  populateDropdown(elementId, values, defaultValue, Object.fromEntries(values.map((v,i) => [v, labels[i]])));
}

/**
 * Whenever the year selector changes, update the corresponding start/end inputs.
 *
 * @param {"main"|"summary"} prefix
 */
export function updateStartAndEndDatesFromYear(prefix) {
  const yearEl = document.getElementById(`${prefix}-year`);
  if (!yearEl) return;
  const yr = parseInt(yearEl.value, 10);
  if (isNaN(yr)) return;

  const startEl = document.getElementById(`${prefix}-startDate`);
  const endEl   = document.getElementById(`${prefix}-endDate`);
  if (startEl) startEl.value = `${yr}-01-01`;

  if (endEl) {
    fetch(`/api/get_end_date?year=${yr}`)
      .then(resp => {
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        return resp.json();
      })
      .then(json => { endEl.value = json.endDate; })
      .catch(() => { endEl.value = `${yr}-12-31`; });
  }
}
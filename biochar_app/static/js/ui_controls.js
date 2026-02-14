// ui_controls.js

/**
 * Define which dropdowns live on each tab and
 * where to pull their options from the server payload.
 */
export const dropdownConfigs = {
  main: [
    { id: "year",           source: "years" },
    { id: "variable",       source: "variables" },
    { id: "strip",          source: "strips" },
    { id: "granularity",    source: "granularities" },
    { id: "loggerLocation", source: "loggerLocations" },
    { id: "depth",          source: "depths" },
    { id: "traceOption",    source: "traceOptions" },
  ],
  summary: [
    { id: "year",        source: "years" },
    { id: "variable",    source: "variables" },
    { id: "strip",       source: "strips" },
    { id: "granularity", source: "granularities" },
    { id: "depth",       source: "depths" },
  ],
};

/**
 * Apply DATE_RANGES[year][granularity] to the main start/end date inputs.
 * Falls back to year-wide dates only if no mapping exists.
 */
export function applyDateRangeFromDefaults(year, granularity, dateRanges) {
  const y = String(year);

  // Default fallback (only used if no DATE_RANGES entry exists)
  let start = `${y}-01-01`;
  let end   = `${y}-12-31`;

  const r = dateRanges?.[y]?.[granularity] || dateRanges?.[Number(y)]?.[granularity];
  if (r?.min && r?.max) {
    start = r.min;
    end   = r.max;
  }

  const startInput = document.getElementById("main-startDate");
  const endInput   = document.getElementById("main-endDate");

  if (startInput) startInput.value = start;
  if (endInput)   endInput.value   = end;
}

/**
 * Wire up listeners so changing year or granularity updates the main date inputs
 * using DATE_RANGES.
 */
export function wireMainDateRangeListeners() {
  const yearEl = document.getElementById("main-year");
  const granEl = document.getElementById("main-granularity");
  const startEl = document.getElementById("main-startDate");
  const endEl   = document.getElementById("main-endDate");

  if (!yearEl || !granEl || !startEl || !endEl) {
    console.warn("wireMainDateRangeListeners: missing main-year, main-granularity, or date inputs");
    return;
  }

  // Track whether the user has manually edited dates
  const markUserEdited = () => {
    startEl.dataset.userEdited = "1";
    endEl.dataset.userEdited = "1";
  };
  startEl.addEventListener("change", markUserEdited);
  endEl.addEventListener("change", markUserEdited);

  const applyDefaults = () => {
    const year = yearEl.value;        // keep as string key
    const granularity = granEl.value;
    applyDateRangeFromDefaults(year, granularity, window.dateRanges || {});
  };

  // Initial set from DATE_RANGES is fine on first load
  applyDefaults();

  // YEAR change: reset to defaults for the new year and clear "user edited"
  yearEl.addEventListener("change", () => {
    delete startEl.dataset.userEdited;
    delete endEl.dataset.userEdited;
    applyDefaults();
  });

  // GRANULARITY change: only apply defaults if user hasn't edited dates
  granEl.addEventListener("change", () => {
    const userEdited = startEl.dataset.userEdited === "1" || endEl.dataset.userEdited === "1";
    if (!userEdited) {
      applyDefaults();
    } else {
      console.log("🗓️ Keeping user-selected date range (not overwriting on granularity change).");
    }
  });
}

/**
 * 1) Fetch the JSON of defaults & options from your API.
 */
export async function fetchDefaultsAndOptions() {
  console.log("📡 Fetching default values and dropdown options...");
  try {
    const response = await fetch("/api/get_defaults_and_options");
    if (!response.ok) {
      throw new Error(
        `Server error: ${response.status} – ${response.statusText}`
      );
    }
    const options = await response.json();
    console.log("✅ Parsed JSON response:", options);
    if (!options.defaults) {
      throw new Error("🚨 Defaults not found in response!");
    }

    // Persist for later modules
    window.dropdownOptions = options;
    window.depthMapping    = options.depthMapping;
    window.loggerLocationMapping = options.loggerLocations?.reduce(
      (m, o) => ({ ...m, [o.value]: o.label }),
      {}
    ) || {};

    // ✅ Persist DATE_RANGES globally (this is what your UI needs)
    // Depending on your backend, this might be in defaults.dateRanges or top-level dateRanges.
    window.dateRanges =
      options.defaults.dateRanges ||
      options.dateRanges ||
      {};

    // Variable label/name mappings from backend
    window.variableNameMapping =
      options.variableNameMapping || options.variable_name_mapping || {};

    // Pretty labels for variables (used in summary tables, seasonal UI, etc.)
    window.labelNameMapping =
      options.labelNameMapping || options.label_name_mapping || {};

    // Growing-season periods (JSON version of DEFAULT_GSEASON_PERIODS)
    window.gseasonPeriods =
      options.gseasonPeriods || options.gseason_periods || {};

    console.log("🧭 depthMapping from backend:", window.depthMapping);
    console.log("🗓️ dateRanges from backend:", window.dateRanges);
    console.log("🌱 gseasonPeriods from backend:", window.gseasonPeriods);
    console.log("🏷️ labelNameMapping from backend:", window.labelNameMapping);

    return options;
  } catch (err) {
    console.error("❌ fetchDefaultsAndOptions failed:", err);
    return null;
  }
}

/**
 * 2) Populate every <select> across both tabs using your mapping.
 *    We no longer sort — we respect the server’s order.
 */
export function populateAllDropdowns(options) {
  console.log("🔑 Populating dropdowns; sources =", Object.keys(options));

  ["main", "summary"].forEach((tab) => {
    dropdownConfigs[tab].forEach(({ id, source }) => {
      const selectId = `${tab}-${id}`;
      const list     = options[source];
      if (!Array.isArray(list)) {
        console.warn(`⚠️ Skipping '${source}', not an array:`, list);
        return;
      }

      let values, labels;
      if (
        list.length > 0 &&
        list[0] != null &&
        typeof list[0] === "object" &&
        "value" in list[0] &&
        "label" in list[0]
      ) {
        values = list.map((item) => item.value);
        labels = list.map((item) => item.label);
      } else {
        values = list;
        labels = list.map((item) => String(item));
      }

      console.log(`[${tab}] ${id}:`, values, labels);
      populateDropdown(selectId, values, options.defaults[id], labels);
    });
  });
}

/**
 * Helper to fill a single <select> with <option>s.
 */
export function populateDropdown(
  elementId,
  values,
  defaultValue,
  labelMapping = {}
) {
  const selectEl = document.getElementById(elementId);
  if (!selectEl) {
    console.warn(`⚠️ Dropdown not found: ${elementId}`);
    return;
  }

  const defStr = String(defaultValue);
  const html = values
    .map((v, idx) => {
      const val = String(v);
      let label;
      if (Array.isArray(labelMapping)) {
        label = labelMapping[idx] ?? val;
      } else {
        label = labelMapping[val] ?? val;
      }
      const sel = val === defStr ? " selected" : "";
      return `<option value="${val}"${sel}>${label}</option>`;
    })
    .join("");

  selectEl.innerHTML = html;
}

/**
 * Collects all of the controls for the given tab, and if on the Main tab
 * with granularity="gseason", also pulls in your custom-season rows.
 */
export function getSelectedFilters(tab) {
  const keys = [
    "year",
    "startDate",
    "endDate",
    "variable",
    "strip",
    "granularity",
    "loggerLocation",
    "depth",
    "traceOption",
  ];

  const filters = keys.reduce((acc, id) => {
    const el = document.getElementById(`${tab}-${id}`);
    if (el) acc[id] = el.value;
    return acc;
  }, {});

  if (tab === "main" && filters.granularity === "gseason") {
    const periods = Array.from(document.querySelectorAll(".period-row")).map(
      (row) => {
        const code  = row.dataset.code;
        const label = row.querySelector(".period-label")?.value;
        const start = row.querySelector(".period-start")?.value;
        const end   = row.querySelector(".period-end")?.value;
        return { code, label, start, end };
      }
    );
    filters.periods = periods;
  }

  return filters;
}

/**
 * Update the depth dropdown labels on both tabs
 * based on window.depthMapping and the current unit system.
 */
export function updateDepthLabels(unitSystem) {
  console.log("🔁 [updateDepthLabels] unitSystem =", unitSystem);
  console.log("🔁 [updateDepthLabels] depthMapping =", window.depthMapping);

  if (!window.depthMapping) {
    console.warn("[updateDepthLabels] ❗ window.depthMapping is missing");
    return;
  }

  const selects = document.querySelectorAll("select.depth-dropdown");
  if (!selects.length) {
    console.warn(
      "[updateDepthLabels] ⚠️ No <select class='depth-dropdown'> elements found"
    );
    return;
  }

  selects.forEach((select) => {
    console.log(
      `[updateDepthLabels] Updating select#${select.id} with ${select.options.length} options`
    );

    Array.from(select.options).forEach((opt, idx) => {
      const rawValue = opt.value;
      const key =
        rawValue && window.depthMapping[rawValue]
          ? rawValue
          : String(idx + 1);

      const mapping = window.depthMapping[key];

      if (mapping && mapping[unitSystem]) {
        const oldText = opt.text;
        const newText = mapping[unitSystem];
        opt.text = newText;
        console.log(
          `[updateDepthLabels]  • ${select.id}: option index=${idx}, key=${key}, ` +
            `old="${oldText}" → new="${newText}"`
        );
      }
    });
  });
}

/**
 * Wire up the two main‐tab date inputs.
 * Uses DATE_RANGES if available; falls back to defaults.
 */
export function initializeMainDatepickers() {
  if (!window.dropdownOptions?.defaults) return;

  const startEl = document.getElementById("main-startDate");
  const endEl   = document.getElementById("main-endDate");
  if (!startEl || !endEl) {
    console.warn("⚠️ Main date inputs not found");
    return;
  }

  startEl.type = "date";
  endEl.type   = "date";

  // Prefer DATE_RANGES for initial values
  const defaults = window.dropdownOptions.defaults;
  const year = String(defaults.year);
  const granularity = defaults.granularity;

  // This sets start/end to DATE_RANGES if present, else year-wide
  applyDateRangeFromDefaults(year, granularity, window.dateRanges || {});

  // If you really want to fall back to backend defaults when no dateRanges exist:
  if (!startEl.value) startEl.value = defaults.startDate;
  if (!endEl.value)   endEl.value   = defaults.endDate;

  window.mainDatepickers = { start: startEl, end: endEl };
}

/**
 * Placeholder for traceOption logic (kept so imports don’t break).
 */
export function handleTraceOptionChange(event) {
  // Implement as needed if you change trace behavior in the future.
}
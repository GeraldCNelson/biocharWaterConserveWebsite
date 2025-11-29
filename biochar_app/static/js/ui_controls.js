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
    window.loggerLocationMapping = options.loggerLocations.reduce(
      (m, o) => ({ ...m, [o.value]: o.label }),
      {}
    );
    window.variableNameMapping = options.variableNameMapping || {};

    console.log("🧭 depthMapping from backend:", window.depthMapping);

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
        // object form
        values = list.map((item) => item.value);
        labels = list.map((item) => item.label);
      } else {
        // primitive form
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
 * with granularity="gseason", also pulls in your custom‐season rows.
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

  // 1) collect all the simple dropdown/text values
  const filters = keys.reduce((acc, id) => {
    const el = document.getElementById(`${tab}-${id}`);
    if (el) acc[id] = el.value;
    return acc;
  }, {});

  // 2) if we're on Main + “Growing Season”, scrape all your .period-row cards
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
 *
 * depthMapping is expected to look like:
 * {
 *   "1": { us: "6 inches",  metric: "15 cm" },
 *   "2": { us: "12 inches", metric: "30 cm" },
 *   "3": { us: "18 inches", metric: "45 cm" }
 * }
 */
export function updateDepthLabels(unitSystem) {
  console.log("🔁 [updateDepthLabels] unitSystem =", unitSystem);
  console.log("🔁 [updateDepthLabels] depthMapping =", window.depthMapping);

  if (!window.depthMapping) {
    console.warn("[updateDepthLabels] ❗ window.depthMapping is missing");
    return;
  }

  // Use the class shared by both Main & Summary depth selects
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
      // Prefer the option value as the key; fall back to 1-based index
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
      } else {
        console.log(
          `[updateDepthLabels]  • ${select.id}: no mapping for key=${key} ` +
            `under unitSystem=${unitSystem}`
        );
      }
    });
  });
}

/**
 * Wire up the two main‐tab date inputs.
 */
export function initializeMainDatepickers() {
  if (!window.dropdownOptions?.defaults) return;
  const { startDate, endDate } = window.dropdownOptions.defaults;
  const startEl = document.getElementById("main-startDate");
  const endEl   = document.getElementById("main-endDate");
  if (!startEl || !endEl) {
    console.warn("⚠️ Main date inputs not found");
    return;
  }

  // force them to be HTML5 date inputs
  startEl.type  = "date";
  endEl.type    = "date";
  startEl.value = startDate;
  endEl.value   = endDate;

  // store references so updateStartAndEndDatesFromYear can reset them
  window.mainDatepickers = { start: startEl, end: endEl };
}

/**
 * Reset the start/end inputs to the full selected year.
 */
export function updateStartAndEndDatesFromYear(year) {
  const y = String(year);
  const start = `${y}-01-01`;
  const end   = `${y}-12-31`;

  if (window.mainDatepickers?.start && window.mainDatepickers?.end) {
    window.mainDatepickers.start.value = start;
    window.mainDatepickers.end.value   = end;
  } else {
    console.warn(
      "updateStartAndEndDatesFromYear: date inputs not initialized yet",
      window.mainDatepickers
    );
  }
}

/**
 * Placeholder for traceOption logic (kept so imports don’t break).
 */
export function handleTraceOptionChange(event) {
  // Implement as needed if you change trace behaviour in the future.
}
// @ts-check
// ui_controls.js

/**
 * @typedef {Window & {
 *   dateRanges?: Record<string, any>,
 *   dropdownOptions?: Record<string, any>,
 *   depthMapping?: Record<string, any>,
 *   loggerLocationMapping?: Record<string, any>,
 *   variableNameMapping?: Record<string, any>,
 *   labelNameMapping?: Record<string, any>,
 *   gseasonPeriods?: Record<string, any>,
 *   unitSystem?: string,
 *   mainDatepickers?: { start: HTMLInputElement, end: HTMLInputElement }
 * }} UiWindow
 */

/** @type {UiWindow} */
const uiWindow = /** @type {UiWindow} */ (window);

/**
 * Define which dropdowns live on each tab and
 * where to pull their options from the server payload.
 */
export const dropdownConfigs = {
  main: [
    { id: "year", source: "years" },
    { id: "variable", source: "variables" },
    { id: "strip", source: "strips" },
    { id: "granularity", source: "granularities" },
    { id: "loggerLocation", source: "loggerLocations" },
    { id: "depth", source: "depths" },
    { id: "traceOption", source: "traceOptions" },
  ],
  summary: [
    { id: "year", source: "years" },
    { id: "variable", source: "variables" },
    { id: "strip", source: "strips" },
    { id: "granularity", source: "granularities" },
    { id: "depth", source: "depths" },
  ],
};

/**
 * Apply DATE_RANGES[year][granularity] to the main start/end date inputs.
 * Falls back to year-wide dates only if no mapping exists.
 *
 * @param {string|number} year
 * @param {string} granularity
 * @param {any} dateRanges
 */
export function applyDateRangeFromDefaults(year, granularity, dateRanges) {
  const y = String(year);

  let start = `${y}-01-01`;
  let end = `${y}-12-31`;

  const r = dateRanges?.[y]?.[granularity] || dateRanges?.[Number(y)]?.[granularity];
  if (r?.min && r?.max) {
    start = r.min;
    end = r.max;
  }

  const startInput = /** @type {HTMLInputElement | null} */ (
    document.getElementById("main-startDate")
  );
  const endInput = /** @type {HTMLInputElement | null} */ (
    document.getElementById("main-endDate")
  );

  if (startInput) startInput.value = start;
  if (endInput) endInput.value = end;
}

/**
 * Wire up listeners so changing year or granularity updates the main date inputs
 * using DATE_RANGES.
 */
export function wireMainDateRangeListeners() {
  const yearEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("main-year")
  );
  const granEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("main-granularity")
  );
  const startEl = /** @type {HTMLInputElement | null} */ (
    document.getElementById("main-startDate")
  );
  const endEl = /** @type {HTMLInputElement | null} */ (
    document.getElementById("main-endDate")
  );

  if (!yearEl || !granEl || !startEl || !endEl) {
    console.warn("wireMainDateRangeListeners: missing main-year, main-granularity, or date inputs");
    return;
  }

  const markUserEdited = () => {
    startEl.dataset.userEdited = "1";
    endEl.dataset.userEdited = "1";
  };
  startEl.addEventListener("change", markUserEdited);
  endEl.addEventListener("change", markUserEdited);

  const applyDefaults = () => {
    const year = yearEl.value;
    const granularity = granEl.value;
    applyDateRangeFromDefaults(year, granularity, uiWindow.dateRanges || {});
  };

  applyDefaults();

  yearEl.addEventListener("change", () => {
    delete startEl.dataset.userEdited;
    delete endEl.dataset.userEdited;
    applyDefaults();
  });

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
 * @returns {Promise<any>}
 */
export async function fetchDefaultsAndOptions() {
  console.log("📡 Fetching default values and dropdown options...");
  try {
    const response = await fetch("/api/get_defaults_and_options");
    if (!response.ok) {
      throw new Error(`Server error: ${response.status} – ${response.statusText}`);
    }
    const options = await response.json();
    console.log("✅ Parsed JSON response:", options);
    if (!options.defaults) {
      throw new Error("🚨 Defaults not found in response!");
    }

    uiWindow.dropdownOptions = options;
    uiWindow.depthMapping = options.depthMapping;
    uiWindow.loggerLocationMapping = options.loggerLocations?.reduce(
      /** @param {Record<string, string>} m @param {{value:string,label:string}} o */
      (m, o) => ({ ...m, [o.value]: o.label }),
      {}
    ) || {};

    uiWindow.dateRanges =
      options.defaults.dateRanges ||
      options.dateRanges ||
      {};

    uiWindow.variableNameMapping =
      options.variableNameMapping || options.variable_name_mapping || {};

    uiWindow.labelNameMapping =
      options.labelNameMapping || options.label_name_mapping || {};

    uiWindow.gseasonPeriods =
      options.gseasonPeriods || options.gseason_periods || {};

    console.log("🧭 depthMapping from backend:", uiWindow.depthMapping);
    console.log("🗓️ dateRanges from backend:", uiWindow.dateRanges);
    console.log("🌱 gseasonPeriods from backend:", uiWindow.gseasonPeriods);
    console.log("🏷️ labelNameMapping from backend:", uiWindow.labelNameMapping);

    return options;
  } catch (err) {
    console.error("❌ fetchDefaultsAndOptions failed:", err);
    return null;
  }
}

/**
 * 2) Populate every <select> across both tabs using your mapping.
 *    We no longer sort — we respect the server’s order.
 *
 * @param {any} options
 */
export function populateAllDropdowns(options) {
  console.log("🔑 Populating dropdowns; sources =", Object.keys(options));

  /** @type {("main"|"summary")[]} */
  const tabs = ["main", "summary"];

  tabs.forEach((tab) => {
    dropdownConfigs[tab].forEach(({ id, source }) => {
      const selectId = `${tab}-${id}`;
      const list = options[source];
      if (!Array.isArray(list)) {
        console.warn(`⚠️ Skipping '${source}', not an array:`, list);
        return;
      }

      /** @type {any[]} */
      let values;
      /** @type {any[]|Record<string, string>} */
      let labels;

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
 *
 * @param {string} elementId
 * @param {any[]} values
 * @param {any} defaultValue
 * @param {any[]|Record<string, string>} [labelMapping={}]
 */
export function populateDropdown(
  elementId,
  values,
  defaultValue,
  labelMapping = {}
) {
  const selectEl = /** @type {HTMLSelectElement | null} */ (
    document.getElementById(elementId)
  );
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
 *
 * @param {string} tab
 * @returns {Record<string, any> | null}
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

  /** @type {{
   *   year?: string,
   *   startDate?: string,
   *   endDate?: string,
   *   variable?: string,
   *   strip?: string,
   *   granularity?: string,
   *   loggerLocation?: string,
   *   depth?: string,
   *   traceOption?: string,
   *   periods?: Array<{code?: string, label?: string, start?: string, end?: string}>,
   *   unitSystem?: string
   * } & Record<string, any>} */
  const filters = keys.reduce((acc, id) => {
    const el = /** @type {HTMLInputElement | HTMLSelectElement | null} */ (
      document.getElementById(`${tab}-${id}`)
    );
    if (el) acc[id] = el.value;
    return acc;
  }, /** @type {any} */ ({}));

  if (tab === "main") {
    const start = (filters.startDate || "").trim();
    const end = (filters.endDate || "").trim();

    /**
     * @param {string} value
     * @returns {Date | null}
     */
    function parseStrictDate(value) {
      if (!value) return null;

      let m = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (m) {
        const year = parseInt(m[1], 10);
        const month = parseInt(m[2], 10);
        const day = parseInt(m[3], 10);

        if (month < 1 || month > 12 || day < 1) return null;

        const daysInMonth = new Date(year, month, 0).getDate();
        if (day > daysInMonth) return null;

        return new Date(year, month - 1, day);
      }

      m = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
      if (m) {
        const month = parseInt(m[1], 10);
        const day = parseInt(m[2], 10);
        const year = parseInt(m[3], 10);

        if (month < 1 || month > 12 || day < 1) return null;

        const daysInMonth = new Date(year, month, 0).getDate();
        if (day > daysInMonth) return null;

        return new Date(year, month - 1, day);
      }

      return null;
    }

    console.log("📅 main date values from getSelectedFilters:", {
      startDate: filters.startDate,
      endDate: filters.endDate,
    });

    const startDateObj = parseStrictDate(start);
    const endDateObj = parseStrictDate(end);

    const startMissingOrInvalid = !start || !startDateObj;
    const endMissingOrInvalid = !end || !endDateObj;

    if (startMissingOrInvalid || endMissingOrInvalid) {
      let whichDate = "date range";
      if (startMissingOrInvalid && endMissingOrInvalid) {
        whichDate = "start and end dates";
      } else if (startMissingOrInvalid) {
        whichDate = "start date";
      } else if (endMissingOrInvalid) {
        whichDate = "end date";
      }

      alert(
        `The ${whichDate} is invalid.\n\n` +
        `Please revise the ${whichDate} and click Update Plots to see the new plots.`
      );

      return null;
    }

    if (startDateObj > endDateObj) {
      alert(
        "The start date is after the end date.\n\n" +
        "Please revise the date range and click Update Plots to see the new plots."
      );
      return null;
    }
  }

  if (tab === "main" && filters.granularity === "gseason") {
    const periods = Array.from(document.querySelectorAll(".period-row")).map((row) => {
      const rowEl = /** @type {HTMLElement} */ (row);
      const code = rowEl.dataset.code;

      const labelEl = /** @type {HTMLInputElement | null} */ (
        rowEl.querySelector(".period-label")
      );
      const startEl = /** @type {HTMLInputElement | null} */ (
        rowEl.querySelector(".period-start")
      );
      const endEl = /** @type {HTMLInputElement | null} */ (
        rowEl.querySelector(".period-end")
      );

      const label = labelEl?.value;
      const start = startEl?.value;
      const end = endEl?.value;
      return { code, label, start, end };
    });
    filters.periods = periods;
  }

  if (tab === "main") {
    filters.unitSystem = uiWindow.unitSystem || "us";
  }

  return filters;
}

/**
 * Update the depth dropdown labels on both tabs
 * based on window.depthMapping and the current unit system.
 *
 * @param {string} unitSystem
 */
export function updateDepthLabels(unitSystem) {
  console.log("🔁 [updateDepthLabels] unitSystem =", unitSystem);
  console.log("🔁 [updateDepthLabels] depthMapping =", uiWindow.depthMapping);

  if (!uiWindow.depthMapping) {
    console.warn("[updateDepthLabels] ❗ uiWindow.depthMapping is missing");
    return;
  }

  const selects = /** @type {NodeListOf<HTMLSelectElement>} */ (
    document.querySelectorAll("select.depth-dropdown")
  );
  if (!selects.length) {
    console.warn("[updateDepthLabels] ⚠️ No <select class='depth-dropdown'> elements found");
    return;
  }

  selects.forEach((select) => {
    console.log(
      `[updateDepthLabels] Updating select#${select.id} with ${select.options.length} options`
    );

    Array.from(select.options).forEach((opt, idx) => {
      const rawValue = opt.value;
      const key =
        rawValue && uiWindow.depthMapping?.[rawValue]
          ? rawValue
          : String(idx + 1);

      const mapping = uiWindow.depthMapping?.[key];

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
 * Wire up the two main-tab date inputs.
 * Uses DATE_RANGES if available; falls back to defaults.
 */
export function initializeMainDatepickers() {
  if (!uiWindow.dropdownOptions?.defaults) return;

  const startEl = /** @type {HTMLInputElement | null} */ (
    document.getElementById("main-startDate")
  );
  const endEl = /** @type {HTMLInputElement | null} */ (
    document.getElementById("main-endDate")
  );
  if (!startEl || !endEl) {
    console.warn("⚠️ Main date inputs not found");
    return;
  }

  startEl.type = "date";
  endEl.type = "date";
  attachNativeDateInputGuards(startEl, endEl);

  /**
   * @param {string} value
   * @returns {string}
   */
  function toIsoDate(value) {
    if (!value) return "";

    const s = String(value).trim();

    let m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (m) return s;

    m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) {
      const month = m[1].padStart(2, "0");
      const day = m[2].padStart(2, "0");
      const year = m[3];
      return `${year}-${month}-${day}`;
    }

    console.warn("⚠️ Could not parse date:", value);
    return "";
  }

  const defaults = uiWindow.dropdownOptions.defaults;
  const year = String(defaults.year);
  const granularity = defaults.granularity;

  applyDateRangeFromDefaults(year, granularity, uiWindow.dateRanges || {});

  startEl.value = toIsoDate(startEl.value) || `${year}-01-01`;
  endEl.value = toIsoDate(endEl.value) || `${year}-12-31`;

  const startFallback = toIsoDate(defaults.startDate);
  const endFallback = toIsoDate(defaults.endDate);

  if (startFallback) startEl.value = startFallback;
  if (endFallback) endEl.value = endFallback;

  uiWindow.mainDatepickers = { start: startEl, end: endEl };
}

/**
 * Placeholder for traceOption logic (kept so imports don’t break).
 *
 * @param {Event} event
 */
export function handleTraceOptionChange(event) {
  void event;
}

/**
 * @param {HTMLInputElement} startEl
 * @param {HTMLInputElement} endEl
 */
function attachNativeDateInputGuards(startEl, endEl) {
  /**
   * @param {HTMLInputElement} el
   */
  function clearIfInvalid(el) {
    el.addEventListener("blur", () => {
      if (!el.value) {
        el.value = "";
      }
    });

    el.addEventListener("change", () => {
      if (!el.value) {
        el.value = "";
      }
    });
  }

  clearIfInvalid(startEl);
  clearIfInvalid(endEl);
}
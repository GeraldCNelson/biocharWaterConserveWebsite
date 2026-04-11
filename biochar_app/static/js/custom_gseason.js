// @ts-check
// static/js/custom_gseason.js

/**
 * Initialize the “Custom Season” editor.
 *
 * @param {{
 *   defaultYear: number,
 *   years: number[],
 *   defaultPeriods: Array<{code:string,label:string,start:string,end:string}>
 * }} cfg
 */
export function initCustomGseason(cfg) {
  const { defaultYear, years, defaultPeriods } = cfg;

  /** @type {Array<{
   *   code: string,
   *   isDefault: boolean,
   *   label: string,
   *   start: string,
   *   end: string,
   *   startMD?: string,
   *   endMD?: string
   * }>} */
  let periodsData = [];

  const yearSelect = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("anchor-year")
  );
  const addPeriodBtn = /** @type {HTMLButtonElement | null} */ (
    document.getElementById("add-period")
  );
  const container = /** @type {HTMLDivElement | null} */ (
    document.getElementById("periods-container")
  );

  if (!yearSelect || !addPeriodBtn || !container) {
    console.warn("⚠️ Custom gseason controls not found.");
    return () => periodsData;
  }

  const yearSelectEl = yearSelect;
  const addPeriodBtnEl = addPeriodBtn;
  const containerEl = container;

  // 1) populate anchor-year dropdown
  yearSelectEl.innerHTML = "";
  years.forEach((y) => {
    const opt = document.createElement("option");
    opt.value = String(y);
    opt.textContent = String(y);
    if (y === defaultYear) opt.selected = true;
    yearSelectEl.appendChild(opt);
  });

  // 2) initialize periodsData from defaults
  function initPeriodsData() {
    periodsData = defaultPeriods.map((p) => {
      const [sm] = p.start.split("-");
      const [em] = p.end.split("-");
      const wraps = parseInt(sm, 10) > parseInt(em, 10);
      const startYear = wraps ? defaultYear - 1 : defaultYear;
      const endYear = wraps ? defaultYear : defaultYear;

      return {
        code: p.code,
        isDefault: true,
        label: p.label,
        startMD: p.start,
        endMD: p.end,
        start: `${startYear}-${p.start}`,
        end: `${endYear}-${p.end}`,
      };
    });
  }

  // 3) render all period rows
  function renderPeriods() {
    containerEl.innerHTML = "";
    const anchor = parseInt(yearSelectEl.value, 10);

    periodsData.forEach((p, idx) => {
      // if default, recompute on year change
      if (p.isDefault && p.startMD && p.endMD) {
        const [sm] = p.startMD.split("-");
        const [em] = p.endMD.split("-");
        const wraps = parseInt(sm, 10) > parseInt(em, 10);
        const startYear = wraps ? anchor - 1 : anchor;
        const endYear = wraps ? anchor : anchor;
        p.start = `${startYear}-${p.startMD}`;
        p.end = `${endYear}-${p.endMD}`;
      }

      const row = document.createElement("div");
      row.className = "mb-3 p-3 border rounded bg-light";
      row.classList.add("period-row");
      row.dataset.index = String(idx);
      row.dataset.code = p.code;

      row.innerHTML = `
        <button
          type="button"
          class="btn-close float-end remove-period"
          aria-label="Remove period"
        ></button>
        <div class="mb-2">
          <label class="form-label">Period ${idx + 1} Name</label>
          <input
            type="text"
            class="form-control period-label"
            value="${p.label}"
            placeholder="Enter name"
          />
        </div>
        <div class="row g-2">
          <div class="col">
            <label class="form-label">Start</label>
            <input
              type="date"
              class="form-control period-start"
              value="${p.start}"
            />
          </div>
          <div class="col">
            <label class="form-label">End</label>
            <input
              type="date"
              class="form-control period-end"
              value="${p.end}"
            />
          </div>
        </div>
      `;
      containerEl.appendChild(row);

      const removeBtn = /** @type {HTMLButtonElement | null} */ (
        row.querySelector(".remove-period")
      );
      const labelInput = /** @type {HTMLInputElement | null} */ (
        row.querySelector(".period-label")
      );
      const startInput = /** @type {HTMLInputElement | null} */ (
        row.querySelector(".period-start")
      );
      const endInput = /** @type {HTMLInputElement | null} */ (
        row.querySelector(".period-end")
      );

      if (removeBtn) {
        removeBtn.onclick = () => {
          periodsData.splice(idx, 1);
          renderPeriods();
        };
      }

      if (labelInput) {
        labelInput.oninput = (e) => {
          const target = /** @type {HTMLInputElement | null} */ (e.target);
          periodsData[idx].label = target?.value || "";
        };
      }

      if (startInput) {
        startInput.onchange = (e) => {
          const target = /** @type {HTMLInputElement | null} */ (e.target);
          periodsData[idx].start = target?.value || "";
        };
      }

      if (endInput) {
        endInput.onchange = (e) => {
          const target = /** @type {HTMLInputElement | null} */ (e.target);
          periodsData[idx].end = target?.value || "";
        };
      }
    });
  }

  // 4) “+ Add Period” button
  addPeriodBtn.onclick = () => {
    const anchor = parseInt(yearSelectEl.value, 10);
    const newIdx = periodsData.length + 1;

    periodsData.push({
      code: `CUSTOM_${newIdx}`,
      isDefault: false,
      label: "",
      start: `${anchor}-01-01`,
      end: `${anchor}-12-31`,
    });

    renderPeriods();
  };

  // 5) re-render on anchor-year change
  yearSelectEl.onchange = renderPeriods;

  // 6) initial bootstrap
  initPeriodsData();
  renderPeriods();

  // 7) expose periodsData for debugging just before you POST
  return () => periodsData;
}
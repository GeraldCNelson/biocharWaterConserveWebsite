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

  // in-memory model
  let periodsData = [];

  // 1) populate anchor-year dropdown
  const yearSelect = document.getElementById("anchor-year");
  yearSelect.innerHTML = "";
  years.forEach((y) => {
    const opt = document.createElement("option");
    opt.value = y;
    opt.textContent = y;
    if (y === defaultYear) opt.selected = true;
    yearSelect.appendChild(opt);
  });

  // 2) initialize periodsData from defaults
  function initPeriodsData() {
    periodsData = defaultPeriods.map((p) => {
      const [sm] = p.start.split("-");
      const [em] = p.end.split("-");
      const wraps     = parseInt(sm, 10) > parseInt(em, 10);
      const startYear = wraps ? defaultYear - 1 : defaultYear;
      const endYear   = wraps ? defaultYear     : defaultYear;
      return {
        code:      p.code,
        isDefault: true,
        label:     p.label,
        startMD:   p.start,
        endMD:     p.end,
        start:     `${startYear}-${p.start}`,  // ISO format
        end:       `${endYear}-${p.end}`,
      };
    });
  }

  // 3) render all period rows
  function renderPeriods() {
    const container = document.getElementById("periods-container");
    container.innerHTML = "";
    const anchor = parseInt(yearSelect.value, 10);

    periodsData.forEach((p, idx) => {
      // if default, recompute on year–change
      if (p.isDefault) {
        const [sm] = p.startMD.split("-");
        const [em] = p.endMD.split("-");
        const wraps     = parseInt(sm, 10) > parseInt(em, 10);
        const startYear = wraps ? anchor - 1 : anchor;
        const endYear   = wraps ? anchor     : anchor;
        p.start = `${startYear}-${p.startMD}`;
        p.end   = `${endYear}-${p.endMD}`;
      }

      const row = document.createElement("div");
      row.className = "mb-3 p-3 border rounded bg-light";
      row.classList.add("period-row");
      row.dataset.index = idx;
      row.dataset.code  = p.code;

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
      container.appendChild(row);

      // remove‐period handler
      row.querySelector(".remove-period").onclick = () => {
        periodsData.splice(idx, 1);
        renderPeriods();
      };
      // live updates
      row.querySelector(".period-label").oninput  = e => { periodsData[idx].label = e.target.value; };
      row.querySelector(".period-start").onchange = e => { periodsData[idx].start = e.target.value; };
      row.querySelector(".period-end").onchange   = e => { periodsData[idx].end   = e.target.value; };
    });
  }

  // 4) “+ Add Period” button
  document.getElementById("add-period").onclick = () => {
    const anchor = parseInt(yearSelect.value, 10);
    const newIdx = periodsData.length + 1;
    periodsData.push({
      code:      `CUSTOM_${newIdx}`,
      isDefault: false,
      label:     "",
      start:     `${anchor}-01-01`,
      end:       `${anchor}-12-31`,
    });
    renderPeriods();
  };

  // 5) re-render on anchor-year change
  yearSelect.onchange = renderPeriods;

  // 6) initial bootstrap
  initPeriodsData();
  renderPeriods();

  // 7) expose periodsData for debugging just before you POST…
  //    (call this from your main.js right before fetch)
  return () => periodsData;
}
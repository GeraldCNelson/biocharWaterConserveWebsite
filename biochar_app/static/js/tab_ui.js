// biochar_app/static/js/tab_ui.js

/**
 * Shared section title builder for “table tabs” (NIR / Soil / Biomass).
 *
 * variant:
 *  - "nir"  -> uses nir-* CSS classes
 *  - "soil" -> uses soil-* CSS classes
 *  - any other string -> generic classes
 */
export function makeSetSectionTitle(titleText, subtitleText = "", variant = "nir") {
  const section = document.createElement("div");

  const v = (variant || "").toLowerCase().trim();
  if (v === "soil") {
    section.className = "soil-set-section mb-4";
  } else if (v === "nir") {
    section.className = "nir-set-section";
  } else {
    section.className = "set-section mb-4";
  }

  const h4 = document.createElement("h4");
  if (v === "soil") h4.className = "soil-set-title mb-2";
  else if (v === "nir") h4.className = "nir-set-title";
  else h4.className = "set-title mb-2";
  h4.textContent = titleText;
  section.appendChild(h4);

  if (subtitleText) {
    const p = document.createElement("p");
    if (v === "soil") p.className = "text-muted soil-set-subtitle";
    else if (v === "nir") p.className = "text-muted nir-set-subtitle";
    else p.className = "text-muted set-subtitle";
    p.textContent = subtitleText;
    section.appendChild(p);
  }

  return section;
}
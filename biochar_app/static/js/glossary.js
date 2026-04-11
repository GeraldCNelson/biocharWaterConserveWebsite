// glossary.js

export const glossarySections = [
  {
    key: "biochar_soil_basics",
    label: "Biochar & Soil Basics",
    items: [
      {
        term: "Biochar",
        definition:
          "A carbon-rich material produced by heating biomass under low-oxygen conditions. In soil, it can affect water holding, nutrient retention, bulk density, and microbial habitat.",
      },
      {
        term: "Pyrolysis",
        definition:
          "The thermal conversion process used to make biochar from biomass in a low-oxygen environment.",
      },
      {
        term: "Soil Organic Matter (SOM)",
        definition:
          "The organic fraction of soil, including decomposing plant material, microbial residues, and stable carbon compounds.",
      },
      {
        term: "Carbon Sequestration",
        definition:
          "The long-term storage of carbon in soil or biomass. Biochar is often studied for its potential to increase carbon storage.",
      },
      {
        term: "Bulk Density",
        definition:
          "The mass of dry soil per unit volume, usually expressed as g/cm³. Lower bulk density often indicates greater pore space and improved root penetration.",
      },
      {
        term: "Cation Exchange Capacity (CEC)",
        definition:
          "A measure of the soil’s ability to hold and exchange positively charged nutrients such as potassium, calcium, magnesium, and sodium.",
      },
      {
        term: "Elemental Saturation",
        definition:
          "The proportion of the soil’s cation exchange capacity occupied by specific cations, such as K, Ca, Mg, Na, or H.",
      },
    ],
  },
  {
    key: "experimental_design",
    label: "Experimental Design",
    items: [
      {
        term: "Strip",
        definition:
          "One of the four field treatment areas in the biochar experiment: S1, S2, S3, and S4.",
      },
      {
        term: "Treatment Strip",
        definition:
          "A strip that received biochar. In this experiment, S1 and S3 are treatment strips.",
      },
      {
        term: "Control Strip",
        definition:
          "A strip that did not receive biochar. In this experiment, S2 and S4 are control strips.",
      },
      {
        term: "Irrigation Regime",
        definition:
          "The water application schedule used for a strip pair within the experiment.",
      },
      {
        term: "Baseline",
        definition:
          "Measurements taken before biochar application, used as a pre-treatment reference.",
      },
      {
        term: "Directional Change",
        definition:
          "Whether a variable increases or decreases over time or differs positively or negatively between treatment and control.",
      },
    ],
  },
  {
    key: "time_periods",
    label: "Time Periods & Aggregation",
    items: [
      {
        term: "Granularity",
        definition:
          "The time resolution at which data are displayed or summarized, such as 15-minute, hourly, daily, monthly, or growing season.",
      },
      {
        term: "Aggregation",
        definition:
          "The process of summarizing higher-frequency data into lower-frequency periods, such as averaging 15-minute measurements into daily values.",
      },
      {
        term: "Growing Season",
        definition:
          "In the default app configuration, the period from April 1 through October 31. It is defined by the irrigation window.",
      },
      {
        term: "Winter / Dormant Period",
        definition:
          "In the default app configuration, the period from November 1 through March 31.",
      },
      {
        term: "Custom Season",
        definition:
          "A user-defined seasonal grouping used to summarize data outside the default periods.",
      },
    ],
  },
  {
    key: "water_sensor_metrics",
    label: "Water & Sensor Metrics",
    items: [
      {
        term: "Volumetric Water Content (VWC)",
        definition:
          "The percentage of soil volume occupied by water. This is a primary soil moisture measurement from the dataloggers.",
      },
      {
        term: "Soil Water Content (SWC)",
        definition:
          "The total amount of water stored in a defined soil layer. In this app, SWC is derived from VWC and depth assumptions.",
      },
      {
        term: "Field Capacity",
        definition:
          "The amount of water remaining in soil after excess gravitational water has drained away.",
      },
      {
        term: "Wilting Point",
        definition:
          "The soil moisture level below which plants can no longer extract sufficient water.",
      },
      {
        term: "Electrical Conductivity (EC)",
        definition:
          "A measure of the soil’s ability to conduct electricity, related to dissolved salts and ion concentration.",
      },
      {
        term: "Depth",
        definition:
          "The soil depth associated with a sensor or summarized value.",
      },
      {
        term: "Logger Location",
        definition:
          "The physical datalogger position within a strip, used to distinguish measurements from different points in the field.",
      },
    ],
  },
  {
    key: "ratios_deltas",
    label: "Ratios, Differences, and Derived Metrics",
    items: [
      {
        term: "Ratio",
        definition:
          "A comparison between paired strips, used to highlight treatment effects while accounting for shared environmental conditions.",
      },
      {
        term: "VWC Ratio",
        definition:
          "A ratio comparing volumetric water content between paired strips, used to detect relative treatment effects in soil moisture.",
      },
      {
        term: "EC Ratio",
        definition:
          "A ratio comparing electrical conductivity between paired strips, used to highlight relative differences in salts or dissolved ions.",
      },
      {
        term: "Temperature Ratio",
        definition:
          "A ratio comparing temperature values between paired strips. It should be interpreted cautiously, especially when values are near zero.",
      },
      {
        term: "Delta (Δ)",
        definition:
          "A difference value, often used to compare treatment and control or to compare one time period with another.",
      },
      {
        term: "ΔSWC",
        definition:
          "The difference in soil water content between strips or between time periods.",
      },
      {
        term: "ΔT",
        definition:
          "The difference in temperature between strips or between time periods.",
      },
    ],
  },
  {
    key: "soil_chemistry",
    label: "Soil Chemistry",
    items: [
      {
        term: "pH",
        definition: "A measure of soil acidity or alkalinity.",
      },
      {
        term: "Salinity",
        definition: "The concentration of soluble salts in the soil.",
      },
      {
        term: "Potassium (K)",
        definition:
          "An essential plant nutrient involved in water regulation, enzyme activation, and stress tolerance.",
      },
      {
        term: "Phosphorus (P)",
        definition:
          "An essential plant nutrient involved in energy transfer, root development, and metabolism.",
      },
      {
        term: "Nitrogen (N)",
        definition:
          "An essential plant nutrient strongly associated with protein formation and plant growth.",
      },
      {
        term: "Nitrate-N",
        definition:
          "A plant-available form of nitrogen commonly measured in soil tests.",
      },
      {
        term: "Olsen P",
        definition:
          "A soil-test phosphorus measure commonly used in alkaline or calcareous soils.",
      },
      {
        term: "Organic Matter",
        definition:
          "The portion of soil made up of plant, microbial, and other carbon-based residues.",
      },
    ],
  },
  {
    key: "soil_biology",
    label: "Soil Biology",
    items: [
      {
        term: "PLFA",
        definition:
          "Phospholipid fatty acid analysis, a method used to estimate microbial biomass and microbial community composition in soil.",
      },
      {
        term: "Total Microbial Biomass",
        definition:
          "An estimate of the total living microbial mass in the soil sample.",
      },
      {
        term: "Bacterial Biomass",
        definition:
          "An estimate of the bacterial component of the soil microbial community.",
      },
      {
        term: "Fungal Biomass",
        definition:
          "An estimate of the fungal component of the soil microbial community.",
      },
      {
        term: "Fungal:Bacterial Ratio",
        definition:
          "A comparison of fungal biomass to bacterial biomass, often used as an indicator of soil biological balance.",
      },
      {
        term: "Actinomycetes",
        definition:
          "A group of filamentous bacteria important in decomposition of complex organic matter.",
      },
      {
        term: "Microbial Diversity",
        definition:
          "A measure of the variety of microbial groups present in the soil.",
      },
      {
        term: "Biological Buffering",
        definition:
          "A working term for the stabilizing effect of microbial processes on nutrient availability over time.",
      },
    ],
  },
  {
    key: "plant_forage_metrics",
    label: "Plant, Biomass, and Forage Metrics",
    items: [
      {
        term: "Biomass",
        definition:
          "The mass of plant material produced in the field.",
      },
      {
        term: "Dry Weight",
        definition:
          "The plant mass after water has been removed, used for comparing biomass production across samples.",
      },
      {
        term: "NIR",
        definition:
          "Near-infrared reflectance analysis, used to estimate forage nutrient composition and quality.",
      },
      {
        term: "Crude Protein (CP)",
        definition:
          "An estimate of plant protein content, usually derived from nitrogen concentration.",
      },
      {
        term: "ADF",
        definition:
          "Acid detergent fiber, a forage quality measure associated with lower digestibility at higher values.",
      },
      {
        term: "NDF",
        definition:
          "Neutral detergent fiber, a forage quality measure associated with fiber content and intake potential.",
      },
      {
        term: "RFV",
        definition:
          "Relative Feed Value, a combined index of forage quality.",
      },
      {
        term: "Plant Uptake",
        definition:
          "The amount of a nutrient removed from the field through plant growth and harvest.",
      },
      {
        term: "Plant Withdrawal",
        definition:
          "A practical term for nutrient removal in harvested biomass.",
      },
    ],
  },
  {
    key: "irrigation_management",
    label: "Irrigation & Management",
    items: [
      {
        term: "Irrigation Event",
        definition:
          "A single application of irrigation water over a defined time window.",
      },
      {
        term: "Irrigation Overlay",
        definition:
          "A visual display of irrigation timing on a plot, often shown as shaded regions or annotations.",
      },
      {
        term: "Gallons Applied",
        definition:
          "The amount of irrigation water delivered during an event.",
      },
      {
        term: "Fertilizer Application",
        definition:
          "The addition of nutrients to a strip, typically recorded by date, nutrient, and amount.",
      },
      {
        term: "Nutrient Budget",
        definition:
          "A comparison of nutrient inputs, outputs, and changes in storage, often using fertilizer, plant uptake, and soil stock change.",
      },
    ],
  },
  {
    key: "interpretation_terms",
    label: "Interpretation Terms",
    items: [
      {
        term: "Retention",
        definition:
          "The ability of soil to hold water or nutrients rather than losing them through drainage, leaching, or other pathways.",
      },
      {
        term: "Availability",
        definition:
          "The extent to which a nutrient is accessible for plant uptake.",
      },
      {
        term: "Buffering",
        definition:
          "The tendency of a soil system to resist rapid change in nutrient or moisture conditions.",
      },
      {
        term: "Turnover",
        definition:
          "The cycling of nutrients through soil, microbes, and plants over time.",
      },
      {
        term: "Efficiency",
        definition:
          "A relative measure of how effectively a system converts inputs such as water or fertilizer into plant growth or nutrient uptake.",
      },
    ],
  },
];

/**
 * @typedef {Window & {
 *   bootstrap?: {
 *     Tooltip?: {
 *       getOrCreateInstance: (el: Element) => unknown
 *     }
 *   }
 * }} GlossaryWindow
 */

/** @type {GlossaryWindow} */
const glossaryWindow = /** @type {GlossaryWindow} */ (window);

/**
 * @returns {void}
 */
export function renderGlossary() {
  const container = document.getElementById("glossary-content");
  if (!container) {
    console.warn("Glossary container #glossary-content not found.");
    return;
  }

  if (container.dataset.rendered === "true") return;

  container.innerHTML = "";

  const intro = document.createElement("div");
  intro.className = "mb-3";
  intro.innerHTML = `
    <p class="mb-2">
      This glossary defines terms used throughout the biochar research website, including field measurements,
      soil chemistry, soil biology, irrigation, forage quality, and app-specific analysis terms.
    </p>
  `;
  container.appendChild(intro);

  const accordion = document.createElement("div");
  accordion.className = "accordion";
  accordion.id = "glossary-accordion";

  glossarySections.forEach((section, index) => {
    const item = document.createElement("div");
    item.className = "accordion-item";

    const headerId = `glossary-heading-${section.key}`;
    const collapseId = `glossary-collapse-${section.key}`;
    const isFirst = index === 0;

    item.innerHTML = `
      <h2 class="accordion-header" id="${headerId}">
        <button
          class="accordion-button ${isFirst ? "" : "collapsed"}"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target="#${collapseId}"
          aria-expanded="${isFirst ? "true" : "false"}"
          aria-controls="${collapseId}">
          ${section.label}
        </button>
      </h2>
      <div
        id="${collapseId}"
        class="accordion-collapse collapse ${isFirst ? "show" : ""}"
        aria-labelledby="${headerId}"
        data-bs-parent="#glossary-accordion">
        <div class="accordion-body"></div>
      </div>
    `;

    const body = item.querySelector(".accordion-body");
    if (body instanceof HTMLElement) {
      section.items.forEach((entry) => {
        const termBlock = document.createElement("div");
        termBlock.className = "mb-3 glossary-entry";
        termBlock.innerHTML = `
          <div class="glossary-term">${entry.term}</div>
          <div class="glossary-definition">${entry.definition}</div>
        `;
        body.appendChild(termBlock);
      });
    }

    accordion.appendChild(item);
  });

  container.appendChild(accordion);
  container.dataset.rendered = "true";
}

/**
 * @returns {Record<string, string>}
 */
export function buildGlossaryLookup() {
  /** @type {Record<string, string>} */
  const lookup = {};

  glossarySections.forEach((section) => {
    section.items.forEach((entry) => {
      lookup[entry.term] = entry.definition;
    });
  });

  return lookup;
}

/**
 * @param {ParentNode} [root=document]
 * @returns {void}
 */
export function applyGlossaryTooltips(root = document) {
  const lookup = buildGlossaryLookup();

  const nodes = root.querySelectorAll("[data-glossary-term]");

  nodes.forEach((node) => {
    const term = node.getAttribute("data-glossary-term");
    if (!term) return;

    const definition = lookup[term];
    if (!definition) return;

    node.setAttribute("title", definition);
    node.setAttribute("data-bs-toggle", "tooltip");
    node.setAttribute("data-bs-placement", "top");
    node.setAttribute("data-bs-trigger", "hover focus");
  });

  const TooltipCtor = glossaryWindow.bootstrap?.Tooltip;
  if (TooltipCtor) {
    root.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
      TooltipCtor.getOrCreateInstance(el);
    });
  }
}
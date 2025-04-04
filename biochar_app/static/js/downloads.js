import { getDropdownValue, getElementByIdSafe } from "./ui_utils.js";

function downloadPlot(plotType, format) {
    console.log(`📡 Downloading ${plotType} plot as ${format}...`);

    // ✅ Retrieve selected values safely
    const year = getDropdownValue("main-year") || "unknown";
    const variable = getDropdownValue("main-variable") || "unknown";
    const strip = getDropdownValue("main-strip") || "unknown";
    const loggerLocation = getDropdownValue("main-loggerLocation") || "unknown";
    const depth = (getDropdownValue("main-depth") || "unknown").replace(" ", "");

    const filename = `${plotType}_plot_${year}_${variable}_${strip}_${loggerLocation}_${depth}`;
    console.log(`📂 Generated filename: ${filename}.${format}`);

    const plotElement = getElementByIdSafe(`${plotType}-plot`);
    if (!plotElement) {
        console.error(`❌ Plot container not found: ${plotType}-plot`);
        return;
    }

    Plotly.downloadImage(plotElement, {
        format,
        filename,
        height: 500,
        width: 800,
    });
}
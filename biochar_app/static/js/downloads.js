function downloadPlot(plotType, format) {
    console.log(`📡 Downloading ${plotType} plot as ${format}...`);

    // ✅ Retrieve selected values from dropdowns
    const year = document.getElementById("main-year")?.value || "unknown";
    const variable = document.getElementById("main-variable")?.value || "unknown";
    const strip = document.getElementById("main-strip")?.value || "unknown";
    const loggerLocation = document.getElementById("main-loggerLocation")?.value || "unknown";
    const depth = document.getElementById("main-depth")?.value.replace(" ", "") || "unknown";

    // ✅ Construct filename and ensure only one extension
    const filename = `${plotType}_plot_${year}_${variable}_${strip}_${loggerLocation}_${depth}`;

    console.log(`📂 Generated filename: ${filename}.${format}`);

    // ✅ Trigger Plotly image download
    Plotly.downloadImage(document.getElementById(`${plotType}-plot`), {
        format: format,
        filename: filename,  // <-- ✅ Now does NOT include extra extension
        height: 500,
        width: 800,
    });
}
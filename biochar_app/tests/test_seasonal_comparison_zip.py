"""Exercise the actual browser download and independently verify its ZIP."""
import base64
import csv
import io
from pathlib import Path
import shutil
import subprocess
import zipfile

import pytest


def test_comparison_image_export_keeps_compact_notes_above_legend():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required")
    source = Path(__file__).resolve().parents[1] / "static/js/downloads.js"
    script = r'''
import fs from "node:fs";
import vm from "node:vm";
let rendered, image, purged = false, removed = false;
const chart = {data: [{type:"bar", y:[1.5]}], layout: {
 title: {text:"old combined title"}, annotations:[{text:"Dark outline: ratio below 1"}]}};
const original = JSON.stringify(chart);
const window = {unitSystem:"us", __seasonalComparisonDownload: {
 rows:[{}], years:[2025], variable:"VWC", strip:"S1", depth:"1", periodLabel:"Growing Season",
 ratioChart:chart, rawChart:chart, headings:{ratio: {
 heading:"Growing Season: ratios of seasonal means by year",
 context:"Strip ratios S1/S2 and S3/S4, 6 in, anchor years 2023–2026",
 note:"* Some observations missing: 2023. Means use available valid observations. See the comparison data download and its README for coverage details. Season unfinished: 2026."}}},
 Plotly:{async newPlot(el,data,layout) {rendered=layout;},
 async downloadImage(el,options) {image=options;}, purge() {purged=true;}}};
const context = vm.createContext({window, document:{body:{appendChild() {}},
 createElement() {return {style:{}, remove(){removed=true;}};}}, console, alert(){throw Error("Unexpected alert");}});
vm.runInContext(fs.readFileSync(process.argv[1], "utf8").replace(/^import .*;\n/gm, "").replace(/^export /gm, ""), context);
await vm.runInContext('downloadSeasonalComparisonPlot("ratio")', context);
if (rendered.title.text !== "") throw Error("Old oversized title retained");
const notes = rendered.annotations.filter(a => a.font?.size === 16 && !a.text.startsWith("Dark outline"));
if (notes.length < 2) throw Error("Expected wrapped compact notes");
for(let i=1;i<notes.length;i++) if(notes[i-1].yshift-notes[i].yshift !== 21) throw Error("Wrong leading");
if(notes.at(-1).yshift < 55) throw Error("Notes collide with legend");
if(rendered.legend.y !== 1.01) throw Error("Wrong legend placement");
const outline = rendered.annotations.find(a=>a.text === "Dark outline indicates ratio below 1");
if(!outline || outline.y !== rendered.legend.y || outline.yanchor !== "bottom" || outline.borderwidth !== 0) throw Error("Outline explanation not alongside legend");
if(outline.x !== 0 || outline.xshift !== 225) throw Error("Explanation not immediately after legend entries");
if(JSON.stringify(chart)!==original || !purged || !removed || image.scale!==2) throw Error("Export mutated screen or leaked chart");
'''
    subprocess.run([node, "--input-type=module", "-e", script, str(source)],
                   capture_output=True, text=True, check=True)


def test_comparison_chart_selects_ratio_of_means_for_vwc():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the browser comparison test")
    source = Path(__file__).resolve().parents[1] / "static/js/tab_summary.js"
    script = r'''
import fs from "node:fs";
import vm from "node:vm";
const source = fs.readFileSync(process.argv[1], "utf8");
const start = source.indexOf("function comparisonRowsForPeriod(");
const end = source.indexOf("\n}", start) + 2;
const context = vm.createContext({getIncompletePeriodNotice: () => ""});
vm.runInContext(source.slice(start, end), context);
context.entries = [{year: 2025, periods: [{code: "GROWING"}], gseason_stats: [
  {period_code: "GROWING", logger_location: "T", ratio_group: "S1/S2",
   ratio_mean: 2, ratio_of_means: 1.5, ratio_coverage_pct: 99, ratio_of_means_coverage_pct: 75}
]}];
const vwc = vm.runInContext('comparisonRowsForPeriod(entries, "GROWING", "VWC")', context);
const ec = vm.runInContext('comparisonRowsForPeriod(entries, "GROWING", "EC")', context);
if (vwc[0].s1s2Mean !== 1.5 || vwc[0].s1s2Coverage !== 75) throw Error("VWC used old aggregation");
if (ec[0].s1s2Mean !== 2) throw Error("Other variables changed unexpectedly");
if (vwc[1].s1s2Mean !== null) throw Error("Missing pairs should stay unavailable");
'''
    subprocess.run([node, "--input-type=module", "-e", script, str(source)],
                   capture_output=True, text=True, check=True)


def test_seasonal_comparison_download_includes_csv_and_column_readme():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the browser download test")
    source = Path(__file__).resolve().parents[1] / "static/js/downloads.js"
    script = r'''
import fs from "node:fs";
import vm from "node:vm";
let blob, filename, alerts = 0;
const window = {unitSystem: "us", __seasonalComparisonDownload: {
  periodLabel: "Growing Season (04-01–10-31)", variable: "VWC", strip: "S1", depth: "1", years: [2026],
  rows: [{year: 2026, status: "Partial", position: "Top", rawMean: null,
    rawCoverage: 80, s1s2Mean: 1.2, s1s2Coverage: 75, s3s4Mean: 0.8, s3s4Coverage: 70}]
}, URL: {createObjectURL(value) {blob = value; return "blob:test";}, revokeObjectURL() {}}};
const context = vm.createContext({window, document: {body: {appendChild() {}},
  createElement() {return {set download(value) {filename = value;}, click() {}, remove() {}};}},
  Blob, TextEncoder, DataView, Uint8Array, Date, console, alert() {alerts++;}});
vm.runInContext(fs.readFileSync(process.argv[1], "utf8").replace(/^import .*;\n/gm, "").replace(/^export /gm, ""), context);
vm.runInContext("downloadSeasonalComparisonData()", context);
if (!filename.endsWith(".zip") || blob.type !== "application/zip") throw Error("Not a ZIP download");
if (vm.runInContext('csvCell(\'a,"b"\')', context) !== '"a,""b"""') throw Error("CSV quoting");
window.__seasonalComparisonDownload = {rows: []};
vm.runInContext("downloadSeasonalComparisonData()", context);
if (alerts !== 1) throw Error("Empty selection was not rejected");
console.log(Buffer.from(await blob.arrayBuffer()).toString("base64"));
'''
    run = subprocess.run([node, "--input-type=module", "-e", script, str(source)],
                         capture_output=True, text=True, check=True)
    with zipfile.ZipFile(io.BytesIO(base64.b64decode(run.stdout.strip()))) as archive:
        assert archive.testzip() is None
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        assert len(csv_names) == 1
        assert len(archive.namelist()) == 2
        readme = archive.read("README.txt").decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(archive.read(csv_names[0]).decode("utf-8"))))
        assert len(rows) == 1 and len(rows[0]) == 14
        assert rows[0]["raw_mean"] == ""
        assert rows[0]["s1_s2_ratio_mean"] == "1.2"
        assert all(f"{column}:" in readme for column in rows[0])
        assert "04-01–10-31" in readme
        assert "seasonal mean S1 divided by seasonal mean S2" in readme
        assert "matching timestamps" in readme

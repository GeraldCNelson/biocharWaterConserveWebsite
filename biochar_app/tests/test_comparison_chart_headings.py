import shutil
import subprocess
from pathlib import Path

import pytest


def test_chart_titles_wrap_and_historical_partial_years_are_not_unfinished():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required")
    source = Path(__file__).resolve().parents[1] / "static/js/tab_summary.js"
    script = r'''
const fs=require("fs"),vm=require("vm"),assert=require("assert");
const context={window:{},console}; vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[1],"utf8").replace(/^import .*;$/gm,"").replace(/^export /gm,""),context);
const heading="Growing Season test: ratios of seasonal means by year";
const subtitle="Strip ratios S1/S2 and S3/S4, 6 in, anchor years 2023–2026";
const note="* Incomplete data coverage: 2023, 2024. Season unfinished: 2026.";
const narrow=context.comparisonChartHeading(heading,subtitle,note,350);
const wide=context.comparisonChartHeading(heading,subtitle,note,1000);
assert.ok(narrow.title.text.split("<br>").length>wide.title.text.split("<br>").length);
assert.ok(narrow.margin.t>wide.margin.t);
assert.equal(narrow.height-narrow.margin.t-narrow.margin.b,290);
assert.equal(narrow.title.xanchor,"left");
assert.equal(narrow.title.yref,"container");
assert.equal(narrow.title.yanchor,"top");
assert.ok(narrow.title.pad.t>=24);
assert.ok(context.comparisonChartHeading("A < B",subtitle,"",640).title.text.includes("&lt;"));
const rows=[{year:2023,status:"Partial"},{year:2024,status:"Partial"},{year:2026,status:"Partial"}];
const entries=rows.map(r=>({year:r.year,periods:[{code:"G",start:r.year+"-04-01",end:r.year+"-10-31"}]}));
let text=context.comparisonPartialNote(rows,entries,"G",new Date(2026,9,7));
assert.ok(text.includes("Some observations missing: 2023, 2024."));
assert.ok(text.includes("README for coverage details"));
assert.ok(text.includes("Season unfinished: 2026."));
text=context.comparisonPartialNote(rows,entries,"G",new Date(2026,10,1));
assert.ok(!text.includes("unfinished"));
assert.ok(text.includes("2023, 2024, 2026"));
'''
    subprocess.run([node, "-e", script, str(source)], check=True, capture_output=True, text=True)

import shutil
import subprocess
import re
from pathlib import Path

import pytest


def test_trace_plots_use_responsive_positioned_cards():
    app_root = Path(__file__).resolve().parents[1]
    template = (app_root / "templates/index.html").read_text(encoding="utf-8")
    styles = (app_root / "static/css/styles.css").read_text(encoding="utf-8")

    assert 'class="plot-card mb-2" data-plot-card="plot-1"' in template
    assert 'id="plot-1" class="plot-container"' in template
    assert 'class="plot-card" data-plot-card="plot-2"' in template
    assert 'id="plot-2" class="plot-container"' in template

    plot_card_rule = re.search(r"\.plot-card\s*\{(?P<body>[^}]*)\}", styles)
    assert plot_card_rule is not None
    assert "position: relative" in plot_card_rule.group("body")
    assert "min-width: 0" in plot_card_rule.group("body")


def test_trace_plot_legend_clears_secondary_axis_and_pair_gap_is_compact():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required")

    source = Path(__file__).resolve().parents[1] / "static/js/plot_utils.js"
    script = r'''
const fs=require("fs"),vm=require("vm"),assert=require("assert");
const context={
  window:{},
  document:{getElementById:()=>({clientWidth:1400})},
  console,
};
vm.createContext(context);
vm.runInContext(
  fs.readFileSync(process.argv[1],"utf8")
    .replace(/^import .*;$/gm,"")
    .replace(/^export /gm,""),
  context
);

const raw={legend:{},yaxis2:{title:{text:"Precipitation (in)"}}};
const rawResult=context.applyResponsiveLegend(raw,160,"plot-1");
assert.equal(rawResult.mode,"right");
assert.equal(raw.legend.x,1.10);
assert.ok(rawResult.minRight>=210);

const ratio={legend:{}};
const ratioResult=context.applyResponsiveLegend(ratio,160,"plot-2");
assert.equal(ratio.legend.x,1.01);
assert.equal(ratioResult.minRight,160);

assert.deepEqual(
  context.compactTracePairMargins("plot-1",115,70,"right"),
  {topMargin:115,bottomMargin:55}
);
assert.deepEqual(
  context.compactTracePairMargins("plot-2",60,70,"right"),
  {topMargin:50,bottomMargin:70}
);
assert.deepEqual(
  context.compactTracePairMargins("plot-1",115,95,"below"),
  {topMargin:115,bottomMargin:95}
);
'''
    subprocess.run(
        [node, "-e", script, str(source)],
        check=True,
        capture_output=True,
        text=True,
    )

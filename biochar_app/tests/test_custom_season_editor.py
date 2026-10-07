"""Exercise the real editor module with native-control lifecycle stubs."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_anchor_dates_and_detached_controls():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required")
    source = Path(__file__).resolve().parents[1] / "static/js/custom_gseason.js"
    script = r'''
const fs = require("fs"), vm = require("vm"), assert = require("assert");
class Element {
  constructor() { this.value=""; this.children=[]; this.dataset={}; this.isConnected=true; this.classList={add(){}}; }
  set innerHTML(html) {
    this.html=html;
    for (const child of this.children) for (const input of Object.values(child.inputs||{})) input.isConnected=false;
    this.children=[];
    if (html.includes("period-start")) this.inputs=Object.fromEntries(
      [".period-start",".period-end",".period-label",".remove-period"].map(k=>[k,new Element()]));
  }
  appendChild(child) { this.children.push(child); if (child.selected) this.value=child.value; }
  querySelector(key) { return this.inputs[key]; }
}
const ids=Object.fromEntries(["anchor-year","add-period","periods-container","seasonal-period-summary"].map(k=>[k,new Element()]));
const context={console,document:{getElementById:k=>ids[k],createElement:()=>new Element()}};
vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[1],"utf8").replace(/^export /gm,""),context);
for (const dated of [false,true]) {
 const defaults=[{code:"WINTER",label:"Winter",start:"11-01",end:"03-31"},
                 {code:"GROWING",label:"Growing",start:"04-01",end:"10-31"}];
 if(dated) for(const p of defaults){p.start="2026-"+p.start;p.end="2026-"+p.end;}
 const get=context.initCustomGseason({defaultYear:2026,years:[2025,2026],defaultPeriods:defaults});
 const container=ids["periods-container"], year=ids["anchor-year"];
 const oldEnd=container.children[0].querySelector(".period-end");
 year.value="2025"; year.onchange();
 assert.equal(get()[0].start,"2024-11-01"); assert.equal(get()[0].end,"2025-03-31");
 assert.equal(container.children[0].querySelector(".period-end").value,"2025-03-31");
 oldEnd.value=""; oldEnd.onchange({target:oldEnd});
 assert.equal(get()[0].end,"2025-03-31");
 assert.equal(get()[1].start,"2025-04-01"); assert.equal(get()[1].end,"2025-10-31");
 year.value="2026"; year.onchange();
 assert.equal(get()[0].end,"2026-03-31");
 const edited=container.children[0].querySelector(".period-end");
 edited.value="2026-03-15"; edited.onchange({target:edited});
 year.value="2025"; year.onchange();
 assert.equal(get()[0].end,"2026-03-15"); // Preserve deliberate custom dates.
}
'''
    subprocess.run([node, "-e", script, str(source)], check=True, capture_output=True, text=True)

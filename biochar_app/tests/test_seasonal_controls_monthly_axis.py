from pathlib import Path
import re
import shutil
import subprocess

import pytest

from biochar_app.scripts.plot_components import common_xaxis_config


def test_monthly_axis_labels_every_month():
    axis = common_xaxis_config("monthly", "2025-01-31", "2025-12-31")
    assert axis["dtick"] == "M1"
    assert axis["tick0"] == "2025-01-01T00:00:00"
    assert axis["tickformat"] == "%b<br>%Y"


def test_seasonal_mode_disables_dates_but_not_year():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required")
    template = (Path(__file__).resolve().parents[1] / "templates/index.html").read_text()
    body = re.search(r"function toggleDateControls\(\) \{(.*?)\n        \}", template, re.S).group(1)
    script = '''
const assert=require("assert");
const gran={value:"gseason"};
const year={disabled:false};
const dates=[{disabled:false,classList:{toggle(){}}},{disabled:false,classList:{toggle(){}}}];
const dateControls={querySelectorAll(selector){return selector==="input"?dates:[year,...dates];}};
function toggleDateControls(){BODY}
toggleDateControls();
assert.equal(year.disabled,false);
assert.ok(dates.every(d=>d.disabled));
gran.value="monthly";
toggleDateControls();
assert.ok(dates.every(d=>!d.disabled));
'''.replace("BODY", body)
    subprocess.run([node, "-e", script], check=True, capture_output=True, text=True)

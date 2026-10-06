"""Exercise actual browser feedback code without a server or live download."""
from pathlib import Path
import shutil
import subprocess

import pytest


def test_bulk_download_feedback_pending_success_failure_and_retry():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for browser feedback checks")
    source = Path(__file__).resolve().parents[1] / "static/js/downloads.js"
    script = r'''
import fs from "node:fs";
import vm from "node:vm";
import assert from "node:assert/strict";
const elements = new Map();
const document = {
  getElementById(id) {return elements.get(id) || null;},
  createElement() {return {attributes: {}, setAttribute(key, value) {this.attributes[key] = value;}};}
};
const context = vm.createContext({window: {}, document, console: {error() {}}});
vm.runInContext(fs.readFileSync(process.argv[1], "utf8").replace(/^export /gm, ""), context);
const run = vm.runInContext("runBulkDownloadWithFeedback", context);
const btn = {id: "bulk-download-weather", dataset: {}, textContent: "Download weather data", disabled: false,
  attributes: {}, setAttribute(key, value) {this.attributes[key] = value;},
  insertAdjacentElement(position, element) {
    assert.equal(position, "afterend"); elements.set(element.id, element);
  }};
let resolve, requests = 0, refreshes = 0;
const pending = new Promise(done => {resolve = done;});
const refresh = () => {refreshes++; btn.disabled = false;};
const first = run(btn, () => {requests++; return pending;}, refresh);
assert.equal(btn.textContent, "Preparing download…");
assert.equal(btn.disabled, true);
assert.equal(btn.attributes["aria-busy"], "true");
const status = elements.get("bulk-download-weather-status");
assert.equal(status.attributes["role"], "status");
assert.equal(status.attributes["aria-live"], "polite");
assert.match(status.textContent, /Preparing ZIP/);
assert.equal(await run(btn, () => {requests++;}, refresh), false);
assert.equal(requests, 1);
assert.equal(refreshes, 0);
resolve();
assert.equal(await first, true);
assert.match(status.textContent, /Download started/);
assert.equal(btn.textContent, "Download weather data");
assert.equal(btn.attributes["aria-busy"], "false");
assert.equal(btn.disabled, false);
assert.equal(btn.dataset.bulkDownloading, undefined);
assert.equal(refreshes, 1);
assert.equal(await run(btn, async () => {throw Error("HTTP 500");}, refresh), false);
assert.match(status.textContent, /Download failed.*try again/);
assert.equal(btn.disabled, false);
assert.equal(btn.textContent, "Download weather data");
assert.equal(await run(btn, async () => {}, refresh), true);
assert.match(status.textContent, /Download started/);
assert.equal(elements.size, 1); // Reuse the same accessible message on retry.
assert.equal(refreshes, 3);
// Changing the controls while awaiting a request must not re-enable its button.
const code = fs.readFileSync(process.argv[1], "utf8");
const start = code.indexOf("  function setButtonState(");
const end = code.indexOf("\n  }", start) + 4;
vm.runInContext(code.slice(start, end), context);
btn.dataset.bulkDownloading = "true";
btn.classList = {toggle() {}};
context.btn = btn;
vm.runInContext("setButtonState(btn, {visualEnabled: true, hardDisable: false})", context);
assert.equal(btn.disabled, true);
assert.equal(btn.attributes["aria-disabled"], "true");
'''
    subprocess.run([node, "--input-type=module", "-e", script, str(source)],
                   capture_output=True, text=True, check=True)

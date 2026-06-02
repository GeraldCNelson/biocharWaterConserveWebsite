import os
import re

# Define paths
root_dir = "/Users/gcn/Documents/workspace/biocharWaterConserveWebsite/biochar_app"
js_dir = os.path.join(root_dir, "static", "js")
templates_dir = os.path.join(root_dir, "templates")

# Regex patterns
function_pattern = re.compile(r"function (\w+)|const (\w+)\s*=\s*\(")

report = []

# Search .js files
for subdir, _, files in os.walk(js_dir):
    for file in files:
        if file.endswith(".js"):
            path = os.path.join(subdir, file)
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                matches = function_pattern.findall(content)
                if matches:
                    report.append(f"File: {path}")
                    for match in matches:
                        func_name = match[0] or match[1]
                        report.append(f"  Function: {func_name}")

# Search .html files in templates for <script> blocks
script_block_pattern = re.compile(r"<script[\s\S]*?</script>", re.MULTILINE)
for subdir, _, files in os.walk(templates_dir):
    for file in files:
        if file.endswith(".html"):
            path = os.path.join(subdir, file)
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                scripts = script_block_pattern.findall(content)
                for script in scripts:
                    matches = function_pattern.findall(script)
                    if matches:
                        report.append(f"File: {path} (script block)")
                        for match in matches:
                            func_name = match[0] or match[1]
                            report.append(f"  Function: {func_name}")

# Write report
output_path = os.path.join(root_dir, "js_function_report.txt")
with open(output_path, "w", encoding="utf-8") as f:
    f.write("=== JavaScript Function Report (Including HTML Scripts) ===\n\n")
    f.write("\n".join(report))

print(f"Report written to {output_path}")

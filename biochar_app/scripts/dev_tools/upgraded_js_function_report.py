import os
import re

project_path = "/Users/gcn/Documents/workspace/biocharWaterConserveWebsite/biochar_app"
output_file = "upgraded_js_function_report.txt"

js_function_pattern = re.compile(r"function (\w+)|const (\w+) = \(|let (\w+) = \(")
html_script_pattern = re.compile(r"function (\w+)")

report_lines = ["=== Upgraded JavaScript Function Report ===\n"]

# Walk through the directory
for root, dirs, files in os.walk(project_path):
    # Skip vendor directory
    if "static/js/vendor" in root:
        continue

    for file in files:
        if file.endswith(".js"):
            file_path = os.path.join(root, file)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                functions = js_function_pattern.findall(content)
                functions = [f[0] or f[1] or f[2] for f in functions if any(f)]
                if functions:
                    report_lines.append(f"File: {file_path}")
                    for func in set(functions):
                        report_lines.append(f"  Function: {func}")

# Check index.html separately
index_html = os.path.join(project_path, "templates/index.html")
if os.path.exists(index_html):
    with open(index_html, "r", encoding="utf-8") as f:
        content = f.read()
        script_functions = html_script_pattern.findall(content)
        if script_functions:
            report_lines.append(f"File: {index_html}")
            for func in set(script_functions):
                report_lines.append(f"  Function: {func}")

# Write to output file
with open(os.path.join(project_path, output_file), "w", encoding="utf-8") as out:
    out.write("\n".join(report_lines))

print(f"✅ Report saved to {output_file}")

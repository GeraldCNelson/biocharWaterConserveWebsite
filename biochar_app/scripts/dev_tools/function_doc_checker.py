import os
import re


def extract_functions_with_doc(file_path, file_type="py"):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    report = []
    report.append(f"=== {file_path} ===\n")

    if file_type == "py":
        for i, line in enumerate(lines):
            func_match = re.match(r'\s*def (\w+)\(', line)
            if func_match:
                func_name = func_match.group(1)
                docstring = "⚠️ Missing docstring"
                next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""
                if next_line.startswith('"""') or next_line.startswith("'''"):
                    docstring = next_line
                report.append(f"Line {i + 1}: def {func_name} -> {docstring}\n")

    elif file_type == "js":
        for i, line in enumerate(lines):
            func_match = re.search(r'function (\w+)', line) or re.search(r'(\w+)\s*=\s*\(.*\)\s*=>', line)
            if func_match:
                func_name = func_match.group(1)
                doc_line = "⚠️ Missing doc"
                # Look upward for JSDoc comment
                j = i - 1
                while j >= 0 and lines[j].strip() == '':
                    j -= 1
                if j >= 0 and (lines[j].strip().startswith('/**') or lines[j].strip().startswith('//')):
                    doc_line = lines[j].strip()
                report.append(f"Line {i + 1}: function {func_name} -> {doc_line}\n")

    return report


def scan_directory(base_path):
    output = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.py'):
                output.extend(extract_functions_with_doc(os.path.join(root, file), file_type="py"))
            if file.endswith('.js'):
                output.extend(extract_functions_with_doc(os.path.join(root, file), file_type="js"))
    return output


if __name__ == "__main__":
    # Change this to your project directory
    project_dir = "/Users/gcn/Documents/workspace/biocharWaterConserveWebsite/biochar_app"
    report = scan_directory(project_dir)

    with open("docs/function_doc_report_with_doc.txt", "w") as out_file:
        out_file.write('\n'.join(report))

    print("✅ Function documentation report generated: function_doc_report_with_doc.txt")

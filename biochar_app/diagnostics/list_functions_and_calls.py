import os
import re

# Folder to scan (you can change this to your project root)
PROJECT_DIR = "../scripts"

# Regex patterns
function_def_pattern = re.compile(r'^\s*def\s+(\w+)\s*\(')
function_call_pattern = re.compile(r'(\w+)\s*\(')

# Collect all functions
all_functions = set()
function_locations = {}

# First pass: find all function definitions
for root, _, files in os.walk(PROJECT_DIR):
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                for lineno, line in enumerate(f, 1):
                    match = function_def_pattern.match(line)
                    if match:
                        func_name = match.group(1)
                        all_functions.add(func_name)
                        function_locations.setdefault(func_name, []).append((filepath, lineno, 'definition'))

# Second pass: find all function calls
for root, _, files in os.walk(PROJECT_DIR):
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                for lineno, line in enumerate(f, 1):
                    for match in function_call_pattern.finditer(line):
                        func_name = match.group(1)
                        if func_name in all_functions:
                            function_locations.setdefault(func_name, []).append((filepath, lineno, 'call'))

# Output results
for func, locations in function_locations.items():
    print(f"\n🔹 Function: {func}")
    for filepath, lineno, kind in locations:
        print(f"  [{kind}] {filepath}:{lineno}")
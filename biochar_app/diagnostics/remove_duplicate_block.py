"""
remove_duplicate_block.py

Finds and removes a duplicated contiguous block of lines in a text/CSV file.
Uses a hash-based approach to efficiently handle large files.
Usage: python remove_duplicate_block.py <input_file> [output_file]
"""

import sys
import hashlib


def hash_line(line):
    return hashlib.md5(line.encode("utf-8", errors="replace")).digest()


def find_duplicate_block(lines):
    """
    Efficiently finds the largest contiguous duplicate block using hashing.
    Hashes each line, then uses a rolling hash approach to find matching blocks.
    Returns (start, end, block_len) of the SECOND occurrence.
    """
    n = len(lines)
    hashes = [hash_line(l) for l in lines]

    # Build a lookup: hash of single line -> list of line indices
    from collections import defaultdict
    index = defaultdict(list)
    for i, h in enumerate(hashes):
        index[h].append(i)

    best_start = -1
    best_end = -1
    best_len = 0

    # For each pair of lines with matching hashes, try to expand the match
    for h, positions in index.items():
        if len(positions) < 2:
            continue
        for a in positions:
            for b in positions:
                if b <= a:
                    continue
                # Expand match forward from (a, b)
                length = 0
                while (a + length < n and
                       b + length < n and
                       hashes[a + length] == hashes[b + length] and
                       lines[a + length] == lines[b + length]):
                    length += 1
                if length > best_len:
                    best_len = length
                    best_start = b        # remove the second occurrence
                    best_end = b + length

    return best_start, best_end, best_len


def remove_duplicate_block(input_path, output_path=None):
    if output_path is None:
        base = input_path.rsplit(".", 1)
        output_path = base[0] + "_deduped." + base[1] if len(base) == 2 else input_path + "_deduped"

    print(f"Reading file: {input_path}")
    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    print(f"Total lines in file: {len(lines)}")
    print("Searching for duplicate block...")

    start, end, block_len = find_duplicate_block(lines)

    if block_len == 0:
        print("No duplicate block found.")
        return

    cleaned = lines[:start] + lines[end:]

    with open(output_path, "w", encoding="utf-8") as f:
        f.writelines(cleaned)

    print("Success! Cleaned file created.")
    print(f"  Output file  : {output_path}")
    print(f"  Lines removed: {block_len}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python remove_duplicate_block.py <input_file> [output_file]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) >= 3 else None
    remove_duplicate_block(input_file, output_file)
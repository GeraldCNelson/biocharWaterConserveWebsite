#!/usr/bin/env python3
import argparse
import subprocess
from pathlib import Path
import sys



def run(cmd: list[str]):
    """Run a subprocess, printing its output on error."""
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Command failed: {' '.join(cmd)}", file=sys.stderr)
        sys.exit(e.returncode)

def main():
    p = argparse.ArgumentParser(
        description="Convert a .tex file (in docs/) to Word (.docx), HTML (with MathJax), and embed HTML in Markdown"
    )
    p.add_argument(
        "tex_file",
        help="Name or path of your source .tex file (will look in docs/ by default)",
    )
    p.add_argument(
        "--docs-dir",
        type=Path,
        default=Path("docs"),
        help="Directory for input .tex and output .docx/.html (default: %(default)s)",
    )
    p.add_argument(
        "--md-dir",
        type=Path,
        default=Path("markdown"),
        help="Where to write the embedded .md (default: %(default)s)",
    )
    args = p.parse_args()

    docs_dir = args.docs_dir
    md_dir = args.md_dir

    # Resolve the input .tex path, preferring docs_dir if no explicit path given
    tex_path = Path(args.tex_file)
    if not tex_path.exists():
        candidate = docs_dir / tex_path
        if candidate.exists():
            tex_path = candidate
        else:
            print(f"❌ Source file {tex_path} not found (nor {candidate})", file=sys.stderr)
            sys.exit(1)

    stem = tex_path.stem
    docs_dir.mkdir(parents=True, exist_ok=True)
    md_dir.mkdir(parents=True, exist_ok=True)

    # 1) Word (.docx) with native Office math
    docx_out = docs_dir / f"{stem}.docx"
    run([
        "pandoc",
        str(tex_path),
        "--from=latex",
        "--to=docx",
        "-s",
        "--output", str(docx_out),
    ])

    # 2) HTML with MathJax support
    html_out = docs_dir / f"{stem}.html"
    run([
        "pandoc",
        str(tex_path),
        "--from=latex",
        "--to=html5",
        "-s",
        "--mathjax",
        "--output", str(html_out),
    ])

    # 3) embed the HTML into a Markdown file
    md_out = md_dir / f"{stem}.md"
    html = html_out.read_text(encoding="utf-8")
    with md_out.open("w", encoding="utf-8") as md:
        md.write(f"<!-- Auto-generated from `{tex_path.relative_to(docs_dir)}` -->\n")
        md.write("<!-- math via MathJax -->\n\n")
        md.write(html)

    print("✅ Generated:")
    print(f"   • Word document: {docx_out}")
    print(f"   • HTML export:   {html_out}")
    print(f"   • Markdown file: {md_out}")

if __name__ == "__main__":
    main()
import os
import re
import subprocess
from markdown_config import image_config, modal_config

markdown_dir = "markdown"
images_dir = "static/images"
os.makedirs(images_dir, exist_ok=True)

# 🚀 Process Word docs into Markdown
for docx, config in image_config.items():
    input_path = os.path.join(markdown_dir, docx)
    output_md = config["output_md"]
    output_path = os.path.join(markdown_dir, output_md)

    print(f"\n📄 Processing: {docx} → {output_md}")

    subprocess.run([
        "pandoc", input_path,
        "-f", "docx",
        "-t", "gfm",
        "--wrap=none",
        "-o", output_path
    ])

    with open(output_path, "r", encoding="utf-8") as f:
        md_text = f.read()

    images = config.get("images", [])
    replacements = []

    # Replace <figure>...</figure> blocks
    figure_pattern = re.compile(r"<figure>.*?</figure>", re.DOTALL)
    figure_matches = figure_pattern.findall(md_text)

    for match in figure_matches:
        replacements.append(match)

    for i, img_info in enumerate(images):
        filename = img_info["file"]
        alt = img_info.get("alt", "")
        caption = img_info.get("caption", "")
        img_path = f"/static/images/{filename}"

        new_block = f'''<figure>
<img src="{img_path}" alt="{alt}" style="max-width: 100%;" />
<figcaption style="text-align: center;"><p><em>{caption}</em></p></figcaption>
</figure><br>'''

        if i < len(replacements):
            md_text = md_text.replace(replacements[i], new_block)
        else:
            print(f"⚠️ Could not find placeholder for {filename}. Appending at end.")
            md_text += f"\n{new_block}\n"

    # Remove leftover Pandoc junk (media refs and embedded tables)
    md_text = re.sub(r'<img src="media/[^>]+>"', '', md_text)
    md_text = re.sub(r'<table>.*?</table>', '', md_text, flags=re.DOTALL)

    # Insert cleaned side-by-side image block (once)
    if config.get("side_by_side"):
        for pair in config["side_by_side"]:
            img1 = next((img for img in images if img["file"] == pair[0]), None)
            img2 = next((img for img in images if img["file"] == pair[1]), None)

            if img1 and img2:
                block = f'''
<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th><p><img src="/static/images/{img1['file']}" alt="{img1['alt']}" style="max-width: 100%;" /></p>
<p><em>{img1['caption']}</em></p></th>
<th><p><img src="/static/images/{img2['file']}" alt="{img2['alt']}" style="max-width: 100%;" /></p>
<p><em>{img2['caption']}</em></p></th>
</tr>
</thead>
<tbody>
</tbody>
</table><br>
'''
                if block not in md_text:
                    md_text += f"\n{block}\n"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md_text)

    print(f"✅ Finished: {output_md}")

# 🔧 Handle modals
for modal_id, modal in modal_config.items():
    source = os.path.join(markdown_dir, modal["source"])
    target = os.path.join(markdown_dir, modal["output"])
    try:
        subprocess.run([
            "pandoc", source,
            "-f", "docx",
            "-t", "gfm",
            "--wrap=none",
            "-o", target
        ])
        print(f"✅ Modal converted: {modal['source']} → {modal['output']}")
    except Exception as e:
        print(f"❌ Modal failed: {modal['source']} — {e}")

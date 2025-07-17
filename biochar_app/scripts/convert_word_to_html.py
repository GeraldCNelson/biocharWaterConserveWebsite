import os
import subprocess
from bs4 import BeautifulSoup
from markdown_config import image_config, modal_config

# 📁 Directories
docx_dir = "markdown"
html_dir = os.path.join(docx_dir, "converted_html")
os.makedirs(html_dir, exist_ok=True)

# 🧪 Full Pandoc default CSS (manually trimmed for readability)
pandoc_css = '''
<style>
html {
  color: #1a1a1a;
  background-color: #fdfdfd;
}
body {
  margin: 0 auto;
  max-width: 1500px;
  padding-left: 50px;
  padding-right: 50px;
  padding-top: 50px;
  padding-bottom: 50px;
  hyphens: auto;
  overflow-wrap: break-word;
  text-rendering: optimizeLegibility;
  font-kerning: normal;
  font-family: Georgia, serif;
}
img {
  max-width: 100%;
}
table {
  width: 100%;
  border-collapse: collapse;
  margin: 1em auto;
  display: table;
}
th, td {
  padding: 0.5em;
  text-align: center;
  border: 1px solid #ddd;
}
figcaption, caption {
  font-style: italic;
  text-align: center;
  margin-top: 0.5em;
}
</style>
'''

# 🔁 Convert and post-process all .docx files
for filename in os.listdir(docx_dir):
    if filename.endswith(".docx") and not filename.startswith("~$"):
        input_path = os.path.join(docx_dir, filename)

        if filename in image_config:
            output_md = os.path.join(docx_dir, image_config[filename]["output_md"])
        elif filename in modal_config.values():
            output_md = os.path.join(docx_dir, os.path.splitext(filename)[0] + ".md")
        else:
            output_md = os.path.join(docx_dir, filename.replace(".docx", ".md"))

        print(f"\n📄 Converting {filename} → {os.path.basename(output_md)}")

        try:
            result = subprocess.run([
                "pandoc", input_path,
                "-f", "docx",
                "-t", "html",
                "--wrap=none"
            ], stdout=subprocess.PIPE, check=True, text=True)

            soup = BeautifulSoup(result.stdout, "html.parser")

            # Inject full Pandoc CSS
            if not soup.head:
                soup.insert(0, soup.new_tag("head"))
            if soup.head:
                style_tag = soup.new_tag("style")
                style_tag.string = pandoc_css.replace("<style>", "").replace("</style>", "")
                soup.head.append(style_tag)

            # 🔁 Fix image paths using image_config
            if filename in image_config:
                img_names = image_config[filename]["images"]
                img_tags = soup.find_all("img")
                for i, (img_tag, mapping) in enumerate(zip(img_tags, img_names)):
                    img_tag["src"] = f"/static/images/{mapping['file']}"

            # 🧾 Fix figure captions (add numbering if missing)
            figure_count = 1
            for fig in soup.find_all("figure"):
                if fig.figcaption and fig.figcaption.string:
                    text = fig.figcaption.get_text(strip=True)
                    if not text.lower().startswith("figure"):
                        fig.figcaption.string = f"Figure {figure_count}. {text}"
                    else:
                        fig.figcaption.string = f"Figure {figure_count}. {text.split('.', 1)[-1].strip()}"
                    figure_count += 1

            # 🧾 Fix table captions (if any)
            table_count = 1
            for tbl in soup.find_all("table"):
                if tbl.caption and tbl.caption.string:
                    text = tbl.caption.get_text(strip=True)
                    if not text.lower().startswith("table"):
                        tbl.caption.string = f"Table {table_count}. {text}"
                    else:
                        tbl.caption.string = f"Table {table_count}. {text.split('.', 1)[-1].strip()}"
                    table_count += 1

            # ✅ Save cleaned content
            with open(output_md, "w", encoding="utf-8") as f:
                f.write(str(soup))

            print(f"✅ Saved cleaned HTML to: {output_md}")

        except Exception as e:
            print(f"❌ Failed to convert {filename}: {e}")

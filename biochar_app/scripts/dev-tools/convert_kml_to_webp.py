from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from PIL import Image


# Path to biochar_app/static/images, assuming this script is run from project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATH_TO_IMAGES = PROJECT_ROOT / "biochar_app" / "static" / "images"

KML_FILE_NAME = "Biochar Injection Concept.kml"
PNG_OUTPUT_NAME = "biocharExperimentalDesign.png"
WEBP_OUTPUT_NAME = "biocharExperimentalDesign.webp"

kml_path = PATH_TO_IMAGES / KML_FILE_NAME
png_output = PATH_TO_IMAGES / PNG_OUTPUT_NAME
webp_output = PATH_TO_IMAGES / WEBP_OUTPUT_NAME


def main() -> None:
    if not kml_path.exists():
        raise FileNotFoundError(f"KML file not found: {kml_path}")

    gdf = gpd.read_file(kml_path)

    if gdf.empty:
        raise ValueError(f"KML file loaded but contains no features: {kml_path}")

    fig, ax = plt.subplots(figsize=(10, 10))

    gdf.plot(
        ax=ax,
        edgecolor="black",
        linewidth=1.0,
        alpha=0.7,
    )

    ax.set_axis_off()

    fig.savefig(
        png_output,
        dpi=300,
        bbox_inches="tight",
        pad_inches=0,
    )
    plt.close(fig)

    with Image.open(png_output) as img:
        img.save(webp_output, "WEBP", quality=80, method=6)

    print(f"Saved PNG:  {png_output}")
    print(f"Saved WebP: {webp_output}")


if __name__ == "__main__":
    main()
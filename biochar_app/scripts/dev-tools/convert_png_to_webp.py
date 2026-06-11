#!/usr/bin/env python3
"""
convert_png_to_webp.py

Crop excess white space from an existing PNG and save as WebP.
"""

from pathlib import Path
from PIL import Image, ImageChops


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATH_TO_IMAGES = PROJECT_ROOT / "static" / "images"

PNG_INPUT_NAME = "biocharExperimentalDesign.png"
WEBP_OUTPUT_NAME = "biocharExperimentalDesign.webp"

png_input = PATH_TO_IMAGES / PNG_INPUT_NAME
webp_output = PATH_TO_IMAGES / WEBP_OUTPUT_NAME


def crop_white_border(img: Image.Image, padding: int = 20) -> Image.Image:
    img = img.convert("RGB")

    bg = Image.new("RGB", img.size, (255, 255, 255))
    diff = ImageChops.difference(img, bg)
    bbox = diff.getbbox()

    if bbox is None:
        return img

    left, upper, right, lower = bbox

    left = max(left - padding, 0)
    upper = max(upper - padding, 0)
    right = min(right + padding, img.width)
    lower = min(lower + padding, img.height)

    return img.crop((left, upper, right, lower))


def main() -> None:
    if not png_input.exists():
        raise FileNotFoundError(f"PNG file not found: {png_input}")

    with Image.open(png_input) as img:
        cropped = crop_white_border(img, padding=20)
        cropped.save(webp_output, "WEBP", quality=90, method=6)

    print(f"Source PNG : {png_input}")
    print(f"Saved WebP : {webp_output}")


if __name__ == "__main__":
    main()
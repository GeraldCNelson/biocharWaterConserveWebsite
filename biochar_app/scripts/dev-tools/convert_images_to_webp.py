#!/usr/bin/env python3
"""
Convert a PNG or JPEG image to WebP.

The input and optional output paths may be absolute or relative to
``biochar_app/static/images``. If no output is supplied, the WebP is written
beside the source image with the same stem. Excess white border is cropped
before conversion.

Commands
--------
Convert the historical default image::

    python biochar_app/scripts/dev-tools/convert_images_to_webp.py

Convert a JPEG and write the WebP beside it::

    python biochar_app/scripts/dev-tools/convert_images_to_webp.py \
        jpgs/CS650s_in_field.jpg

Choose an output path relative to ``static/images``::

    python biochar_app/scripts/dev-tools/convert_images_to_webp.py \
        jpgs/CS650s_in_field.jpg \
        experiment_design/cs650s_in_field.webp
"""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageChops


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATH_TO_IMAGES = PROJECT_ROOT / "static" / "images"
DEFAULT_INPUT_NAME = "biocharExperimentalDesign.png"
SUPPORTED_INPUT_SUFFIXES = {".png", ".jpg", ".jpeg"}


def image_path(value: str | Path) -> Path:
    """Resolve relative image paths beneath the static image directory."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else PATH_TO_IMAGES / path

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

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a PNG or JPEG image to an optimized WebP image."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=DEFAULT_INPUT_NAME,
        help=(
            "PNG/JPG/JPEG input path. Relative paths are resolved beneath "
            "biochar_app/static/images."
        ),
    )
    parser.add_argument(
        "output",
        nargs="?",
        help=(
            "Optional .webp output path. Relative paths are resolved beneath "
            "biochar_app/static/images; the default is beside the input."
        ),
    )
    parser.add_argument("--padding", type=int, default=20)
    parser.add_argument("--quality", type=int, default=90)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = image_path(args.input)

    if input_path.suffix.lower() not in SUPPORTED_INPUT_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_INPUT_SUFFIXES))
        raise ValueError(
            f"Unsupported input format {input_path.suffix!r}; expected {supported}."
        )

    if not input_path.exists():
        raise FileNotFoundError(f"Image file not found: {input_path}")

    output_path = (
        image_path(args.output)
        if args.output
        else input_path.with_suffix(".webp")
    )
    if output_path.suffix.lower() != ".webp":
        raise ValueError(f"Output must use the .webp extension: {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(input_path) as img:
        cropped = crop_white_border(img, padding=max(args.padding, 0))
        cropped.save(
            output_path,
            "WEBP",
            quality=max(0, min(args.quality, 100)),
            method=6,
        )

    print(f"Source image: {input_path}")
    print(f"Saved WebP : {output_path}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
extract_field_photo_locations.py

Extract GPS metadata from field-mapping photos and write reusable location files.

Purpose
-------
This utility reads geotagged field photos from:

    biochar_app/data-raw/field-mapping/logger-photos/
    biochar_app/data-raw/field-mapping/other-photos/

and creates:

    biochar_app/data-processed/field-mapping/field_photo_locations.csv
    biochar_app/data-processed/field-mapping/field_photo_locations.geojson

The CSV is intended as a first-pass field-location inventory. Photos can be
renamed later after reviewing the image contents.

Expected input layout
---------------------
    biochar_app/data-raw/field-mapping/
        logger-photos/
            2026-05-31/
                IMG_7385.jpeg
                ...
        other-photos/
            2026-05-31/
                IMG_7372.jpeg
                ...

Output columns
--------------
photo_group
    logger-photos or other-photos.

description
    Blank field for manual notes such as PB10, NE field corner, meter,
    tailwater outlet, gated pipe, etc.

filename
    Original image filename.

relative_path
    Path relative to the project root.

pakbus_id
    Guessed from filename if the filename contains something like PB10.

latitude
    Decimal latitude from EXIF GPS metadata.

longitude
    Decimal longitude from EXIF GPS metadata.

altitude_m
    GPS altitude in meters, if present.

datetime_original
    Original photo timestamp from EXIF, if present.

Requirements
------------
Install exiftool once:
    brew install exiftool

Run from repo root:
    python biochar_app/scripts/dev-tools/extract_field_photo_locations.py
"""

from __future__ import annotations

from pathlib import Path
import csv
import json
import re
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
from typing import Any
from biochar_app.config.core import PLOT_COLORS

BASE_DIR = Path(__file__).resolve().parents[3]

FIELD_MAPPING_RAW_DIR = BASE_DIR / "biochar_app" / "data-raw" / "field-mapping"
LOGGER_PHOTOS_DIR = FIELD_MAPPING_RAW_DIR / "logger-photos"
OTHER_PHOTOS_DIR = FIELD_MAPPING_RAW_DIR / "other-photos"

OUTPUT_DIR = BASE_DIR / "biochar_app" / "data-processed" / "field-mapping"
OUT_CSV = OUTPUT_DIR / "field_photo_locations.csv"
OUT_GEOJSON = OUTPUT_DIR / "field_photo_locations.geojson"
OUT_PNG = OUTPUT_DIR / "field_photo_locations.png"

PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".JPG", ".JPEG"}

STRIP_LABELS = {
    "S1": "Half Water\nBiochar\n(BC50)",
    "S2": "Half Water\nControl\n(CON50)",
    "S3": "Full Water\nBiochar\n(BC100)",
    "S4": "Full Water\nControl\n(CON100)",
}

STRIP_COLORS = {
    "S1": PLOT_COLORS["strip_S1"],
    "S2": PLOT_COLORS["strip_S2"],
    "S3": PLOT_COLORS["strip_S3"],
    "S4": PLOT_COLORS["strip_S4"],
}

LABEL_OFFSETS = {
    "IMG_7373.JPG": (8, 10),
    "IMG_7374.JPG": (8, -2),
    "IMG_7375.JPG": (8, -14),
    "IMG_7390.JPG": (8, 10),
    "IMG_7391.JPG": (8, -2),
    "IMG_7392.JPG": (8, -14),
}

def strip_from_description(description: str) -> str:
    label = description.strip()
    if label.startswith("S1"):
        return "S1"
    if label.startswith("S2"):
        return "S2"
    if label.startswith("S3"):
        return "S3"
    if label.startswith("S4"):
        return "S4"
    return ""

def add_compass_rose(ax) -> None:
    ax.annotate(
        "",
        xy=(0.93, 0.14),       # arrow tip
        xytext=(0.93, 0.08),  # arrow base
        xycoords="axes fraction",
        textcoords="axes fraction",
        arrowprops=dict(
            arrowstyle="-|>",
            lw=1.2,
        ),
    )

    ax.text(
        0.93,
        0.065,
        "N",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=8,
        fontweight="bold",
    )

def write_png(rows: list[dict[str, object]]) -> None:
    plot_rows = []

    for row in rows:
        try:
            lat = float(row["latitude"])
            lon = float(row["longitude"])
        except (TypeError, ValueError):
            continue

        plot_rows.append(
            {
                "photo_group": row.get("photo_group", ""),
                "description": row.get("description", ""),
                "pakbus_id": row.get("pakbus_id", ""),
                "filename": row.get("filename", ""),
                "include_in_map": row.get("include_in_map", "TRUE"),
                "latitude": lat,
                "longitude": lon,
            }
        )


    if not plot_rows:
        print("No GPS points available for PNG plot.")
        return

    df = pd.DataFrame(plot_rows)
    if "include_in_map" in df.columns:
        df = df[
            df["include_in_map"]
            .astype(str)
            .str.upper()
            .isin(["TRUE", "YES", "Y", "1", ""])
        ].copy()
    df["strip"] = df["description"].apply(strip_from_description)
    df["is_corner"] = df["description"].str.contains("corner", case=False, na=False)

    fig, ax = plt.subplots(figsize=(10, 8))

    logger_df = df[df["photo_group"] == "logger-photos"].copy()
    other_df = df[(df["photo_group"] == "other-photos") & (~df["is_corner"])].copy()
    corner_df = df[df["is_corner"]].copy()

    def find_by_description(pattern: str) -> pd.Series | None:
        matches = df[
            df["description"]
            .astype(str)
            .str.contains(pattern, case=False, na=False)
        ]
        if matches.empty:
            return None
        return matches.iloc[0]

    def find_logger(label: str) -> pd.Series | None:
        matches = logger_df[
            logger_df["description"]
            .astype(str)
            .str.startswith(label, na=False)
        ]
        if matches.empty:
            return None
        return matches.iloc[0]

    # Draw strip boundary lines from logger centerlines.
    if not logger_df.empty:
        strip_centers = (
            logger_df[logger_df["strip"].isin(["S1", "S2", "S3", "S4"])]
            .groupby("strip")["longitude"]
            .mean()
            .reindex(["S1", "S2", "S3", "S4"])
        )

        valid_centers = strip_centers.dropna()

        if len(valid_centers) >= 2:
            centers = valid_centers.to_dict()
            ordered_strips = list(valid_centers.index)

            for left_strip, right_strip in zip(ordered_strips[:-1], ordered_strips[1:]):
                boundary_lon = (centers[left_strip] + centers[right_strip]) / 2.0

                ax.axvline(
                    boundary_lon,
                    color="0.35",
                    linewidth=1.2,
                    linestyle="-",
                    alpha=0.6,
                    zorder=0,
                )

            if len(valid_centers) >= 4:
                spacing_left = centers["S2"] - centers["S1"]
                spacing_right = centers["S4"] - centers["S3"]

                outer_west = centers["S1"] - spacing_left / 2.0
                outer_east = centers["S4"] + spacing_right / 2.0

                for boundary_lon in [outer_west, outer_east]:
                    ax.axvline(
                        boundary_lon,
                        color="0.15",
                        linewidth=1.8,
                        linestyle="-",
                        alpha=0.8,
                        zorder=0,
                    )

            plot_y_max = df["latitude"].max()
            plot_y_min = df["latitude"].min()
            label_y = plot_y_max - (plot_y_max - plot_y_min) * 0.055

            for strip, center_lon in centers.items():
                ax.text(
                    center_lon,
                    label_y,
                    STRIP_LABELS.get(strip, strip),
                    ha="center",
                    va="top",
                    fontsize=8,
                    fontweight="bold",
                    color=STRIP_COLORS.get(strip, "black"),
                )

    # Plot logger points by strip.
    for strip in ["S1", "S2", "S3", "S4"]:
        subset = logger_df[logger_df["strip"] == strip]

        if subset.empty:
            continue

        ax.scatter(
            subset["longitude"],
            subset["latitude"],
            color=STRIP_COLORS[strip],
            s=90,
            zorder=3,
        )

    # Plot non-corner field/infrastructure photos.
    if not other_df.empty:
        ax.scatter(
            other_df["longitude"],
            other_df["latitude"],
            color="black",
            marker="s",
            s=45,
            zorder=2,
        )

    # Plot field corners distinctly.
    if not corner_df.empty:
        ax.scatter(
            corner_df["longitude"],
            corner_df["latitude"],
            marker="D",
            color="black",
            s=45,
            zorder=6,
        )

        nw = find_by_description("NW corner")
        ne = find_by_description("NE corner")
        sw = find_by_description("SW corner")
        se = find_by_description("SE corner")

        s1t = find_logger("S1T")
        s1m = find_logger("S1M")
        s1b = find_logger("S1B")

        # Left-side logger spacing and top/bottom distances.
        add_furrow_reference_lines(
            ax=ax,
            nw=nw,
            ne=ne,
            sw=sw,
            se=se,
            logger_df=logger_df,
        )


        # Left-side logger spacing and distance from furrow start to top logger.
        if nw is not None and s1t is not None:
            add_distance_label(
                ax,
                nw,
                s1t,
                label_offset=(-0.000040, 0.000010),
                draw_line=False,
            )
            add_horizontal_tick_from_logger_to_edge(ax, nw, s1t)

        if s1t is not None and s1m is not None:
            add_distance_label(ax, s1t, s1m, label_offset=(-0.000040, 0))
            add_horizontal_tick_from_logger_to_edge(
                ax,
                nw if nw is not None else s1t,
                s1m,
            )

        if s1m is not None and s1b is not None:
            add_distance_label(ax, s1m, s1b, label_offset=(-0.000040, 0))
            add_horizontal_tick_from_logger_to_edge(
                ax,
                nw if nw is not None else s1m,
                s1b,
            )

        # Right-side total field length.
        if ne is not None and se is not None:
            add_distance_label(ax, ne, se, label_offset=(0.000045, -0.000020))

        # One bottom width measure: Field SW corner to Field SE corner.
        if sw is not None and se is not None:
            add_width_arrow_between_corners(ax, sw, se)

    # Labels for all points.
    for _, row in df.iterrows():
        label = (
                str(row.get("description", "")).strip()
                or str(row.get("pakbus_id", "")).strip()
                or str(row.get("filename", "")).strip()
        )

        label = label.replace("\\\\n", "\n").replace("\\n", "\n")

        if row.get("photo_group") == "logger-photos":
            ax.annotate(
                label,
                (row["longitude"], row["latitude"]),
                textcoords="offset points",
                xytext=(0, 8),
                ha="center",
                va="bottom",
                fontsize=7,
                color="black",
                fontweight="bold",
                zorder=5,
            )
        else:
            description = str(row.get("description", "")).lower()
            is_corner = bool(row.get("is_corner"))

            if is_corner:
                if "nw corner" in description or "sw corner" in description:
                    offset = (6, 0)
                    ha = "left"
                elif "ne corner" in description or "se corner" in description:
                    offset = (-6, 0)
                    ha = "right"
                else:
                    offset = (5, 5)
                    ha = "left"

                ax.annotate(
                    label,
                    (row["longitude"], row["latitude"]),
                    textcoords="offset points",
                    xytext=offset,
                    ha=ha,
                    va="center",
                    fontsize=7,
                    zorder=7,
                )
            else:
                offset = LABEL_OFFSETS.get(str(row.get("filename", "")), (5, 5))

                ax.annotate(
                    label,
                    (row["longitude"], row["latitude"]),
                    textcoords="offset points",
                    xytext=offset,
                    fontsize=7,
                    zorder=4,
                )

    ax.set_title("Field Locations", fontsize=14, fontweight="bold")

    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.grid(False)

    x_min = df["longitude"].min()
    x_max = df["longitude"].max()
    y_min = df["latitude"].min()
    y_max = df["latitude"].max()

    x_pad = (x_max - x_min) * 0.06
    y_pad = (y_max - y_min) * 0.06

    ax.set_xlim(x_min - x_pad, x_max + x_pad)
    ax.set_ylim(y_min - y_pad, y_max + y_pad)

    ax.set_aspect("equal", adjustable="box")
    add_compass_rose(ax)
    fig.tight_layout()
    fig.savefig(OUT_PNG, dpi=300)
    plt.close(fig)

def guess_pakbus_from_filename(path: Path) -> str:
    """
    Guess PakBus ID from filenames such as:
        PB10_IMG_7385.jpeg
        pb_10.jpg
        logger_PB02.jpeg

    Returns blank string if no PakBus-like pattern is found.
    """
    match = re.search(r"PB[\s_-]*0*(\d+)", path.stem.upper())
    if match:
        return f"PB{int(match.group(1))}"
    return ""

def photo_group_from_path(path: Path) -> str:
    parts = path.parts
    if "logger-photos" in parts:
        return "logger-photos"
    if "other-photos" in parts:
        return "other-photos"
    return "unknown"

def load_existing_manual_fields() -> dict[str, dict[str, str]]:
    if not OUT_CSV.exists():
        return {}

    old = pd.read_csv(OUT_CSV, dtype=str).fillna("")

    manual_fields: dict[str, dict[str, str]] = {}

    for _, row in old.iterrows():
        filename = str(row.get("filename", "")).strip()
        if not filename:
            continue

        manual_fields[filename] = {
            "description": str(row.get("description", "")).strip(),
            "pakbus_id": str(row.get("pakbus_id", "")).strip(),
            "notes": str(row.get("notes", "")).strip(),
            "include_in_map": str(
                row.get("include_in_map", "TRUE")
            ).strip(),
        }

    return manual_fields

def find_photos() -> list[Path]:
    photo_dirs = [LOGGER_PHOTOS_DIR, OTHER_PHOTOS_DIR]
    photos: list[Path] = []

    for photo_dir in photo_dirs:
        if not photo_dir.exists():
            print(f"Warning: photo directory not found: {photo_dir}")
            continue

        for path in photo_dir.rglob("*"):
            if path.is_file() and path.suffix in PHOTO_EXTENSIONS:
                photos.append(path)

    return sorted(photos)

def read_exif(photo: Path) -> dict[str, Any]:
    cmd = [
        "exiftool",
        "-json",
        "-GPSLatitude#",
        "-GPSLongitude#",
        "-GPSAltitude#",
        "-DateTimeOriginal",
        str(photo),
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
    )

    parsed = json.loads(result.stdout)
    if not parsed:
        return {}

    return parsed[0]

def build_rows(photos: list[Path]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    manual_fields = load_existing_manual_fields()

    for photo in photos:
        data = read_exif(photo)
        existing = manual_fields.get(photo.name, {})

        rows.append(
            {
                "photo_group": photo_group_from_path(photo),
                "description": existing.get("description", ""),
                "filename": photo.name,
                "relative_path": str(photo.relative_to(BASE_DIR)),
                "pakbus_id": existing.get("pakbus_id") or guess_pakbus_from_filename(photo),
                "latitude": data.get("GPSLatitude", ""),
                "longitude": data.get("GPSLongitude", ""),
                "altitude_m": data.get("GPSAltitude", ""),
                "datetime_original": data.get("DateTimeOriginal", ""),
                "include_in_map": existing.get("include_in_map", "TRUE"),
                "notes": existing.get("notes", ""),
            }
        )
    return rows

def distance_ft(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Approximate distance between two lat/lon points in feet.
    Good enough for this small field-scale map.
    """
    import math

    lat_mid = math.radians((lat1 + lat2) / 2.0)

    feet_per_degree_lat = 364_000
    feet_per_degree_lon = 364_000 * math.cos(lat_mid)

    dx = (lon2 - lon1) * feet_per_degree_lon
    dy = (lat2 - lat1) * feet_per_degree_lat

    return math.sqrt(dx * dx + dy * dy)

def add_distance_label(
        ax,
        p1: pd.Series,
        p2: pd.Series,
        label_offset: tuple[float, float] = (0, 0),
        fontsize: int = 7,
        draw_line: bool = True,
        linestyle: str = "--",
) -> None:
    x1, y1 = p1["longitude"], p1["latitude"]
    x2, y2 = p2["longitude"], p2["latitude"]

    dist = distance_ft(y1, x1, y2, x2)

    if draw_line:
        ax.plot(
            [x1, x2],
            [y1, y2],
            color="0.25",
            linewidth=1.0,
            linestyle=linestyle,
            zorder=1,
        )

    ax.text(
        (x1 + x2) / 2 + label_offset[0],
        (y1 + y2) / 2 + label_offset[1],
        f"{dist:.0f} ft",
        ha="center",
        va="center",
        fontsize=fontsize,
        color="0.2",
        bbox=dict(facecolor="white", edgecolor="none", alpha=0.8, pad=1.5),
        zorder=8,
        )


def add_furrow_reference_lines(
        ax,
        nw: pd.Series | None,
        ne: pd.Series | None,
        sw: pd.Series | None,
        se: pd.Series | None,
        logger_df: pd.DataFrame,
) -> None:
    # Start of furrows: line from NW corner to NE corner.
    if nw is not None and ne is not None:
        ax.plot(
            [nw["longitude"], ne["longitude"]],
            [nw["latitude"], ne["latitude"]],
            color="0.25",
            linewidth=1.0,
            linestyle="--",
            zorder=1,
        )

        ax.text(
            (nw["longitude"] + ne["longitude"]) / 2,
            (nw["latitude"] + ne["latitude"]) / 2 - 0.000018,
            "Start of furrows",
            ha="center",
            va="top",
            fontsize=8,
            fontstyle="italic",
            color="0.2",
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.8, pad=1.5),
            zorder=8,
            )

    # End of furrows: line from SW corner to SE corner.
    if sw is not None and se is not None:
        ax.plot(
            [sw["longitude"], se["longitude"]],
            [sw["latitude"], se["latitude"]],
            color="0.25",
            linewidth=1.0,
            linestyle="--",
            zorder=1,
        )

        ax.text(
            (sw["longitude"] + se["longitude"]) / 2,
            (sw["latitude"] + se["latitude"]) / 2 - 0.000030,
            "End of furrows",
            ha="center",
            va="top",
            fontsize=8,
            fontstyle="italic",
            color="0.2",
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.8, pad=1.5),
            zorder=8,
            )

    # Vertical furrow-flow lines from top loggers to bottom loggers.
    for strip in ["S1", "S2", "S3", "S4"]:
        strip_loggers = logger_df[
            logger_df["description"]
            .astype(str)
            .str.startswith(strip, na=False)
        ].copy()

        if strip_loggers.empty:
            continue

        strip_loggers = strip_loggers.sort_values("latitude", ascending=False)

        if len(strip_loggers) >= 2:
            ax.plot(
                strip_loggers["longitude"],
                strip_loggers["latitude"],
                color="0.25",
                linewidth=1.0,
                linestyle="--",
                zorder=1,
            )

    # Vertical continuation lines from bottom loggers to SW-SE furrow-end line.
    if sw is not None and se is not None:
        bottom_loggers = logger_df[
            logger_df["description"]
            .astype(str)
            .str.startswith(("S1B", "S2B", "S3B", "S4B"), na=False)
        ]

        for _, row in bottom_loggers.iterrows():
            x = row["longitude"]
            y_logger = row["latitude"]

            x1, y1 = sw["longitude"], sw["latitude"]
            x2, y2 = se["longitude"], se["latitude"]

            if x2 != x1:
                frac = (x - x1) / (x2 - x1)
                y_end = y1 + frac * (y2 - y1)
            else:
                y_end = y1

            ax.plot(
                [x, x],
                [y_logger, y_end],
                color="0.25",
                linewidth=1.0,
                linestyle="--",
                zorder=1,
            )

            dist = distance_ft(y_logger, x, y_end, x)

            ax.text(
                x + 0.000012,
                (y_logger + y_end) / 2,
                f"{dist:.0f} ft",
                ha="left",
                va="center",
                fontsize=7,
                color="0.2",
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.8, pad=1.5),
                zorder=8,
                )


def add_distance_tick(
        ax,
        edge_point: pd.Series,
        logger_point: pd.Series,
        tick_fraction: float = 0.45,
) -> None:
    x1, y1 = edge_point["longitude"], edge_point["latitude"]
    x2, y2 = logger_point["longitude"], logger_point["latitude"]

    ax.plot(
        [x1, x1 + (x2 - x1) * tick_fraction],
        [y2, y2],
        color="0.25",
        linewidth=1.0,
        linestyle="--",
        zorder=1,
    )

def add_horizontal_tick_from_logger_to_edge(
        ax,
        edge_point: pd.Series,
        logger_point: pd.Series,
        tick_fraction: float = 0.45,
) -> None:
    x_edge = edge_point["longitude"]
    x_logger = logger_point["longitude"]
    y_logger = logger_point["latitude"]

    ax.plot(
        [x_edge, x_edge + (x_logger - x_edge) * tick_fraction],
        [y_logger, y_logger],
        color="0.25",
        linewidth=1.0,
        linestyle="--",
        zorder=1,
    )

def add_width_arrow_between_corners(
        ax,
        sw: pd.Series,
        se: pd.Series,
        y_offset: float = -0.000055,
) -> None:
    x1, y1 = sw["longitude"], sw["latitude"]
    x2, y2 = se["longitude"], se["latitude"]

    y_arrow = min(y1, y2) + y_offset
    dist = distance_ft(y1, x1, y2, x2)

    ax.annotate(
        "",
        xy=(x1, y_arrow),
        xytext=(x2, y_arrow),
        arrowprops=dict(arrowstyle="<->", lw=1.0, color="0.2"),
        zorder=8,
    )

    ax.text(
        (x1 + x2) / 2,
        y_arrow + 0.000010,
        f"{dist:.0f} ft (Field SW corner to Field SE corner)",
        ha="center",
        va="bottom",
        fontsize=7,
        color="0.2",
        bbox=dict(facecolor="white", edgecolor="none", alpha=0.8, pad=1.5),
        zorder=9,
        )

def write_csv(rows: list[dict[str, object]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "photo_group",
        "description",
        "filename",
        "relative_path",
        "pakbus_id",
        "latitude",
        "longitude",
        "altitude_m",
        "datetime_original",
        "include_in_map",
        "notes",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_geojson(rows: list[dict[str, object]]) -> None:
    features: list[dict[str, object]] = []

    for row in rows:
        try:
            lat = float(row["latitude"])
            lon = float(row["longitude"])
        except (TypeError, ValueError):
            continue

        properties = {
            key: value
            for key, value in row.items()
            if key not in {"latitude", "longitude"}
        }

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],
                },
                "properties": properties,
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    OUT_GEOJSON.write_text(
        json.dumps(geojson, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    photos = find_photos()

    if not photos:
        print("No photos found.")
        print(f"Checked: {LOGGER_PHOTOS_DIR}")
        print(f"Checked: {OTHER_PHOTOS_DIR}")
        return 1

    rows = build_rows(photos)

    write_csv(rows)
    write_geojson(rows)
    write_png(rows)

    n_with_gps = sum(
        bool(row["latitude"]) and bool(row["longitude"])
        for row in rows
    )

    print(f"Wrote CSV    : {OUT_CSV}")
    print(f"Wrote GeoJSON: {OUT_GEOJSON}")
    print(f"Photos found : {len(rows)}")
    print(f"With GPS     : {n_with_gps}")
    print(f"Wrote PNG    : {OUT_PNG}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
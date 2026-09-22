"""Plot modeled upper retained water on a nominal Fruita field schematic.

The water-volume footprint uses 47-foot experimental strips and along-furrow
zone lengths. Photo-GPS geometry belongs in the general-purpose location map;
it is not precise enough to define the capacity calculation footprint.
"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile

import pandas as pd

_MPL_CONFIG = Path(tempfile.gettempdir()) / "biochar-matplotlib"
_MPL_CONFIG.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CONFIG))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize

from matplotlib.patches import Rectangle

from biochar_app.config.field_management_metadata import (
    LOGGER_ZONE_SEGMENTS_FT,
    NOMINAL_STRIP_WIDTH_FT,
)


REPO = Path(__file__).resolve().parents[4]
RESEARCH_DIR = (
    REPO / "biochar_app/docs/research/irrigation_flow_water_balance"
)
CAPACITY_CSV = RESEARCH_DIR / "zone_capacity_summary.csv"
OUTPUT_PNG = RESEARCH_DIR / "zone_capacity_field_layout.png"

STRIP_DESCRIPTIONS = {
    "S1": "Biochar · monthly",
    "S2": "Control · monthly",
    "S3": "Biochar · biweekly",
    "S4": "Control · biweekly",
}


def build_plot_data() -> pd.DataFrame:
    if not CAPACITY_CSV.exists():
        raise FileNotFoundError(
            f"Missing {CAPACITY_CSV}. Run analyze_flow_infiltration.py first."
        )

    capacity = pd.read_csv(CAPACITY_CSV)
    capacity = capacity.rename(columns={"zone_position": "logger_position"})
    capacity["feature_id"] = capacity["strip"] + capacity["logger_position"]
    expected = {f"S{s}{p}" for s in range(1, 5) for p in "TMB"}
    missing = sorted(expected - set(capacity["feature_id"]))
    if missing:
        raise ValueError(f"Missing capacity estimates for: {missing}")
    return capacity


def render() -> Path:
    capacity = build_plot_data()
    values = capacity["upper_retained_water_gal"] / 1000.0
    norm = Normalize(vmin=float(values.min()), vmax=float(values.max()))
    cmap = plt.get_cmap("YlGnBu")

    fig, ax = plt.subplots(figsize=(10, 11))
    strip_totals = capacity.drop_duplicates("strip").set_index("strip")[
        "strip_total_upper_retained_water_gal"
    ]
    max_length = max(sum(LOGGER_ZONE_SEGMENTS_FT[s].values()) for s in LOGGER_ZONE_SEGMENTS_FT)
    for strip_number, strip in enumerate(("S1", "S2", "S3", "S4")):
        x = strip_number * NOMINAL_STRIP_WIDTH_FT
        segments = LOGGER_ZONE_SEGMENTS_FT[strip]
        logger_y = {
            "T": segments["start_to_top"],
            "M": segments["start_to_top"] + segments["top_to_middle"],
            "B": segments["start_to_top"] + segments["top_to_middle"] + segments["middle_to_bottom"],
        }
        y = 0.0
        for position in ("T", "M", "B"):
            estimate = capacity.loc[
                capacity["feature_id"].eq(strip + position)
            ].iloc[0]
            zone_length = float(estimate["zone_length_ft"])
            value = float(estimate["upper_retained_water_gal"]) / 1000.0
            ax.add_patch(Rectangle(
                (x, y), NOMINAL_STRIP_WIDTH_FT, zone_length,
                facecolor=cmap(norm(value)), edgecolor="white", linewidth=1.0,
                alpha=0.72, zorder=1,
            ))
            y += zone_length
            ly = logger_y[position]
            ax.scatter(x + NOMINAL_STRIP_WIDTH_FT / 2, ly, s=55, color="black",
                       edgecolor="white", linewidth=0.8, zorder=6)
            review = "review" in str(estimate.estimate_status)
            marker = "†" if review else ""
            ax.annotate(
                f"{strip}{position}\n{value:.1f}k gal{marker}",
                (x + NOMINAL_STRIP_WIDTH_FT / 2, ly), xytext=(0, 8),
                textcoords="offset points", ha="center", va="bottom",
                fontsize=8, fontweight="bold", linespacing=1.05,
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.82, "pad": 1.1},
                zorder=7,
            )
        ax.add_patch(Rectangle((x, 0), NOMINAL_STRIP_WIDTH_FT, y,
                               fill=False, edgecolor="0.25", linewidth=1.5, zorder=4))
        ax.text(
            x + NOMINAL_STRIP_WIDTH_FT / 2, 5,
            f"{strip}\n{STRIP_DESCRIPTIONS[strip].split(' · ')[0]}\n"
            f"{STRIP_DESCRIPTIONS[strip].split(' · ')[1]}\n"
            f"{strip_totals[strip] / 1000:.1f}k gal total",
            ha="center", va="top", fontsize=7.2, fontweight="bold",
            bbox={"facecolor": "white", "edgecolor": "0.75", "alpha": 0.9, "pad": 2.5},
            zorder=8,
        )

    total_width = 4 * NOMINAL_STRIP_WIDTH_FT
    ax.set_xlim(-8, total_width + 8)
    ax.set_ylim(max_length + 14, -14)
    ax.set_aspect("equal")

    ax.text(
        total_width / 2, -5,
        "Start of furrows",
        ha="center",
        va="bottom",
        fontsize=9,
        style="italic",
    )
    ax.text(
        total_width / 2, max_length + 5,
        f"End of furrows · {total_width:.0f} ft nominal width",
        ha="center",
        va="top",
        fontsize=9,
        style="italic",
    )

    colorbar = fig.colorbar(
        ScalarMappable(norm=norm, cmap=cmap),
        ax=ax,
        fraction=0.035,
        pad=0.025,
    )
    colorbar.set_label("Upper retained water (thousand gallons)", fontsize=9)
    colorbar.ax.tick_params(labelsize=8)

    ax.set_title(
        "Modeled Upper Retained Water by Logger Influence Zone",
        fontsize=15,
        fontweight="bold",
        pad=34,
    )
    ax.text(
        0.5,
        1.012,
        "P90 retained-water state; nominal 47-ft strips and approximately 3–21 inch profile",
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=9,
        color="0.25",
    )
    fig.text(
        0.08,
        0.025,
        "Capacity footprints are nominal 47-ft strips divided at midpoints between logger positions.\n"
        "Values are modeled upper retained water, not event-specific available storage. "
        "† Component maximum and P90 differ enough to require review.\n"
        "Photo-GPS locations and boundaries are retained separately for mapping diagnostics.",
        ha="left",
        va="bottom",
        fontsize=8,
        color="0.25",
    )

    ax.set_axis_off()
    fig.subplots_adjust(left=0.07, right=0.88, top=0.91, bottom=0.10)

    OUTPUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_PNG, dpi=220, facecolor="white")
    plt.close(fig)
    return OUTPUT_PNG


if __name__ == "__main__":
    print(f"Wrote: {render()}")

"""
view_overlaps.py

Displays the overlapping segment of each image in a pair side by side.
Pixels within the shared bounding box but outside the overlap polygon
are masked out.
"""

import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from shapely import contains_xy
import sys

from geo_utils import prepare_pair


def make_overlap_mask(overlap_polygon, lon_vec, lat_vec):
    """
    Boolean mask (H, W) — True where the pixel centre falls inside the
    overlap polygon.
    """
    lon_grid, lat_grid = np.meshgrid(lon_vec, lat_vec)
    return contains_xy(overlap_polygon, lon_grid.ravel(), lat_grid.ravel()).reshape(lon_grid.shape)


def view_overlaps(pairs_dir="./pairs", resolution=512):
    folders = sorted([p for p in Path(pairs_dir).iterdir() if p.is_dir()])
    if len(folders) != 2:
        print(f"Expected exactly 2 folders in '{pairs_dir}', found {len(folders)}")
        sys.exit(1)

    print(f"Preparing pair: {folders[0].name} / {folders[1].name}")
    pair = prepare_pair(folders[0], folders[1], resolution=resolution)

    mask = make_overlap_mask(pair["overlap"], pair["lon_vec"], pair["lat_vec"])

    grid_a = pair["grid_a"].copy()
    grid_b = pair["grid_b"].copy()
    grid_a[~mask] = np.nan
    grid_b[~mask] = np.nan

    lon_min, lon_max = pair["lon_vec"][0], pair["lon_vec"][-1]
    lat_min, lat_max = pair["lat_vec"][-1], pair["lat_vec"][0]
    extent = [lon_min, lon_max, lat_min, lat_max]

    fig, (ax_a, ax_b) = plt.subplots(1, 2, figsize=(14, 7),
                                      subplot_kw={"aspect": "equal"})

    for ax, grid, name in [
        (ax_a, grid_a, pair["name_a"]),
        (ax_b, grid_b, pair["name_b"]),
    ]:
        ax.imshow(grid, cmap="gray", extent=extent)
        ax.set_xlim(lon_min, lon_max)
        ax.set_ylim(lat_min, lat_max)
        ax.set_title(name, fontsize=8)
        ax.set_xlabel("lon")
        ax.set_ylabel("lat")

    plt.suptitle("Overlap region — polygon-masked, north-up", fontsize=10)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    pairs_dir = sys.argv[1] if len(sys.argv) > 1 else "./pairs"
    resolution = int(sys.argv[2]) if len(sys.argv) > 2 else 512
    view_overlaps(pairs_dir, resolution)
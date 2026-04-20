"""
display_overlayed_images.py

Warps both full images to a common geographic grid covering their union
extent and displays them in the same frame with lon/lat axes.

RGB composite:
  Red channel  → image A
  Cyan channel → image B
  Gray         → both images agree
  White        → no data
"""

import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from shapely.ops import unary_union
import sys

from geo_utils import (
    get_gcps_for_folder,
    build_tps_transformer,
    image_footprint,
    warp_to_geo_grid,
)


def overlay_pair(pairs_dir="./pairs", resolution=1024):
    folders = sorted([p for p in Path(pairs_dir).iterdir() if p.is_dir()])
    if len(folders) != 2:
        print(f"Expected exactly 2 folders in '{pairs_dir}', found {len(folders)}")
        sys.exit(1)

    path_a, gcps_a = get_gcps_for_folder(folders[0])
    path_b, gcps_b = get_gcps_for_folder(folders[1])

    fp_a = image_footprint(gcps_a)
    fp_b = image_footprint(gcps_b)
    union = unary_union([fp_a, fp_b])

    print(f"Union bounds: {[round(x, 4) for x in union.bounds]}")

    print("Building TPS for A...")
    tf_a = build_tps_transformer(gcps_a)
    print("Building TPS for B...")
    tf_b = build_tps_transformer(gcps_b)

    print("Warping A...")
    grid_a, lon_vec, lat_vec = warp_to_geo_grid(path_a, tf_a, union, resolution)
    print("Warping B...")
    grid_b, _, _ = warp_to_geo_grid(path_b, tf_b, union, resolution)

    a = np.nan_to_num(grid_a, nan=0.0)
    b = np.nan_to_num(grid_b, nan=0.0)

    rgb = np.stack([a, b, b], axis=-1)  # R=A, G+B=cyan=B

    no_data = np.isnan(grid_a) & np.isnan(grid_b)
    rgb[no_data] = 1.0  # white where neither image has data

    extent = [lon_vec[0], lon_vec[-1], lat_vec[-1], lat_vec[0]]

    fig, ax = plt.subplots(figsize=(12, 9))
    ax.imshow(rgb, extent=extent, aspect="equal")
    ax.set_xlabel("lon")
    ax.set_ylabel("lat")
    ax.set_title(
        f"Red: {folders[0].name}\nCyan: {folders[1].name}",
        fontsize=8
    )

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    pairs_dir = sys.argv[1] if len(sys.argv) > 1 else "./pairs"
    resolution = int(sys.argv[2]) if len(sys.argv) > 2 else 1024
    overlay_pair(pairs_dir, resolution)
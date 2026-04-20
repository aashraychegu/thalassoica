import matplotlib.pyplot as plt
import numpy as np
import rasterio
from pathlib import Path
import sys

from geo_utils import get_gcps_for_folder, build_tps_transformer, image_footprint, warp_to_geo_grid


def view_tiffs(folder="./images", resolution=512):
    tiffs = sorted(Path(folder).rglob("*.tif")) + sorted(Path(folder).rglob("*.tiff"))
    if not tiffs:
        print(f"No TIFF files found under '{folder}'")
        sys.exit(1)

    n = len(tiffs)
    cols = min(n, 4)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 6 * rows))
    axes = [axes] if n == 1 else list(axes.flat)

    for ax, path in zip(axes, tiffs):
        print(f"Processing {path.parent.name}/{path.name}...")

        _, gcps = get_gcps_for_folder(path.parent)
        tf = build_tps_transformer(gcps)
        footprint = image_footprint(gcps)

        grid, lon_vec, lat_vec = warp_to_geo_grid(path, tf, footprint, resolution)

        extent = [lon_vec[0], lon_vec[-1], lat_vec[-1], lat_vec[0]]
        ax.imshow(grid, cmap="gray", extent=extent, aspect="equal")
        ax.set_title(f"{path.parent.name}\n{path.name}", fontsize=8)
        ax.set_xlabel("lon", fontsize=7)
        ax.set_ylabel("lat", fontsize=7)
        ax.tick_params(labelsize=6)

    for ax in axes[n:]:
        ax.set_visible(False)

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "./images"
    resolution = int(sys.argv[2]) if len(sys.argv) > 2 else 512
    view_tiffs(folder, resolution)
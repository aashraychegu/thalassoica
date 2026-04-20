"""
geo_utils.py

Utilities for GCP-referenced SAR TIFFs. Uses scipy RBFInterpolator
with thin-plate spline kernel — mathematically equivalent to gdalwarp -tps,
no GDAL Python bindings required.
"""

import numpy as np
import rasterio
from pathlib import Path
from shapely.geometry import MultiPoint
from scipy.interpolate import RBFInterpolator


# ---------------------------------------------------------------------------
# GCP reading
# ---------------------------------------------------------------------------

def get_gcps_for_folder(folder):
    """
    Return (tiff_path, gcps_array) using the first TIFF in the folder.
    All files share the same GCPs, so one suffices.

    gcps_array columns: [pixel_col, pixel_row, lon, lat]
    """
    folder = Path(folder)
    tiffs = sorted(folder.glob("*.tif")) + sorted(folder.glob("*.tiff"))
    if not tiffs:
        raise FileNotFoundError(f"No TIFFs found in {folder}")

    path = tiffs[0]
    with rasterio.open(path) as src:
        raw = src.gcps[0]
    if not raw:
        raise ValueError(f"No GCPs found in {path}")

    gcps = np.array([[g.col, g.row, g.x, g.y] for g in raw])
    return path, gcps


# ---------------------------------------------------------------------------
# Footprint & overlap
# ---------------------------------------------------------------------------

def image_footprint(gcps):
    """Convex hull of GCP geo coords as a Shapely Polygon."""
    pts = MultiPoint(list(zip(gcps[:, 2], gcps[:, 3])))
    return pts.convex_hull


def compute_overlap(fp_a, fp_b):
    overlap = fp_a.intersection(fp_b)
    if overlap.is_empty:
        raise ValueError("The two image footprints do not overlap")
    return overlap


# ---------------------------------------------------------------------------
# TPS geo → pixel via scipy RBFInterpolator
# ---------------------------------------------------------------------------

def build_tps_transformer(gcps):
    """
    Fit a thin-plate spline from (lon, lat) → (col, row) using GCPs.
    Mathematically equivalent to GDAL's GCP_TPS transform.

    Parameters
    ----------
    gcps : np.ndarray (N, 4)  [col, row, lon, lat]

    Returns
    -------
    transform : callable
        transform(lon_arr, lat_arr) -> (col_arr, row_arr)
    """
    geo = gcps[:, 2:4]      # (lon, lat)  — inputs
    cols = gcps[:, 0]       # pixel col   — target
    rows = gcps[:, 1]       # pixel row   — target

    rbf_col = RBFInterpolator(geo, cols, kernel="thin_plate_spline")
    rbf_row = RBFInterpolator(geo, rows, kernel="thin_plate_spline")

    def transform(lon_arr, lat_arr):
        pts = np.column_stack([
            np.asarray(lon_arr, dtype=float).ravel(),
            np.asarray(lat_arr, dtype=float).ravel(),
        ])
        return rbf_col(pts), rbf_row(pts)

    return transform


# ---------------------------------------------------------------------------
# Warp to common geographic grid
# ---------------------------------------------------------------------------

def warp_to_geo_grid(tiff_path, transform_fn, overlap_polygon, resolution=512):
    """
    Sample a GCP-referenced TIFF onto a regular lon/lat grid covering
    the overlap polygon.

    Returns
    -------
    grid    : np.ndarray (H, W), float, normalised 0–1, NaN outside image
    lon_vec : np.ndarray (W,)
    lat_vec : np.ndarray (H,)  descending (north-up)
    """
    minx, miny, maxx, maxy = overlap_polygon.bounds

    aspect = (maxx - minx) / (maxy - miny)
    if aspect >= 1:
        W, H = resolution, max(1, int(resolution / aspect))
    else:
        H, W = resolution, max(1, int(resolution * aspect))

    lon_vec = np.linspace(minx, maxx, W)
    lat_vec = np.linspace(maxy, miny, H)   # north-up: decreasing lat
    lon_grid, lat_grid = np.meshgrid(lon_vec, lat_vec)

    print(f"  Transforming {W}×{H} grid via TPS...")
    col_grid, row_grid = transform_fn(lon_grid.ravel(), lat_grid.ravel())
    col_grid = col_grid.reshape(H, W)
    row_grid = row_grid.reshape(H, W)

    with rasterio.open(tiff_path) as src:
        img_h, img_w = src.height, src.width
        data = src.read(1).astype(float)

    valid = (
        (col_grid >= 0) & (col_grid < img_w) &
        (row_grid >= 0) & (row_grid < img_h)
    )

    col_idx = np.clip(np.round(col_grid).astype(int), 0, img_w - 1)
    row_idx = np.clip(np.round(row_grid).astype(int), 0, img_h - 1)

    grid = data[row_idx, col_idx].astype(float)
    grid[~valid] = np.nan

    finite = grid[np.isfinite(grid)]
    if finite.size:
        p2, p98 = np.percentile(finite, (2, 98))
        grid = np.clip((grid - p2) / (p98 - p2 + 1e-9), 0, 1)

    return grid, lon_vec, lat_vec


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------

def prepare_pair(folder_a, folder_b, resolution=512):
    """
    Warp both images to a common north-up geographic grid over their overlap.

    Returns dict with: grid_a, grid_b, lon_vec, lat_vec, overlap, name_a, name_b
    """
    folder_a, folder_b = Path(folder_a), Path(folder_b)

    path_a, gcps_a = get_gcps_for_folder(folder_a)
    path_b, gcps_b = get_gcps_for_folder(folder_b)

    fp_a = image_footprint(gcps_a)
    fp_b = image_footprint(gcps_b)
    overlap = compute_overlap(fp_a, fp_b)

    print(f"Footprint A area : {fp_a.area:.6f} deg²")
    print(f"Footprint B area : {fp_b.area:.6f} deg²")
    print(f"Overlap area     : {overlap.area:.6f} deg²")
    print(f"Overlap bounds   : {[round(x, 4) for x in overlap.bounds]}")

    print("Building TPS for A...")
    tf_a = build_tps_transformer(gcps_a)
    print("Building TPS for B...")
    tf_b = build_tps_transformer(gcps_b)

    print("Warping A...")
    grid_a, lon_vec, lat_vec = warp_to_geo_grid(path_a, tf_a, overlap, resolution)
    print("Warping B...")
    grid_b, _, _ = warp_to_geo_grid(path_b, tf_b, overlap, resolution)

    return {
        "grid_a":  grid_a,
        "grid_b":  grid_b,
        "lon_vec": lon_vec,
        "lat_vec": lat_vec,
        "overlap": overlap,
        "name_a":  folder_a.name,
        "name_b":  folder_b.name,
    }
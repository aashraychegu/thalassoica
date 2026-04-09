#!/usr/bin/env python3
import argparse
import os
import numpy as np
import polars as pl
import xarray as xr
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed


def refine_centers_xr_interp(mslp_slice, clats, clons, dlat, dlon, method="cubic"):
    clats = np.asarray(clats, float)
    clons = np.asarray(clons, float)

    lat_points = clats[:, None] + dlat[None, :]
    lon_points = clons[:, None] + dlon[None, :]

    vals = mslp_slice.interp(
        latitude=xr.DataArray(lat_points, dims=("center", "p")),
        longitude=xr.DataArray(lon_points, dims=("center", "p")),
        method=method,
    ).values

    min_j = np.nanargmin(vals, axis=1)
    new_lats = lat_points[np.arange(clats.size), min_j]
    new_lons = lon_points[np.arange(clats.size), min_j]
    return new_lats, new_lons


def ensure_zarr_intermediate(mslp_glob: str, zarr_path: str, time_chunk: int = 1):
    if os.path.exists(zarr_path):
        return zarr_path

    print(f"Creating zarr intermediate at: {zarr_path}")
    ds = xr.open_mfdataset(mslp_glob, combine="by_coords")

    # drop the string var causing the warning
    if "expver" in ds:
        ds = ds.drop_vars("expver")

    # chunking: choose what you want; 1 is fine, but you can also try 8/16 to reduce overhead
    ds = ds.chunk({"valid_time": time_chunk})

    # write Zarr v2 + consolidated metadata
    ds.to_zarr(zarr_path, mode="w", consolidated=True, zarr_version=2)
    ds.close()

    print("Zarr intermediate created.")
    return zarr_path


def worker_one_time(store_path, store_kind, t, idxs, clats, clons, dlat, dlon, method):
    # open inside worker so each process has its own handles
    if store_kind == "zarr":
        ds = xr.open_zarr(store_path, consolidated=True)
    else:
        ds = xr.open_mfdataset(store_path, combine="by_coords")

    mslp_slice = ds["msl"].sel(valid_time=t, method="nearest")
    new_lats, new_lons = refine_centers_xr_interp(mslp_slice, clats, clons, dlat, dlon, method=method)
    ds.close()
    return idxs, new_lats, new_lons


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        default="intermediates/te_out/tracks_mslp.parquet",
        help="Input tracks Parquet file"
    )
    parser.add_argument(
        "--input-dir",
        default="intermediates/era5_mslp/*.nc",
        help="Input directory containing NetCDF MSLP files"
    )
    parser.add_argument(
        "--output",
        default="intermediates/te_out/tracks_mslp_refined.parquet",
        help="Output refined tracks Parquet file"
    )
    parser.add_argument(
        "--method",
        choices=["linear", "cubic"],
        default="cubic",
        help="Interpolation method for finding MSLP (default: cubic)"
    )
    parser.add_argument(
        "--radius",
        type=float,
        default=0.125,
        help="Search radius in degrees for MSLP interpolation (default: 0.125)"
    )
    parser.add_argument(
        "--nr",
        type=int,
        default=10,
        help="Number of radial points (default: 10)"
    )
    parser.add_argument(
        "--ntheta",
        type=int,
        default=64,
        help="Number of theta points (default: 64)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=24,
        help="Number of parallel workers (default: 24)"
    )
    parser.add_argument(
        "--zarr_intermediate",
        default="intermediates/era5_mslp/mslp.zarr",
        help="Path to a zarr store to use/create for faster parallel reads.",
    )
    parser.add_argument(
        "--zarr_time_chunk",
        type=int,
        default=1,
        help="Chunk size along valid_time when creating zarr (default 1).",
    )

    args = parser.parse_args()

    tracks = pl.read_parquet(args.input_file)

    # candidate offsets
    radius = np.linspace(0, args.radius, args.nr)
    theta = np.linspace(0, 2 * np.pi, args.ntheta, endpoint=False)
    r_grid, t_grid = np.meshgrid(radius, theta)
    dlat = (r_grid * np.sin(t_grid)).ravel()
    dlon = (r_grid * np.cos(t_grid)).ravel()

    times = tracks["datetime"].to_numpy()
    lats0 = tracks["latc"].to_numpy().astype(float)
    lons0 = tracks["lonc"].to_numpy().astype(float)

    refined_lats = lats0.copy()
    refined_lons = lons0.copy()

    # group indices by time
    time_to_idxs = {}
    for i, t in enumerate(times):
        time_to_idxs.setdefault(t, []).append(i)
    items = list(time_to_idxs.items())

    print(f"Unique Datetime Tracks: {len(items)}")
    print(f"Using {args.workers} workers; candidates per center: {dlat.size}; method={args.method}")

    # choose data source: zarr if provided, else netcdf glob
    if args.zarr_intermediate:
        store_path = ensure_zarr_intermediate(args.input_dir, args.zarr_intermediate, time_chunk=args.zarr_time_chunk)
        store_kind = "zarr"
        print(f"Using zarr store: {store_path}")
    else:
        store_path = args.input_dir
        store_kind = "netcdf"
        print(f"Using NetCDF glob: {store_path}")

    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = []
        for t, idxs in items:
            idxs = np.asarray(idxs, dtype=np.int64)
            futures.append(
                ex.submit(
                    worker_one_time,
                    store_path,
                    store_kind,
                    t,
                    idxs,
                    lats0[idxs],
                    lons0[idxs],
                    dlat,
                    dlon,
                    args.method,
                )
            )

        for fut in tqdm(as_completed(futures), total=len(futures), desc="Refining (parallel by time)"):
            idxs, new_lats, new_lons = fut.result()
            refined_lats[idxs] = new_lats
            refined_lons[idxs] = new_lons

    out = tracks.with_columns([
        pl.Series("latc", refined_lats),
        pl.Series("lonc", refined_lons),
    ])
    out.write_parquet(args.output)
    print(f"Wrote refined tracks to {args.output}")


if __name__ == "__main__":
    main()
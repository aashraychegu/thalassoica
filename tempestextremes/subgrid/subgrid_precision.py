#!/usr/bin/env python3
import argparse
import numpy as np
import polars as pl
import xarray as xr
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

REFINEMENT_FACTOR = 3

def refine_centers_xr_interp(mslp_slice, clats, clons, dlat, dlon, method="cubic"):
    """
    Refines the cyclone centers using xarray's interpolation.
    """
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


def worker_one_time(args):
    """
    Worker function that refines cyclone centers for a single time slice.
    This function is executed by each thread.
    """
    (mslp_slice, idxs, clats, clons, initial_search_radius, 
     theta_pts, method, refine_iter) = args
    
    curr_lats = clats.astype(float)
    curr_lons = clons.astype(float)
    search_radius = initial_search_radius * 3
    
    for _ in range(refine_iter):
        if search_radius < 0.01:
            break

        r_vals = np.linspace(0, search_radius, 10)
        theta = np.linspace(0, 2 * np.pi, theta_pts, endpoint=False)
        r_grid, t_grid = np.meshgrid(r_vals, theta)
        dlat = (r_grid * np.sin(t_grid)).ravel()
        dlon = (r_grid * np.cos(t_grid)).ravel()

        new_lats, new_lons = refine_centers_xr_interp(
            mslp_slice, curr_lats, curr_lons, dlat, dlon, method=method
        )
        
        curr_lats = new_lats
        curr_lons = new_lons
        search_radius /= REFINEMENT_FACTOR
    
    return idxs, curr_lats, curr_lons


def load_dataset(nc_pattern):
    """
    Load NetCDF dataset into an xarray DataArray in memory.
    """
    print("Loading dataset into memory...")
    # Use combine="by_coords" and load the data into memory.
    # No chunks are needed as we want it all in memory for threads to access.
    with xr.open_mfdataset(nc_pattern, combine="by_coords", parallel=False) as ds:
        mslp_da = ds["msl"].load()
    print(f"Dataset loaded. Total size: {mslp_da.nbytes / 1e9:.2f} GB")
    return mslp_da


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file", default="intermediates/te_out/tracks_mslp.parquet")
    parser.add_argument("--input-dir", default="intermediates/era5_mslp/*.nc")
    parser.add_argument("--output", default="intermediates/te_out/tracks_mslp_refined.parquet")
    parser.add_argument("--method", choices=["linear", "cubic"], default="cubic")
    parser.add_argument("--radius", type=float, default=0.125)
    parser.add_argument("--ntheta", type=int, default=64)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--refine-iter", type=int, default=1)
    
    args = parser.parse_args()

    tracks = pl.read_parquet(args.input_file)

    refine_iter = args.refine_iter
    theta_pts = args.ntheta
    initial_search_radius = args.radius * REFINEMENT_FACTOR

    times = tracks["datetime"].to_numpy()
    lats0 = tracks["latc"].to_numpy().astype(float)
    lons0 = tracks["lonc"].to_numpy().astype(float)

    refined_lats = lats0.copy()
    refined_lons = lons0.copy()

    time_to_idxs = {}
    for i, t in enumerate(times):
        time_to_idxs.setdefault(t, []).append(i)
    items = list(time_to_idxs.items())

    print(f"Unique Datetime Tracks: {len(items)}")
    print(f"Using {args.workers} threads; Refinement Iterations: {refine_iter}; Method={args.method}")

    # Load the full dataset into memory. It will be shared across threads.
    mslp_full_da = load_dataset(args.input_dir)
    
    tasks = []
    for t, idxs_list in items:
        # Select the required time slice from the full dataset
        mslp_slice = mslp_full_da.sel(valid_time=t, method="nearest")
        idxs = np.asarray(idxs_list, dtype=np.int64)
        
        task = (mslp_slice, idxs, lats0[idxs], lons0[idxs], 
                initial_search_radius, theta_pts, args.method, refine_iter)
        tasks.append(task)
        
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks to the thread pool
        futures = {executor.submit(worker_one_time, task): task for task in tasks}
        
        # Process results as they are completed
        for future in tqdm(as_completed(futures), total=len(tasks), desc="Refining"):
            idxs, new_lats, new_lons = future.result()
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
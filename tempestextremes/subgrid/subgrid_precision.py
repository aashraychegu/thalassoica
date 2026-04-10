#!/usr/bin/env python3
import argparse
import os
import numpy as np
import polars as pl
import xarray as xr
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from multiprocessing import shared_memory
import sys

REFINEMENT_FACTOR = 3

def refine_centers_xr_interp(mslp_slice, clats, clons, dlat, dlon, method="cubic"):
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
    """Worker function that accesses shared memory dataset"""
    (shm_info, t, idxs, clats, clons, initial_search_radius, 
     theta_pts, method, refine_iter) = args
    
    # Reconstruct xarray Dataset from shared memory
    shm_name, shape, dtype, coords_info = shm_info
    
    # Attach to existing shared memory
    shm = shared_memory.SharedMemory(name=shm_name)
    
    try:
        # Create numpy array view of shared memory
        mslp_data = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
        
        # Reconstruct xarray DataArray with coordinates
        time_coords, lat_coords, lon_coords = coords_info
        mslp_full = xr.DataArray(
            mslp_data,
            dims=["valid_time", "latitude", "longitude"],
            coords={
                "valid_time": time_coords,
                "latitude": lat_coords,
                "longitude": lon_coords
            }
        )
        
        # Select the time slice we need
        mslp_slice = mslp_full.sel(valid_time=t, method="nearest")
        
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
        
    finally:
        # Close shared memory (but don't unlink - main process will do that)
        shm.close()


def load_dataset_to_shared_memory(nc_pattern):
    """Load NetCDF dataset into shared memory"""
    print("Loading dataset into memory...")
    ds = xr.open_mfdataset(nc_pattern, combine="by_coords", chunks=None, parallel=False)
    
    # Load the full mslp data into memory
    mslp_da = ds["msl"].load()
    
    # Get the data as numpy array
    mslp_np = mslp_da.values
    
    # Create shared memory block
    shm = shared_memory.SharedMemory(create=True, size=mslp_np.nbytes)
    
    # Create numpy array backed by shared memory
    shm_array = np.ndarray(mslp_np.shape, dtype=mslp_np.dtype, buffer=shm.buf)
    
    # Copy data into shared memory
    print(f"Copying {mslp_np.nbytes / 1e9:.2f} GB to shared memory...")
    shm_array[:] = mslp_np[:]
    
    # Store coordinate information
    coords_info = (
        mslp_da.coords["valid_time"].values,
        mslp_da.coords["latitude"].values,
        mslp_da.coords["longitude"].values
    )
    
    # Package metadata for workers
    shm_info = (shm.name, mslp_np.shape, mslp_np.dtype, coords_info)
    
    ds.close()
    
    print(f"Dataset loaded into shared memory: {shm.name}")
    return shm, shm_info


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
    parser.add_argument("--mp-context", choices=["fork", "spawn", "forkserver"], default="fork",
                        help="Multiprocessing start method (use 'fork' for shared memory)")
    
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
    print(f"Using {args.workers} workers; Refinement Iterations: {refine_iter}; Method={args.method}")
    print(f"Multiprocessing context: {args.mp_context}")

    # Load dataset into shared memory
    shm, shm_info = load_dataset_to_shared_memory(args.input_dir)
    
    try:
        tasks = [
            (shm_info, t, np.asarray(idxs, dtype=np.int64), lats0[idxs], lons0[idxs], 
             initial_search_radius, theta_pts, args.method, refine_iter)
            for t, idxs in items
        ]
        
        # Use fork context for shared memory (spawn won't work)
        ctx = mp.get_context(args.mp_context)
        
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=ctx) as executor:
            futures = {executor.submit(worker_one_time, task): task for task in tasks}
            for future in tqdm(as_completed(futures), total=len(tasks), desc="Refining"):
                idxs, new_lats, new_lons = future.result()
                refined_lats[idxs] = new_lats
                refined_lons[idxs] = new_lons
    
    finally:
        # Clean up shared memory
        print("Cleaning up shared memory...")
        shm.close()
        shm.unlink()

    out = tracks.with_columns([
        pl.Series("latc", refined_lats),
        pl.Series("lonc", refined_lons),
    ])
    out.write_parquet(args.output)
    print(f"Wrote refined tracks to {args.output}")


if __name__ == "__main__":
    main()
import argparse
import os
from pathlib import Path
import multiprocessing as mp

import numpy as np
import xarray as xr
from osgeo import gdal, osr
from tqdm import tqdm
gdal.UseExceptions()

def create_geotiff_chunks(ssha_karin, lats, lons, output_dir, num_chunks=20, file_stem="swot"):
    os.makedirs(output_dir, exist_ok=True)

    total_lines = ssha_karin.shape[0]
    starts = np.linspace(0, total_lines, num_chunks + 1).round().astype(int)

    for i in range(num_chunks):
        start = starts[i]
        end = starts[i + 1]

        ssha = ssha_karin[start:end, :].values
        lat = lats[start:end, :].values
        lon = lons[start:end, :].values

        lon_min, lon_max = np.nanmin(lon), np.nanmax(lon)
        lat_min, lat_max = np.nanmin(lat), np.nanmax(lat)
        num_lines, num_pixels = ssha.shape

        pixel_size_x = (lon_max - lon_min) / num_pixels
        pixel_size_y = (lat_max - lat_min) / num_lines

        output_path = os.path.join(output_dir, f"{file_stem}_ssha_karin_chunk_{i+1:02d}.tiff")

        driver = gdal.GetDriverByName("GTiff")
        ds_out = driver.Create(
            output_path,
            num_pixels,
            num_lines,
            1,
            gdal.GDT_Float32,
            options=["COMPRESS=LZW", "PREDICTOR=3", "TILED=YES"],
        )

        geotransform = (lon_min, pixel_size_x, 0, lat_max, 0, -pixel_size_y)
        ds_out.SetGeoTransform(geotransform)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds_out.SetProjection(srs.ExportToWkt())

        band = ds_out.GetRasterBand(1)
        band.WriteArray(ssha)
        band.SetNoDataValue(np.nan)
        band.SetDescription("Sea Surface Height Anomaly")

        band.FlushCache()
        ds_out = None


def process_one_file(nc_path: Path, output_dir: Path, num_chunks: int):
    file_out_dir = output_dir / nc_path.stem
    file_out_dir.mkdir(parents=True, exist_ok=True)

    try:
        ds = xr.open_dataset(nc_path, cache=True)
        ssha_karin = ds["ssha_karin"]
        lat = ds["latitude"]
        lon = ds["longitude"]

        create_geotiff_chunks(
            ssha_karin=ssha_karin,
            lats=lat,
            lons=lon,
            output_dir=str(file_out_dir),
            num_chunks=num_chunks,
            file_stem=nc_path.stem,
        )
        return (str(nc_path), True, None)
    except Exception as e:
        return (str(nc_path), False, str(e))
    finally:
        try:
            ds.close()
        except:
            pass  # Handle cases where ds might not be properly initialized


def process_file_wrapper(args):
    """Wrapper function for process_one_file to unpack arguments."""
    return process_one_file(*args)


def build_arg_parser():
    p = argparse.ArgumentParser(
        description="Chunk all SWOT .nc files in a directory into GeoTIFFs (multiprocessed)."
    )
    p.add_argument(
        "-i",
        "--input-dir",
        default="intermediates/swot/",
        help="Directory containing .nc files",
    )
    p.add_argument(
        "-o",
        "--output-dir",
        default="intermediates/swot_tiffs/",
        help="Directory where outputs will be written",
    )
    p.add_argument(
        "-n",
        "--num-chunks",
        type=int,
        default=20,
        help="Number of chunks per file (default: 20)",
    )
    p.add_argument(
        "-p",
        "--processes",
        type=int,
        default=mp.cpu_count(),
        help="Number of worker processes (default: cpu_count)",
    )
    p.add_argument(
        "--pattern",
        default="*.nc",
        help="Glob pattern for input files (default: *.nc)",
    )
    return p


if __name__ == "__main__":
    args = build_arg_parser().parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    nc_files = sorted(input_dir.glob(args.pattern))
    if not nc_files:
        raise SystemExit(f"No files found in {input_dir} matching pattern: {args.pattern}")

    print(f"Files: {len(nc_files)} | Chunks per file: {args.num_chunks} | Processes: {args.processes}")

    # Create a list of arguments for each file to be processed
    file_args = [(nc_path, output_dir, args.num_chunks) for nc_path in nc_files]

    with mp.Pool(processes=args.processes) as pool:
        results = list(tqdm(pool.imap(process_file_wrapper, file_args), total=len(nc_files), desc="Files", unit="file"))

    ok = sum(1 for _, success, _ in results if success)
    bad = len(results) - ok

    print(f"Completed. Success: {ok}, Failed: {bad}")
    if bad:
        print("Failures:")
        for path, success, err in results:
            if not success:
                print(f"  - {path}: {err}")
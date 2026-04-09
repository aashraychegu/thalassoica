import xarray as xr
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tqdm import tqdm
import argparse
from multiprocessing import Pool, cpu_count
from functools import partial

from pathlib import Path
from typing import Dict, Union
import re

def parse_swot_filename(file_path: Union[str, Path]) -> Dict[str, str]:
    filename_stem = file_path.stem
    
    # Define regex pattern for SWOT filename
    pattern = r'(SWOT)_(L\d)_(LR)_(SSH)_(Basic)_(\d{3})_(\d{3})_(\d{8}T\d{6})_(\d{8}T\d{6})_([A-Z0-9]+)_(\d+)'
    
    match = re.match(pattern, filename_stem)
        
    parsed = {
        'mission': match.group(1),
        'level': match.group(2),
        'lr': match.group(3),
        'product': match.group(4),
        'type': match.group(5),
        'cycle': match.group(6),
        'pass': match.group(7),
        'start_time': match.group(8),
        'end_time': match.group(9),
        'center': match.group(10),
        'version': match.group(11),
        'filename': filename_stem
    }
    
    return parsed


def process_single_file(file_path, step):
    chunks = []
    ds = xr.open_dataset(file_path, cache=True)

    lats = ds["latitude"]
    lons = ds["longitude"]
    time = ds["time"]

    parsed = parse_swot_filename(Path(file_path))
    template = f"{parsed['cycle']}-{parsed['pass']}-{parsed['product']}-"

    bounds = np.linspace(0, lats.shape[0], step).round().astype(int)
    for start, end in zip(bounds[:-1], bounds[1:]):
        chunk_lats = lats[start:end].values
        chunk_lons = lons[start:end].values
        chunk_time = time[start:end].values

        min_lat = chunk_lats.min()
        max_lat = chunk_lats.max()
        min_lon = chunk_lons.min()
        max_lon = chunk_lons.max()

        # Convert to int nanoseconds since epoch (UTC)
        min_time_ns = int(chunk_time.min().astype("int64"))
        max_time_ns = int(chunk_time.max().astype("int64"))

        wkt = (
            f"POLYGON(({min_lon} {min_lat},"
            f"{max_lon} {min_lat},"
            f"{max_lon} {max_lat},"
            f"{min_lon} {max_lat},"
            f"{min_lon} {min_lat}))"
        )

        chunks.append({
            "geometry": wkt,
            "datetime_start": min_time_ns,
            "datetime_end": max_time_ns,
            "path": str(file_path),
            "start_idx": int(start),
            "end_idx": int(end),
            "id": template + f"{start}-{end}",
        })

        if max_lat >= -65:
            break

    ds.close()
    return chunks

def main():
    parser = argparse.ArgumentParser(description='Process SWOT NetCDF files into chunked Parquet index')
    parser.add_argument(
        '--input-dir',
        type=str,
        default='./intermediates/swot/',
        help='Directory containing NetCDF files (default: ./intermediates/swot/)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='./intermediates/shapes/swot/swot.parquet',
        help='Output Parquet file path (default: ./intermediates/shapes/swot/swot.parquet)'
    )
    parser.add_argument(
        '--step',
        type=int,
        default=20,
        help='Number of chunks to divide each file into (default: 20)'
    )
    parser.add_argument(
        '--pattern',
        type=str,
        default='*.nc',
        help='File pattern to match (default: *.nc)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of worker processes (default: CPU count)'
    )

    args = parser.parse_args()

    # Determine number of workers
    n_workers = args.workers if args.workers else cpu_count()
    print(f"Using {n_workers} worker processes")

    # Directory containing the NetCDF files
    data_dir = Path(args.input_dir)
    nc_files = list(data_dir.glob(args.pattern))

    if not nc_files:
        print(f"No files found matching pattern '{args.pattern}' in {data_dir}")
        return

    print(f"Found {len(nc_files)} files to process")

    # Create a partial function with the step parameter fixed
    process_func = partial(process_single_file, step=args.step)

    # Process files in parallel
    all_chunks = []
    with Pool(processes=n_workers) as pool:
        # Use imap_unordered for progress bar
        results = list(tqdm(
            pool.imap_unordered(process_func, nc_files),
            total=len(nc_files),
            desc="Processing files"
        ))

        # Flatten the list of lists
        for file_chunks in results:
            all_chunks.extend(file_chunks)

    if not all_chunks:
        print("No chunks were created!")
        return

    # Convert to PyArrow Table
    print(f"Creating PyArrow table from {len(all_chunks)} chunks...")
    schema = pa.schema([
        ('geometry', pa.string()),
        ('datetime_start', pa.timestamp('ns', tz='UTC')),
        ('datetime_end', pa.timestamp('ns', tz='UTC')),
        ('path', pa.string()),
        ('start_idx', pa.int64()),
        ('end_idx', pa.int64()),
        ('id', pa.string()),
        ('product_type', pa.string())
    ])

    # Extract columns from chunks
    geometry = [c['geometry'] for c in all_chunks]
    datetime_start = [c['datetime_start'] for c in all_chunks]
    datetime_end = [c['datetime_end'] for c in all_chunks]
    path = [c['path'] for c in all_chunks]
    start_idx = [c['start_idx'] for c in all_chunks]
    end_idx = [c['end_idx'] for c in all_chunks]
    id_str = [c['id'] for c in all_chunks]
    KaRIn = ["KaRIn" for c in all_chunks]

    table = pa.table({
        'geometry': geometry,
        'datetime_start': datetime_start,
        'datetime_end': datetime_end,
        'path': path,
        'start_idx': start_idx,
        'end_idx': end_idx,
        'id': id_str,
        'product_type': KaRIn
    }, schema=schema)

    # Write to Parquet
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing to {output_path}...")
    pq.write_table(table, output_path)

    print(f"\nSuccessfully wrote {len(all_chunks)} chunks to {output_path}")

if __name__ == "__main__":
    main()
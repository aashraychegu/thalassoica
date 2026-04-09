#!/usr/bin/env python3
"""
Download Sentinel-1 TIFF files from S3 based on a CSV file.

Usage:
    python download_s3_tiffs.py --csv input.csv --output /path/to/output --workers 4
"""

import argparse
import polars as pl
import subprocess
import os
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


def download_tiffs(s3_path, output_dir, s3cfg_path=".s3cfg"):
    """
    Download all TIFF files from an S3 path to the output directory.
    
    Args:
        s3_path: S3 path without s3:/ prefix and without /measurement/
        output_dir: Local directory to save files
        s3cfg_path: Path to s3cfg configuration file
    
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    # Construct full S3 path
    full_s3_path = f"s3:/{s3_path}/measurement/"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Build s3cmd command
    cmd = [
        "s3cmd",
        "-c", s3cfg_path,
        "get",
        "--recursive",
        "--include=*.tiff",
        "--include=*.tif",
        full_s3_path,
        output_dir + "/"
    ]
    
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return True, None


def download_single_entry(row, output_base, s3cfg_path):
    """
    Download files for a single entry.
    
    Args:
        row: Dictionary with 'id' and 's3_path' keys
        output_base: Base output directory path
        s3cfg_path: Path to s3cfg file
    
    Returns:
        tuple: (id_name, success, error_message)
    """
    id_name = str(row['id'])
    s3_path = str(row['s3_path'])
    
    # Create folder for this ID
    id_output_dir = output_base / id_name
    
    # Download TIFFs
    success, error = download_tiffs(s3_path, str(id_output_dir), s3cfg_path)
    
    return id_name, success, error


def main():
    parser = argparse.ArgumentParser(
        description="Download Sentinel-1 TIFF files from S3 based on CSV"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file with 'id' and 's3_path' columns"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory where folders with ID names will be created"
    )
    parser.add_argument(
        "--s3cfg",
        default=".s3cfg",
        help="Path to s3cfg configuration file (default: .s3cfg)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)"
    )
    
    args = parser.parse_args()
    
    # Read CSV
    try:
        df = pl.read_csv(args.csv)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    # Validate columns
    if 'id' not in df.columns or 's3_path' not in df.columns:
        print("Error: CSV must contain 'id' and 's3_path' columns")
        print(f"Found columns: {df.columns}")
        return
    
    # Create output directory
    output_base = Path(args.output)
    output_base.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting download with {args.workers} parallel workers...")
    
    # Track statistics
    success_count = 0
    fail_count = 0
    failed_ids = []
    
    # Convert to list of dicts for parallel processing
    rows = list(df.iter_rows(named=True))
    
    # Process with parallel workers
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(download_single_entry, row, output_base, args.s3cfg): row
            for row in rows
        }
        
        # Process completed tasks with progress bar
        with tqdm(total=len(rows), desc="Downloading", unit="file") as pbar:
            for future in as_completed(futures):
                id_name, success, error = future.result()
                
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                    failed_ids.append((id_name, error))
                
                # Update progress bar with stats
                pbar.set_description(f"✓ {success_count} | ✗ {fail_count}")
                pbar.set_postfix_str(f"Last: {id_name}")
                pbar.update(1)
    
    # Summary
    print("\n" + "=" * 80)
    print("DOWNLOAD SUMMARY")
    print(f"Total entries: {len(df)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    
    if failed_ids:
        print("\nFailed downloads:")
        for id_name, error in failed_ids:
            print(f"  - {id_name}: {error}")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
import subprocess
import argparse
import os
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from threading import Lock
import time
import csv

# Thread-safe print lock
print_lock = Lock()


def safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)


def load_token():
    """Load ACCESS_TOKEN from .env file"""
    load_dotenv()
    access_token = os.getenv("ACCESS_TOKEN")
    if not access_token:
        safe_print("Error: ACCESS_TOKEN not found. Run get_token.py first.")
        return None
    return access_token


def load_product_ids_from_csv(csv_file):
    """Load product IDs from CSV file with 'id' column"""
    product_ids = []
    with open(csv_file, "r", newline="") as f:
        reader = csv.DictReader(f)

        if "id" not in reader.fieldnames:
            safe_print(
                f"Error: CSV file must have an 'id' column. Found columns: {reader.fieldnames}"
            )
            return None

        for row in reader:
            product_id = (row.get("id") or "").strip()
            if product_id:
                product_ids.append(product_id)

    safe_print(f"Loaded {len(product_ids)} product ID(s) from {csv_file}")
    return product_ids


def download_with_curl(product_id, output_filename, access_token):
    """Download using curl"""
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"

    safe_print(f"[{product_id}] Starting download")

    output_path = Path(output_filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "curl",
        "-L",
        "-#",
        "-H",
        f"Authorization: Bearer {access_token}",
        "-o",
        output_filename,
        url,
    ]

    subprocess.run(cmd, check=True, capture_output=True)
    safe_print(f"[{product_id}] ✓ Downloaded to {output_filename}")
    return True


def extract_tiff_files(zip_path, product_id, output_dir, keep_zip=False):
    """
    Extract TIFF files into: output_dir/<product_id>/
    Keep original TIFF filenames.
    """
    output_root = Path(output_dir)
    product_folder = output_root / product_id
    product_folder.mkdir(parents=True, exist_ok=True)

    temp_dir = product_folder / f"temp_{product_id}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    safe_print(f"[{product_id}] Extracting TIFF files from {zip_path}...")

    # List contents and filter for TIFF files
    list_cmd = ["unzip", "-l", str(zip_path)]
    result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)

    tiff_files = []
    for line in result.stdout.split("\n"):
        if ".tif" in line.lower():
            parts = line.split()
            if len(parts) >= 4:
                filename = " ".join(parts[3:])
                tiff_files.append(filename)

    if not tiff_files:
        safe_print(f"[{product_id}] ⚠ No TIFF files found in {zip_path}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False

    safe_print(f"[{product_id}] Found {len(tiff_files)} TIFF file(s)")

    # Extract TIFF files (flatten paths into temp_dir)
    for tiff_file in tiff_files:
        extract_cmd = ["unzip", "-j", "-o", str(zip_path), tiff_file, "-d", str(temp_dir)]
        subprocess.run(extract_cmd, check=True, capture_output=True)

    # Move extracted files into product folder, preserving original names
    extracted_files = sorted(
        [p for p in temp_dir.iterdir() if p.is_file() and ".tif" in p.name.lower()]
    )
    for src in extracted_files:
        dst = product_folder / src.name
        if dst.exists():
            dst.unlink()
        shutil.move(str(src), str(dst))
        safe_print(f"[{product_id}] ✓ Extracted: {dst}")

    # Clean up temp directory
    shutil.rmtree(temp_dir, ignore_errors=True)

    if not keep_zip:
        os.remove(zip_path)
        safe_print(f"[{product_id}] ✓ Removed zip file: {zip_path}")
    else:
        safe_print(f"[{product_id}] ✓ Kept zip file: {zip_path}")

    return True


def process_product(product_id, output_dir, zip_dir, keep_zip=False, skip_download=False, access_token=None):
    """Process a single product (download and extract)"""
    zip_path = Path(zip_dir) / f"{product_id}.zip"

    if not skip_download:
        download_with_curl(product_id, str(zip_path), access_token)

    ok = extract_tiff_files(str(zip_path), product_id, output_dir, keep_zip)
    return product_id, ok, ("Success" if ok else "Extraction failed")


def main():
    parser = argparse.ArgumentParser(
        description="Download and extract TIFF files from Copernicus Data Space (parallel)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--csv-file",
        required=True,
        help='CSV file containing product IDs (must have an "id" column)',
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for TIFF files")
    parser.add_argument("--zip-dir", required=True, help="Directory to store zip files")
    parser.add_argument("--keep-zip", action="store_true", help="Keep the original zip files after extraction")
    parser.add_argument("--skip-download", action="store_true", help="Skip download, only extract existing zip files")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")

    args = parser.parse_args()

    product_ids = load_product_ids_from_csv(args.csv_file)
    if product_ids is None:
        return
    if not product_ids:
        safe_print("Error: No product IDs found in CSV file")
        return

    access_token = load_token()
    if not access_token:
        return

    safe_print(f"Processing {len(product_ids)} product(s) with {args.workers} worker(s)")
    safe_print(f"Zip files directory: {args.zip_dir}")
    safe_print(f"TIFF output directory: {args.output_dir}")
    safe_print(f"Keep zip files: {args.keep_zip}")
    safe_print("=" * 80)

    start_time = time.time()
    results = {"success": [], "failed": []}

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_id = {
            executor.submit(
                process_product,
                product_id,
                args.output_dir,
                args.zip_dir,
                args.keep_zip,
                args.skip_download,
                access_token,
            ): product_id
            for product_id in product_ids
        }

        for future in as_completed(future_to_id):
            product_id = future_to_id[future]
            try:
                pid, success, message = future.result()
            except Exception as e:
                results["failed"].append((product_id, str(e)))
                safe_print(f"[{product_id}] ✗ Failed: {e}")
                continue

            if success:
                results["success"].append(pid)
            else:
                results["failed"].append((pid, message))

    elapsed_time = time.time() - start_time
    safe_print("=" * 80)
    safe_print("\nSummary:")
    safe_print(f"  Total processed: {len(product_ids)}")
    safe_print(f"  Successful: {len(results['success'])}")
    safe_print(f"  Failed: {len(results['failed'])}")
    safe_print(f"  Time elapsed: {elapsed_time:.2f} seconds")

    if results["failed"]:
        safe_print("\nFailed products:")
        for product_id, message in results["failed"]:
            safe_print(f"  - {product_id}: {message}")


if __name__ == "__main__":
    main()
import subprocess
import argparse
import os
import shutil
from pathlib import Path
from dotenv import load_dotenv

def load_token():
    """Load ACCESS_TOKEN from .env file"""
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    
    if not access_token:
        print("Error: ACCESS_TOKEN not found. Run get_token.py first.")
        return None
    
    return access_token

def download_with_curl(product_id, output_filename):
    """Download using curl"""
    access_token = load_token()
    if not access_token:
        return False
    
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    
    print(f"Downloading: {product_id}")
    
    cmd = [
        'curl',
        '-L',
        '-#',
        '-H', f'Authorization: Bearer {access_token}',
        '-o', output_filename,
        url
    ]
    
    subprocess.run(cmd, check=True)
    print(f"✓ Downloaded to {output_filename}")
    return True

def extract_tiff_files(zip_path, product_id, output_dir, keep_zip=False):
    """
    Extract TIFF files using unzip and rename them to UUID
    
    Args:
        zip_path: Path to the zip file
        product_id: Product UUID (used for renaming)
        output_dir: Directory to save extracted TIFFs
        keep_zip: Whether to keep the original zip file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    temp_dir = output_path / 'temp'
    temp_dir.mkdir(exist_ok=True)
    
    print(f"Extracting TIFF files from {zip_path}...")
    
    # List contents and filter for TIFF files
    list_cmd = ['unzip', '-l', str(zip_path)]
    result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)
    
    tiff_files = []
    for line in result.stdout.split('\n'):
        if '.tif' in line.lower():
            parts = line.split()
            if len(parts) >= 4:
                filename = ' '.join(parts[3:])
                tiff_files.append(filename)
    
    if not tiff_files:
        print(f"⚠ No TIFF files found in {zip_path}")
        return False
    
    print(f"Found {len(tiff_files)} TIFF file(s)")
    
    # Extract TIFF files
    for tiff_file in tiff_files:
        extract_cmd = ['unzip', '-j', '-o', str(zip_path), tiff_file, '-d', str(temp_dir)]
        subprocess.run(extract_cmd, check=True, capture_output=True)
    
    # Rename extracted files
    extracted_files = sorted(temp_dir.glob('*.tif*'))
    for i, src in enumerate(extracted_files):
        if len(extracted_files) == 1:
            new_name = f"{product_id}.tif"
        else:
            new_name = f"{product_id}_{i+1}.tif"
        
        dst = output_path / new_name
        shutil.move(str(src), str(dst))
        print(f"  ✓ Extracted: {new_name}")
    
    # Clean up
    shutil.rmtree(temp_dir)
    
    if not keep_zip:
        os.remove(zip_path)
        print(f"  ✓ Removed zip file: {zip_path}")
    
    return True

def main():
    parser = argparse.ArgumentParser(
        description='Download and extract TIFF files from Copernicus Data Space',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--product-id',
        required=True,
        help='Product UUID to download'
    )
    parser.add_argument(
        '--output-dir',
        default='tiff_files',
        help='Output directory for TIFF files',
        required=True
    )
    parser.add_argument(
        '--keep-zip',
        action='store_true',
        help='Keep the original zip file after extraction'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download, only extract existing zip file'
    )
    
    args = parser.parse_args()
    
    zip_filename = f"{args.product_id}.zip"
    
    # Download if not skipped
    if not args.skip_download:
        download_with_curl(args.product_id, zip_filename)
    
    # Extract TIFF files
    extract_tiff_files(zip_filename, args.product_id, args.output_dir, args.keep_zip)

if __name__ == "__main__":
    main()
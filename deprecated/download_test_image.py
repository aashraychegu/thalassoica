import subprocess
import argparse
import os
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
    
    try:
        subprocess.run(cmd, check=True)
        print(f"✓ Downloaded to {output_filename}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ curl download failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print("✗ curl not found. Please install curl.")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Download products from Copernicus Data Space',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--product-id',
        required=True,
        help='Product UUID to download'
    )
    parser.add_argument(
        '-o', '--output',
        help='Output filename (defaults to product_id.zip)'
    )
    
    args = parser.parse_args()
    
    output = args.output or f"{args.product_id}.zip"
    success = download_with_curl(args.product_id, output)
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
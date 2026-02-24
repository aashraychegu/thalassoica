import subprocess
import argparse
import os
import requests
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm

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
    
    print(f"Downloading with curl: {product_id}")
    
    cmd = [
        'curl',
        '-L',  # Follow redirects
        '-#',  # Progress bar
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

def download_with_wget(product_id, output_filename):
    """Download using wget"""
    access_token = load_token()
    if not access_token:
        return False
    
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    
    print(f"Downloading with wget: {product_id}")
    
    cmd = [
        'wget',
        f'--header=Authorization: Bearer {access_token}',
        '--show-progress',
        '--progress=bar:force',
        '-O', output_filename,
        url
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print(f"✓ Downloaded to {output_filename}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ wget download failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print("✗ wget not found. Please install wget.")
        return False

def download_with_aria2c(product_id, output_filename, connections=1):
    """Download using aria2c"""
    access_token = load_token()
    if not access_token:
        return False
    
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    
    print(f"Downloading with aria2c: {product_id} (connections: {connections})")
    
    cmd = [
        'aria2c',
        f'--header=Authorization: Bearer {access_token}',
        f'--max-connection-per-server={connections}',
        f'--split={connections}',
        '--min-split-size=1M',
        '--continue=true',
        f'--out={output_filename}',
        url
    ]
    
    subprocess.run(cmd, check=True)
    print(f"✓ Downloaded to {output_filename}")
    return True


def download_with_requests(product_id, output_filename, chunk_size=8*1024*1024):
    """Download using Python requests library"""
    access_token = load_token()
    if not access_token:
        return False
    
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    print(f"Downloading with requests: {product_id}")
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, stream=True)
        
        if response.status_code != 200:
            print(f"✗ Download failed. Status: {response.status_code}")
            return False
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_filename, 'wb', buffering=chunk_size) as file:
            with tqdm(
                desc=Path(output_filename).name,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    file.write(chunk)
                    bar.update(len(chunk))
        
        print(f"✓ Downloaded to {output_filename}")
        session.close()
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Download error: {e}")
        return False

def download_product(product_id, output_filename=None, method='curl', connections=1):
    """
    Download a product using the specified method
    
    Args:
        product_id: Product UUID to download
        output_filename: Output file path (defaults to product_id.zip)
        method: Download method ('curl', 'wget', 'aria2c', 'requests')
        connections: Number of parallel connections for aria2c (default: 1)
    """
    if output_filename is None:
        output_filename = f"{product_id}.zip"
    
    methods = {
        'curl': lambda: download_with_curl(product_id, output_filename),
        'wget': lambda: download_with_wget(product_id, output_filename),
        'aria2c': lambda: download_with_aria2c(product_id, output_filename, connections),
        'requests': lambda: download_with_requests(product_id, output_filename),
    }
    
    if method not in methods:
        print(f"✗ Unknown method: {method}")
        print(f"Available methods: {', '.join(methods.keys())}")
        return False
    
    return methods[method]()

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
    parser.add_argument(
        '-m', '--method',
        choices=['curl', 'wget', 'aria2c', 'requests'],
        default='curl',
        help='Download method to use'
    )
    parser.add_argument(
        '-c', '--connections',
        type=int,
        default=1,
        help='Number of parallel connections (aria2c only, use 1 for Copernicus)'
    )
    
    args = parser.parse_args()
    
    success = download_product(
        args.product_id,
        args.output,
        args.method,
        args.connections
    )
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
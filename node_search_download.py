import pycurl
import polars as pl
import argparse
from io import BytesIO
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from sentinelhub import SHConfig, SentinelHubDownloadClient, DownloadRequest
import os
from dotenv import load_dotenv
import copernicus_access_token
from threading import Lock

parser = argparse.ArgumentParser(
    description="Download Sentinel-1 TIFF files from S3 based on a uuid parquet"
)
parser.add_argument("--uuids", required=True, help="Path to uuids file with 'id' and 's3_path' columns")
parser.add_argument("--output", required=True, help="Output directory where folders with ID names will be created")
parser.add_argument("--dotenv", default=".env", help="Path to .env file")
parser.add_argument("--username", type=str)
parser.add_argument('--password', type=str)
parser.add_argument("--search-workers", type=int, default=32, help="Number of parallel workers for searching (default: 32)")
parser.add_argument("--download-workers", type=int, default=4, help="Number of parallel workers (default: 4)")

args = parser.parse_args()
df = pl.read_parquet(args.uuids)

copernicus_access_token.authenticate(args.username, args.password, args.dotenv)
load_dotenv(args.dotenv)

token_lock = Lock()

def get_current_token():
    """Thread-safe token retrieval"""
    with token_lock:
        load_dotenv(args.dotenv, override=True)
        return os.getenv("ACCESS_TOKEN")

def refresh_token_safe():
    """Thread-safe token refresh - only one thread refreshes at a time"""
    with token_lock:
        copernicus_access_token.refresh_access_token(args.dotenv)
        load_dotenv(args.dotenv, override=True)
        return os.getenv("ACCESS_TOKEN")

def process_pair(pair):
    """Process a single pair and return download information"""
    cid = pair["id"]
    s3path = pair["s3_path"]
    
    archive_name = s3path.split("/")[-1]
    
    search_url = (f"https://download.dataspace.copernicus.eu/odata/v1/"
                f"Products({cid})/"
                f"Nodes({archive_name})/Nodes(measurement)/Nodes")
    
    c = pycurl.Curl()
    c.setopt(c.URL, search_url)
    buffer = BytesIO()
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    
    http_code = c.getinfo(pycurl.HTTP_CODE)
    c.close()
    
    if http_code != 200:
        return []
    
    result = json.loads(buffer.getvalue().decode('utf-8'))["result"]
        
    download_list = []
    for i in result:
        filename = i["Id"]
        download_information = {
            "download_url": search_url + f"({filename})/$value",
            "folder": cid,
            "filename": filename
        }
        download_list.append(download_information)
    
    return download_list

# Parallelize the processing
all_downloads = []

with ThreadPoolExecutor(max_workers=args.search_workers) as executor:
    futures = {executor.submit(process_pair, pair): pair 
               for pair in df.iter_rows(named=True)}

    for future in tqdm(as_completed(futures), total=len(df), desc="Building download list"):
        try:
            downloads = future.result()
            all_downloads.extend(downloads)
        except Exception as e:
            print(f"Error processing pair: {e}")

download_dataframe = pl.DataFrame(all_downloads)

def download_file(row):
    """Download a single file using pycurl with automatic token refresh"""
    output_path = Path(args.output) / row["folder"]
    output_path.mkdir(parents=True, exist_ok=True)
    
    file_path = output_path / row["filename"]
    
    max_retries = 3
    for attempt in range(max_retries):
        token = get_current_token()
        
        c = pycurl.Curl()
        c.setopt(c.URL, row["download_url"])
        c.setopt(c.HTTPHEADER, [f"Authorization: Bearer {token}"])
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.FAILONERROR, False)
        
        with open(file_path, 'wb') as f:
            c.setopt(c.WRITEDATA, f)
            c.perform()
            
            http_code = c.getinfo(pycurl.HTTP_CODE)
            c.close()
            
            if http_code == 200:
                return
            elif http_code == 401 and attempt < max_retries - 1:
                # Token expired, refresh and retry (thread-safe)
                token = refresh_token_safe()
                continue
            else:
                file_path.unlink(missing_ok=True)
                raise Exception(f"HTTP {http_code} for {row['filename']}")
    
    raise Exception(f"Failed after {max_retries} attempts")

# Download all files
with ThreadPoolExecutor(max_workers=args.download_workers) as executor:
    futures = {executor.submit(download_file, row): row 
               for row in download_dataframe.iter_rows(named=True)}

    for future in tqdm(as_completed(futures), total=len(download_dataframe), desc="Downloading files"):
        try:
            future.result()
        except Exception as e:
            print(f"Error downloading file: {e}")

print(f"\nDownloaded {len(download_dataframe)} files to {args.output}")
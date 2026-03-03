import subprocess
import argparse
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from threading import Lock, BoundedSemaphore
import time
import csv
import random
from tqdm import tqdm
from datetime import datetime, timedelta
import requests

print_lock = Lock()
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"


def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


class TokenManager:
    def __init__(self):
        self.lock = Lock()
        load_dotenv()
        self.access_token = os.getenv('ACCESS_TOKEN')
        self.refresh_token = os.getenv('REFRESH_TOKEN')
        expires_str = os.getenv('ACCESS_TOKEN_EXPIRES_AT')
        self.expires_at = datetime.fromisoformat(expires_str)
        
    def get_valid_token(self) -> str:
        with self.lock:
            if (self.expires_at - datetime.now()).total_seconds() < 60:
                safe_print("🔄 Refreshing token...")
                response = requests.post(
                    TOKEN_URL,
                    data={
                        'grant_type': 'refresh_token',
                        'refresh_token': self.refresh_token,
                        'client_id': 'cdse-public'
                    },
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                response.raise_for_status()
                token_data = response.json()
                
                self.access_token = token_data['access_token']
                self.refresh_token = token_data['refresh_token']
                self.expires_at = datetime.now() + timedelta(seconds=token_data['expires_in'])
                safe_print("✅ Token refreshed")
            
            return self.access_token


def load_product_ids_from_csv(csv_file):
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        ids = [row["id"].strip() for row in reader if row.get("id", "").strip()]
    safe_print(f"Loaded {len(ids)} product IDs")
    return ids


def is_rate_limited(stderr_text: str) -> bool:
    s = (stderr_text or "").lower()
    return "429" in s or "too many requests" in s


def is_auth_error(stderr_text: str) -> bool:
    s = (stderr_text or "").lower()
    return "401" in s or "403" in s


def download_with_curl(product_id, zip_path: Path, token_manager: TokenManager, semaphore: BoundedSemaphore):
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with semaphore:
        attempt = 1
        backoff = 2.0
        
        while attempt <= 8:
            access_token = token_manager.get_valid_token()
            
            cmd = [
                "curl", "-L", "--fail", "-sS",
                "-H", f"Authorization: Bearer {access_token}",
                "-o", str(zip_path),
                url,
            ]

            p = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)

            if p.returncode == 0:
                return True

            zip_path.unlink(missing_ok=True)

            if is_auth_error(p.stderr):
                print("Auth issues")
                quit()

            if is_rate_limited(p.stderr):
                sleep_s = min(60, backoff) * (0.8 + 0.4 * random.random())
                safe_print(f"[{product_id}] Rate limited, sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
                backoff *= 2
                attempt += 1
                continue

            raise RuntimeError(f"curl failed: {p.stderr.strip()}")
        
        raise RuntimeError(f"Failed after {attempt} attempts")


def extract_tiffs(zip_path: Path, product_id: str, output_dir: str, keep_zip: bool):
    product_folder = Path(output_dir) / product_id
    product_folder.mkdir(parents=True, exist_ok=True)

    cmd = [
        "unzip", "-j", "-o", str(zip_path),
        "*/measurement/*.tif", "*/measurement/*.tiff",
        "-d", str(product_folder),
    ]

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    extracted = list(product_folder.glob("*.tif")) + list(product_folder.glob("*.tiff"))
    
    if not keep_zip:
        zip_path.unlink(missing_ok=True)

    return len(extracted) > 0


def process_product(product_id, output_dir, zip_dir, keep_zip, skip_download, token_manager, dl_semaphore):
    zip_path = Path(zip_dir) / f"{product_id}.zip"

    if not skip_download:
        download_with_curl(product_id, zip_path, token_manager, dl_semaphore)

    ok = extract_tiffs(zip_path, product_id, output_dir, keep_zip)
    return product_id, ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-file", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--zip-dir", required=True)
    parser.add_argument("--keep-zip", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--download-slots", type=int, default=4)
    args = parser.parse_args()

    product_ids = load_product_ids_from_csv(args.csv_file)
    token_manager = TokenManager()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    Path(args.zip_dir).mkdir(parents=True, exist_ok=True)

    dl_semaphore = BoundedSemaphore(args.download_slots)
    start = time.time()
    ok_count = 0
    failed = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(process_product, pid, args.output_dir, args.zip_dir, 
                     args.keep_zip, args.skip_download, token_manager, dl_semaphore): pid
            for pid in product_ids
        }

        with tqdm(total=len(product_ids), desc="Products", unit="product") as pbar:
            for fut in as_completed(futures):
                pid = futures[fut]
                try:
                    _, ok = fut.result()
                    if ok:
                        ok_count += 1
                    else:
                        failed.append((pid, "No TIFFs found"))
                        safe_print(f"failed: {pid}")
                except Exception as e:
                    safe_print(f"failed: {pid} \n{str(e)}")
                    
                    failed.append((pid, str(e)))
                finally:
                    pbar.update(1)

    elapsed = time.time() - start
    safe_print(f"\nProcessed: {len(product_ids)} | Success: {ok_count} | Failed: {len(failed)} | Time: {elapsed:.1f}s")

    if failed:
        safe_print("\nFailed products:")
        for pid, msg in failed[:50]:
            safe_print(f"  {pid}: {msg}")


if __name__ == "__main__":
    main()
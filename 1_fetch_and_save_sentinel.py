#!/usr/bin/env python3
import sys
import time
from datetime import datetime, timezone, timedelta
import requests
from pathlib import Path
import multiprocessing
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
import json
import argparse
from typing import Optional, Dict, Any, List, Tuple

# ==============================================================================
# CONFIGURATION
# ==============================================================================

DEFAULT_START_DATE = datetime(2014, 1, 1, tzinfo=timezone.utc)
DEFAULT_END_DATE = datetime.now(timezone.utc)
DEFAULT_AREA_OF_INTEREST_WKT = "POLYGON((-180 -80, 180 -80, 180 -60, -180 -60, -180 -80))"
DEFAULT_OUTPUT_DIR = Path("intermediates/shapes/sentinel")
API_URL_BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
DEFAULT_ITEMS_PER_REQUEST = 1000
DEFAULT_NUM_WORKERS = 16
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1
DEFAULT_DAYS_PER_CHUNK = 2

SCHEMA = pa.schema([
    ('id', pa.string()),
    ('name', pa.string()),
    ('datetime_start', pa.timestamp('us', tz='UTC')),
    ('datetime_end', pa.timestamp('us', tz='UTC')),
    ('geometry', pa.string()),
    ('s3_path', pa.string()),
    ('orbit_number', pa.int64()),
    ('relative_orbit_number', pa.int64()),
    ('orbit_direction', pa.string()),
    ('operational_mode', pa.string()),
    ('swath_identifier', pa.string()),
    ('instrument_short_name', pa.string()),
    ('product_type', pa.string()),
    ('platform_serial', pa.string()),
    ('polarisation', pa.string()),
])

# Required fields for validation
REQUIRED_FIELDS = frozenset([
    "operationalMode", "swathIdentifier", "instrumentShortName",
    "polarisationChannels", "productType", "orbitNumber"
])

# Reusable session per worker
_session = None

def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({'Connection': 'keep-alive'})
    return _session

# ==============================================================================
# PARSING - OPTIMIZED
# ==============================================================================

def parse_data_fast(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Optimized parsing - works directly with dict, no JSON serialization"""
    result = {}
    
    # Direct key extraction
    for key in ['Id', 'Name', 'S3Path', 'Footprint']:
        val = data.get(key)
        if val not in (None, ''):
            result[key] = val
    
    # ContentDate
    cd = data.get('ContentDate')
    if cd:
        start = cd.get('Start')
        end = cd.get('End')
        if start not in (None, ''):
            result['ContentDate_Start'] = start
        if end not in (None, ''):
            result['ContentDate_End'] = end
    
    # Attributes - fast dict creation
    attrs = data.get('Attributes')
    if attrs:
        for attr in attrs:
            name = attr.get('Name')
            if name:
                val = attr.get('Value')
                if val not in (None, ''):
                    result[name] = val
    
    # Quick validation of required fields
    for key in REQUIRED_FIELDS:
        if key not in result or result[key] in (None, ''):
            return None
    
    # Type conversions
    result['orbitNumber'] = int(result['orbitNumber'])
    if 'relativeOrbitNumber' in result:
        result['relativeOrbitNumber'] = int(result['relativeOrbitNumber'])
    
    return result

# ==============================================================================
# HELPERS
# ==============================================================================

def generate_date_chunks(start_date: datetime, end_date: datetime, days_per_chunk: int = DEFAULT_DAYS_PER_CHUNK) -> List[Tuple[datetime, datetime]]:
    """Pre-generate all chunks as a list for better memory access"""
    chunks = []
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + timedelta(days=days_per_chunk)
        chunks.append((current_start, min(current_end, end_date)))
        current_start = current_end
    return chunks

def make_request_with_retry(url: str, session: requests.Session, attempts: int = DEFAULT_RETRY_ATTEMPTS, delay: int = DEFAULT_RETRY_DELAY) -> Optional[requests.Response]:
    """Retry logic with session reuse"""
    for attempt in range(attempts):
        try:
            response = session.get(url, timeout=180)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            if attempt < attempts - 1:
                time.sleep(delay * (attempt + 1))
                tqdm.write(f"Delaying - Attempt {attempt}")
    return None

def parse_iso_datetime_fast(dt_str: str) -> Optional[datetime]:
    """Faster datetime parsing"""
    if not dt_str:
        return None
    # Direct replace is faster than regex
    return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

def clean_footprint_fast(footprint: str) -> Optional[str]:
    """Optimized footprint cleaning"""
    if not footprint:
        return None
    # Single operation instead of chained
    if footprint.startswith("geography'SRID=4326;"):
        return footprint[20:-1]
    return footprint

# ==============================================================================
# WORKER - OPTIMIZED
# ==============================================================================

# Global variables for worker configuration
WORKER_ITEMS_PER_REQUEST = DEFAULT_ITEMS_PER_REQUEST
WORKER_AREA_OF_INTEREST_WKT = DEFAULT_AREA_OF_INTEREST_WKT

def fetch_and_process_chunk(date_chunk: Tuple[datetime, datetime]) -> Optional[pa.Table]:
    """Optimized worker with session reuse and minimal allocations"""
    chunk_start, chunk_end = date_chunk
    offset = 0
    session = get_session()
    
    rows = []
    
    # Pre-format the base URL parts that don't change
    base_filter = (
        f"$filter=Collection/Name eq 'SENTINEL-1' "
        f"and ContentDate/Start ge {chunk_start:%Y-%m-%dT%H:%M:%S.%fZ} "
        f"and ContentDate/Start lt {chunk_end:%Y-%m-%dT%H:%M:%S.%fZ} "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{WORKER_AREA_OF_INTEREST_WKT}')"
        "&$orderby=ContentDate/Start asc"
        f"&$top={WORKER_ITEMS_PER_REQUEST}"
        "&$expand=Attributes"
    )
    
    while True:
        url = f"{API_URL_BASE}?{base_filter}&$skip={offset}"
        
        try:
            response = make_request_with_retry(url, session)
            if response is None:
                return None
            items = response.json().get("value", [])
        except:
            return None

        if not items:
            break

        # Process items in batch
        for item in items:
            parsed = parse_data_fast(item)
            
            if not parsed:
                continue
            
            # Direct dict construction - no .get() overhead
            row = {
                'id': parsed['Id'],
                'name': parsed['Name'],
                'datetime_start': parse_iso_datetime_fast(parsed['ContentDate_Start']),
                'datetime_end': parse_iso_datetime_fast(parsed['ContentDate_End']),
                'geometry': clean_footprint_fast(parsed['Footprint']),
                's3_path': parsed['S3Path'],
                'orbit_number': parsed['orbitNumber'],
                'relative_orbit_number': parsed.get('relativeOrbitNumber'),
                'orbit_direction': parsed.get('orbitDirection'),
                'operational_mode': parsed['operationalMode'],
                'swath_identifier': parsed['swathIdentifier'],
                'instrument_short_name': parsed['instrumentShortName'],
                'product_type': parsed['productType'],
                'platform_serial': parsed.get('platformSerialIdentifier'),
                'polarisation': parsed['polarisationChannels'],
            }
            rows.append(row)

        offset += WORKER_ITEMS_PER_REQUEST
        
        # Early exit if we got fewer items than requested
        if len(items) < WORKER_ITEMS_PER_REQUEST:
            break

    if not rows:
        return None

    # Single efficient conversion
    return pa.Table.from_pylist(rows, schema=SCHEMA)

# ==============================================================================
# ARGUMENT PARSING
# ==============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description='Fetch Sentinel-1 data from Copernicus Data Space')
    
    parser.add_argument('--start-date', type=str, 
                        default=DEFAULT_START_DATE.strftime('%Y-%m-%d'),
                        help=f'Start date (YYYY-MM-DD) (default: {DEFAULT_START_DATE.strftime("%Y-%m-%d")})')
    
    parser.add_argument('--end-date', type=str,
                        default=DEFAULT_END_DATE.strftime('%Y-%m-%d'),
                        help='End date (YYYY-MM-DD) (default: today)')
    
    parser.add_argument('--area-wkt', type=str,
                        default=DEFAULT_AREA_OF_INTEREST_WKT,
                        help='Area of interest as WKT POLYGON (default: Antarctic region)')
    
    parser.add_argument('--output-dir', type=str,
                        default=str(DEFAULT_OUTPUT_DIR),
                        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})')
    
    parser.add_argument('--output-file', type=str,
                        help='Output parquet filename (default: auto-generated from dates)')
    
    parser.add_argument('--workers', type=int,
                        default=DEFAULT_NUM_WORKERS,
                        help=f'Number of worker processes (default: {DEFAULT_NUM_WORKERS})')
    
    parser.add_argument('--days-per-chunk', type=int,
                        default=DEFAULT_DAYS_PER_CHUNK,
                        help=f'Days per chunk (default: {DEFAULT_DAYS_PER_CHUNK})')
    
    parser.add_argument('--items-per-request', type=int,
                        default=DEFAULT_ITEMS_PER_REQUEST,
                        help=f'Items per API request (default: {DEFAULT_ITEMS_PER_REQUEST})')
    
    return parser.parse_args()

# ==============================================================================
# MAIN
# ==============================================================================

if __name__ == "__main__":
    args = parse_args()
    
    # Parse dates
    START_DATE = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    END_DATE = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    AREA_OF_INTEREST_WKT = args.area_wkt
    OUTPUT_DIR = Path(args.output_dir)
    NUM_WORKERS = args.workers
    DAYS_PER_CHUNK = args.days_per_chunk
    ITEMS_PER_REQUEST = args.items_per_request
    
    # Set global worker variables
    WORKER_ITEMS_PER_REQUEST = ITEMS_PER_REQUEST
    WORKER_AREA_OF_INTEREST_WKT = AREA_OF_INTEREST_WKT
    
    # Determine output file
    if args.output_file:
        OUTPUT_PARQUET_FILE = OUTPUT_DIR / args.output_file
    else:
        OUTPUT_PARQUET_FILE = OUTPUT_DIR / f"sentinel_{START_DATE.strftime('%Y%m%d')}_to_{END_DATE.strftime('%Y%m%d')}.parquet"
    
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    
    # Pre-generate all tasks
    tasks = generate_date_chunks(START_DATE, END_DATE, DAYS_PER_CHUNK)
    
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Chunks: {len(tasks)} ({NUM_WORKERS} workers, {DAYS_PER_CHUNK} days/chunk)")
    print(f"Output: {OUTPUT_PARQUET_FILE}")
    
    total_items = 0
    
    with pq.ParquetWriter(OUTPUT_PARQUET_FILE, SCHEMA, compression="GZIP") as writer:
        # Use maxtasksperchild to prevent memory leaks
        with multiprocessing.Pool(processes=NUM_WORKERS, maxtasksperchild=100) as pool:
            # imap_unordered is faster than map
            results = pool.imap_unordered(fetch_and_process_chunk, tasks, chunksize=1)
            
            for table in tqdm(results, total=len(tasks), desc="Processing:", unit=f"{DAYS_PER_CHUNK} day chunks"):
                if table is not None:
                    writer.write_table(table)
                    total_items += len(table)
    
    print(f"Total items written: {total_items}")
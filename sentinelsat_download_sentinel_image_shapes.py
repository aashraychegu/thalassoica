"""
Get ALL Sentinel-1 GRD metadata for Southern Ocean using sentinelsat
"""
from sentinelsat import SentinelAPI
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import box
from shapely import wkt, wkb
import os

bbox = [-180, -90, 180, -50]  # Southern Ocean - below 50°S
batch_size = 5000

print("="*60)
print("Sentinel-1 Southern Ocean - Copernicus (sentinelsat)")
print("="*60)
print(f"Region: Southern Ocean (below 50°S)")
print("="*60)

# Connect to Copernicus Data Space Ecosystem
api = SentinelAPI(
    None,  # No username needed for queries
    None,  # No password needed for queries
    'https://catalogue.dataspace.copernicus.eu/odata/v1'
)

schema = pa.schema([
    ('id', pa.string()),
    ('title', pa.string()),
    ('datetime', pa.timestamp('us', tz='UTC')),
    ('geometry', pa.binary()),
    ('min_lon', pa.float64()),
    ('min_lat', pa.float64()),
    ('max_lon', pa.float64()),
    ('max_lat', pa.float64()),
    ('platform', pa.string()),
    ('orbit_direction', pa.string()),
    ('polarization', pa.string()),
    ('product_type', pa.string()),
    ('sensor_mode', pa.string()),
    ('relative_orbit', pa.int32()),
])

output_file = 'intermediates/shapes/grd-sentinel1_southern_ocean_all.parquet'
writer = pq.ParquetWriter(output_file, schema, compression='snappy')

# Create AOI polygon
aoi = box(bbox[0], bbox[1], bbox[2], bbox[3])

# Time ranges (monthly chunks) - return datetime objects
def generate_time_ranges(start_date_str, end_date_str, days=30):
    """Generate time ranges in chunks as datetime objects"""
    ranges = []
    current = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    while current < end:
        next_date = current + timedelta(days=days)
        if next_date > end:
            next_date = end
        ranges.append((current, next_date))
        current = next_date + timedelta(days=1)
    
    return ranges

# Generate monthly ranges as datetime objects
time_ranges = generate_time_ranges(
    "2014-04-01", 
    datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    days=30
)

batch = []
total_records = 0
skipped_records = 0

print(f"\nQuerying {len(time_ranges)} time periods...\n")

for start_date, end_date in time_ranges:
    # Format for display
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    print(f"Processing {start_str} to {end_str}...")
    
    try:
        # Query Copernicus Data Space - pass datetime objects directly
        products = api.query(
            area=aoi,
            date=(start_date, end_date),  # Pass datetime objects, not strings
            platformname='Sentinel-1',
            producttype='GRD',
        )
        
        chunk_count = len(products)
        print(f"  Found {chunk_count:,} products")
        
        if chunk_count == 0:
            continue
        
        pbar = tqdm(desc=f"  {start_str[:7]}", total=chunk_count, unit=" records")
        
        for product_id, product_info in products.items():
            try:
                # Get geometry - footprint is in WKT format
                footprint_wkt = product_info.get('footprint')
                
                if footprint_wkt:
                    geom = wkt.loads(footprint_wkt)
                else:
                    # Skip if no footprint
                    skipped_records += 1
                    pbar.update(1)
                    continue
                
                bounds = geom.bounds
                
                # Extract datetime
                begin_position = product_info.get('beginposition')
                if isinstance(begin_position, str):
                    begin_position = datetime.fromisoformat(begin_position.replace('Z', '+00:00'))
                
                batch.append({
                    'id': product_id,
                    'title': product_info.get('title', ''),
                    'datetime': begin_position,
                    'geometry': wkb.dumps(geom),
                    'min_lon': bounds[0],
                    'min_lat': bounds[1],
                    'max_lon': bounds[2],
                    'max_lat': bounds[3],
                    'platform': product_info.get('platformname', ''),
                    'orbit_direction': product_info.get('orbitdirection', ''),
                    'polarization': product_info.get('polarisationmode', ''),
                    'product_type': product_info.get('producttype', ''),
                    'sensor_mode': product_info.get('sensoroperationalmode', ''),
                    'relative_orbit': product_info.get('relativeorbitnumber'),
                })
                
                total_records += 1
                pbar.update(1)
                
                if len(batch) >= batch_size:
                    table = pa.Table.from_pylist(batch, schema=schema)
                    writer.write_table(table)
                    batch = []
            
            except Exception as e:
                print(f"\n  Error processing product {product_id}: {e}")
                skipped_records += 1
                pbar.update(1)
                continue
        
        pbar.close()
        print(f"  ✓ {chunk_count:,} records from this period")
        
    except Exception as e:
        print(f"  ✗ Error querying period: {e}")
        import traceback
        traceback.print_exc()
        continue

# Write remaining
if batch:
    table = pa.Table.from_pylist(batch, schema=schema)
    writer.write_table(table)

writer.close()

print("\n" + "="*60)
print("DOWNLOAD COMPLETE")
print("="*60)
print(f"✓ Total saved: {total_records:,} records")
if skipped_records > 0:
    print(f"✗ Skipped: {skipped_records:,} records (errors/missing data)")

if os.path.exists(output_file):
    file_size_mb = os.path.getsize(output_file) / (1024**2)
    print(f"File size: {file_size_mb:.2f} MB")

# Verification
import duckdb
con = duckdb.connect()
result = con.execute(f"""
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT id) as unique_ids,
        MIN(datetime) as first,
        MAX(datetime) as last,
        COUNT(DISTINCT platform) as platforms,
        COUNT(DISTINCT product_type) as product_types,
        COUNT(DISTINCT sensor_mode) as sensor_modes
    FROM read_parquet('{output_file}')
""").fetchone()

print(f"\nVerified: {result[0]:,} records ({result[1]:,} unique)")
print(f"Date range: {result[2]} to {result[3]}")
print(f"Platforms: {result[4]}")
print(f"Product types: {result[5]}")
print(f"Sensor modes: {result[6]}")

if skipped_records > 0:
    print(f"\nSuccess rate: {total_records/(total_records+skipped_records)*100:.2f}%")

con.close()
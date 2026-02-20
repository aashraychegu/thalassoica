import pyarrow as pa
import pyarrow.parquet as pq
import pystac_client
from shapely.geometry import shape
from shapely import wkb
from datetime import datetime, timezone
from tqdm import tqdm
import os

bbox = [-180, -80, 180, -50]
batch_size = 5000

print("="*60)
print("Sentinel-1 Southern Ocean - Download to Parquet")
print("="*60)

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1"
)

schema = pa.schema([
    ('id', pa.string()),
    ('datetime', pa.timestamp('us', tz='UTC')),
    ('geometry', pa.binary()),
    ('min_lon', pa.float64()),
    ('min_lat', pa.float64()),
    ('max_lon', pa.float64()),
    ('max_lat', pa.float64()),
    ('platform', pa.string()),
    ('constellation', pa.string()),
    ('orbit_direction', pa.string()),
    ('polarization', pa.string()),
    ('product_type', pa.string()),
    ('sensor_mode', pa.string())
])

output_file = 'intermediates/shapes/grd-sentinel1_southern_ocean_all.parquet'

# Ensure directory exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

writer = pq.ParquetWriter(output_file, schema, compression='snappy')

# Time ranges
time_ranges = [
    ("2014-04-01", "2014-12-31"),
    ("2015-01-01", "2015-06-30"),
    ("2015-07-01", "2015-12-31"),
    ("2016-01-01", "2016-06-30"),
    ("2016-07-01", "2016-12-31"),
    ("2017-01-01", "2017-06-30"),
    ("2017-07-01", "2017-12-31"),
    ("2018-01-01", "2018-06-30"),
    ("2018-07-01", "2018-12-31"),
    ("2019-01-01", "2019-06-30"),
    ("2019-07-01", "2019-12-31"),
    ("2020-01-01", "2020-06-30"),
    ("2020-07-01", "2020-12-31"),
    ("2021-01-01", "2021-06-30"),
    ("2021-07-01", "2021-12-31"),
    ("2022-01-01", "2022-06-30"),
    ("2022-07-01", "2022-12-31"),
    ("2023-01-01", "2023-06-30"),
    ("2023-07-01", "2023-12-31"),
    ("2024-01-01", "2024-06-30"),
    ("2024-07-01", "2024-12-31"),
    ("2025-01-01", "2025-06-30"),
    ("2025-07-01", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
]

def clean_geometry(geom_dict):
    """Clean malformed MultiPolygon geometries"""
    if geom_dict['type'] == 'MultiPolygon':
        coords = geom_dict['coordinates']
        cleaned = [poly for poly in coords if poly and len(poly) > 0 and poly[0]]
        
        if not cleaned:
            return None
        
        if len(cleaned) == 1:
            return {
                'type': 'Polygon',
                'coordinates': cleaned[0]
            }
        
        return {
            'type': 'MultiPolygon',
            'coordinates': cleaned
        }
    
    return geom_dict

batch = []
total_records = 0
skipped_records = 0

for start_date, end_date in time_ranges:
    print(f"\nProcessing {start_date} to {end_date}...")
    
    search = catalog.search(
        collections=["sentinel-1-grd"],
        bbox=bbox,
        datetime=f"{start_date}/{end_date}",
        limit=1000,
    )
    
    chunk_count = 0
    pbar = tqdm(desc=f"  {start_date[:7]}", unit=" records", dynamic_ncols=True)
    
    for item in search.items():
        try:
            # Clean geometry first
            cleaned_geom = clean_geometry(item.geometry)
            
            if cleaned_geom is None:
                skipped_records += 1
                pbar.update(1)
                continue
            
            geom = shape(cleaned_geom)
            bounds = geom.bounds
            
            polarizations = [k.upper() for k in item.assets.keys() if k in ['vh', 'vv', 'hh', 'hv']]
            
            batch.append({
                'id': item.id,
                'datetime': item.datetime,
                'geometry': wkb.dumps(geom),
                'min_lon': bounds[0],
                'min_lat': bounds[1],
                'max_lon': bounds[2],
                'max_lat': bounds[3],
                'platform': item.properties.get('platform'),
                'constellation': item.properties.get('constellation'),
                'orbit_direction': item.properties.get('sat:orbit_state'),
                'polarization': ','.join(polarizations) if polarizations else None,
                'product_type': item.properties.get('s1:product_type'),
                'sensor_mode': item.properties.get('sar:instrument_mode')
            })
            
            chunk_count += 1
            total_records += 1
            pbar.update(1)
            
            if len(batch) >= batch_size:
                table = pa.Table.from_pylist(batch, schema=schema)
                writer.write_table(table)
                batch = []
        
        except Exception as e:
            print(f"\n  Error processing item {item.id}: {e}")
            skipped_records += 1
            pbar.update(1)
            continue
    
    pbar.close()
    print(f"  ✓ {chunk_count:,} records from this period")

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
    print(f"✗ Skipped: {skipped_records:,} records (malformed geometry)")

if os.path.exists(output_file):
    file_size_mb = os.path.getsize(output_file) / (1024**2)
    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Output file: {output_file}")

if skipped_records > 0:
    print(f"\nSuccess rate: {total_records/(total_records+skipped_records)*100:.2f}%")
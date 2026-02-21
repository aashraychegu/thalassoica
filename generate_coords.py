#!/usr/bin/env python3
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timezone, timedelta
from shapely import wkt
from shapely.geometry import Point
import numpy as np
from typing import Tuple, List
import random

# ==============================================================================
# CONFIGURATION
# ==============================================================================

DEFAULT_WKT = "POLYGON((-180 -80, 180 -80, 180 -60, -180 -60, -180 -80))"
DEFAULT_OUTPUT_FILE = "sample_coordinates.parquet"
DEFAULT_NUM_POINTS = 1000
DEFAULT_START_DATE = "2014-10-13"
DEFAULT_END_DATE = "2024-01-01"
DEFAULT_TIMEZONE_OFFSET = -7  # UTC-7

SCHEMA = pa.schema([
    ('datetime_start', pa.timestamp('us', tz='UTC')),
    ('latitude', pa.float64()),
    ('longitude', pa.float64()),
    ('size',pa.float64())
])

# ==============================================================================
# HELPERS
# ==============================================================================

def parse_wkt_bounds(wkt_string: str) -> Tuple[float, float, float, float]:
    """Parse WKT and return bounding box (min_lon, min_lat, max_lon, max_lat)"""
    geom = wkt.loads(wkt_string)
    bounds = geom.bounds
    return bounds  # (minx, miny, maxx, maxy)

def generate_random_point_in_polygon(polygon_wkt: str) -> Tuple[float, float]:
    """Generate a random point inside a polygon using rejection sampling"""
    polygon = wkt.loads(polygon_wkt)
    min_lon, min_lat, max_lon, max_lat = polygon.bounds
    
    max_attempts = 1000
    for _ in range(max_attempts):
        lon = random.uniform(min_lon, max_lon)
        lat = random.uniform(min_lat, max_lat)
        point = Point(lon, lat)
        
        if polygon.contains(point):
            return lat, lon
    
    # Fallback to centroid if rejection sampling fails
    centroid = polygon.centroid
    return centroid.y, centroid.x

def generate_random_datetime(start_date: datetime, end_date: datetime) -> datetime:
    """Generate a random datetime between start and end dates"""
    delta = end_date - start_date
    random_seconds = random.uniform(0, delta.total_seconds())
    random_microseconds = random.randint(0, 999999)
    
    random_dt = start_date + timedelta(seconds=random_seconds)
    random_dt = random_dt.replace(microsecond=random_microseconds)
    
    return random_dt

def generate_data(wkt_string: str, num_points: int, start_date: datetime, 
                  end_date: datetime) -> List[dict]:
    """Generate random coordinates and timestamps within the WKT bounds"""
    rows = []
    
    for _ in range(num_points):
        lat, lon = generate_random_point_in_polygon(wkt_string)
        dt = generate_random_datetime(start_date, end_date)
        
        rows.append({
            'datetime_start': dt,
            'latitude': lat,
            'longitude': lon,
            'size': random.uniform(200,500),
        })
    
    # Sort by datetime for better compression and query performance
    rows.sort(key=lambda x: x['datetime_start'])
    
    return rows

# ==============================================================================
# ARGUMENT PARSING
# ==============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Generate random lat/lon coordinates with timestamps inside a WKT polygon',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Generate 1000 random points in Antarctic region
  %(prog)s --num-points 1000

  # Custom WKT polygon
  %(prog)s --wkt "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))" --num-points 500

  # Custom date range
  %(prog)s --start-date 2020-01-01 --end-date 2024-01-01 --num-points 2000

  # Custom output file
  %(prog)s --output my_points.parquet --num-points 10000
        ''')
    
    parser.add_argument('--wkt', type=str,
                        default=DEFAULT_WKT,
                        help='WKT POLYGON string defining area of interest (default: Antarctic region)')
    
    parser.add_argument('--output', type=str,
                        default=DEFAULT_OUTPUT_FILE,
                        help=f'Output parquet filename (default: {DEFAULT_OUTPUT_FILE})')
    
    parser.add_argument('--num-points', type=int,
                        default=DEFAULT_NUM_POINTS,
                        help=f'Number of random points to generate (default: {DEFAULT_NUM_POINTS})')
    
    parser.add_argument('--start-date', type=str,
                        default=DEFAULT_START_DATE,
                        help=f'Start date (YYYY-MM-DD) (default: {DEFAULT_START_DATE})')
    
    parser.add_argument('--end-date', type=str,
                        default=DEFAULT_END_DATE,
                        help=f'End date (YYYY-MM-DD) (default: {DEFAULT_END_DATE})')
    
    parser.add_argument('--seed', type=int,
                        help='Random seed for reproducibility (default: random)')
    
    return parser.parse_args()

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    args = parse_args()
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
        np.random.seed(args.seed)
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    # Validate WKT
    try:
        polygon = wkt.loads(args.wkt)
        print(f"WKT validated: {polygon.geom_type}")
        print(f"Bounds: {polygon.bounds}")
    except Exception as e:
        print(f"Error parsing WKT: {e}")
        return 1
    
    # Generate data
    print(f"\nGenerating {args.num_points} random points...")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    rows = generate_data(args.wkt, args.num_points, start_date, end_date)
    
    # Create PyArrow table
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    
    # Write to parquet
    output_path = Path(args.output)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    
    pq.write_table(table, output_path, compression='GZIP')
    
    print(f"\nWrote {len(table)} rows to {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024:.2f} KB")
    
    # Show sample
    print("\nSample data (first 5 rows):")
    sample_df = table.to_pandas().head()
    print(sample_df.to_string(index=False))
    
    return 0

if __name__ == "__main__":
    exit(main())
#!/usr/bin/env python3
import argparse
import subprocess
import pyfiglet
import sys
import json

def bigtext(string):
    print(pyfiglet.figlet_format(string, font="slant", width=160))

def get_table_columns(db_path, table_name):
    """Query database to get all column names from the table"""
    result = subprocess.run(
        ['duckdb', db_path],
        input=f"DESCRIBE {table_name};",
        text=True,
        capture_output=True
    )
    
    if result.returncode != 0:
        print(f"ERROR: Could not describe table '{table_name}'")
        print(result.stderr)
        sys.exit(1)
    
    columns = []
    for line in result.stdout.strip().split('\n'):
        if '│' in line and 'column_name' not in line and '─' not in line:
            parts = [p.strip() for p in line.split('│') if p.strip()]
            if parts:
                columns.append(parts[0])
    
    return columns

# ==============================================================================
# CONFIGURATION - ALWAYS INCLUDED COLUMNS
# ==============================================================================

ALWAYS_INCLUDED = {
    'id': 's.id as id',
    'geometry': 's.geometry as geometry',
    'datetime_start': 's.datetime_start as datetime_start',
    'cyclone_id': 'p.point_id as cyclone_id',
    'cyclone_datetime': 'p.datetime_start as cyclone_datetime'
}

# ==============================================================================
# ARGUMENT PARSING
# ==============================================================================

parser = argparse.ArgumentParser(description="Find satellite imagery near points within specified time ranges.")

parser.add_argument("--db", required=True, help="Path to DuckDB database")
parser.add_argument("--table", required=True, help="Name of satellite table")
parser.add_argument("--points", help="Path to Parquet file with points (latitude, longitude, datetime_start, size)")
parser.add_argument("--output", help="Output Parquet file")

parser.add_argument("--before-start", type=float, help="Start of 'before' window (hours before point)")
parser.add_argument("--before-end", type=float, help="End of 'before' window (hours before point)")
parser.add_argument("--after-start", type=float, help="Start of 'after' window (hours after point)")
parser.add_argument("--after-end", type=float, help="End of 'after' window (hours after point)")

parser.add_argument("--product-type", type=str, default= "EW_GRD%", help="Filter by product type (e.g., 'EW_GRDM', 'IW_GRDH', 'EW_GRD%%' for wildcard)")
parser.add_argument("--output-columns", nargs='*', default=[],
                   help=f"Additional columns to include ({', '.join(ALWAYS_INCLUDED.keys())} always included)")
parser.add_argument("--threads", type=int, default=32, help="DuckDB threads (default: 32)")
parser.add_argument("--memory-limit", type=str, default="16GB", help="DuckDB memory limit (default: 16GB)")
parser.add_argument("--list-columns", action="store_true", help="List available columns and exit")
parser.add_argument("--verbose", action="store_true", help="Print SQL script")

args = parser.parse_args()

# ==============================================================================
# LIST COLUMNS MODE
# ==============================================================================

if args.list_columns:
    columns = get_table_columns(args.db, args.table)
    print("\nSatellite columns:")
    for col in columns:
        print(f"  - {col}")
    print(f"\nAlways included: {', '.join(ALWAYS_INCLUDED.keys())}")
    sys.exit(0)

# ==============================================================================
# VALIDATE ARGUMENTS
# ==============================================================================

required = [
    ('points', args.points),
    ('output', args.output),
    ('before-start', args.before_start),
    ('before-end', args.before_end),
    ('after-start', args.after_start),
    ('after-end', args.after_end)
]

for name, value in required:
    if value is None:
        parser.error(f"--{name} is required (unless using --list-columns)")

if args.before_start < args.before_end:
    print("ERROR: --before-start must be >= --before-end")
    sys.exit(1)

if args.after_start > args.after_end:
    print("ERROR: --after-start must be <= --after-end")
    sys.exit(1)

# ==============================================================================
# BUILD COLUMN MAPPINGS
# ==============================================================================

satellite_columns = get_table_columns(args.db, args.table)

# Validate required columns exist
for col in ['id', 'geometry', 'datetime_start']:
    if col not in satellite_columns:
        print(f"ERROR: Table '{args.table}' must have a '{col}' column")
        sys.exit(1)

# Check if product_type filter is requested
if args.product_type and 'product_type' not in satellite_columns:
    print(f"ERROR: Table '{args.table}' must have a 'product_type' column to use --product-type filter")
    sys.exit(1)

satellite_mapping = {col: f's.{col} as {col}' for col in satellite_columns}

# Build SELECT clause
select_columns = list(ALWAYS_INCLUDED.values())
invalid = []

for col in args.output_columns:
    if col in ALWAYS_INCLUDED:
        continue
    if col in satellite_mapping:
        select_columns.append(satellite_mapping[col])
    else:
        invalid.append(col)

if invalid:
    print(f"ERROR: Invalid columns: {', '.join(invalid)}")
    print(f"Use --list-columns to see available columns")
    sys.exit(1)

select_clause = ',\n    '.join(select_columns)

# Build product type filter
product_type_filter = ""
if args.product_type:
    product_type_filter = f"AND s.product_type LIKE '{args.product_type}'"

# ==============================================================================
# SQL SCRIPT
# ==============================================================================

sql_script = f"""
INSTALL spatial;
LOAD spatial;

SET threads TO {args.threads};
SET memory_limit = '{args.memory_limit}';

CREATE OR REPLACE TEMP TABLE input_points AS
SELECT 
    row_number() OVER () as point_id,
    latitude, longitude, datetime_start, size,
    ST_Point(longitude, latitude) as point_geom,
    datetime_start - INTERVAL '{args.before_start} hours' as before_window_start,
    datetime_start - INTERVAL '{args.before_end} hours' as before_window_end,
    datetime_start + INTERVAL '{args.after_start} hours' as after_window_start,
    datetime_start + INTERVAL '{args.after_end} hours' as after_window_end,
    size / 111.0 as buffer_degrees
FROM read_parquet('{args.points}');

CREATE OR REPLACE TEMP TABLE point_stats AS
SELECT 
    COUNT(*) as num_points,
    MIN(size) as min_size, MAX(size) as max_size, AVG(size) as avg_size,
    MIN(before_window_start) as global_min_time,
    MAX(after_window_end) as global_max_time
FROM input_points;

CREATE OR REPLACE TEMP TABLE satellite_filtered AS
SELECT s.*
FROM {args.table} s
CROSS JOIN point_stats ps
WHERE s.datetime_start >= ps.global_min_time
  AND s.datetime_start <= ps.global_max_time
  {product_type_filter};

CREATE OR REPLACE TABLE matches AS
SELECT {select_clause}
FROM input_points p
INNER JOIN satellite_filtered s
ON ST_DWithin(p.point_geom, s.geometry, p.buffer_degrees)
   AND ((s.datetime_start >= p.before_window_start AND s.datetime_start <= p.before_window_end)
     OR (s.datetime_start >= p.after_window_start AND s.datetime_start <= p.after_window_end))
ORDER BY cyclone_id;

CREATE OR REPLACE TEMP TABLE matches_with_point_id AS
SELECT p.point_id, s.id as satellite_id
FROM input_points p
INNER JOIN satellite_filtered s
ON ST_DWithin(p.point_geom, s.geometry, p.buffer_degrees)
   AND ((s.datetime_start >= p.before_window_start AND s.datetime_start <= p.before_window_end)
     OR (s.datetime_start >= p.after_window_start AND s.datetime_start <= p.after_window_end));

CREATE OR REPLACE TEMP TABLE match_stats AS
WITH match_counts AS (
    SELECT point_id, COUNT(*) as cnt
    FROM matches_with_point_id
    GROUP BY point_id
)
SELECT 
    COUNT(*) as points_with_matches,
    SUM(cnt) as total_matches,
    ROUND(SUM(cnt)::FLOAT / COUNT(*), 2) as avg_matches_per_point,
    MIN(cnt) as min_matches,
    MAX(cnt) as max_matches
FROM match_counts;

COPY matches TO '{args.output}' (FORMAT PARQUET, COMPRESSION GZIP);

COPY (
    SELECT 
        ps.num_points, ps.min_size, ps.max_size, ps.avg_size,
        (SELECT COUNT(*) FROM satellite_filtered) as filtered_satellite_count,
        COALESCE(ms.points_with_matches, 0) as points_with_matches,
        ps.num_points - COALESCE(ms.points_with_matches, 0) as points_without_matches,
        COALESCE(ms.total_matches, 0) as total_matches,
        COALESCE(ms.avg_matches_per_point, 0) as avg_matches_per_point,
        COALESCE(ms.min_matches, 0) as min_matches,
        COALESCE(ms.max_matches, 0) as max_matches
    FROM point_stats ps
    CROSS JOIN match_stats ms
) TO '/dev/stdout' (FORMAT JSON);
"""

# ==============================================================================
# EXECUTE
# ==============================================================================

bigtext("Executing")

if args.verbose:
    print("\n" + "="*80 + "\nSQL SCRIPT\n" + "="*80 + "\n")
    print(sql_script)

result = subprocess.run(['duckdb', args.db], input=sql_script, text=True, capture_output=True)

if result.returncode != 0:
    bigtext("FAILED")
    print("\n" + "="*80 + "\nERROR OUTPUT\n" + "="*80 + "\n")
    print(result.stderr)
    sys.exit(1)

# ==============================================================================
# OUTPUT
# ==============================================================================

lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
stats = json.loads(lines[-1] if lines else '{}')

bigtext("SUCCESS")

output_cols = list(ALWAYS_INCLUDED.keys()) + [c for c in args.output_columns if c not in ALWAYS_INCLUDED]

print(f"\n  Database:      {args.db}")
print(f"  Table:         {args.table}")
print(f"  Threads:       {args.threads} | Memory: {args.memory_limit}")
print(f"  Time Window:   -{args.before_start}h to -{args.before_end}h | +{args.after_start}h to +{args.after_end}h")
if args.product_type:
    print(f"  Product Type:  {args.product_type}")
print(f"  Output Cols:   {', '.join(output_cols)}")

print(f"\n  Points:        {stats['num_points']} (size: {stats['min_size']:.1f}-{stats['max_size']:.1f} km)")
print(f"  Filtered Sat:  {stats.get('filtered_satellite_count', 'N/A')}")
print(f"  Matches:       {stats['total_matches']} ({stats['avg_matches_per_point']:.1f} avg, {stats['min_matches']}-{stats['max_matches']} range)")
print(f"  Coverage:      {stats['points_with_matches']}/{stats['num_points']} points")

print(f"\n  ✓ Output: {args.output}\n")
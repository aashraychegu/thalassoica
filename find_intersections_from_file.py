#!/usr/bin/env python3
import argparse
import subprocess
import pyfiglet
import sys
from pathlib import Path
import json

def bigtext(string):
    return print(pyfiglet.figlet_format(string, font="slant", width=160))

def print_section(title):
    """Print a section header"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")

def get_table_columns(db_path, table_name):
    """Query the database to get all column names from the table"""
    sql = f"DESCRIBE {table_name};"
    
    result = subprocess.run(
        ['duckdb', db_path],
        input=sql,
        text=True,
        capture_output=True
    )
    
    if result.returncode != 0:
        print(f"ERROR: Could not describe table '{table_name}'")
        print(result.stderr)
        sys.exit(1)
    
    # Parse the output to extract column names
    # DuckDB DESCRIBE output has column_name in first column
    columns = []
    for line in result.stdout.strip().split('\n'):
        # Skip header and separator lines
        if '│' in line and 'column_name' not in line and '─' not in line:
            # Extract column name (first field between │)
            parts = [p.strip() for p in line.split('│') if p.strip()]
            if parts:
                columns.append(parts[0])
    
    return columns

# ==============================================================================
# ARGUMENT PARSING
# ==============================================================================

parser = argparse.ArgumentParser(
    description="Find satellite imagery near points within specified time ranges.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog='''
Examples:
  # Basic usage - only id column in output
  %(prog)s --db sentinel.duckdb --table sentinel1 \\
    --points points.parquet \\
    --before-start 720 --before-end 240 \\
    --after-start 120 --after-end 360 \\
    --output matches.parquet

  # Include cyclone and satellite columns
  %(prog)s --db sentinel.duckdb --table sentinel1 \\
    --points points.parquet \\
    --before-start 336 --before-end 168 \\
    --after-start 0 --after-end 0 \\
    --output matches.parquet \\
    --output-columns cyclone_lat cyclone_lon name datetime_start orbit_direction

  # Use --list-columns to see available satellite columns
  %(prog)s --db sentinel.duckdb --table sentinel1 --list-columns
    ''')

# Required arguments
parser.add_argument("--db", required=True, help="Path to the DuckDB database file containing satellite data.")
parser.add_argument("--table", required=True, help="Name of the satellite table in the database.")
parser.add_argument("--points", help="Path to Parquet file with points (must have: latitude, longitude, datetime_start, size).")
parser.add_argument("--output", help="Path for the output Parquet file with matches.")

# Time range arguments in HOURS - BEFORE the point
parser.add_argument("--before-start", type=float,
                   help="Start of 'before' window (hours before point datetime)")
parser.add_argument("--before-end", type=float,
                   help="End of 'before' window (hours before point datetime)")

# Time range arguments in HOURS - AFTER the point
parser.add_argument("--after-start", type=float,
                   help="Start of 'after' window (hours after point datetime)")
parser.add_argument("--after-end", type=float,
                   help="End of 'after' window (hours after point datetime)")

# Output column selection
parser.add_argument("--output-columns", nargs='*', 
                   default=[],
                   help="Additional columns to include in output (id always included). "
                        "Cyclone columns: cyclone_id, cyclone_lat, cyclone_lon, cyclone_datetime, cyclone_size. "
                        "Satellite columns: use any column name from the satellite table (use --list-columns to see available columns).")

# Utility arguments
parser.add_argument("--list-columns", action="store_true",
                   help="List all available columns from the satellite table and exit")

# Optional arguments
parser.add_argument("--verbose", action="store_true",
                   help="Print detailed output")
parser.add_argument("--quiet", action="store_true",
                   help="Suppress all non-essential output")

args = parser.parse_args()

# ==============================================================================
# LIST COLUMNS MODE
# ==============================================================================

if args.list_columns:
    print_section(f"COLUMNS IN TABLE: {args.table}")
    columns = get_table_columns(args.db, args.table)
    
    print("Available satellite columns:")
    for col in columns:
        print(f"  - {col}")
    
    print("\nAvailable cyclone columns:")
    print("  - cyclone_id")
    print("  - cyclone_lat")
    print("  - cyclone_lon")
    print("  - cyclone_datetime")
    print("  - cyclone_size")
    
    print(f"\nUsage example:")
    print(f"  --output-columns cyclone_lat cyclone_lon {columns[0] if columns else 'name'} {columns[1] if len(columns) > 1 else 'datetime_start'}")
    sys.exit(0)

# ==============================================================================
# VALIDATE REQUIRED ARGUMENTS
# ==============================================================================

if not args.points:
    parser.error("--points is required (unless using --list-columns)")
if not args.output:
    parser.error("--output is required (unless using --list-columns)")
if args.before_start is None:
    parser.error("--before-start is required (unless using --list-columns)")
if args.before_end is None:
    parser.error("--before-end is required (unless using --list-columns)")
if args.after_start is None:
    parser.error("--after-start is required (unless using --list-columns)")
if args.after_end is None:
    parser.error("--after-end is required (unless using --list-columns)")

# Validate time ranges
if args.before_start < args.before_end:
    print("ERROR: --before-start must be >= --before-end (e.g., 720 hours before to 240 hours before)")
    sys.exit(1)

if args.after_start > args.after_end:
    print("ERROR: --after-start must be <= --after-end (e.g., 120 hours after to 360 hours after)")
    sys.exit(1)

# ==============================================================================
# GET TABLE SCHEMA AND BUILD COLUMN MAPPINGS
# ==============================================================================

# Get available columns from the satellite table
satellite_columns = get_table_columns(args.db, args.table)

# Define cyclone columns (always available from input parquet)
cyclone_column_mapping = {
    'cyclone_id': 'p.point_id as cyclone_id',
    'cyclone_lat': 'p.latitude as cyclone_lat',
    'cyclone_lon': 'p.longitude as cyclone_lon',
    'cyclone_datetime': 'p.datetime_start as cyclone_datetime',
    'cyclone_size': 'p.size as cyclone_size',
}

# Build satellite column mapping dynamically
satellite_column_mapping = {col: f's.{col} as {col}' for col in satellite_columns}

# Combine both mappings
column_mapping = {**cyclone_column_mapping, **satellite_column_mapping}

# Build column list for output (always include id first)
if 'id' not in satellite_columns:
    print(f"ERROR: Table '{args.table}' does not have an 'id' column")
    sys.exit(1)

select_columns = ['s.id as id']
invalid_columns = []

for col in args.output_columns:
    if col == 'id':
        continue  # Already included
    if col in column_mapping:
        select_columns.append(column_mapping[col])
    else:
        invalid_columns.append(col)

# Report invalid columns
if invalid_columns:
    print(f"ERROR: Invalid column names: {', '.join(invalid_columns)}")
    print(f"\nValid cyclone columns: {', '.join(cyclone_column_mapping.keys())}")
    print(f"Valid satellite columns: {', '.join(satellite_columns)}")
    print(f"\nUse --list-columns to see all available columns")
    sys.exit(1)

select_clause = ',\n    '.join(select_columns)

# Determine sort order (prefer cyclone_id if included, otherwise id)
sort_column = 'cyclone_id' if 'cyclone_id' in args.output_columns else 's.id'

# ==============================================================================
# SQL SCRIPT GENERATION
# ==============================================================================

sql_script = f"""
INSTALL spatial;
LOAD spatial;

-- Load input points
CREATE OR REPLACE TEMP TABLE input_points AS
SELECT 
    row_number() OVER () as point_id,
    latitude,
    longitude,
    datetime_start,
    size,
    ST_Point(longitude, latitude) as point_geom
FROM read_parquet('{args.points}');

-- Get point statistics
CREATE OR REPLACE TEMP TABLE point_stats AS
SELECT 
    COUNT(*) as num_points,
    MIN(size) as min_size,
    MAX(size) as max_size,
    AVG(size) as avg_size
FROM input_points;

-- Find all satellite imagery within distance and time windows
CREATE OR REPLACE TABLE matches AS
SELECT 
    {select_clause}
FROM 
    input_points p
INNER JOIN 
    {args.table} s
ON 
    ST_DWithin(p.point_geom, s.geometry, p.size / 111.0)
    AND (
        (s.datetime_start >= p.datetime_start - INTERVAL '{args.before_start} hours'
         AND s.datetime_start <= p.datetime_start - INTERVAL '{args.before_end} hours')
        OR
        (s.datetime_start >= p.datetime_start + INTERVAL '{args.after_start} hours'
         AND s.datetime_start <= p.datetime_start + INTERVAL '{args.after_end} hours')
    )
ORDER BY 
    {sort_column};

-- Calculate match statistics (using original point_id for counting)
CREATE OR REPLACE TEMP TABLE matches_with_point_id AS
SELECT 
    p.point_id,
    s.id as satellite_id
FROM 
    input_points p
INNER JOIN 
    {args.table} s
ON 
    ST_DWithin(p.point_geom, s.geometry, p.size / 111.0)
    AND (
        (s.datetime_start >= p.datetime_start - INTERVAL '{args.before_start} hours'
         AND s.datetime_start <= p.datetime_start - INTERVAL '{args.before_end} hours')
        OR
        (s.datetime_start >= p.datetime_start + INTERVAL '{args.after_start} hours'
         AND s.datetime_start <= p.datetime_start + INTERVAL '{args.after_end} hours')
    );

CREATE OR REPLACE TEMP TABLE match_stats AS
SELECT 
    COUNT(DISTINCT point_id) as points_with_matches,
    COUNT(*) as total_matches,
    ROUND(COUNT(*) / COUNT(DISTINCT point_id)::FLOAT, 2) as avg_matches_per_point,
    MIN(cnt) as min_matches,
    MAX(cnt) as max_matches
FROM (
    SELECT point_id, COUNT(*) as cnt
    FROM matches_with_point_id
    GROUP BY point_id
);

-- Count points with no matches
CREATE OR REPLACE TEMP TABLE no_match_stats AS
SELECT COUNT(*) as points_without_matches
FROM input_points p
LEFT JOIN matches_with_point_id m ON p.point_id = m.point_id
WHERE m.point_id IS NULL;

-- Export results
COPY matches TO '{args.output}' (FORMAT PARQUET, COMPRESSION GZIP);

-- Output summary as JSON for easier parsing
COPY (
    SELECT 
        ps.num_points,
        ps.min_size,
        ps.max_size,
        ps.avg_size,
        COALESCE(ms.points_with_matches, 0) as points_with_matches,
        COALESCE(nms.points_without_matches, 0) as points_without_matches,
        COALESCE(ms.total_matches, 0) as total_matches,
        COALESCE(ms.avg_matches_per_point, 0) as avg_matches_per_point,
        COALESCE(ms.min_matches, 0) as min_matches,
        COALESCE(ms.max_matches, 0) as max_matches
    FROM point_stats ps
    CROSS JOIN match_stats ms
    CROSS JOIN no_match_stats nms
) TO '/dev/stdout' (FORMAT JSON);
"""

# ==============================================================================
# EXECUTE SQL
# ==============================================================================

if not args.quiet:
    bigtext("Executing")

if args.verbose:
    print_section("SQL SCRIPT")
    print(sql_script)

result = subprocess.run(
    ['duckdb', args.db],
    input=sql_script,
    text=True,
    capture_output=True
)

# ==============================================================================
# OUTPUT RESULTS
# ==============================================================================

if result.returncode != 0:
    bigtext("FAILED")
    print_section("ERROR OUTPUT")
    print(result.stderr)
    sys.exit(1)

# Parse JSON output
try:
    # The last line of stdout should be the JSON
    lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
    json_line = lines[-1] if lines else '{}'
    stats = json.loads(json_line)
    
    if not args.quiet:
        bigtext("SUCCESS")
        
        print_section("CONFIGURATION")
        print(f"  Database:      {args.db}")
        print(f"  Table:         {args.table}")
        print(f"  Input Points:  {args.points}")
        print(f"  Output:        {args.output}")
        print(f"  Before Window: {args.before_start}h to {args.before_end}h before point")
        print(f"  After Window:  {args.after_start}h to {args.after_end}h after point")
        
        print_section("OUTPUT COLUMNS")
        output_col_list = ['id'] + [c for c in args.output_columns if c != 'id']
        print(f"  Columns ({len(output_col_list)}): {', '.join(output_col_list)}")
        
        print_section("INPUT POINTS")
        print(f"  Total Points:  {stats['num_points']}")
        print(f"  Size Range:    {stats['min_size']:.2f} km to {stats['max_size']:.2f} km")
        print(f"  Average Size:  {stats['avg_size']:.2f} km")
        
        print_section("MATCH RESULTS")
        print(f"  Total Matches:           {stats['total_matches']}")
        print(f"  Points with Matches:     {stats['points_with_matches']}")
        print(f"  Points without Matches:  {stats['points_without_matches']}")
        print(f"  Avg Matches per Point:   {stats['avg_matches_per_point']:.2f}")
        print(f"  Min Matches:             {stats['min_matches']}")
        print(f"  Max Matches:             {stats['max_matches']}")
        
        print_section("OUTPUT")
        print(f"  ✓ Results written to: {args.output}")
        print()
    else:
        # Quiet mode - just output key stats
        print(f"Points: {stats['num_points']} | Matches: {stats['totalmatches']} | Output: {args.output}")
        
except (json.JSONDecodeError, KeyError, IndexError) as e:
    if not args.quiet:
        print_section("WARNING")
        print(f"  Could not parse statistics: {e}")
        print(f"  Output written to: {args.output}")
        
    if args.verbose:
        print_section("RAW OUTPUT")
        print(result.stdout)

sys.exit(0)
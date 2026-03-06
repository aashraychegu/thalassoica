#!/usr/bin/env python3
import argparse
import subprocess
import pyfiglet
import sys
import json
import time

def bigtext(string):
    """Prints a large ASCII art banner."""
    print(pyfiglet.figlet_format(string, font="slant", width=160))

def get_table_columns(db_path, table_name):
    """Queries the database to get all column names from the specified table."""
    try:
        result = subprocess.run(
            ['duckdb', db_path],
            input=f"DESCRIBE {table_name};",
            text=True,
            capture_output=True,
            check=True
        )
        columns = []
        for line in result.stdout.strip().split('\n'):
            if '│' in line and 'column_name' not in line and '─' not in line:
                parts = [p.strip() for p in line.split('│') if p.strip()]
                if parts:
                    columns.append(parts[0])
        return columns
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Could not describe table '{table_name}'. Make sure the table exists or the DB path is correct.")
        print(e.stderr)
        sys.exit(1)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Columns that are always included in the output.
# 's' is the alias for the satellite table, 'p' for the points table.
ALWAYS_INCLUDED = {
    'id': 's.id as id',
    'geometry': 's.geometry as geometry',
    'datetime_start': 's.datetime_start as datetime_start',
    'point_id': 'p.point_id as point_id',
    'point_datetime': 'p.datetime_start as point_datetime'
}

def main():
    # ==========================================================================
    # ARGUMENT PARSING
    # ==========================================================================
    parser = argparse.ArgumentParser(
        description="Find satellite imagery near points. This script reads a points Parquet file, and saves it as a persistent 'input_points' table in the database. It then finds and outputs matching satellite scenes to a Parquet file.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    io_group = parser.add_argument_group('Input/Output Arguments')
    io_group.add_argument("--db", required=True, help="Path to DuckDB database file. The 'input_points' table will be created here.")
    io_group.add_argument("--table", required=True, help="Name of the source satellite data table in the database.")
    io_group.add_argument("--points", help="Path to the input Parquet file with cyclone points.")
    io_group.add_argument("--output", help="Path for the output Parquet file containing matched imagery.")

    time_group = parser.add_argument_group('Time Window Arguments (in hours relative to point datetime)')
    time_group.add_argument("--before-start", type=float, help="Start of 'before' window (e.g., 24 for 24 hours before). Must be >= --before-end.")
    time_group.add_argument("--before-end", type=float, help="End of 'before' window (e.g., 0 for up to the exact time).")
    time_group.add_argument("--after-start", type=float, help="Start of 'after' window (e.g., 0 for right after). Must be <= --after-end.")
    time_group.add_argument("--after-end", type=float, help="End of 'after' window (e.g., 24 for up to 24 hours after).")

    filter_group = parser.add_argument_group('Filtering and Output Columns')
    filter_group.add_argument("--product-type", type=str, default="EW_GRD%", help="Filter by product type. Supports SQL LIKE wildcards (%%, _). Default: 'EW_GRD%%'.")
    filter_group.add_argument("--output-columns", nargs='*', default=[], help=f"Additional columns from the satellite table to include. \n(Note: {', '.join(ALWAYS_INCLUDED.keys())} are always included).")

    perf_group = parser.add_argument_group('Performance')
    perf_group.add_argument("--threads", type=int, default=32, help="Number of threads for DuckDB (default: 32).")
    perf_group.add_argument("--memory-limit", type=str, default="16GB", help="Memory limit for DuckDB (default: 16GB).")
    
    meta_group = parser.add_argument_group('Metadata and Debugging')
    meta_group.add_argument("--list-columns", action="store_true", help="List available columns in the satellite table and exit.")
    meta_group.add_argument("--verbose", action="store_true", help="Print the full SQL script before execution.")

    args = parser.parse_args()
    
    # ==========================================================================
    # LIST COLUMNS MODE OR VALIDATE ARGUMENTS
    # ==========================================================================
    if args.list_columns:
        columns = get_table_columns(args.db, args.table)
        print(f"\nAvailable columns in satellite table '{args.table}':")
        for col in columns: print(f"  - {col}")
        print(f"\nAlways included columns in output: {', '.join(ALWAYS_INCLUDED.keys())}")
        sys.exit(0)

    required_args = ['points', 'output', 'before_start', 'before_end', 'after_start', 'after_end']
    if any(getattr(args, arg) is None for arg in required_args):
        parser.error("When not using --list-columns, all input/output and time window arguments are required.")

    if args.before_start < args.before_end:
        parser.error("--before-start must be greater than or equal to --before-end.")
    if args.after_start > args.after_end:
        parser.error("--after-start must be less than or equal to --after-end.")
        
    # ==========================================================================
    # BUILD SQL QUERY
    # ==========================================================================

    satellite_columns = get_table_columns(args.db, args.table)
    
    for col in ['id', 'geometry', 'datetime_start', 'product_type']:
        if col not in satellite_columns:
            parser.error(f"Satellite table '{args.table}' is missing required column: '{col}'.")
            
    select_columns = list(ALWAYS_INCLUDED.values())
    for col in args.output_columns:
        if col in ALWAYS_INCLUDED: continue
        if col in satellite_columns:
            select_columns.append(f's.{col} as {col}')
        else:
            print(f"WARNING: Requested column '{col}' not found in satellite table and will be ignored.")

    select_clause = ',\n    '.join(select_columns)
    
    # --- Main SQL Script ---
    sql_script = f"""
    INSTALL spatial;
    LOAD spatial;

    SET threads TO {args.threads};
    SET memory_limit = '{args.memory_limit}';

    -- Step 1: Read input points, rename columns, and save as a persistent table.
    CREATE OR REPLACE TABLE input_points AS
    SELECT
        -- Carry over all original columns from the Parquet file
        *,
        -- Generate required columns with standard names
        row_number() OVER (ORDER BY datetime) as point_id,
        latc AS latitude,
        lonc AS longitude,
        CAST(datetime AS TIMESTAMPTZ) AS datetime_start,
        "Size_km" AS size,
        ST_Point(lonc, latc) as point_geom,
        datetime - INTERVAL '{args.before_start} hours' as before_window_start,
        datetime - INTERVAL '{args.before_end} hours' as before_window_end,
        datetime + INTERVAL '{args.after_start} hours' as after_window_start,
        datetime + INTERVAL '{args.after_end} hours' as after_window_end,
        "Size_km" / 111.0 as buffer_degrees -- Approx conversion from km to degrees
    FROM read_parquet('{args.points}');

    -- Step 2: Pre-filter satellite data to a global time window for efficiency
    CREATE OR REPLACE TEMP TABLE satellite_filtered AS
    SELECT s.*
    FROM {args.table} s
    JOIN (SELECT MIN(before_window_start) as min_time, MAX(after_window_end) as max_time FROM input_points) b
    ON s.datetime_start BETWEEN b.min_time AND b.max_time
    WHERE s.product_type LIKE '{args.product_type}';

    -- Step 3: Find matches and store them in a temporary table (won't be saved in the db file)
    CREATE OR REPLACE TEMP TABLE matches AS
    SELECT {select_clause}
    FROM input_points AS p
    JOIN satellite_filtered AS s
        ON ST_DWithin(p.point_geom, s.geometry, p.buffer_degrees)
        AND (
            (s.datetime_start BETWEEN p.before_window_start AND p.before_window_end) OR
            (s.datetime_start BETWEEN p.after_window_start AND p.after_window_end)
        )
    ORDER BY p.point_id, s.datetime_start;

    -- Step 4: Export the matches to the final output Parquet file
    COPY matches TO '{args.output}' (FORMAT PARQUET, COMPRESSION GZIP);

    -- Step 5: Calculate and output summary statistics as a single JSON object
    COPY (
        WITH point_summary AS (
            SELECT COUNT(*) as num_points, MIN(size) as min_size, MAX(size) as max_size, AVG(size) as avg_size
            FROM input_points
        ),
        match_summary AS (
            SELECT
                COUNT(*) as total_matches,
                COUNT(DISTINCT point_id) as points_with_matches,
                MIN(CASE WHEN point_id IS NOT NULL THEN cnt ELSE 0 END) as min_matches,
                MAX(CASE WHEN point_id IS NOT NULL THEN cnt ELSE 0 END) as max_matches,
                AVG(CASE WHEN point_id IS NOT NULL THEN cnt ELSE 0.0 END) as avg_matches_per_point
            FROM (SELECT point_id, COUNT(*) as cnt FROM matches GROUP BY point_id)
        )
        SELECT
            ps.num_points, ps.min_size, ps.max_size, ps.avg_size,
            (SELECT COUNT(*) FROM satellite_filtered) as filtered_satellite_count,
            COALESCE(ms.points_with_matches, 0) as points_with_matches,
            ps.num_points - COALESCE(ms.points_with_matches, 0) as points_without_matches,
            COALESCE(ms.total_matches, 0) as total_matches,
            ROUND(COALESCE(ms.avg_matches_per_point, 0.0), 2) as avg_matches_per_point,
            COALESCE(ms.min_matches, 0) as min_matches,
            COALESCE(ms.max_matches, 0) as max_matches
        FROM point_summary ps, match_summary ms
    ) TO '/dev/stdout' (FORMAT JSON);
    """

    # ==========================================================================
    # EXECUTE & DISPLAY SUMMARY
    # ==========================================================================
    bigtext("Executing")
    if args.verbose:
        print("\n" + "="*80 + "\nSQL SCRIPT\n" + "="*80 + "\n" + sql_script)

    starttime = time.time()
    try:
        result = subprocess.run(['duckdb', args.db], input=sql_script, text=True, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        bigtext("FAILED")
        print("\n" + "="*80 + "\nERROR OUTPUT\n" + "="*80 + "\n" + e.stderr)
        sys.exit(1)
    endtime = time.time() - starttime
    lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
    stats = json.loads(lines[-1] if lines else '{}')

    bigtext("SUCCESS")

    print(f"\n\tDatabase:        {args.db}")
    print(f"\tSatellite Table: {args.table}")
    print(f"\tPoints File:     {args.points}")
    print(f"\tTime Window:     [-{args.before_start}h to -{args.before_end}h] and [+{args.after_start}h to +{args.after_end}h]")
    print(f"\tPoints saved to 'input_points' table in the database in {endtime} ms")
    print(f"\tInput Points:    {stats.get('num_points', 0)} (size: {stats.get('min_size', 0):.1f}-{stats.get('max_size', 0):.1f} km)")
    print(f"\tFiltered Scenes: {stats.get('filtered_satellite_count', 'N/A')}")
    print(f"\tTotal Matches:   {stats.get('total_matches', 0)} ({stats.get('avg_matches_per_point', 0):.1f} avg, range {stats.get('min_matches', 0)}-{stats.get('max_matches', 0)})")
    print(f"\tPoint Coverage:  {stats.get('points_with_matches', 0)} of {stats.get('num_points', 0)} points have at least one match.")
    print(f"\tMatch data exported to: {args.output}\n")

if __name__ == "__main__":
    main()
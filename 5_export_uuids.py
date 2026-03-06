import subprocess
import sys
import argparse

# --- Argument Parsing ---
parser = argparse.ArgumentParser(
    description='exports uuids for download from an overlaps file'
)
parser.add_argument(
    '--overlaps', 
    type=str, 
    help='Path to the input Parquet file.', 
    required=True
)
parser.add_argument(
    '--uuids', 
    type=str, 
    help='Path for the output Parquet file.', 
    required=True
)
parser.add_argument(
    '--db-file', 
    type=str, 
    help='Path to the DuckDB database file containing the sentinel1 table.', 
    required=True
)

args = parser.parse_args()

# --- DuckDB SQL Command ---
# This multi-step query first processes the data into a temporary table 
# and then exports that table to a Parquet file.
duckdb_command = f"""
-- Install and load the spatial extension for geometry functions.
INSTALL spatial;
LOAD spatial;

-- Attach the external database in read-only mode for safety.
ATTACH '{args.db_file}' AS db (READ_ONLY);

-- Create a temporary table to hold the processed data. This makes the logic modular.
CREATE OR REPLACE TEMP TABLE processed_data AS
WITH 
    -- Step 1: Unpivot the 'a' and 'b' columns into a consistent stream of records.
    -- We use UNION ALL for performance and to keep all records.
    unpivoted_data AS (
        SELECT 
            id_a AS id,
            datetime_start_a AS start_datetime,
            geometry_a AS geometry
        FROM read_parquet('{args.overlaps}')
        WHERE id_a IS NOT NULL

        UNION ALL

        SELECT 
            id_b AS id,
            datetime_start_b AS start_datetime,
            geometry_b AS geometry
        FROM read_parquet('{args.overlaps}')
        WHERE id_b IS NOT NULL
    ),

    -- Step 2: Calculate the centroid geometry for each record.
    -- This CTE prevents re-calculating the centroid multiple times.
    with_centroids AS (
        SELECT
            *,
            ST_Centroid(geometry) AS centroid_geom
        FROM unpivoted_data
    )

-- Step 3: Select the final columns, join with the S3 path table, and format geometry outputs.
SELECT 
    c.id,
    c.start_datetime,
    s1.s3_path,
    ST_AsText(c.centroid_geom) AS centroid_wkt,
    ST_Y(c.centroid_geom) AS lat, -- Latitude (Y coordinate)
    ST_X(c.centroid_geom) AS lon  -- Longitude (X coordinate)
FROM with_centroids c
LEFT JOIN db.sentinel1 s1 ON c.id = s1.id;

-- Finally, export the contents of the temporary table to a Parquet file.
-- The data is ordered to ensure deterministic output.
COPY (
    SELECT * FROM processed_data ORDER BY start_datetime, id
) TO '{args.uuids}' (FORMAT 'parquet');

SUMMARIZE SELECT * FROM '{args.uuids}';
"""

# --- Execution ---
print(f"Loading Parquet file: {args.overlaps}")
print(f"Loading database: {args.db_file}")
print(f"Processing records and preparing for export...")

# Execute the DuckDB command.
# The 'check=True' argument will cause the script to exit with an error
# if the DuckDB process returns a non-zero exit code.
result = subprocess.run(
    ['duckdb', ':memory:'],
    input=duckdb_command,
    text=True,
    capture_output=True,
)

print(f"Processed data saved to Parquet file: {args.uuids}")

if result.stdout:
    print(f"\nDuckDB output:\n{result.stdout}")
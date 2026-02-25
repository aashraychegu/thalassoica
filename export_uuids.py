import subprocess
import sys
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(
    description='Extract unique IDs from id_a and id_b columns in a Parquet file'
)
parser.add_argument('--input-file', type=str, help='Path to input Parquet file', required=True)
parser.add_argument('--output-file', type=str, help='Path to output CSV file', required=True)

args = parser.parse_args()

# Validate input file exists
if not Path(args.input_file).exists():
    print(f"✗ Error: Input file not found: {args.input_file}", file=sys.stderr)
    sys.exit(1)

# Build the DuckDB SQL command to get distinct UUIDs
duckdb_command = f"""
COPY (
    SELECT DISTINCT id
    FROM (
        SELECT id_a AS id FROM read_parquet('{args.input_file}')
        UNION
        SELECT id_b AS id FROM read_parquet('{args.input_file}')
    ) combined_ids
    ORDER BY id
) TO '{args.output_file}' (HEADER, DELIMITER ',');
"""

print(f"Loading Parquet file: {args.input_file}")
print(f"Extracting unique IDs...")

# Execute DuckDB command
result = subprocess.run(
    ['duckdb', ':memory:'],
    input=duckdb_command,
    text=True,
    capture_output=True,
    check=True
)

print(f"✓ Query executed successfully")
print(f"✓ Unique IDs saved to: {args.output_file}")

if result.stdout:
    print(f"\nDuckDB output:\n{result.stdout}")
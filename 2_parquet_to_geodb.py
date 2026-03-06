import argparse
import subprocess
import pyfiglet
import sys

def bigtext(string):
    return print(pyfiglet.figlet_format(string,font="isometric1", width = 160))

# 1. Set up argument parser
parser = argparse.ArgumentParser(
    description="Convert a Parquet file to a geospatial DuckDB table via the CLI."
)
parser.add_argument("--input-parquet", required=True, help="Path to the input Parquet file.")
parser.add_argument("--table-name", required=True, help="Name for the new table in the database.")
parser.add_argument("--output-db", required=True, help="Path for the output DuckDB database file.")

# 2. Parse the command-line arguments
args = parser.parse_args()

# 3. Generate the SQL command as a string
sql_script = f"""
INSTALL spatial;
LOAD spatial;


CREATE OR REPLACE TABLE {args.table_name} AS
SELECT
    * REPLACE (ST_GeomFromText(geometry) AS geometry)
FROM
    read_parquet('{args.input_parquet}');

SUMMARIZE {args.table_name};
"""

result = subprocess.run(
        ['duckdb', args.output_db],  # The command to run
        input=sql_script,           # Pass the SQL script to the command's stdin
        text=True,                  # Handle stdin as text
        capture_output=True         # Suppress output unless an error occurs
)

# bigtext(f"Output: {result.returncode}")

# bigtext(f"stdout")
print(result.stdout)

# bigtext(f"stderr")
# print(result.stderr)

# print(result)
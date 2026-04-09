import argparse
import duckdb
import pyfiglet
import sys

def bigtext(string):
    return print(pyfiglet.figlet_format(string, font="isometric1", width=160))

# 1. Set up argument parser
parser = argparse.ArgumentParser(
    description="Convert a Parquet file to a geospatial DuckDB table via the CLI."
)
parser.add_argument("--input-parquet", required=True, help="Path to the input Parquet file.")
parser.add_argument("--table-name", required=True, help="Name for the new table in the database.")
parser.add_argument("--output-db", required=True, help="Path for the output DuckDB database file.")

# 2. Parse the command-line arguments
args = parser.parse_args()

# 3. Connect to DuckDB and execute commands
# Connect to the database (creates it if it doesn't exist)
con = duckdb.connect(args.output_db)

# Install and load spatial extension
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

# Create the table with geometry
con.execute(f"""
    CREATE OR REPLACE TABLE {args.table_name} AS
    SELECT
        * REPLACE (ST_GeomFromText(geometry) AS geometry)
    FROM
        read_parquet('{args.input_parquet}');
""")

# Get summary of the table
result = con.execute(f"SUMMARIZE {args.table_name};").fetchdf()

# Print the summary
print(result)

# Close the connection
con.close()
#!/usr/bin/env python3
import duckdb
import argparse
from pathlib import Path
import sys

def convert_parquets_to_duckdb(parquet_files: list[str], db_path: str):
    """
    Reads a list of Parquet files with WKB geometry, converts them to
    DuckDB tables with a proper GEOMETRY type, and saves them in a
    single DuckDB database file.

    Args:
        parquet_files: A list of paths to the input Parquet files.
        db_path: The path for the output DuckDB database file.
    """
    if not parquet_files:
        print("No input files provided. Exiting.", file=sys.stderr)
        return

    # Ensure the parent directory for the database exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Connecting to database: {db_path}")
    # The `with` statement ensures the connection is automatically closed.
    with duckdb.connect(database=db_path, read_only=False) as con:
        # Install and load the spatial extension
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        # Process each Parquet file
        for file_path in parquet_files:
            p = Path(file_path)
            if not p.exists():
                print(f"⚠️ Warning: File not found, skipping: {file_path}", file=sys.stderr)
                continue

            # Generate a clean table name from the filename (e.g., "my-file.parquet" -> "my_file")
            table_name = p.stem.replace('-', '_').replace('.', '_')
            
            print(f"  -> Processing '{p.name}' into table '{table_name}'...")

            # Use f-strings to build the SQL command.
            # CREATE OR REPLACE TABLE makes the script idempotent (re-runnable).
            # The REPLACE(...) clause converts the geometry column on the fly.
            sql = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                * REPLACE (ST_GeomFromWKB(geometry) AS geometry)
            FROM read_parquet('{file_path}');
            """
            
            con.execute(sql)

    print(f"\n✅ Success! Database updated at '{db_path}'")
    print(f"You can now connect to it with: duckdb {db_path}")


def main():
    """Main function to parse arguments and run the conversion."""
    parser = argparse.ArgumentParser(
        description="Convert Parquet files with WKB geometry into tables in a DuckDB database.",
        formatter_class=argparse.RawTextHelpFormatter  # For better help text formatting
    )
    
    parser.add_argument(
        "input_parquets",
        nargs="+",  # This means "one or more" arguments
        help="One or more paths to the input Parquet files."
    )
    
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Path for the output DuckDB database file (e.g., my_data.db)."
    )
    
    args = parser.parse_args()
    
    convert_parquets_to_duckdb(args.input_parquets, args.output)

if __name__ == "__main__":
    main()
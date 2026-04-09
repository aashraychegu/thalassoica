#!/usr/bin/env python3
import argparse
import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Export UUIDs for download from an overlaps table'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to the DuckDB database file containing the overlaps and source tables.',
    )
    parser.add_argument(
        '--overlaps-table',
        required=True,
        help='Name of the overlaps table to process.',
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Output file path.',
    )
    parser.add_argument(
        '--output-columns',
        nargs='+',
        default=['s3_path'],
        help='Columns to output from the source table (default: s3_path). Can specify multiple columns.',
    )
    parser.add_argument(
        '--csv',
        action='store_true',
        help='Export as CSV instead of Parquet (default).',
    )
    parser.add_argument('--threads', type=int, default=32)
    parser.add_argument('--memory-limit', default='16GB')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    # First, get the source_table value (assuming all rows have the same source_table)
    source_table_result = con.execute(
        f"SELECT DISTINCT source_table FROM {args.overlaps_table} LIMIT 1"
    ).fetchone()
    
    if not source_table_result:
        raise ValueError(f"No source_table found in {args.overlaps_table}")
    
    source_table = source_table_result[0]

    # Validate that the requested columns exist in the source table
    available_columns_result = con.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table}'"
    ).fetchall()
    available_columns = {col[0] for col in available_columns_result}
    
    for col in args.output_columns:
        if col not in available_columns:
            raise ValueError(
                f"Column '{col}' not found in source table '{source_table}'. "
                f"Available columns: {', '.join(sorted(available_columns))}"
            )

    # Build the SELECT clause for output columns
    output_column_select = ',\n        '.join([f's.{col}' for col in args.output_columns])

    sql = f"""
    WITH 
        -- Step 1: Unpivot the 'before' and 'after' columns into a consistent stream of records.
        unpivoted_data AS (
            SELECT 
                id_before AS id,
                datetime_start_before AS start_datetime
            FROM {args.overlaps_table}
            WHERE id_before IS NOT NULL

            UNION ALL

            SELECT 
                id_after AS id,
                datetime_start_after AS start_datetime
            FROM {args.overlaps_table}
            WHERE id_after IS NOT NULL
        )

    -- Step 2: Join with the source table to get requested columns.
    SELECT 
        u.id,
        u.start_datetime,
        {output_column_select}
    FROM unpivoted_data u
    LEFT JOIN {source_table} s ON u.id = s.id
    ORDER BY start_datetime, u.id
    """

    if args.verbose:
        print(f"Source table: {source_table}")
        print(f"Output columns: {', '.join(args.output_columns)}")
        print(sql)

    print(f"Loading overlaps table: {args.overlaps_table}")
    print(f"Output columns: {', '.join(args.output_columns)}")
    print(f"Processing records and exporting to {args.output}...")

    # Export based on format flag
    if args.csv:
        con.execute(f"COPY ({sql}) TO '{args.output}' (FORMAT CSV, HEADER)")
        format_type = "CSV"
    else:
        con.execute(f"COPY ({sql}) TO '{args.output}' (FORMAT PARQUET)")
        format_type = "Parquet"

    # Get row count for summary
    row_count = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
    
    print(f"\nExported {row_count} records to: {args.output} ({format_type})")


if __name__ == "__main__":
    main()
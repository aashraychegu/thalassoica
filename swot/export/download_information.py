#!/usr/bin/env python3
"""Export SWOT download information from a parquet index."""
import argparse
import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Export SWOT download information from a parquet index',
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to the DuckDB database file containing the SWOT parquet table.',
    )
    parser.add_argument(
        '--input-table',
        required=True,
        help='Name of the SWOT parquet table to export.',
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Output file path.',
    )
    parser.add_argument(
        '--output-columns',
        nargs='+',
        default=['path', "start_idx", "end_idx"],
        help='Columns to output from the SWOT table (default: path). Can specify multiple columns.',
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

    # Validate that the requested columns exist in the source table
    available_columns_result = con.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{args.input_table}'"
    ).fetchall()
    available_columns = {col[0] for col in available_columns_result}

    for col in args.output_columns:
        if col not in available_columns:
            raise ValueError(
                f"Column '{col}' not found in source table '{args.input_table}'. "
                f"Available columns: {', '.join(sorted(available_columns))}"
            )

    # Build the SELECT clause for output columns
    output_column_select = ',\n        '.join([f'{col}' for col in args.output_columns])

    sql = f"""
    SELECT
        {output_column_select}
    FROM {args.input_table}
    ORDER BY datetime_start, id
    """

    if args.verbose:
        print(f"Input table: {args.input_table}")
        print(f"Output columns: {', '.join(args.output_columns)}")
        print(sql)

    print(f"Loading table: {args.input_table}")
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

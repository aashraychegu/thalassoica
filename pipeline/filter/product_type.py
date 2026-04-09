#!/usr/bin/env python3
import duckdb
import pyfiglet


def bigtext(s: str) -> None:
    print(pyfiglet.figlet_format(s, font="slant", width=160))


def get_table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return set(con.execute(f"SELECT * FROM {table} LIMIT 0").df().columns)


def validate_args(args, parser, con: duckdb.DuckDBPyConnection) -> None:
    cols = get_table_columns(con, args.table)
    if "product_type" not in cols:
        parser.error(f"Satellite table '{args.table}' is missing required column: product_type")


def main(args, parser):
    import time

    con = duckdb.connect(args.db)
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    validate_args(args, parser, con)

    out_table = args.out_table or f"{args.table}__product_filtered"

    sql = f"""
    CREATE OR REPLACE TABLE {out_table} AS
    SELECT *
    FROM {args.table}
    WHERE product_type LIKE ?;
    """

    bigtext("Executing")
    t0 = time.time()
    con.execute(sql, [args.product_type])
    elapsed = time.time() - t0

    n = con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]

    bigtext("SUCCESS")
    print(f"\n\tDatabase:        {args.db}")
    print(f"\tSource Table:    {args.table}")
    print(f"\tProduct Type:    {args.product_type}")
    print(f"\tOutput Table:    {out_table}")
    print(f"\tOutput Rows:     {n}")
    print(f"\tRuntime:         {elapsed:.2f} s\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Filter a satellite table by product_type and save results to a new table.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    io = parser.add_argument_group("Input/Output Arguments")
    io.add_argument("--db", required=True, help="Path to DuckDB database file.")
    io.add_argument("--table", required=True, help="Source satellite table name.")
    io.add_argument(
        "--out-table",
        default=None,
        help="Output table name (default: <table>_product_filtered).",
    )

    flt = parser.add_argument_group("Filtering")
    flt.add_argument(
        "--product-type",
        default="EW_GRD%",
        help="SQL LIKE pattern (default: EW_GRD%).",
    )

    perf = parser.add_argument_group("Performance")
    perf.add_argument("--threads", type=int, default=32)
    perf.add_argument("--memory-limit", default="16GB")

    args = parser.parse_args()
    main(args, parser)
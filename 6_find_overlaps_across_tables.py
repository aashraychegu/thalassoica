#!/usr/bin/env python3
import argparse
import duckdb


def main():
    p = argparse.ArgumentParser(
        description=(
            "Compute multi-satellite overlaps (MSOs) by finding, for each SSO row in a primary table, "
            "which SSOs in other tables with the same point_id intersect it. "
            "Assumes each input SSO table already contains a unique row_id column."
        )
    )
    p.add_argument("--db", required=True, help="DuckDB database file.")
    p.add_argument(
        "--tables",
        required=True,
        nargs="+",
        help="List of SSO table names to process.",
    )
    p.add_argument(
        "--primary-table",
        default=None,
        help="Primary SSO table (default: first table in --tables).",
    )
    p.add_argument(
        "--output-table",
        default=None,
        help="Output table name (default: {primary_table}_msos).",
    )
    p.add_argument("--threads", type=int, default=32)
    p.add_argument("--memory-limit", default="16GB")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    primary_table = args.primary_table or args.tables[0]
    out_table = args.output_table or f"{primary_table}_msos"

    if primary_table not in args.tables:
        args.tables = [primary_table] + args.tables

    con = duckdb.connect(args.db)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    # Build correlated subqueries: one LIST(row_id) per other table.
    # This avoids row explosion from multiple LEFT JOINs and keeps 1 output row per primary SSO.
    select_cols = [
        "p.row_id AS primary_row_id",
        "p.point_id",
        # Keep whichever geometry you want as the basis for intersection; here we assume geometry_overlap exists.
        "p.geometry_overlap AS primary_geometry_overlap",
    ]

    for t in args.tables:
        if t == primary_table:
            continue
        # Column name uses the table name; quoted to be safe.
        select_cols.append(
            f"""(
                SELECT LIST(s.row_id ORDER BY s.row_id)
                FROM {t} AS s
                WHERE s.point_id = p.point_id
                  AND ST_Intersects(p.geometry_overlap, s.geometry_overlap)
            ) AS "{t}"
            """
        )

    sql = f"""
    CREATE OR REPLACE TABLE {out_table} AS
    SELECT
      {",\n      ".join(select_cols)}
    FROM {primary_table} AS p
    ORDER BY p.row_id
    ;
    """

    if args.verbose:
        print(sql)

    con.execute(sql)

    n = con.execute(f"SUMMARIZE SELECT * FROM {out_table}").fetchdf()
    mid = (len(n.columns) + 1) // 2
    print(f"Created table: {out_table}")
    print(n.iloc[:, :mid])
    print(n.iloc[:, mid:])


if __name__ == "__main__":
    main()
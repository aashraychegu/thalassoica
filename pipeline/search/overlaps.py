#!/usr/bin/env python3
import argparse

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compute before/after intersecting geometry pairs per point_id "
            "from {table}_matches."
        )
    )
    parser.add_argument(
        "--db",
        required=True,
        help="DuckDB database file used by the matching script.",
    )
    parser.add_argument("--matches-table", required=True, help="Matches table name.")
    parser.add_argument(
        "--output-table",
        default=None,
        help="Output table name (default: {matches_table}_overlaps).",
    )
    parser.add_argument("--threads", type=int, default=32)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    out_table = args.output_table or f"{args.matches_table}_overlaps"

    con = duckdb.connect(args.db)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    sql = f"""
    CREATE OR REPLACE TABLE {out_table} AS
    WITH deduplicated AS (
        SELECT DISTINCT ON (source_table, geometry)
            point_id,
            geometry,
            id,
            datetime_start,
            point_datetime,
            source_table
        FROM {args.matches_table}
    )
    SELECT
        ROW_NUMBER() OVER () AS row_id,
        "before".point_id,
        "before".geometry AS geometry_before,
        "after".geometry AS geometry_after,
        ST_Intersection("before".geometry, "after".geometry) AS geometry_overlap,
        (
            ST_Area(ST_Intersection("before".geometry, "after".geometry))
            / ST_Area("before".geometry)
            * 100
        ) AS pct,
        "before".source_table AS source_table,
        "before".id AS id_before,
        "after".id AS id_after,
        "before".datetime_start AS datetime_start_before,
        "after".datetime_start AS datetime_start_after,
        "before".point_datetime AS point_datetime
    FROM deduplicated AS "before"
    JOIN deduplicated AS "after"
        ON "before".point_id = "after".point_id
        AND ST_Intersects("before".geometry, "after".geometry)
        AND "before".datetime_start < "before".point_datetime
        AND "after".datetime_start > "after".point_datetime
        AND "before".datetime_start < "after".datetime_start
        AND "before".id < "after".id
    WHERE NOT ST_IsEmpty(ST_Intersection("before".geometry, "after".geometry));
    """

    if args.verbose:
        print(sql)

    con.execute(sql)

    summary = con.execute(f"SUMMARIZE SELECT * FROM {out_table}").fetchdf()
    mid = (len(summary.columns) + 1) // 2
    left = summary.iloc[:, :mid]
    right = summary.iloc[:, mid:]

    print(f"Created table: {out_table}")
    print(left)
    print(right)


if __name__ == "__main__":
    main()
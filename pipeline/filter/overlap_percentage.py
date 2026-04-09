#!/usr/bin/env python3
import argparse
import duckdb


parser = argparse.ArgumentParser(
    description=(
        "Filter rows by overlap percentage between geometries from an input table "
        "and write results to an output table."
    )
)
parser.add_argument("--db", required=True, help="DuckDB database file.")
parser.add_argument("--in-table", required=True, help="Input table name.")
parser.add_argument("--out-table", required=False, default=None, help="Output table name.")
parser.add_argument(
    "--min-overlap",
    type=float,
    default=0.0,
    help="Minimum overlap percentage to keep (default: 0).",
)
parser.add_argument(
    "--max-overlap",
    type=float,
    default=100.0,
    help="Maximum overlap percentage to keep (default: 100).",
)
parser.add_argument("--threads", type=int, default=32)
parser.add_argument("--memory-limit", default="16GB")
parser.add_argument("--verbose", action="store_true")
args = parser.parse_args()

con = duckdb.connect(args.db)
con.execute("INSTALL spatial; LOAD spatial;")
con.execute(f"SET threads TO {args.threads}")
con.execute(f"SET memory_limit = '{args.memory_limit}'")

if args.out_table is None:
    args.out_table = args.in_table + "__overlap_filtered"

sql = f"""
CREATE OR REPLACE TABLE {args.out_table} AS
SELECT *
FROM {args.in_table}
WHERE pct >= {args.min_overlap}
    AND pct <= {args.max_overlap};
"""

if args.verbose:
    print(sql)

con.execute(sql)

summary = con.execute(f"SUMMARIZE SELECT * FROM {args.out_table}").fetchdf()
mid = (len(summary.columns) + 1) // 2
left = summary.iloc[:, :mid]
right = summary.iloc[:, mid:]

print(f"Created table: {args.out_table}")
print(left)
print(right)

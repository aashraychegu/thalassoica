import argparse
import duckdb

parser = argparse.ArgumentParser()
parser.add_argument("--db", required=True)
parser.add_argument("--overlaps-table", required=True)
parser.add_argument("--imagery-table", required=True)
parser.add_argument("--cyclone-table", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

con = duckdb.connect(args.db)

# Get column names from each table
overlaps_cols = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{args.overlaps_table}'").fetchall()
imagery_cols = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{args.imagery_table}'").fetchall()
cyclone_cols = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{args.cyclone_table}'").fetchall()

# Build column lists with prefixes
overlaps_cols_str = ", ".join([f"o.{col[0]} AS overlaps_{col[0]}" for col in overlaps_cols])
before_cols = ", ".join([f"ib.{col[0]} AS before_{col[0]}" for col in imagery_cols])
after_cols = ", ".join([f"ia.{col[0]} AS after_{col[0]}" for col in imagery_cols])
cyclone_cols_str = ", ".join([f"c.{col[0]} AS cyclone_{col[0]}" for col in cyclone_cols])

query = f"""
CREATE TEMP TABLE final_result AS
SELECT 
    {overlaps_cols_str},
    {before_cols},
    {after_cols},
    {cyclone_cols_str}
FROM {args.overlaps_table} o
LEFT JOIN {args.imagery_table} ib ON o.id_before = ib.id
LEFT JOIN {args.imagery_table} ia ON o.id_after = ia.id
LEFT JOIN {args.cyclone_table} c ON o.point_id = c.point_id
"""

con.execute(query)

result = con.execute("SUMMARIZE final_result").fetchdf()

mid = len(result.columns) // 2
print("First half of columns:")
print(result.iloc[:, :mid])
print("\nSecond half of columns:")
print(result.iloc[:, mid:])

con.execute(f"COPY final_result TO '{args.output}' (FORMAT PARQUET)")

con.close()
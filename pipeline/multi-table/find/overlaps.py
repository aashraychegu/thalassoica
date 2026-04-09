import duckdb
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--db",required = True)
parser.add_argument("--tables",required = True,nargs = "+")
parser.add_argument("--out-table", default = None)
parser.add_argument("--point-col",default = "point_id")
parser.add_argument("--row-col",default = "row_id")
args = parser.parse_args()

point_col = args.point_col
row_col = args.row_col
if args.out_table is None:
    out_table = "combined_" + "___".join(args.tables)
out_table = args.out_table
tables = args.tables

con = duckdb.connect(args.db)
base = tables[0]
base_alias = f"{base} t0"

select_cols = [f"t0.{point_col} as {point_col}", f"t0.{row_col} as {base}_row_id"]
join_clauses = []

for i, t in enumerate(tables[1:], start=1):
    alias = f"t{i}"
    select_cols.append(f'{alias}.{row_col} as {t}_row_id')
    join_clauses.append(f'JOIN "{t}" {alias} ON {alias}.{point_col} = t0.{point_col}')

sql = f"""
create or replace table "{out_table}" as
select
{",\n  ".join(select_cols)}
from "{base}" t0
{"\n".join(join_clauses)}
;
""".strip()

con.execute(sql)
con.execute(f"summarize {out_table}")
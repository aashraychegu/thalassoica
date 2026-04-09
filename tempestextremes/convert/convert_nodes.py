import duckdb
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--in_file", default="./intermediates/stitchnodes/tracks_mslp.csv")
parser.add_argument("--out_file", default="./intermediates/te_out/tracks_mslp.parquet")
args = parser.parse_args()

# Ensure output directory exists
Path(args.out_file).parent.mkdir(parents=True, exist_ok=True)

# DuckDB query to transform and write parquet
con = duckdb.connect()

query = f"""
COPY (
    SELECT 
        make_timestamp(year::BIGINT, month::BIGINT, day::BIGINT, hour::BIGINT, 0, 0.0) AS datetime,
        track_id,
        lat AS latc,
        lon AS lonc,
        maxdist * (
            111.13292 
            - 0.55982 * cos(2 * lat * pi() / 180)
            + 0.001175 * cos(4 * lat * pi() / 180)
            - 0.000023 * cos(6 * lat * pi() / 180)
        ) AS size_km,
        msl
    FROM read_csv_auto('{args.in_file}')
    ORDER BY track_id, datetime
) TO '{args.out_file}' (FORMAT PARQUET);
"""

con.execute(query)
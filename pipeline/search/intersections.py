#!/usr/bin/env python3
import duckdb
import pyfiglet


def bigtext(s: str) -> None:
    print(pyfiglet.figlet_format(s, font="slant", width=160))


ALWAYS_INCLUDED = {
    "source_table": "CAST(? AS VARCHAR) AS source_table",
    "id": "s.id",
    "geometry": "s.geometry",
    "datetime_start": "s.datetime_start",
    "point_id": "p.point_id",
    "point_datetime": "p.datetime_start",
}


def get_table_columns(con, table: str) -> set[str]:
    """Get column names from a table without pandas overhead."""
    return {row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()}


def validate_args(args, parser, con):
    # Time-window sanity checks
    if args.before_start < args.before_end:
        parser.error("--before-start must be >= --before-end.")
    if args.after_start > args.after_end:
        parser.error("--after-start must be <= --after-end.")

    # Satellite table required columns
    sat_cols = get_table_columns(con, args.table)
    missing = {"id", "geometry", "datetime_start"} - sat_cols
    if missing:
        parser.error(
            f"Satellite table '{args.table}' is missing required columns: {', '.join(sorted(missing))}"
        )

    return sat_cols


def main(args, parser):
    import time

    con = duckdb.connect(args.db)

    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    sat_cols = validate_args(args, parser, con)

    select_cols = [ALWAYS_INCLUDED["source_table"], "s.id", "s.geometry", "s.datetime_start", "p.point_id", "p.datetime_start"]
    for c in args.output_columns:
        if c in ("source_table", "id", "geometry", "datetime_start", "point_id", "point_datetime"):
            continue
        if c in sat_cols:
            select_cols.append(f"s.{c}")
        else:
            print(f"WARNING: requested column '{c}' not found in satellite table; ignoring.")
    select_clause = ",\n    ".join(select_cols)

    matches_table = f"{args.table}_matches"

    sql = f"""
    CREATE OR REPLACE TABLE input_points AS
    SELECT
        *,
        row_number() OVER (ORDER BY datetime) as point_id,
        latc AS latitude,
        lonc AS longitude,
        CAST(datetime AS TIMESTAMPTZ) AS datetime_start,
        "Size_km" AS size,
        ST_Point(lonc, latc) as point_geom,
        datetime - INTERVAL '{args.before_start} hours' as before_window_start,
        datetime - INTERVAL '{args.before_end} hours' as before_window_end,
        datetime + INTERVAL '{args.after_start} hours' as after_window_start,
        datetime + INTERVAL '{args.after_end} hours' as after_window_end,
        "Size_km" / 111.0 as buffer_degrees
    FROM read_parquet('{args.points}');
    
    ALTER TABLE input_points DROP COLUMN latc, lonc;
    
    CREATE OR REPLACE TEMP TABLE satellite_filtered AS
    SELECT s.*
    FROM {args.table} s
    JOIN (
        SELECT MIN(before_window_start) as min_time,
               MAX(after_window_end)   as max_time
        FROM input_points
    ) b
      ON s.datetime_start BETWEEN b.min_time AND b.max_time;

    CREATE OR REPLACE TABLE {matches_table} AS
    SELECT {select_clause}
    FROM input_points p
    JOIN satellite_filtered s
      ON ST_DWithin(p.point_geom, s.geometry, p.buffer_degrees)
     AND (
            s.datetime_start BETWEEN p.before_window_start AND p.before_window_end
         OR s.datetime_start BETWEEN p.after_window_start  AND p.after_window_end
         )
    ORDER BY p.point_id, s.datetime_start;
    """

    bigtext("Executing")
    t0 = time.time()

    # Bind the table name into the SELECT as source_table
    con.execute(sql, [args.table])

    stats = con.execute(
        f"""
        WITH
        ps AS (
            SELECT COUNT(*) num_points,
                   MIN(size) min_size,
                   MAX(size) max_size,
                   AVG(size) avg_size
            FROM input_points
        ),
        mc AS (
            SELECT point_id, COUNT(*) cnt
            FROM {matches_table}
            GROUP BY point_id
        ),
        ms AS (
            SELECT
                (SELECT COUNT(*) FROM {matches_table}) total_matches,
                (SELECT COUNT(*) FROM mc)              points_with_matches,
                COALESCE(MIN(cnt), 0)                  min_matches,
                COALESCE(MAX(cnt), 0)                  max_matches,
                COALESCE(AVG(cnt), 0.0)                avg_matches_per_point
            FROM mc
        )
        SELECT
            ps.*,
            (SELECT COUNT(*) FROM satellite_filtered) filtered_satellite_count,
            ms.points_with_matches,
            ps.num_points - ms.points_with_matches points_without_matches,
            ms.total_matches,
            ROUND(ms.avg_matches_per_point, 2) avg_matches_per_point,
            ms.min_matches,
            ms.max_matches
        FROM ps, ms;
        """
    ).fetchone()

    names = [d[0] for d in con.description]
    stats = dict(zip(names, stats))

    elapsed = time.time() - t0
    bigtext("SUCCESS")

    print(f"\n\tDatabase:        {args.db}")
    print(f"\tSatellite Table: {args.table}")
    print(f"\tPoints File:     {args.points}")
    print(
        f"\tTime Window:     [-{args.before_start}h to -{args.before_end}h] and "
        f"[+{args.after_start}h to +{args.after_end}h]"
    )
    print(f"\tRuntime:         {elapsed:.2f} s")
    print(f"\tMatches Table:   {matches_table}")
    print(
        f"\tInput Points:    {stats['num_points']} "
        f"(size: {stats['min_size']:.1f}-{stats['max_size']:.1f} km)"
    )
    print(f"\tFiltered Scenes: {stats['filtered_satellite_count']}")
    print(
        f"\tTotal Matches:   {stats['total_matches']} "
        f"({stats['avg_matches_per_point']:.1f} avg, range {stats['min_matches']}-{stats['max_matches']})"
    )
    print(f"\tPoint Coverage:  {stats['points_with_matches']} of {stats['num_points']}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Find satellite imagery near points. Reads a points Parquet file into a persistent "
            "'input_points' table, finds matching satellite scenes, and saves matches as a table."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    io = parser.add_argument_group("Input/Output Arguments")
    io.add_argument("--db", required=True, help="Path to DuckDB database file.")
    io.add_argument("--table", required=True, help="Source satellite table name.")
    io.add_argument("--points", required=True, help="Input Parquet file with cyclone points.")

    tw = parser.add_argument_group("Time Window Arguments (hours relative to point datetime)")
    tw.add_argument("--before-start", default=96, type=float)
    tw.add_argument("--before-end", default=12, type=float)
    tw.add_argument("--after-start", default=12, type=float)
    tw.add_argument("--after-end", default=96, type=float)

    flt = parser.add_argument_group("Output Columns")
    flt.add_argument("--output-columns", nargs="*", default=[], help="Extra satellite columns to include.")

    perf = parser.add_argument_group("Performance")
    perf.add_argument("--threads", type=int, default=32)
    perf.add_argument("--memory-limit", default="16GB")

    args = parser.parse_args()
    main(args, parser)
#!/usr/bin/env python3
import argparse
import subprocess

def check_intersections(matches_file, output_file):
    """
    Check for intersecting geometries for each point_id and compute intersection percentages.
    """
    
    print("Analyzing intersections for all point_ids...")
    
    # Direct COPY to parquet
    write_query = f"""
    install spatial;
    load spatial;
    COPY (
        WITH deduplicated AS (
          SELECT DISTINCT ON (geometry) point_id, geometry, id, datetime_start, point_datetime
          FROM '{matches_file}'
        )
        SELECT 
          a.point_id,
          a.geometry as geometry_a,
          b.geometry as geometry_b,
          (ST_Area(ST_Intersection(a.geometry, b.geometry)) / ST_Area(a.geometry) * 100) as pct_a,
          (ST_Area(ST_Intersection(a.geometry, b.geometry)) / ST_Area(b.geometry) * 100) as pct_b,
          a.id as id_a,
          b.id as id_b,
          a.datetime_start as datetime_start_a,
          b.datetime_start as datetime_start_b,
          a.point_datetime as point_datetime
        FROM deduplicated a
        JOIN deduplicated b 
          ON a.point_id = b.point_id
          AND ST_Intersects(a.geometry, b.geometry)
          AND a.id < b.id
          AND a.datetime_start < a.point_datetime
          AND b.datetime_start > a.point_datetime
          AND a.datetime_start < b.datetime_start
    ) TO '{output_file}' (FORMAT PARQUET);
    summarize select * from '{output_file}';
    """
    
    result = subprocess.run(
        ['duckdb', ':memory:'],
        input=write_query,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error writing to parquet: {result.stderr}")
        return
    
    print(result.stdout)

def main():
    parser = argparse.ArgumentParser(
        description='Check for intersecting geometries in matches file for each point_id'
    )
    parser.add_argument(
        '--matches-file',
        help='Path to the matches parquet file',
        required=True,
    )
    parser.add_argument(
        '--output',
        help='Output parquet file to save intersection results',
        required=True
    )
    
    args = parser.parse_args()
    
    check_intersections(args.matches_file, args.output)

if __name__ == '__main__':
    main()
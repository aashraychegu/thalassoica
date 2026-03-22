#!/usr/bin/env python3
import argparse
import datetime
from pathlib import Path
import duckdb
import polars as pl
import cdsapi
import xarray as xr
import numpy as np
from dotenv import load_dotenv 
import os


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Filter overlap pairs based on sea ice concentration data'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to the DuckDB database file.',
    )
    parser.add_argument(
        '--input-table',
        required=True,
        help='Name of the input overlaps table.',
    )
    parser.add_argument(
        '--output-table',
        default=None,
        help='Output table name (default: {input_table}_filtered).',
    )
    parser.add_argument(
        '--key-dotenv',
        required=True,
        help='CDS API key for downloading sea ice data.',
    )
    parser.add_argument(
        '--sic-dir',
        default="intermediates/seaice_concentrations",
        help='Directory to store/read sea ice concentration NetCDF files.',
    )
    parser.add_argument(
        '--sic-threshold',
        type=float,
        default=0.15,
        help='Sea ice concentration threshold (default: 0.15 = 15%%)',
    )
    parser.add_argument(
        '--clean-export',
        action='store_true',
        help='Just export the table to parquet without processing.',
    )
    parser.add_argument(
        '--export-path',
        default=None,
        help='Path for parquet export (required if --clean-export is used).',
    )
    parser.add_argument('--threads', type=int, default=32)
    parser.add_argument('--memory-limit', default='16GB')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    out_table = args.output_table or f"{args.input_table}_filtered"
    sic_dir = Path(args.sic_dir)

    # Connect to database
    con = duckdb.connect(args.db)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    # Handle clean export
    if args.clean_export:
        print(f"Exporting {args.input_table} to {args.export_path}")
        con.execute(f"""
            COPY (SELECT * FROM {args.input_table}) 
            TO '{args.export_path}' (FORMAT 'parquet')
        """)
        print(f"Export complete: {args.export_path}")
        return

    # Load data from database table
    print(f"Loading data from table: {args.input_table}")
    overlaps = con.execute(f"""
        SELECT 
            *,
            ST_AsText(geometry_overlap) as overlap_wkt,
            ST_X(ST_Centroid(geometry_overlap)) as overlap_lon,
            ST_Y(ST_Centroid(geometry_overlap)) as overlap_lat
        FROM {args.input_table}
    """).pl()

    if args.verbose:
        print(f"Loaded {len(overlaps)} rows")
        print(f"Columns: {overlaps.columns}")

    # Find range of years in the data
    year_range = overlaps.select(
        pl.col("point_datetime").dt.year().min().alias("min_year"),
        pl.col("point_datetime").dt.year().max().alias("max_year")
    ).row(0)
    
    min_year, max_year = year_range
    years = list(range(min_year, max_year + 1))
    
    print(f"Data spans years: {min_year} to {max_year}")

    load_dotenv(args.key_dotenv)

    client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=os.getenv("API_KEY"))
    sea_ice_datasets = {}
    
    for year in years:
        sic_path = sic_dir / f"sea_ice_concentration_{year}.nc"
        
        if sic_path.exists():
            print(f"Sea ice data for {year} already exists at {sic_path}, skipping download")
        else:
            print(f"Downloading sea ice data for {year} to {sic_path}")
            
            dataset = "reanalysis-era5-single-levels"
            request = {
                "product_type": ["reanalysis"],
                "variable": ["sea_ice_cover"],
                "year": [str(year)],
                "month": [
                    "01", "02", "03",
                    "04", "05", "06",
                    "07", "08", "09",
                    "10", "11", "12"
                ],
                "day": [
                    "01", "02", "03",
                    "04", "05", "06",
                    "07", "08", "09",
                    "10", "11", "12",
                    "13", "14", "15",
                    "16", "17", "18",
                    "19", "20", "21",
                    "22", "23", "24",
                    "25", "26", "27",
                    "28", "29", "30",
                    "31"
                ],
                "time": ["00:00", "12:00"],
                "data_format": "netcdf",
                "download_format": "unarchived",
                "area": [-60, -180, -80, 180]
            }
            
            sic_dir.mkdir(parents=True, exist_ok=True)
            client.retrieve(dataset, request).download(str(sic_path))
        
        sea_ice_datasets[year] = xr.open_dataset(sic_path)

    def get_sea_ice_concentration(sea_ice_data, dt, lat, lon):
        """
        Look up sea ice concentration from xarray dataset.
        Returns the concentration value (0-1) at the given time and location.
        """
        # Convert datetime to UTC and make it timezone-naive to match ERA5 data
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        
        # Convert to numpy datetime64[ns] to match xarray's expected format
        dt_np = np.datetime64(dt, 'ns')
        
        # Select the data point using nearest neighbor interpolation
        sic_value = sea_ice_data['siconc'].sel(
            valid_time=dt_np,
            latitude=lat,
            longitude=lon,
            method='nearest'
        ).values
        
        # Handle potential NaN values
        if np.isnan(sic_value):
            return 0.0
        
        return float(sic_value)

    def criterion_function(indf: pl.DataFrame, year: int) -> pl.DataFrame:
        """Filter dataframe rows based on sea ice concentration threshold in overlap region."""
        if args.verbose:
            print(f"Processing {len(indf)} rows for year {year}")
        
        sea_ice_data = sea_ice_datasets[year]
        keep_rows = []
        
        for row_dict in indf.to_dicts():
            point_dt = datetime.datetime.fromisoformat(str(row_dict['point_datetime']))
            
            # Convert to UTC and remove timezone info to match ERA5 data format
            if point_dt.tzinfo is not None:
                point_dt = point_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            
            # Use the centroid of the overlap region
            lat = row_dict['overlap_lat']
            lon = row_dict['overlap_lon']
            
            sic = get_sea_ice_concentration(sea_ice_data, point_dt, lat, lon)
            
            # Keep row if sea ice concentration is below threshold
            # (i.e., filter out areas with too much sea ice)
            keep = sic < args.sic_threshold
            
            if args.verbose:
                status = "KEEP" if keep else "FILTER"
                point_id = row_dict.get('point_id', 'N/A')
                print(f"{status}: point_id={point_id}, {point_dt}, lat={lat:.2f}, lon={lon:.2f}, SIC={sic:.3f}")
            
            if keep:
                keep_rows.append(row_dict)
        
        filtered_count = len(indf) - len(keep_rows)
        print(f"  Year {year}: Kept {len(keep_rows)}/{len(indf)} rows (filtered {filtered_count} due to sea ice)")
        
        return pl.DataFrame(keep_rows)

    # Group by year and process
    overlaps_by_year = {
        year[0]: group_df 
        for year, group_df in overlaps.group_by(pl.col("point_datetime").dt.year())
    }

    filtered = []
    for year, df in overlaps_by_year.items():
        print(f"Processing year: {year}")
        filtered.append(criterion_function(df, year))

    filtered_df = pl.concat(filtered)
    
    # Drop the helper columns before saving
    filtered_df = filtered_df.drop(['overlap_wkt', 'overlap_lon', 'overlap_lat'])

    # Save results back to database
    print(f"\nSaving results to table: {out_table}")
    con.execute(f"CREATE OR REPLACE TABLE {out_table} AS SELECT * FROM filtered_df")

    # Print summary
    summary = con.execute(f"SUMMARIZE SELECT * FROM {out_table}").fetchdf()
    mid = (len(summary.columns) + 1) // 2
    left = summary.iloc[:, :mid]
    right = summary.iloc[:, mid:]

    print(f"\nCreated table: {out_table}")
    print(f"Total rows after filtering: {len(filtered_df)}/{len(overlaps)}")
    print(left)
    print(right)


if __name__ == "__main__":
    main()
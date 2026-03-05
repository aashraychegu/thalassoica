import polars as pl
import argparse
import datetime
import cdsapi
import netCDF4 as nc 
import numpy as np 

dataset = "reanalysis-era5-single-levels"
request = {
    "product_type": ["reanalysis"],
    "variable": ["sea_ice_cover"],
    "year": ["2024"],
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

client = cdsapi.Client()
client.retrieve(dataset, request).download("sea_ice_concentration_2024.nc")

sea_ice_data = nc.Dataset("sea_ice_2024.nc", "r")

parser = argparse.ArgumentParser()
parser.add_argument("--input-uuids", required=True)
parser.add_argument("--output-uuids", required=True)

args = parser.parse_args()

def rounded_lat_lon(lat, lon, multiple=0.25):


def criterion_function(indf: pl.DataFrame) -> pl.DataFrame:
    print(indf.columns)
    for dp in indf:
        
    return indf

uuids: pl.DataFrame = pl.read_parquet(args.input_uuids)

images_by_year = {
    year[0]: group_df for year, group_df in uuids.group_by(pl.col("start_datetime").dt.year())
}

filtered = []
for year, df in images_by_year.items():
    print(f"Starting {year}")
    filtered.append(criterion_function(df))

filtered_dfs = pl.concat(filtered)
filtered_dfs.write_parquet(args.output_uuids)
import polars as pl
import argparse
import datetime
import cdsapi
# import netCDF4 as nc 
import numpy as np 
import xarray as xr 

SIC_path = "intermediates/sea_ice_concentration_2024.nc"

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


client = cdsapi.Client(url = "https://cds.climate.copernicus.eu/api", key = "02ef4c7d-7301-4340-82cc-1ae0b2ac824d")
client.retrieve(dataset, request).download(SIC_path)

sea_ice_data = xr.open_dataset(SIC_path)

parser = argparse.ArgumentParser()
parser.add_argument("--input-uuids", required=True)
parser.add_argument("--output-uuids", required=True)

args = parser.parse_args()

def rounded_lat_lon(lat_lon, multiple=0.25):
    lat_lon = list(lat_lon)
    difs = [val % multiple for val in lat_lon]
    for i in range(2):
        if difs[i] < multiple / 2:
            lat_lon[i] -= difs[i]
        else:
            lat_lon[i] += multiple - difs[i]
    return tuple(lat_lon)

def rounded_date_time(dt):
    hour = 0 if dt.hour < 12 else 12
    return dt.replace(hour=hour, minute=0, second=0, microsecond=0)

def criterion_function(indf: pl.DataFrame) -> pl.DataFrame:
    print(indf.columns)
    for dp in indf.iter_rows():
        start = datetime.datetime.fromisoformat(str(dp[1]))
        lat, lon = dp[4], dp[5]
        lat, lon = rounded_lat_lon((lat, lon))
        print(start)
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
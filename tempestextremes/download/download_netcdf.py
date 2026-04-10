import cdsapi
from pathlib import Path
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser(prog="downloads netcdf files for tempestextremes")
parser.add_argument(
    "--input-dir",
    default="./intermediates/era5_mslp",
    help="Input directory for ERA5 MSLP NetCDF files"
)
parser.add_argument(
    "--start-year",
    default=2014,
    type=int,
    help="Start year for data download (default: 2014)"
)
parser.add_argument(
    "--end-year",
    default=2024,
    type=int,
    help="End year for data download (default: 2024)"
)
parser.add_argument(
    "--workers",
    type=int,
    default=8,
    help="Number of parallel download workers (default: 8)"
)

args = parser.parse_args()

input_dir = Path(args.input_dir)

assert input_dir.exists(), f"{input_dir} doesn't exist"

def build_request(yr,path):
    dataset = "reanalysis-era5-single-levels"
    request = {
        "product_type": ["reanalysis"],
        "variable": ["mean_sea_level_pressure"],
        "year": [yr],
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
        "time": [
            "00:00", "06:00", "12:00",
            "18:00"
        ],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": [-60, -180, -90, 180]
    }
    return {"dataset":dataset,"request":request,"path":path}

client = cdsapi.Client()

for yr in tqdm(range(args.start_year, args.end_year + 1)):
    output_path = input_dir / f"{yr}_mslp.nc"
    print(f"Downloading {yr} to {output_path}")
    request_info = build_request(yr, output_path)
    client.retrieve(request_info["dataset"], request_info["request"]).download(request_info["path"])

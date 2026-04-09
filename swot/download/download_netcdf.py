
import argparse
from pathlib import Path
import datetime as dt

import earthaccess


parser = argparse.ArgumentParser(
    description="Download SWOT L2 LR SSH Basic granules from PO.DAAC using earthaccess (netrc auth)."
)
parser.add_argument(
    "--short-name",
    default="SWOT_L2_LR_SSH_Basic_2.0",
    help="SWOT product short name"
)
parser.add_argument(
    "--output-dir",
    type=Path,
    default=Path("intermediates/shapes/swot"),
    help="Output directory for downloaded files"
)
parser.add_argument(
    "--start-date",
    default="2014-01-01",
    help="Start date for data search (YYYY-MM-DD)"
)
parser.add_argument(
    "--end-date",
    default=dt.datetime.now(dt.UTC).strftime("%Y-%m-%d"),
    help="End date for data search (YYYY-MM-DD)"
)
parser.add_argument(
    "--bbox",
    type=float,
    nargs=4,
    metavar=("W", "S", "E", "N"),
    default=(-180.0, -80.0, 180.0, -60.0),
    help="Bounding box: west south east north"
)
parser.add_argument(
    "--workers",
    type=int,
    default=32,
    help="Number of parallel download workers"
)
args = parser.parse_args()

args.output_dir.mkdir(parents=True, exist_ok=True)

earthaccess.login(strategy="netrc")

granules = earthaccess.search_data(
    short_name=args.short_name,
    temporal=(args.start_date, args.end_date),
    bounding_box=tuple(args.bbox),
)

print(f"Granules found: {len(granules)}")
downloaded = earthaccess.download(granules, str(args.output_dir), show_progress=True, threads=args.workers)

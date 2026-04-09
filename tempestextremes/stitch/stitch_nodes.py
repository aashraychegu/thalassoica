import subprocess
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--output-dir",
    default="./intermediates/stitchnodes",
    help="Output directory for stitched track files"
)
parser.add_argument(
    "--input-dir",
    default="./intermediates/detectnodes",
    help="Input directory containing detect node files"
)
parser.add_argument(
    "--data-dir",
    default="./intermediates/tempestextreme_files/",
    help="Directory containing TEMPEST extreme files"
)
parser.add_argument(
    "--input-file",
    default="tracks_mslp.csv",
    help="Input track file name"
)
parser.add_argument(
    "--range",
    default=8.0,
    type=float,
    help="Maximum distance for stitching (default: 8.0)"
)
parser.add_argument(
    "--mintime",
    default=8,
    type=int,
    help="Minimum time gap for stitching (default: 8)"
)
parser.add_argument(
    "--maxgap",
    default=2,
    type=int,
    help="Maximum time gap for stitching (default: 2)"
)
parser.add_argument(
    "--workers",
    type=int,
    default=8,
    help="Number of parallel workers (default: 8)"
)
args = parser.parse_args()

node_dir = Path(args.input_dir)
paths = list(node_dir.glob("*.txt"))
strpaths = [str(path.resolve()) for path in paths]

data_dir = Path(args.data_dir)
tempfile = data_dir / "stitchnodes-in.txt"
tempfile.unlink(missing_ok=True)
tempfile.touch()
tempfile.write_text("\n".join(strpaths))

output_dir = Path(args.output_dir)
output_dir.mkdir(exist_ok=True)
track_file = output_dir / args.input_file
track_file.unlink(missing_ok=True)
track_file.touch()

def add_cmd_subpart(command: list[str], flag: str, arg: str = None):
    """Add a flag and optional argument to the command list."""
    command.append(flag)
    if arg is not None:
        command.append(str(arg))

command = ["StitchNodes"]
add_cmd_subpart(command, "--in_list",str(tempfile))
add_cmd_subpart(command, "--out", str(track_file))
add_cmd_subpart(command, "--out_file_format", "csv")
add_cmd_subpart(command, "--in_fmt", "lon,lat,msl,maxdist")
add_cmd_subpart(command, "--range", args.range)
add_cmd_subpart(command, "--mintime", args.mintime)
add_cmd_subpart(command, "--maxgap", args.maxgap)

subprocess.run(command, check=True)
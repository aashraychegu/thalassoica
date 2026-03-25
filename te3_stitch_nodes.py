import subprocess
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--out_dir", default="./intermediates/stitchnodes")
parser.add_argument("--node_dir", default="./intermediates/detectnodes")
parser.add_argument("--TE_temps", default = "./intermediates/tempestextreme_files/")
parser.add_argument("--track_file", default="tracks_mslp.csv")
parser.add_argument("--range", default="8.0")
parser.add_argument("--mintime", default="8")
parser.add_argument("--maxgap", default="2")
args = parser.parse_args()

node_dir = Path(args.node_dir)
paths = list(node_dir.glob("*.txt"))
strpaths = [str(path.resolve()) for path in paths] 

tempdir = Path(args.TE_temps)
tempfile = tempdir / "stitchnodes-in.txt"
tempfile.unlink(missing_ok=True)
tempfile.touch()
tempfile.write_text("\n".join(strpaths))

out_dir = Path(args.out_dir)
out_dir.mkdir(exist_ok = True)
track_file = out_dir / args.track_file
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
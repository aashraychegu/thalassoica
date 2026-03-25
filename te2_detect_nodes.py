import subprocess
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--in_data_dir", default="./intermediates/era5_mslp")
parser.add_argument("--TE_temps", default = "./intermediates/tempestextreme_files/")
parser.add_argument("--out_data_dir", default="./intermediates/detectnodes/")
parser.add_argument("--mpi",action="store_true")
parser.add_argument("--nproc_limit",default = 16)
parser.add_argument("--verbosity",default = "0")
args = parser.parse_args()

in_data_dir = Path(args.in_data_dir)
out_data_dir = Path(args.out_data_dir)
tetf_dir = Path(args.TE_temps)
logdir = tetf_dir / "logs"
logdir.mkdir(exist_ok = True)

detectnodes_in = tetf_dir / "detectnodes-in.txt"
detectnodes_in.unlink(missing_ok=True)
detectnodes_in.touch()

detectnodes_out = tetf_dir / "detectnodes-out.txt"
detectnodes_out.unlink(missing_ok=True)
detectnodes_out.touch()

in_data_file_paths = list(in_data_dir.glob(f"*.nc"))

in_data_files = [str(path.resolve()) for path in in_data_file_paths]
in_data_files.sort()

filenames = [path.stem for path in in_data_file_paths]

out_data_files = [str(out_data_dir / f"{name}.txt") for name in filenames]


detectnodes_in.write_text("\n".join(in_data_files))
detectnodes_out.write_text("\n".join(out_data_files))

def add_cmd_subpart(command: list[str], flag: str, arg: str = None):
    """Add a flag and optional argument to the command list."""
    command.append(flag)
    if arg is not None:
        command.append(arg)

nprocs = min(len(filenames),args.nproc_limit)

command = []
base_cmd: str = ""
if args.mpi:
    command = ["mpirun", "-np", f"{nprocs}", "--oversubscribe", "DetectNodes"]
else:
    command = ["DetectNodes"]

add_cmd_subpart(command, "--in_data_list", str(detectnodes_in))
add_cmd_subpart(command, "--out_file_list", str(detectnodes_out))
add_cmd_subpart(command, "--searchbymin", "msl")  # Changed from mslp_snapshot to msl
add_cmd_subpart(command, "--closedcontourcmd", "msl,200.0,4.0,0")  # Changed variable name
add_cmd_subpart(command, "--mergedist", "6.0")
add_cmd_subpart(command, "--outputcmd", "msl,min,0;msl,maxdist,4")  # Changed variable name
add_cmd_subpart(command, "--latname", "latitude")  # Changed from lat to latitude
add_cmd_subpart(command, "--lonname", "longitude")  # Changed from lon to longitude
add_cmd_subpart(command, "--timefilter", "6hr")
add_cmd_subpart(command, "--logdir", str(logdir))
add_cmd_subpart(command, "--verbosity", args.verbosity)
add_cmd_subpart(command, "--out_header")
subprocess.run(command,check = True)
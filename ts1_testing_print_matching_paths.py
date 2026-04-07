import pyfiglet
import pathlib
from pathlib import Path
import polars as pl
import argparse
import duckdb

parser = argparse.ArgumentParser(description = "This is a testing utility for getting paths of overlapping imagery")
parser.add_argument("--dir",required=True)
parser.add_argument("--db",required=True)
parser.add_argument("--table",required=True)
parser.add_argument("--num",type=int, default=5)
args = parser.parse_args()

def bigtext(string):
    """Prints a large ASCII art banner."""
    print(pyfiglet.figlet_format(string, font="slant", width=160))

bigtext("This script is just for testing")
tiff_dir = Path(args.dir)
valid_paths = tiff_dir.glob("**/*.tiff")

existing_ids = set()
lookup = dict()
for path in valid_paths:
    existing_ids.add(path.parts[-2])
    if lookup.get(path.parts[-2]) == None:
        lookup[path.parts[-2]] = [path]
    else:
        lookup[path.parts[-2]].append(path)

con = duckdb.connect(args.db)
overlaps = con.execute(f"SELECT * FROM {args.table}").pl()


# Filter to only rows where both IDs exist
good_matches = overlaps.filter(
    pl.col("id_before").is_in(existing_ids) & pl.col("id_before").is_in(existing_ids)
)

def print_paths(path_lists: list[Path]):
    for i in path_lists:
        print(f"\t \t {str(i)}")

# Print args.num rows in the format
bigtext(f"{args.num} matches:")
for i, row in enumerate(good_matches.iter_rows(named=True)):
    if i >= args.num:
        break
    print(f"Match {i+1}:")
    print(f"\t{row['id_before']}:")
    print_paths(lookup[row["id_before"]])
    print(f"\t{row['id_before']}:")
    print_paths(lookup[row["id_before"]])
    print()
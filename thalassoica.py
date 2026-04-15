#!/usr/bin/env python3
"""run_thalassoica - Interactive thalassoica pipeline orchestrator (refactored)

Changes:
- Removed Runner class (use plain functions + a Context object).
- Steps are defined once and reused for menus + batch execution.
- Added DUMP steps for Sentinel and SWOT (blank placeholders for you to fill).
"""

import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Tuple


# Colors
class Colors:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    NC = "\033[0m"


# -------------------------
# Context / paths
# -------------------------
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class Ctx:
    script_dir: Path = Path(__file__).resolve().parent
    db_file: Path = script_dir / "intermediates/db/database.duckdb"

# -------------------------
# Command runner
# -------------------------
def run_cmd(cmd: str, description: str, step_name: str) -> bool:
    print()
    print(f"{Colors.GREEN}Running: {description}{Colors.NC}")
    print("-" * 60)
    try:
        subprocess.run(cmd, shell=True, check=True)
        print(f"{Colors.GREEN}SUCCESS: {step_name}{Colors.NC}\n{'=' * 60}")
        return True
    except subprocess.CalledProcessError:
        print(f"{Colors.RED}FAILED: {step_name}{Colors.NC}")
        return False


# -------------------------
# Step implementations
# -------------------------
# 1 - TEMPEST EXTREMES
def download_era5_mslp(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/tempestextremes/download/download_netcdf.py" '
        f'--input-dir "{ctx.script_dir}/intermediates/era5_mslp" '
        f"--start-year 2014 --end-year 2024 --workers 8"
    )
    return run_cmd(cmd, "Downloading ERA5 MSLP data (2014-2024)...", "download_era5_mslp")


def detect_nodes(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/tempestextremes/detect/detect_nodes.py" --mpi '
        f'--in_data_dir "{ctx.script_dir}/intermediates/era5_mslp" '
        f'--TE_temps "{ctx.script_dir}/intermediates/tempestextreme_files" '
        f'--out_data_dir "{ctx.script_dir}/intermediates/detectnodes"'
    )
    return run_cmd(cmd, "Detecting cyclone nodes...", "detect_nodes")


def stitch_nodes(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/tempestextremes/stitch/stitch_nodes.py" '
        f'--input-dir "{ctx.script_dir}/intermediates/detectnodes" '
        f'--output-dir "{ctx.script_dir}/intermediates/stitchnodes" '
        f'--data-dir "{ctx.script_dir}/intermediates/tempestextreme_files"'
    )
    return run_cmd(cmd, "Stitching nodes into tracks...", "stitch_nodes")


def convert_tracks(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/tempestextremes/convert/convert_nodes.py" '
        f'--in_file "{ctx.script_dir}/intermediates/stitchnodes/tracks_mslp.csv" '
        f'--out_file "{ctx.script_dir}/intermediates/te_out/tracks_mslp.parquet"'
    )
    return run_cmd(cmd, "Converting tracks to parquet...", "convert_tracks")


def refine_centers(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/tempestextremes/subgrid/subgrid_precision.py" '
        f'--input-file "{ctx.script_dir}/intermediates/te_out/tracks_mslp.parquet" '
        f'--input-dir "{ctx.script_dir}/intermediates/era5_mslp/*.nc" '
        f'--output "{ctx.script_dir}/intermediates/te_out/tracks_mslp_refined.parquet" '
        f"--workers 16"
    )
    return run_cmd(cmd, "Refining cyclone centers...", "refine_centers")


# 2 - SENTINEL-1
def fetch_sentinel(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/sentinel1/download/download_metadata.py" '
        f'--start-date "2014-01-01" --end-date "2025-05-03" '
        f"--workers 16 --days-per-chunk 2 --items-per-request 1000"
    )
    return run_cmd(cmd, "Fetching Sentinel-1 metadata...", "fetch_sentinel")


# 3 - SWOT
def download_swot(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/swot/download/download_netcdf.py" '
        f'--start-date "2014-01-01" --end-date "2025-05-03" '
        f"--bbox -180 -80 180 -60 --workers 32"
    )
    return run_cmd(cmd, "Downloading SWOT data...", "download_swot")


def extract_swot(ctx: Ctx) -> bool:
    cmd = (
        f'uv run "{ctx.script_dir}/swot/extract/extract_netcdf_to_parquet.py" '
        f'--input-dir "{ctx.script_dir}/intermediates/swot" '
        f'--output "{ctx.script_dir}/intermediates/shapes/swot/swot.parquet" '
        f"--step 20"
    )
    return run_cmd(cmd, "Converting SWOT to parquet index...", "extract_swot")


# 4 - PIPELINE
def load_data(ctx: Ctx) -> bool:
    ok = True

    print("  Loading SWOT data...")
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/utils/load_parquet.py" '
        f'--input-parquet "{ctx.script_dir}/intermediates/shapes/swot/swot.parquet" '
        f'--table-name swot --output-db "{ctx.db_file}"',
        "Loading SWOT into DuckDB",
        "load_swot",
    )

    print("  Loading Sentinel-1 data...")
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/utils/load_parquet.py" '
        f'--input-parquet "{ctx.script_dir}/intermediates/shapes/sentinel1/sentinel1_*.parquet" '
        f'--table-name sentinel1 --output-db "{ctx.db_file}"',
        "Loading Sentinel-1 into DuckDB",
        "load_sentinel",
    )

    return bool(ok)


def filter_product_type(ctx: Ctx) -> bool:
    return run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/filter/product_type.py" '
        f'--db "{ctx.db_file}" --table sentinel1 --product-type "EW_GRD%"',
        "Filtering by product type",
        "filter_product_type",
    )


def find_intersections(ctx: Ctx) -> bool:
    ok = True
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/search/intersections.py" '
        f'--db "{ctx.db_file}" --table sentinel1__product_filtered '
        f'--points "{ctx.script_dir}/intermediates/te_out/tracks_mslp_refined.parquet"',
        "Finding cyclone-satellite intersections - Sentinel",
        "find_intersections_sentinel",
    )
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/search/intersections.py" '
        f'--db "{ctx.db_file}" --table swot '
        f'--points "{ctx.script_dir}/intermediates/te_out/tracks_mslp_refined.parquet"',
        "Finding cyclone-satellite intersections - SWOT",
        "find_intersections_swot",
    )
    return bool(ok)


def find_overlaps(ctx: Ctx) -> bool:
    ok = True
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/search/overlaps.py" '
        f'--db "{ctx.db_file}" --matches-table sentinel1__product_filtered_matches '
        f'--output-table sentinel1__product_filtered_matches_overlaps',
        "Computing geometry overlaps - Sentinel",
        "find_overlaps_sentinel",
    )
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/search/overlaps.py" '
        f'--db "{ctx.db_file}" --matches-table swot_matches '
        f'--output-table swot_matches_overlaps',
        "Computing geometry overlaps - SWOT",
        "find_overlaps_swot",
    )
    return bool(ok)


def filter_overlap(ctx: Ctx) -> bool:
    ok = True
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/filter/overlap_percentage.py" '
        f'--db "{ctx.db_file}" --in-table sentinel1__product_filtered_matches_overlaps '
        f"--min-overlap 15 --max-overlap 100",
        "Filtering by overlap percentage - Sentinel",
        "filter_overlap_sentinel",
    )
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/filter/overlap_percentage.py" '
        f'--db "{ctx.db_file}" --in-table swot_matches_overlaps '
        f"--min-overlap 15 --max-overlap 100",
        "Filtering by overlap percentage - SWOT",
        "filter_overlap_swot",
    )
    return bool(ok)


def filter_era5(ctx: Ctx) -> bool:
    ok = True
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/filter/era5_criterion.py" '
        f'--db "{ctx.db_file}" '
        f'--input-table sentinel1__product_filtered_matches_overlaps__overlap_filtered '
        f'--key-dotenv "{ctx.script_dir}/cdsapikey" '
        f"--era5-variable sea_ice_cover --threshold 0.15",
        "Filtering by ERA5 criterion - Sentinel",
        "filter_era5_sentinel",
    )
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/filter/era5_criterion.py" '
        f'--db "{ctx.db_file}" '
        f'--input-table swot_matches_overlaps__overlap_filtered '
        f'--key-dotenv "{ctx.script_dir}/cdsapikey" '
        f"--era5-variable sea_ice_cover --threshold 0.15",
        "Filtering by ERA5 criterion - SWOT",
        "filter_era5_swot",
    )
    return bool(ok)


def export_uuids(ctx: Ctx) -> bool:
    ok = True
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/sentinel1/export/download_information.py" '
        f'--db "{ctx.db_file}" '
        f'--overlaps-table sentinel1__product_filtered_matches_overlaps__overlap_filtered__era5_filtered '
        f'--output "{ctx.script_dir}/intermediates/uuids/sentinel_download_uuids.parquet"',
        "Exporting UUIDs - Sentinel",
        "export_uuids_sentinel",
    )
    ok &= run_cmd(
        f'uv run "{ctx.script_dir}/swot/export/download_information.py" '
        f'--db "{ctx.db_file}" '
        f'--overlaps-table swot_matches_overlaps__overlap_filtered__era5_filtered '
        f'--output "{ctx.script_dir}/intermediates/uuids/swot_download_uuids.parquet"',
        "Exporting UUIDs - SWOT",
        "export_uuids_swot",
    )
    return bool(ok)


def dump_sentinel(ctx: Ctx) -> bool:
    return run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/export/dump_satellite.py" '
        f' --db "{ctx.db_file}" '
        f' --overlaps-table sentinel1__product_filtered_matches_overlaps__overlap_filtered__era5_filtered '
        f' --imagery-table sentinel1 '
        f' --cyclone-table input_points '
        f' --output "{ctx.script_dir}/intermediates/dump/sentinel1/dump_sentinel1.py" ', "Exporting All Sentinel Data", "dump_sentinel"
    )


def dump_swot(ctx: Ctx) -> bool:
    return run_cmd(
        f'uv run "{ctx.script_dir}/pipeline/export/dump_satellite.py" '
        f' --db "{ctx.db_file}" '
        f' --overlaps-table swot_matches_overlaps__overlap_filtered__era5_filtered '
        f' --imagery-table swot '
        f' --cyclone-table input_points '
        f' --output "{ctx.script_dir}/intermediates/dump/swot/dump_swot.py" ', "Exporting All SWOT Data", "dump_SWOT"
    )

def download_sentinel(ctx: Ctx) -> bool:
    print("Your username and password must be set in the copernicus_login.env file.")
    return run_cmd(
        f'uv run "{ctx.script_dir}/sentinel1/download/download_images.py" '
        f' --uuids "{ctx.script_dir}/intermediates/uuids/sentinel_download_uuids.parquet" '
        f' --output "{ctx.script_dir}/intermediates/tiffs/" ', "Downloading All Sentinel1 Data", "download_sentinel1"
    )

def swot_to_images(ctx: Ctx) -> bool:
    return run_cmd(f"uv run {ctx.script_dir}/swot/convert/netcdf_to_tiff.py", "Reformat SWOT Data", "reformat_swot", )

def setup_intermediates(ctx: Ctx) -> bool:
    return run_cmd("sh setup_intermediates.sh","Setting up Intermediates","setup_intermediates",)

def rescale_sentinel(cts: Ctx) -> bool:
    return run_cmd(f"uv run {ctx.script_dir}/sentinel1/convert/rescale_sentinel1.py", "Rescale Sentinel1 Data","rescale_sentinel1", )

# Utility commands
def skip_step(_: Ctx) -> bool:
    print("Skipping completed step.")
    return True


def restart_all(ctx: Ctx) -> bool:
    print()
    if input("This will remove all intermediate data. Continue? (y/N): ").lower() == "y":
        import shutil

        intermediates = ctx.script_dir / "intermediates"
        if intermediates.exists():
            shutil.rmtree(intermediates)
        if ctx.db_file.exists():
            ctx.db_file.unlink()
        print("Cleared intermediates. Ready to start fresh.")
    return True


# -------------------------
# Step registry
# -------------------------
@dataclass(frozen=True)
class Step:
    code: str
    label: str
    section: str
    action: Callable[[Ctx], bool]


def build_steps() -> Tuple[Dict[str, Step], Dict[str, List[str]]]:
    steps: List[Step] = [
        Step("0", "Build Intermediates", "0", setup_intermediates),
        Step("1a", "Download ERA5 MSLP netcdf files", "1", download_era5_mslp),
        Step("1b", "Detect cyclone nodes", "1", detect_nodes),
        Step("1c", "Stitch nodes into tracks", "1", stitch_nodes),
        Step("1d", "Convert tracks to parquet", "1", convert_tracks),
        Step("1e", "Refine cyclone centers", "1", refine_centers),
        Step("2a", "Fetch Sentinel-1 metadata", "2", fetch_sentinel),
        Step("3a", "Download SWOT netcdf files", "3", download_swot),
        Step("3b", "Convert SWOT to parquet index", "3", extract_swot),
        Step("4a", "Load data into DuckDB", "4", load_data),
        Step("4b", "Filter Sentinel by product type", "4", filter_product_type),
        Step("4c", "Find cyclone-satellite intersections", "4", find_intersections),
        Step("4d", "Compute geometry overlaps", "4", find_overlaps),
        Step("4e", "Filter by overlap percentage", "4", filter_overlap),
        Step("4f", "Filter by ERA5 criterion", "4", filter_era5),
        Step("4g", "Export UUIDs", "4", export_uuids),
        Step("5a", "Dump Sentinel outputs", "5", dump_sentinel),
        Step("5b", "Dump SWOT outputs", "5", dump_swot),
        Step("6a", "Download Sentinel1 outputs", "6", download_sentinel),
        Step("6b", "Rescale Sentinel1 imagery", "6", rescale_sentinel),
        Step("6c", "Convert SWOT imagery","6",swot_to_images)
    ]

    by_code: Dict[str, Step] = {s.code.lower(): s for s in steps}
    section_to_codes: Dict[str, List[str]] = {}
    for s in steps:
        section_to_codes.setdefault(s.section, []).append(s.code.lower())
    return by_code, section_to_codes


# -------------------------
# Menu / execution helpers
# -------------------------
def run_codes_in_order(ctx: Ctx, by_code: Dict[str, Step], codes: List[str]) -> None:
    for code in codes:
        c = code.strip().lower()
        step = by_code.get(c)
        if not step:
            print(f"{Colors.RED}Invalid step: {code}{Colors.NC}")
            continue
        print(f"\n{Colors.GREEN}Running step {step.code}: {step.label}{Colors.NC}")
        step.action(ctx)


def show_menu(by_code: Dict[str, Step], section_to_codes: Dict[str, List[str]]) -> None:
    def print_section(title: str, section: str, header_code: str) -> None:
        print(f"  {Colors.GREEN}{header_code}{Colors.NC} - {title} (full pipeline)")
        for code in section_to_codes.get(section, []):
            print(f"      {code} - {by_code[code].label}")
        print()

    print()
    print("========================================")
    print("   THALASSOICA PIPELINE - Interactive")
    print("========================================")
    print()
    print("Select a step to run (or q to quit):")
    print()

    print_section("BUILD INTERMEDIATES", "0", "0")
    print_section("TEMPEST EXTREMES", "1", "1")
    print_section("SENTINEL-1", "2", "2")
    print_section("SWOT", "3", "3")
    print_section("PIPELINE", "4", "4")
    print_section("DUMP OUTPUTS", "5", "5")
    print_section("DOWNLOAD/CONVERT", "6", "6")

    print(f"  {Colors.YELLOW}ALL{Colors.NC} - Run all steps (0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 6)")
    print(f"  {Colors.YELLOW}CUSTOM ({Colors.NC}c{Colors.YELLOW}){Colors.NC} [steps] - Run a custom sequence (e.g., c 1a 1b)")
    print(f"  {Colors.YELLOW}SECTION ({Colors.NC}1,2,3,4,5{Colors.YELLOW}){Colors.NC} - Run an entire section")
    print(f"  {Colors.YELLOW}SKIP{Colors.NC} - Skip a completed step")
    print(f"  {Colors.YELLOW}RESTART{Colors.NC} - Clear intermediates and restart")
    print()
    print(f"  {Colors.RED}q{Colors.NC} - Quit")
    print()


def parse_steps(parts: List[str]) -> List[str]:
    tokens: List[str] = []
    for p in parts:
        tokens.extend([t for t in p.replace(",", " ").split() if t.strip()])
    return tokens


def run_all(ctx: Ctx, by_code: Dict[str, Step], section_to_codes: Dict[str, List[str]]) -> None:
    print("========================================")
    print("RUNNING FULL PIPELINE")
    print("========================================")
    print()

    for section in ["0","1", "2", "3", "4", "5", "6"]:
        run_codes_in_order(ctx, by_code, section_to_codes.get(section, []))

    print()
    print("========================================")
    print("FULL PIPELINE COMPLETE")
    print("========================================")
    print(f"Database: {ctx.db_file}")
    print(f"UUIDs exported to: {ctx.script_dir}/intermediates/uuids/")
    print()


def run_interactive(ctx: Ctx) -> None:
    by_code, section_to_codes = build_steps()

    while True:
        show_menu(by_code, section_to_codes)

        try:
            line = input("Enter your choice: ").strip()
            if not line:
                continue

            parts = parse_steps(line.split())
            cmd = parts[0].lower()
            args = parts[1:]

            if cmd in ["q", "quit", "exit"]:
                print("Goodbye!")
                sys.exit(0)

            if cmd == "skip":
                skip_step(ctx)
                continue

            if cmd == "restart":
                restart_all(ctx)
                continue

            if cmd == "all":
                run_all(ctx, by_code, section_to_codes)
                continue

            if cmd in ["c", "custom"]:
                if not args:
                    steps_input = input("Steps: ").strip()
                    if not steps_input:
                        print("No steps entered.")
                        continue
                    args = parse_steps(steps_input.split())
                run_codes_in_order(ctx, by_code, args)
                continue

            if cmd in section_to_codes:
                run_codes_in_order(ctx, by_code, section_to_codes[cmd])
                continue

            run_codes_in_order(ctx, by_code, parts)

        except KeyboardInterrupt:
            print("\n\nExiting...")
            sys.exit(0)
        except EOFError:
            print("\nGoodbye!")
            sys.exit(0)


if __name__ == "__main__":
    ctx = Ctx()
    by_code, _section_to_codes = build_steps()

    if len(sys.argv) > 1 and sys.argv[1].lower() == "--batch":
        steps = [s for s in sys.argv[2:] if s.strip()]
        if not steps:
            print("No batch steps provided.")
            sys.exit(1)
        print(f"Running batch mode with steps: {', '.join(steps)}")
        run_codes_in_order(ctx, by_code, steps)
        sys.exit(0)

    run_interactive(ctx)
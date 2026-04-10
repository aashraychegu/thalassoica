#!/usr/bin/env python3
"""run_thalassoica - Interactive thalassoica pipeline orchestrator"""
import os
import sys
import subprocess
from pathlib import Path

# Colors
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'

def get_script_dir():
    script_path = Path(__file__).resolve()
    return script_path.parent

def get_db_file():
    script_dir = get_script_dir()
    return script_dir / "intermediates/db/database.duckdb"

def show_menu():
    print()
    print("========================================")
    print("   THALASSOICA PIPELINE - Interactive")
    print("========================================")
    print()
    print("Select a step to run (or q to quit):")
    print()
    print(f"  {Colors.GREEN}1{Colors.NC} - TEMPEST EXTREMES (full pipeline)")
    print("      1a - Download ERA5 MSLP netcdf files")
    print("      1b - Detect cyclone nodes")
    print("      1c - Stitch nodes into tracks")
    print("      1d - Convert tracks to parquet")
    print("      1e - Refine cyclone centers")
    print()
    print(f"  {Colors.GREEN}2{Colors.NC} - SENTINEL-1 (full pipeline)")
    print("      2a - Fetch Sentinel-1 metadata")
    print()
    print(f"  {Colors.GREEN}3{Colors.NC} - SWOT (full pipeline)")
    print("      3a - Download SWOT netcdf files")
    print("      3b - Convert SWOT to parquet index")
    print()
    print(f"  {Colors.GREEN}4{Colors.NC} - PIPELINE (full pipeline)")
    print("      4a - Load data into DuckDB")
    print("      4b - Filter Sentinel by product type")
    print("      4c - Find cyclone-satellite intersections")
    print("      4d - Compute geometry overlaps")
    print("      4e - Filter by overlap percentage")
    print("      4f - Filter by ERA5 criterion")
    print("      4g - Export UUIDs")
    print()
    print(f"  {Colors.YELLOW}ALL{Colors.NC} - Run all steps (tempestextremes -> sentinel -> swot -> pipeline)")
    print(f"  {Colors.YELLOW}CUSTOM ({Colors.NC}c{Colors.YELLOW}){Colors.NC} [steps] - Run a custom sequence of steps (e.g., c 1a 1b)")
    print(f"  {Colors.YELLOW}SECTION ({Colors.NC}1,2,3,4{Colors.YELLOW}){Colors.NC} Run a single subsection")
    print(f"  {Colors.YELLOW}SKIP{Colors.NC} - Skip a completed step")
    print(f"  {Colors.YELLOW}RESTART{Colors.NC} - Clear intermediates and restart")
    print()
    print(f"  {Colors.RED}q{Colors.NC} - Quit")
    print()

class Runner:
    def __init__(self):
        self.script_dir = get_script_dir()
        self.db_file = get_db_file()

    def run_cmd(self, cmd, description, step_name):
        print()
        print(f"{Colors.GREEN}Running: {description}{Colors.NC}")
        print("-" * 60)

        try:
            result = subprocess.run(cmd, shell=True, check=True)
            print(f"{Colors.GREEN}SUCCESS: {step_name}{Colors.NC}\n{'='*60}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"{Colors.RED}FAILED: {step_name}{Colors.NC}")
            return False

    def download_era5_mslp(self):
        cmd = f'uv run "{self.script_dir}/tempestextremes/download/download_netcdf.py" ' \
              f'--input-dir "{self.script_dir}/intermediates/era5_mslp" ' \
              f'--start-year 2014 --end-year 2024 --workers 8'
        return self.run_cmd(cmd, "Downloading ERA5 MSLP data (2014-2024)...", 
                           "download_era5_mslp")

    def detect_nodes(self):
        cmd = f'uv run "{self.script_dir}/tempestextremes/detect/detect_nodes.py" --mpi ' \
              f'--in_data_dir "{self.script_dir}/intermediates/era5_mslp" ' \
              f'--TE_temps "{self.script_dir}/intermediates/tempestextreme_files" ' \
              f'--out_data_dir "{self.script_dir}/intermediates/detectnodes"'
        return self.run_cmd(cmd, "Detecting cyclone nodes...", "detect_nodes")

    def stitch_nodes(self):
        cmd = f'uv run "{self.script_dir}/tempestextremes/stitch/stitch_nodes.py" ' \
              f'--input-dir "{self.script_dir}/intermediates/detectnodes" ' \
              f'--output-dir "{self.script_dir}/intermediates/stitchnodes" ' \
              f'--data-dir "{self.script_dir}/intermediates/tempestextreme_files"'
        return self.run_cmd(cmd, "Stitching nodes into tracks...", "stitch_nodes")

    def convert_tracks(self):
        cmd = f'uv run "{self.script_dir}/tempestextremes/convert/convert_nodes.py" ' \
              f'--in_file "{self.script_dir}/intermediates/stitchnodes/tracks_mslp.csv" ' \
              f'--out_file "{self.script_dir}/intermediates/te_out/tracks_mslp.parquet"'
        return self.run_cmd(cmd, "Converting tracks to parquet...", "convert_tracks")

    def refine_centers(self):
        cmd = f'uv run "{self.script_dir}/tempestextremes/subgrid/subgrid_precision.py" ' \
              f'--input-file "{self.script_dir}/intermediates/te_out/tracks_mslp.parquet" ' \
              f'--input-dir "{self.script_dir}/intermediates/era5_mslp/*.nc" ' \
              f'--output "{self.script_dir}/intermediates/te_out/tracks_mslp_refined.parquet" ' \
              f'--workers 16'
        return self.run_cmd(cmd, "Refining cyclone centers...", "refine_centers")

    def run_tempestextremes(self):
        print(f"====== TEMPEST EXTREMES {Colors.NC}")
        self.download_era5_mslp()
        self.detect_nodes()
        self.stitch_nodes()
        self.convert_tracks()
        self.refine_centers()
        print(f"====== TEMPEST EXTREMES COMPLETE {Colors.NC}")

    def fetch_sentinel(self):
        cmd = f'uv run "{self.script_dir}/sentinel1/download/download_metadata.py" ' \
              f'--start-date "2014-01-01" --end-date "2025-05-03" ' \
              f'--workers 16 --days-per-chunk 2 --items-per-request 1000'
        return self.run_cmd(cmd, "Fetching Sentinel-1 metadata...", "fetch_sentinel")

    def run_sentinel(self):
        print(f"====== SENTINEL-1 {Colors.NC}")
        self.fetch_sentinel()
        print(f"====== SENTINEL-1 COMPLETE {Colors.NC}")

    def download_swot(self):
        cmd = f'uv run "{self.script_dir}/swot/download/download_netcdf.py" ' \
              f'--start-date "2014-01-01" --end-date "2025-05-03" ' \
              f'--bbox -180 -80 180 -60 --workers 32'
        return self.run_cmd(cmd, "Downloading SWOT data...", "download_swot")

    def extract_swot(self):
        cmd = f'uv run "{self.script_dir}/swot/extract/extract_netcdf_to_parquet.py" ' \
              f'--input-dir "{self.script_dir}/intermediates/swot" ' \
              f'--output "{self.script_dir}/intermediates/shapes/swot/swot.parquet" ' \
              f'--step 20'
        return self.run_cmd(cmd, "Converting SWOT to parquet index...", "extract_swot")

    def run_swot(self):
        print(f"====== SWOT {Colors.NC}")
        self.download_swot()
        self.extract_swot()
        print(f"====== SWOT COMPLETE {Colors.NC}")

    def load_data(self):
        print(f"  Loading SWOT data...")
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/utils/load_parquet.py" '
            f'--input-parquet "{self.script_dir}/intermediates/shapes/swot/swot.parquet" '
            f'--table-name swot --output-db "{self.db_file}"',
            "Loading SWOT into DuckDB", "load_swot")

        print(f"  Loading Sentinel-1 data...")
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/utils/load_parquet.py" '
            f'--input-parquet "{self.script_dir}/intermediates/shapes/sentinel1/sentinel1_*.parquet" '
            f'--table-name sentinel1 --output-db "{self.db_file}"',
            "Loading Sentinel-1 into DuckDB", "load_sentinel")

    def filter_product_type(self):
        return self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/filter/product_type.py" '
            f'--db "{self.db_file}" --table sentinel1 --product-type "EW_GRD%"',
            "Filtering by product type", "filter_product_type")

    def find_intersections(self):
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/search/intersections.py" '
            f'--db "{self.db_file}" --table sentinel1__product_filtered '
            f'--points "{self.script_dir}/intermediates/te_out/tracks_mslp_refined.parquet"',
            "Finding cyclone-satellite intersections - Sentinel", "find_intersections")

        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/search/intersections.py" '
            f'--db "{self.db_file}" --table swot '
            f'--points "{self.script_dir}/intermediates/te_out/tracks_mslp_refined.parquet"',
            "Finding cyclone-satellite intersections - SWOT", "find_intersections")

    def find_overlaps(self):
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/search/overlaps.py" '
            f'--db "{self.db_file}" --matches-table sentinel1__product_filtered_matches '
            f'--output-table sentinel1__product_filtered_matches_overlaps',
            "Computing geometry overlaps", "find_overlaps")
        
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/search/overlaps.py" '
            f'--db "{self.db_file}" --matches-table swot_matches '
            f'--output-table swot_matches_overlaps',
            "Computing geometry overlaps", "find_overlaps")

    def filter_overlap(self):
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/filter/overlap_percentage.py" '
            f'--db "{self.db_file}" --in-table sentinel1__product_filtered_matches_overlaps '
            f'--min-overlap 15 --max-overlap 100',
            "Filtering by overlap percentage", "filter_overlap")
        
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/filter/overlap_percentage.py" '
            f'--db "{self.db_file}" --in-table swot_matches_overlaps '
            f'--min-overlap 15 --max-overlap 100',
            "Filtering by overlap percentage", "filter_overlap")

        
    def filter_era5(self):
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/filter/era5_criterion.py" '
            f'--db "{self.db_file}" '
            f'--input-table sentinel1__product_filtered_matches_overlaps__overlap_filtered '
            f'--key-dotenv "{self.script_dir}/copernicus_api_key.env" '
            f'--era5-variable sea_ice_cover --threshold 0.15',
            "Filtering by ERA5 criterion", "filter_era5")
        
        self.run_cmd(
            f'uv run "{self.script_dir}/pipeline/filter/era5_criterion.py" '
            f'--db "{self.db_file}" '
            f'--input-table swot_matches_overlaps__overlap_filtered '
            f'--key-dotenv "{self.script_dir}/copernicus_api_key.env" '
            f'--era5-variable sea_ice_cover --threshold 0.15',
            "Filtering by ERA5 criterion", "filter_era5")

    def export_uuids(self):
        self.run_cmd(
            f'uv run "{self.script_dir}/sentinel1/export/download_information.py" '
            f'--db "{self.db_file}" '
            f'--overlaps-table sentinel1__product_filtered_matches_overlaps__overlap_filtered__era5_filtered '
            f'--output "{self.script_dir}/intermediates/uuids/sentinel_download_uuids.parquet"',
            "Exporting UUIDs", "export_uuids")
        
        self.run_cmd(
            f'uv run "{self.script_dir}/swot/export/download_information.py" '
            f'--db "{self.db_file}" '
            f'--overlaps-table swot_matches_overlaps__overlap_filtered__era5_filtered '
            f'--output "{self.script_dir}/intermediates/uuids/swot_download_uuids.parquet"',
            "Exporting UUIDs", "export_uuids")

    def run_pipeline(self):
        print(f"====== PIPELINE {Colors.NC}")
        self.load_data()
        self.filter_product_type()
        self.find_intersections()
        self.find_overlaps()
        self.filter_overlap()
        self.filter_era5()
        self.export_uuids()
        print(f"====== PIPELINE COMPLETE {Colors.NC}")

    def skip_step(self):
        print("Skipping completed step.")

    def restart_all(self):
        print()
        if input("This will remove all intermediate data. Continue? (y/N): ").lower() == 'y':
            import shutil
            intermediates = self.script_dir / "intermediates"
            if intermediates.exists():
                shutil.rmtree(intermediates)
            if self.db_file.exists():
                self.db_file.unlink()
            print("Cleared intermediates. Ready to start fresh.")

    def run_all(self):
        print(f"========================================")
        print("RUNNING FULL PIPELINE")
        print(f"========================================")
        print()

        self.run_tempestextremes()
        self.run_sentinel()
        self.run_swot()
        self.run_pipeline()

        print()
        print(f"========================================")
        print("FULL PIPELINE COMPLETE")
        print(f"========================================")
        print(f"Database: {self.db_file}")
        print(f"UUIDs exported to: {self.script_dir}/intermediates/sentinel_download_uuids.parquet")
        print()

    def run_batch_steps(self, steps):
        """
        Run a list of steps specified by the user.
        Steps can be: 1, 1a, 1b, 2, 2a, 3, 3a, 3b, 4, 4a, 4b, 4c, 4d, 4e, 4f, 4g, ALL
        """
        if not steps:
            print("No steps specified.")
            return

        for step in steps:
            step = step.strip().lower()
            
            if step == 'all':
                print(f"\n{Colors.YELLOW}Running ALL steps{Colors.NC}")
                self.run_all()
                continue

            # Step 1 (tempestextremes substeps)
            if step in ['1a', '1b', '1c', '1d', '1e']:
                self._run_tempestextremes_substep(step)
            elif step == '1':
                print(f"\n{Colors.YELLOW}Running full TEMPEST EXTREMES{Colors.NC}")
                self.run_tempestextremes()
            # Step 2 (sentinel substeps)
            elif step in ['2a']:
                self._run_sentinel_substep(step)
            elif step == '2':
                print(f"\n{Colors.YELLOW}Running full SENTINEL-1{Colors.NC}")
                self.run_sentinel()
            # Step 3 (swot substeps)
            elif step in ['3a', '3b']:
                self._run_swot_substep(step)
            elif step == '3':
                print(f"\n{Colors.YELLOW}Running full SWOT{Colors.NC}")
                self.run_swot()
            # Step 4 (pipeline substeps)
            elif step in ['4a', '4b', '4c', '4d', '4e', '4f', '4g']:
                self._run_pipeline_substep(step)
            elif step == '4':
                print(f"\n{Colors.YELLOW}Running full PIPELINE{Colors.NC}")
                self.run_pipeline()
            # Other commands
            elif step == 'skip':
                self.skip_step()
            elif step == 'restart':
                self.restart_all()
            else:
                print(f"{Colors.RED}Invalid step: {step}{Colors.NC}")

    def _run_tempestextremes_substep(self, step):
        methods = {
            '1a': self.download_era5_mslp,
            '1b': self.detect_nodes,
            '1c': self.stitch_nodes,
            '1d': self.convert_tracks,
            '1e': self.refine_centers,
        }
        if step in methods:
            print(f"\n{Colors.GREEN}Running step {step}: TEMPEST EXTREMES substep{Colors.NC}")
            methods[step]()

    def _run_sentinel_substep(self, step):
        methods = {
            '2a': self.fetch_sentinel,
        }
        if step in methods:
            print(f"\n{Colors.GREEN}Running step {step}: SENTINEL-1 substep{Colors.NC}")
            methods[step]()

    def _run_swot_substep(self, step):
        methods = {
            '3a': self.download_swot,
            '3b': self.extract_swot,
        }
        if step in methods:
            print(f"\n{Colors.GREEN}Running step {step}: SWOT substep{Colors.NC}")
            methods[step]()

    def _run_pipeline_substep(self, step):
        methods = {
            '4a': self.load_data,
            '4b': self.filter_product_type,
            '4c': self.find_intersections,
            '4d': self.find_overlaps,
            '4e': self.filter_overlap,
            '4f': self.filter_era5,
            '4g': self.export_uuids,
        }
        if step in methods:
            print(f"\n{Colors.GREEN}Running step {step}: PIPELINE substep{Colors.NC}")
            methods[step]()

def run_interactive_menu(runner):
    """Run the interactive menu loop"""
    while True:
        show_menu()

        try:
            line = input("Enter your choice: ").strip()
            if not line:
                continue

            parts = line.split()
            command = parts[0]
            args = parts[1:]
            command_lower = command.lower()

            if command_lower in ['c', 'custom']:
                if args:
                    print(f"\n{Colors.YELLOW}Running custom steps: {' '.join(args)}{Colors.NC}")
                    runner.run_batch_steps(args)
                else:
                    # If only 'c' or 'custom' is entered, go to the interactive step entry
                    run_custom_steps_menu(runner)
                continue

            command_upper = command.upper()

            if len(parts) == 1:
                if command_upper == '1':
                    run_tempestextremes_menu(runner)
                    continue
                elif command_upper == '2':
                    run_sentinel_menu(runner)
                    continue
                elif command_upper == '3':
                    run_swot_menu(runner)
                    continue
                elif command_upper == '4':
                    run_pipeline_menu(runner)
                    continue
                elif command_upper == 'ALL':
                    runner.run_all()
                    continue
                elif command_upper == 'SKIP':
                    runner.skip_step()
                    continue
                elif command_upper == 'RESTART':
                    runner.restart_all()
                    continue
                elif command_lower in ['q', 'quit']:
                    print("Goodbye!")
                    sys.exit(0)

            # If none of the single commands match, or there are multiple parts,
            # assume it's a list of batch steps. This allows '1a 1b ...'
            runner.run_batch_steps(parts)

        except KeyboardInterrupt:
            print("\n\nExiting...")
            sys.exit(0)
        except EOFError:
            print("\nGoodbye!")
            sys.exit(0)

def run_tempestextremes_menu(runner):
    print(f"\n====== STEP 1: TEMPEST EXTREMES =====")
    runner.run_tempestextremes()

def run_sentinel_menu(runner):
    print(f"\n====== STEP 2: SENTINEL-1 =====")
    runner.run_sentinel()

def run_swot_menu(runner):
    print(f"\n====== STEP 3: SWOT =====")
    runner.run_swot()

def run_pipeline_menu(runner):
    print(f"\n====== STEP 4: PIPELINE =====")
    runner.run_pipeline()

def run_custom_steps_menu(runner):
    print()

    try:
        steps_input = input(f"Steps {Colors.NC}: ").strip()
        if not steps_input:
            print("No steps entered.")
            return

        # Parse steps - handle both comma and space separators
        # Also handle various delimiters
        steps = [s.strip() for s in steps_input.replace(',', ' ').replace('\t', ' ').split() if s.strip()]

        # Filter out empty strings and common non-step values
        steps = [s for s in steps if s and s not in ['', 'q', 'quit', 'exit']]

        if steps:
            print(f"\n{Colors.YELLOW}Processing {len(steps)} step(s)...{Colors.NC}")
            runner.run_batch_steps(steps)
        else:
            print("No valid steps entered.")

    except KeyboardInterrupt:
        print("\nOperation cancelled.")
    except EOFError:
        print("\nGoodbye!")
        sys.exit(0)

if __name__ == "__main__":
    runner = Runner()

    # Check if batch steps were provided via command line
    if len(sys.argv) > 1 and sys.argv[1].lower() == '--batch':
        if len(sys.argv) > 2:
            steps = sys.argv[2:]
            print(f"Running batch mode with steps: {', '.join(steps)}")
            runner.run_batch_steps(steps)
            sys.exit(0)

    # Otherwise, run the interactive menu
    try:
        run_interactive_menu(runner)
    except KeyboardInterrupt:
        print("\n\nExiting...")
        sys.exit(0)
    except EOFError:
        print("\nGoodbye!")
        sys.exit(0)
#!/bin/bash
# run_thalassoica - Interactive thalassoica pipeline orchestrator
# Usage: ./run_thalassoica

set -e

# Default configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_FILE="${SCRIPT_DIR}/intermediates/db/database.duckdb"

# Color codes for interactive menu
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to display the main menu
show_menu() {
    echo ""
    echo "========================================"
    echo "   THALASSOICA PIPELINE - Interactive"
    echo "========================================"
    echo ""
    echo "Select a step to run (or q to quit):"
    echo ""
    echo "  ${GREEN}1${NC} - TEMPEST EXTREMES (full pipeline)"
    echo "      1a - Download ERA5 MSLP netcdf files"
    echo "      1b - Detect cyclone nodes"
    echo "      1c - Stitch nodes into tracks"
    echo "      1d - Convert tracks to parquet"
    echo "      1e - Refine cyclone centers"
    echo ""
    echo "  ${GREEN}2${NC} - SENTINEL-1 (full pipeline)"
    echo "      2a - Fetch Sentinel-1 metadata"
    echo ""
    echo "  ${GREEN}3${NC} - SWOT (full pipeline)"
    echo "      3a - Download SWOT netcdf files"
    echo "      3b - Convert SWOT to parquet index"
    echo ""
    echo "  ${GREEN}4${NC} - PIPELINE (full pipeline)"
    echo "      4a - Load data into DuckDB"
    echo "      4b - Filter Sentinel by product type"
    echo "      4c - Find cyclone-satellite intersections"
    echo "      4d - Compute geometry overlaps"
    echo "      4e - Filter by overlap percentage"
    echo "      4f - Filter by ERA5 criterion"
    echo "      4g - Export UUIDs"
    echo ""
    echo "  ${YELLOW}ALL${NC} - Run all steps (tempestextremes -> sentinel -> swot -> pipeline)"
    echo "  ${YELLOW}SKIP${NC} - Skip a completed step"
    echo "  ${YELLOW}RESTART${NC} - Clear intermediates and restart"
    echo ""
    echo "  ${RED}q${NC} - Quit"
    echo ""
}

# Function to run a step with logging
run_step() {
    local step_name="$1"
    shift
    echo ""
    echo -e "${GREEN}Running: ${step_name}${NC}"
    echo "------------------------------------------------------------"

    "$@"

    echo -e "${GREEN}SUCCESS: ${step_name}${NC}"
    echo ""
}

# Function to download ERA5 MSLP data
download_era5_mslp() {
    echo "  Downloading ERA5 MSLP data (2014-2024)..."
    uv run "${SCRIPT_DIR}/tempestextremes/download/download_netcdf.py" \
        --input-dir "${SCRIPT_DIR}/intermediates/era5_mslp" \
        --start-year 2014 \
        --end-year 2024 \
        --workers 8
}

# Function to detect nodes
detect_nodes() {
    uv run "${SCRIPT_DIR}/tempestextremes/detect/detect_nodes.py" \
        --in_data_dir "${SCRIPT_DIR}/intermediates/era5_mslp" \
        --TE_temps "${SCRIPT_DIR}/intermediates/tempestextreme_files" \
        --out_data_dir "${SCRIPT_DIR}/intermediates/detectnodes"
}

# Function to stitch nodes
stitch_nodes() {
    uv run "${SCRIPT_DIR}/tempestextremes/stitch/stitch_nodes.py" \
        --input-dir "${SCRIPT_DIR}/intermediates/detectnodes" \
        --output-dir "${SCRIPT_DIR}/intermediates/stitchnodes" \
        --data-dir "${SCRIPT_DIR}/intermediates/tempestextreme_files"
}

# Function to convert tracks
convert_tracks() {
    uv run "${SCRIPT_DIR}/tempestextremes/convert/convert_nodes.py" \
        --in_file "${SCRIPT_DIR}/intermediates/stitchnodes/tracks_mslp.csv" \
        --out_file "${SCRIPT_DIR}/intermediates/te_out/tracks_mslp.parquet"
}

# Function to refine centers
refine_centers() {
    uv run "${SCRIPT_DIR}/tempestextremes/subgrid/subgrid_precision.py" \
        --input-file "${SCRIPT_DIR}/intermediates/te_out/tracks_mslp.parquet" \
        --input-dir "${SCRIPT_DIR}/intermediates/era5_mslp/*.nc" \
        --output "${SCRIPT_DIR}/intermediates/te_out/tracks_mslp_refined.parquet" \
        --workers 24
}

# Function to run tempestextremes
run_tempestextremes() {
    echo "====== TEMPEST EXTREMES ======"
    download_era5_mslp
    detect_nodes
    stitch_nodes
    convert_tracks
    refine_centers
    echo "====== TEMPEST EXTREMES COMPLETE ======"
}

# Function to fetch sentinel metadata
fetch_sentinel() {
    echo "  Fetching Sentinel-1 metadata..."
    uv run "${SCRIPT_DIR}/sentinel1/download/download_metadata.py" \
        --start-date "2014-01-01" \
        --end-date "2025-05-03" \
        --workers 16 \
        --days-per-chunk 2 \
        --items-per-request 1000
}

# Function to run sentinel
run_sentinel() {
    echo "====== SENTINEL-1 ======"
    fetch_sentinel
    echo "====== SENTINEL-1 COMPLETE ======"
}

# Function to download SWOT data
download_swot() {
    echo "  Downloading SWOT data..."
    uv run "${SCRIPT_DIR}/swot/download/download_netcdf.py" \
        --start-date "2014-01-01" \
        --end-date "2025-05-03" \
        --bbox -180 -80 180 -60 \
        --workers 32
}

# Function to convert SWOT
extract_swot() {
    echo "  Converting SWOT to parquet index..."
    uv run "${SCRIPT_DIR}/swot/extract/extract_netcdf_to_parquet.py" \
        --input-dir "${SCRIPT_DIR}/intermediates/swot" \
        --output "${SCRIPT_DIR}/intermediates/shapes/swot/swot.parquet" \
        --step 20 \
        --workers
}

# Function to run SWOT
run_swot() {
    echo "====== SWOT ======"
    download_swot
    extract_swot
    echo "====== SWOT COMPLETE ======"
}

# Function to load data into duckdb
load_data() {
    echo "  Loading SWOT data..."
    uv run "${SCRIPT_DIR}/pipeline/utils/load_parquet.py" \
        --input-parquet "${SCRIPT_DIR}/intermediates/shapes/swot/swot.parquet" \
        --table-name swot \
        --output-db "${DB_FILE}"

    echo "  Loading Sentinel-1 data..."
    uv run "${SCRIPT_DIR}/pipeline/utils/load_parquet.py" \
        --input-parquet "${SCRIPT_DIR}/intermediates/shapes/sentinel/sentinel_*.parquet" \
        --table-name sentinel \
        --output-db "${DB_FILE}"

    echo "  Loading cyclone tracks..."
    uv run "${SCRIPT_DIR}/pipeline/utils/load_parquet.py" \
        --input-parquet "${SCRIPT_DIR}/intermediates/te_out/tracks_mslp_refined.parquet" \
        --table-name cyclones \
        --output-db "${DB_FILE}"
}

# Function to filter by product type
filter_product_type() {
    uv run "${SCRIPT_DIR}/pipeline/filter/product_type.py" \
        --db "${DB_FILE}" \
        --table sentinel \
        --product-type "EW_GRD%"
}

# Function to find intersections
find_intersections() {
    uv run "${SCRIPT_DIR}/pipeline/search/intersections.py" \
        --db "${DB_FILE}" \
        --table sentinel \
        --points "${SCRIPT_DIR}/intermediates/te_out/tracks_mslp_refined.parquet"
}

# Function to find overlaps
find_overlaps() {
    uv run "${SCRIPT_DIR}/pipeline/search/overlaps.py" \
        --db "${DB_FILE}" \
        --matches-table sentinel_matches \
        --output-table sentinel_matches_overlaps
}

# Function to filter by overlap percentage
filter_overlap() {
    uv run "${SCRIPT_DIR}/pipeline/filter/overlap_percentage.py" \
        --db "${DB_FILE}" \
        --in-table sentinel_matches_overlaps \
        --min-overlap 15 \
        --max-overlap 100
}

# Function to filter by era5
filter_era5() {
    uv run "${SCRIPT_DIR}/pipeline/filter/era5_criterion.py" \
        --db "${DB_FILE}" \
        --input-table sentinel_matches_overlaps__overlap_filtered \
        --key-dotenv "${SCRIPT_DIR}/copernicus_api_key.env" \
        --era5-variable sea_ice_cover \
        --threshold 0.15 \
        --lookup-method vectorized
}

# Function to export uuids
export_uuids() {
    uv run "${SCRIPT_DIR}/sentinel1/export/download_information.py" \
        --db "${DB_FILE}" \
        --overlaps-table sentinel_matches_overlaps__overlap_filtered__era5_filtered \
        --output "${SCRIPT_DIR}/intermediates/sentinel_download_uuids.parquet"
}

# Function to run pipeline
run_pipeline() {
    echo "====== PIPELINE ======"
    load_data
    filter_product_type
    find_intersections
    find_overlaps
    filter_overlap
    filter_era5
    export_uuids
    echo "====== PIPELINE COMPLETE ======"
}

# Function to skip step
skip_step() {
    echo "Skipping completed step."
}

# Function to restart
restart_all() {
    echo ""
    read -p "This will remove all intermediate data. Continue? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "${SCRIPT_DIR}/intermediates"
        rm -f "${DB_FILE}"
        echo "Cleared intermediates. Ready to start fresh."
    fi
}

# Function to run all
run_all() {
    echo "========================================"
    echo "RUNNING FULL PIPELINE"
    echo "========================================"
    echo ""

    run_tempestextremes
    run_sentinel
    run_swot
    run_pipeline

    echo ""
    echo "========================================"
    echo "FULL PIPELINE COMPLETE"
    echo "========================================"
    echo "Database: ${DB_FILE}"
    echo "UUIDs exported to: ${SCRIPT_DIR}/intermediates/sentinel_download_uuids.parquet"
    echo ""
}

# Main interactive loop
while true; do
    show_menu

    read -p "Choice: " choice

    case $choice in
        1)
            echo ""
            echo "===== STEP 1: TEMPEST EXTREMES ====="
            echo "Which sub-step?"
            echo "  a - Download ERA5 MSLP"
            echo "  b - Detect nodes"
            echo "  c - Stitch nodes"
            echo "  d - Convert tracks"
            echo "  e - Refine centers"
            echo "  1 - Run all"
            read -p "Choice (a-e or 1): " subchoice

            case $subchoice in
                a) download_era5_mslp ;;
                b) detect_nodes ;;
                c) stitch_nodes ;;
                d) convert_tracks ;;
                e) refine_centers ;;
                1) run_tempestextremes ;;
                *) echo "Invalid choice";;
            esac
            ;;
        2)
            echo ""
            echo "===== STEP 2: SENTINEL-1 ====="
            echo "Which sub-step?"
            echo "  a - Fetch metadata"
            echo "  2 - Run all"
            read -p "Choice (a or 2): " subchoice

            case $subchoice in
                a) fetch_sentinel ;;
                2) run_sentinel ;;
                *) echo "Invalid choice";;
            esac
            ;;
        3)
            echo ""
            echo "===== STEP 3: SWOT ====="
            echo "Which sub-step?"
            echo "  a - Download netcdf files"
            echo "  b - Convert to parquet index"
            echo "  3 - Run all"
            read -p "Choice (a-b or 3): " subchoice

            case $subchoice in
                a) download_swot ;;
                b) extract_swot ;;
                3) run_swot ;;
                *) echo "Invalid choice";;
            esac
            ;;
        4)
            echo ""
            echo "===== STEP 4: PIPELINE ====="
            echo "Which sub-step?"
            echo "  a - Load data into DuckDB"
            echo "  b - Filter by product type"
            echo "  c - Find intersections"
            echo "  d - Find overlaps"
            echo "  e - Filter by overlap"
            echo "  f - Filter by ERA5"
            echo "  g - Export UUIDs"
            echo "  4 - Run all"
            read -p "Choice (a-g or 4): " subchoice

            case $subchoice in
                a) load_data ;;
                b) filter_product_type ;;
                c) find_intersections ;;
                d) find_overlaps ;;
                e) filter_overlap ;;
                f) filter_era5 ;;
                g) export_uuids ;;
                4) run_pipeline ;;
                *) echo "Invalid choice";;
            esac
            ;;
        ALL)
            run_all
            ;;
        SKIP)
            skip_step
            ;;
        RESTART)
            restart_all
            ;;
        q|Q)
            echo "Goodbye!"
            exit 0
            ;;
        *)
            echo "Invalid choice. Please try again."
            ;;
    esac
done
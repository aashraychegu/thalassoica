#!/bin/bash
#SBATCH --job-name=sentinel_download
#SBATCH --output=sentinel_download_%j.out
#SBATCH --error=sentinel_download_%j.err
#SBATCH --partition=batch
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=32

# Download Sentinel-1 imagery for thalassoica pipeline
# Prerequisites: UUIDs file exported from run_thalassoica

set -e

PROJECT_ROOT="/scratch/groups/dustinms/apptainer-tests/thalassoica/opt/project"
CONTAINER="thalassoica"
CONTAINER_PATH="/opt/project"

# Environment variables
TOKEN_DOTENV="${PROJECT_ROOT}/copernicus_tokens.env"
LOGIN_DOTENV="${PROJECT_ROOT}/copernicus_login.env"
DB_DIR="${PROJECT_ROOT}/intermediates/db"
DB_FILE="${DB_DIR}/thalassoica.db"
UUIDS_FILE="${PROJECT_ROOT}/intermediates/db/sentinel_download_uuids.parquet"
OUTPUT_DIR="${PROJECT_ROOT}/intermediates/tiffs"

# Check prerequisites
if [ ! -f "${TOKEN_DOTENV}" ]; then
    echo "ERROR: Token file not found: ${TOKEN_DOTENV}"
    echo "Run copernicus_access_token.py first to authenticate"
    exit 1
fi

if [ ! -f "${UUIDS_FILE}" ]; then
    echo "ERROR: UUIDs file not found: ${UUIDS_FILE}"
    echo "Run run_thalassoica first to generate UUIDs"
    exit 1
fi

echo "========================================"
echo "SENTINEL-1 IMAGERY DOWNLOAD"
echo "========================================"
echo "Container: ${CONTAINER}"
echo "UUIDs: ${UUIDS_FILE}"
echo "Output: ${OUTPUT_DIR}"
echo ""

# Run download inside the apptainer container
apptainer run --writable "${CONTAINER}" \
    python "${CONTAINER_PATH}/sentinel1/download/download_images.py" \
    --uuids "${UUIDS_FILE}" \
    --output "${OUTPUT_DIR}" \
    --token-dotenv "${TOKEN_DOTENV}" \
    --login-dotenv "${LOGIN_DOTENV}" \
    --search-workers 32 \
    --download-workers 4

echo ""
echo "========================================"
echo "DOWNLOAD COMPLETE"
echo "========================================"
echo "Downloaded imagery to: ${OUTPUT_DIR}"

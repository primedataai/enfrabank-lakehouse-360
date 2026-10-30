#!/usr/bin/env bash

set -Eeuo pipefail

# Always execute from the repository root.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON="$ROOT_DIR/.venv/bin/python"
INGESTION_SCRIPT="$ROOT_DIR/ingestion/batch/bronze_postgres_to_parquet.py"

# Optional arguments:
#   $1 = batch ID
#   $2 = load date (YYYY-MM-DD)
BATCH_ID="${1:-FULL_BANKING_$(date +%Y%m%d_%H%M%S)}"
LOAD_DATE="${2:-$(date +%Y-%m-%d)}"

echo "=================================================="
echo "       ENFRABANK FULL BRONZE INGESTION"
echo "=================================================="
echo "Repository : $ROOT_DIR"
echo "Batch ID   : $BATCH_ID"
echo "Load date  : $LOAD_DATE"
echo "Load type  : FULL"
echo "Scope      : All 14 configured banking tables"
echo "=================================================="

if [[ ! -f "$INGESTION_SCRIPT" ]]; then
    echo "ERROR: Bronze ingestion script not found."
    exit 1
fi

if [[ ! -x "$PYTHON" ]]; then
    echo "ERROR: Python virtual environment not found at $PYTHON"
    exit 1
fi

if [[ ! -f "$ROOT_DIR/.env" ]]; then
    echo "ERROR: .env file not found in repository root."
    exit 1
fi

echo
echo "Starting PySpark ingestion..."
echo

"$PYTHON" "$INGESTION_SCRIPT" \
    --batch-id "$BATCH_ID" \
    --load-type FULL \
    --load-date "$LOAD_DATE"

echo
echo "=================================================="
echo "FULL BRONZE INGESTION COMPLETED"
echo "Batch ID: $BATCH_ID"
echo "=================================================="

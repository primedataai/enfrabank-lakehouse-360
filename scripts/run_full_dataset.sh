#!/usr/bin/env bash
set -euo pipefail

# EnfraBank Lakehouse 360
# Full synthetic banking dataset execution

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ ! -x ".venv/bin/python" ]]; then
    echo "ERROR: Python virtual environment not found."
    echo "Create it and install requirements.txt first."
    exit 1
fi

if [[ ! -f ".env" ]]; then
    echo "ERROR: .env file not found. Database credentials are required."
    exit 1
fi

echo "Starting EnfraBank full synthetic dataset generation..."
echo "Target: 230,000 records across 12 banking entities."
echo "WARNING: --reset clears existing demo banking records."

.venv/bin/python source/synthetic_data/generate_banking_data.py \
    --seed 42 \
    --customers 10000 \
    --deposit-accounts 15000 \
    --deposit-transactions 100000 \
    --credit-card-accounts 5000 \
    --card-transactions 50000 \
    --loan-accounts 3000 \
    --loan-payments 20000 \
    --atm-terminals 100 \
    --atm-transactions 25000 \
    --corporate-customers 500 \
    --corporate-accounts 800 \
    --credit-facilities 600 \
    --reset

echo "Full dataset generation completed."

#!/usr/bin/env bash
set -euo pipefail

echo "Checking Stage III repository artifacts..."
python3 scripts/check_stage3_artifacts.py

echo "Building Superset-ready Stage IV database..."
python3 scripts/create_stage4_superset_db.py

if [[ "${STAGE4_LOAD_POSTGRES:-0}" == "1" ]]; then
  echo "Loading Stage IV dashboard tables into PostgreSQL..."
  python3 scripts/load_stage4_postgres.py
fi

echo "Checking Stage IV dashboard source assets..."
python3 scripts/check_stage4_assets.py

echo "Stage IV local prerequisites are ready."
echo "Next manual steps:"
echo "1. In Superset, use the existing PostgreSQL connection for team20_projectdb."
echo "2. Create datasets from stage4_* tables, or upload the CSV files if needed."
echo "3. Create charts using reports/dashboard.md and publish: IEEE-CIS Fraud Risk: EDA to Spark ML."

#!/usr/bin/env bash
set -euo pipefail

echo "Checking Stage III repository artifacts..."
python3 scripts/check_stage3_artifacts.py

echo "Checking Stage IV dashboard source assets..."
python3 scripts/check_stage4_assets.py

echo "Stage IV local prerequisites are ready."
echo "Next manual steps:"
echo "1. Create Superset datasets from Stage II and Stage III result tables/files."
echo "2. Build one dashboard with data description, EDA insights, and ML results."
echo "3. Publish the dashboard in Superset."

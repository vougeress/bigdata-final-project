#!/usr/bin/env bash
set -euo pipefail
# Stage II entry point: Hive table setup, partitioned/bucketed table, and EDA queries.
# Runs on the IU Hadoop cluster from the project root directory.

: "${TEAM_NAME:=team20}"
export TEAM_NAME

bash scripts/stage2_spark.sh

echo "Generating EDA charts..."
python3 scripts/create_stage2_charts.py

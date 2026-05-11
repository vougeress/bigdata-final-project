#!/usr/bin/env bash
set -euo pipefail

export PYLINTHOME="${PWD}/.pylint.d"
mkdir -p "${PYLINTHOME}"

python3 -m pylint \
  --disable=consider-using-f-string \
  --fail-under=8.0 \
  scripts/check_stage4_assets.py

bash -n scripts/stage4.sh

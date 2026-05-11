#!/usr/bin/env bash
set -euo pipefail

export PYLINTHOME="${PWD}/.pylint.d"
mkdir -p "${PYLINTHOME}"

if python3 -c "import pylint" >/dev/null 2>&1; then
  python3 -m pylint \
    --disable=consider-using-f-string \
    --fail-under=8.0 \
    scripts/check_stage4_assets.py \
    scripts/create_stage4_superset_db.py \
    scripts/load_stage4_postgres.py
else
  echo "pylint is not installed; running Python syntax checks instead."
  python3 -m py_compile \
    scripts/check_stage4_assets.py \
    scripts/create_stage4_superset_db.py \
    scripts/load_stage4_postgres.py
fi

bash -n scripts/stage4.sh

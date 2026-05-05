#!/usr/bin/env bash
set -euo pipefail

python3 -m py_compile scripts/generate_stage2_hql.py

if command -v pylint >/dev/null 2>&1; then
  pylint scripts/generate_stage2_hql.py
else
  echo "pylint is not installed; Python compile check passed."
fi

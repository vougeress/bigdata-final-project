#!/usr/bin/env bash
set -euo pipefail
# Run pylint on all Stage II Python scripts and report scores.

SCRIPTS=(
    scripts/generate_stage2_hql.py
    scripts/stage2_spark.py
    scripts/create_stage2_charts.py
)

PASS=0
FAIL=0

for script in "${SCRIPTS[@]}"; do
    if [ ! -f "$script" ]; then
        echo "SKIP (not found): $script"
        continue
    fi
    echo "=== pylint: $script ==="
    if pylint --fail-under=7 "$script"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "Lint summary: ${PASS} passed, ${FAIL} failed."
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi

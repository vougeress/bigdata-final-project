#!/usr/bin/env bash
set -euo pipefail

export PYLINTHOME="${PWD}/.pylint.d"
mkdir -p "${PYLINTHOME}"

python3 -m pylint \
  --disable=import-error,consider-using-f-string,too-many-lines,too-many-locals,too-many-statements,too-many-branches,too-few-public-methods,super-with-arguments,duplicate-code,unspecified-encoding,unsubscriptable-object \
  --fail-under=7.0 \
  scripts/stage3_train.py

bash -n scripts/stage3.sh

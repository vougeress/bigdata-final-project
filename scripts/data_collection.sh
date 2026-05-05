#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="data"
DATASET="ieee-fraud-detection"
FILES=(
  "train_transaction.csv"
  "train_identity.csv"
  "test_transaction.csv"
  "test_identity.csv"
)
OPTIONAL_FILES=(
  "sample_submission.csv"
)

mkdir -p "$DATA_DIR"

missing=0
for file in "${FILES[@]}"; do
  if [[ ! -s "$DATA_DIR/$file" ]]; then
    missing=1
  fi
done

if [[ "$missing" -eq 0 ]]; then
  echo "All dataset files already exist and are non-empty in $DATA_DIR."
  exit 0
fi

if command -v kaggle >/dev/null 2>&1; then
  echo "Downloading $DATASET with Kaggle CLI."
  kaggle competitions download -c "$DATASET" -p "$DATA_DIR"
  unzip -o "$DATA_DIR/$DATASET.zip" -d "$DATA_DIR"
else
  echo "Kaggle CLI is not installed. Place the IEEE-CIS CSV files in $DATA_DIR/ and rerun this script." >&2
  exit 1
fi

for file in "${FILES[@]}"; do
  if [[ ! -s "$DATA_DIR/$file" ]]; then
    echo "Required file is missing or empty after collection: $DATA_DIR/$file" >&2
    exit 1
  fi
done

for file in "${OPTIONAL_FILES[@]}"; do
  if [[ ! -s "$DATA_DIR/$file" ]]; then
    echo "Optional file is not present: $DATA_DIR/$file"
  fi
done

echo "Dataset collection completed."

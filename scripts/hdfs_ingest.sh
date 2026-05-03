#!/usr/bin/env bash
set -euo pipefail

: "${TEAM_NAME:=team20}"
: "${PGHOST:=hadoop-04.uni.innopolis.ru}"
: "${PGPORT:=5432}"
: "${PGDATABASE:=${TEAM_NAME}_projectdb}"
: "${PGUSER:=${TEAM_NAME}}"
: "${PGPASSWORD:?Set PGPASSWORD}"

WAREHOUSE_DIR="${WAREHOUSE_DIR:-project/warehouse}"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
CONNECT="jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}"
EXPECTED_TABLES=(
  "train_transaction"
  "train_identity"
  "test_transaction"
  "test_identity"
)

mkdir -p "$OUTPUT_DIR"

echo "Checking PostgreSQL tables before Sqoop import."
tables="$(sqoop list-tables --connect "$CONNECT" --username "$PGUSER" --password "$PGPASSWORD")"
for table in "${EXPECTED_TABLES[@]}"; do
  if ! grep -qx "$table" <<<"$tables"; then
    echo "Required PostgreSQL table is missing: $table" >&2
    echo "Run bash scripts/data_storage.sh successfully before HDFS ingestion." >&2
    exit 1
  fi
done

echo "Removing old HDFS warehouse path: $WAREHOUSE_DIR"
hdfs dfs -rm -r -f "$WAREHOUSE_DIR" >/dev/null 2>&1 || true

echo "Importing PostgreSQL tables to HDFS via Sqoop."
sqoop import-all-tables \
  --connect "$CONNECT" \
  --username "$PGUSER" \
  --password "$PGPASSWORD" \
  --compression-codec snappy \
  --compress \
  --as-avrodatafile \
  --warehouse-dir "$WAREHOUSE_DIR" \
  --m 1 \
  --outdir "$OUTPUT_DIR"

echo "Sqoop ingestion completed. Generated Avro schemas are in $OUTPUT_DIR."

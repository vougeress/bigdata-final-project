#!/usr/bin/env bash
set -euo pipefail

: "${TEAM_NAME:=team20}"
: "${HIVE_HOST:=hadoop-03.uni.innopolis.ru}"
: "${HIVE_PORT:=10001}"
: "${HIVE_DATABASE:=${TEAM_NAME}_projectdb}"
: "${HIVE_PASSWORD:=${PGPASSWORD:-}}"

if [[ -z "$HIVE_PASSWORD" ]]; then
  echo "Set HIVE_PASSWORD or PGPASSWORD before running Stage II." >&2
  exit 1
fi

HDFS_AVSC_DIR="/user/${TEAM_NAME}/project/warehouse/avsc"
HDFS_HIVE_WAREHOUSE="/user/${TEAM_NAME}/project/hive/warehouse"
HDFS_OUTPUT_DIR="/user/${TEAM_NAME}/project/output"
HDFS_SCRATCH_DIR="/user/${TEAM_NAME}/tmp/hive"
LOCAL_TMP_DIR="/home/${TEAM_NAME}/tmp/hive"
BEELINE_URL="jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}/default?hive.execution.engine=mr;hive.exec.scratchdir=${HDFS_SCRATCH_DIR};hive.downloaded.resources.dir=${LOCAL_TMP_DIR}/resources;hive.querylog.location=${LOCAL_TMP_DIR};hive.server2.logging.operation.log.location=${LOCAL_TMP_DIR}/operation_logs"
QUERIES=(q1 q2 q3 q4 q5)

mkdir -p output
python3 scripts/generate_stage2_hql.py

mkdir -p .tmp "$LOCAL_TMP_DIR"
export TMPDIR="${TMPDIR:-$LOCAL_TMP_DIR}"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Djava.io.tmpdir=$TMPDIR"

if ! ls output/*.avsc >/dev/null 2>&1; then
  echo "No Avro schema files found in output/. Copy Sqoop *.avsc files from Stage I before running Stage II." >&2
  exit 1
fi

hdfs dfs -mkdir -p "$HDFS_AVSC_DIR" "$HDFS_HIVE_WAREHOUSE" "$HDFS_OUTPUT_DIR" "$HDFS_SCRATCH_DIR"
hdfs dfs -put -f output/*.avsc "$HDFS_AVSC_DIR/"

run_beeline() {
  local hql_file="$1"
  beeline \
    -u "$BEELINE_URL" \
    -n "$TEAM_NAME" \
    -p "$HIVE_PASSWORD" \
    -f "$hql_file"
}

run_beeline sql/stage2_tables.hql

for query in "${QUERIES[@]}"; do
  run_beeline "sql/${query}.hql"
done

declare -A HEADERS
HEADERS[q1]="productcd,total_transactions,fraud_transactions,fraud_rate,avg_amount"
HEADERS[q2]="amount_band,total_transactions,fraud_transactions,fraud_rate,avg_amount"
HEADERS[q3]="card4,card6,total_transactions,fraud_transactions,fraud_rate,avg_amount"
HEADERS[q4]="email_domain,total_transactions,fraud_transactions,fraud_rate,avg_amount"
HEADERS[q5]="transaction_day,total_transactions,fraud_transactions,fraud_rate,total_amount"

for query in "${QUERIES[@]}"; do
  printf '%s\n' "${HEADERS[$query]}" > "output/${query}.csv"
  hdfs dfs -cat "${HDFS_OUTPUT_DIR}/${query}/*" >> "output/${query}.csv"
done

echo "Stage II completed. EDA CSV exports are in output/q1.csv ... output/q5.csv."
echo "Create Superset datasets from q1_results ... q5_results and export charts as output/q1.jpg ... output/q5.jpg."

#!/usr/bin/env bash
set -euo pipefail

: "${TEAM_NAME:=team20}"
export TEAM_NAME

SPARK_MASTER="${SPARK_MASTER:-local[*]}"
LOCAL_TMP="${HOME}/tmp/hive"
HDFS_TMP="/user/${TEAM_NAME}/tmp/hive"

mkdir -p "$LOCAL_TMP"
hdfs dfs -mkdir -p "$HDFS_TMP" 2>/dev/null || true
hdfs dfs -mkdir -p "/user/${TEAM_NAME}/project/output" 2>/dev/null || true

SPARK_CONF=(
    "--conf" "spark.sql.warehouse.dir=/user/${TEAM_NAME}/project/hive/warehouse"
    "--conf" "spark.hadoop.hive.exec.scratchdir=/user/${TEAM_NAME}/tmp/hive/scratch"
    "--conf" "spark.hadoop.hive.querylog.location=${LOCAL_TMP}"
    "--conf" "spark.sql.sources.partitionOverwriteMode=dynamic"
    "--conf" "spark.driver.extraJavaOptions=-Djava.io.tmpdir=${LOCAL_TMP}"
    "--conf" "spark.executor.extraJavaOptions=-Djava.io.tmpdir=${LOCAL_TMP}"
)

echo "Launching Stage II Spark SQL job (master=${SPARK_MASTER})..."
spark-submit \
    --master "${SPARK_MASTER}" \
    "${SPARK_CONF[@]}" \
    scripts/stage2_spark.py

echo "Stage II complete."

#!/usr/bin/env bash
set -euo pipefail

: "${TEAM_NAME:=team20}"
: "${STAGE3_MODE:=full}"
: "${STAGE3_SEED:=20}"
: "${SPARK_MASTER:=yarn}"
: "${SPARK_DEPLOY_MODE:=client}"
: "${PYSPARK_PYTHON:=python3}"
: "${SPARK_DRIVER_MEMORY:=2g}"
: "${SPARK_EXECUTOR_MEMORY:=6g}"
: "${SPARK_EXECUTOR_MEMORY_OVERHEAD:=1536}"
: "${SPARK_EXECUTOR_CORES:=3}"
: "${SPARK_EXECUTOR_INSTANCES:=6}"
: "${SPARK_SQL_SHUFFLE_PARTITIONS:=96}"
: "${SPARK_DEFAULT_PARALLELISM:=24}"
: "${STAGE3_NUMERIC_IMPUTE_STRATEGY:=median}"
: "${STAGE3_IMPUTER_BATCH_SIZE:=32}"
: "${STAGE3_CV_PARALLELISM:=1}"
: "${STAGE3_MODEL_ORDER:=gbt,rf}"
: "${STAGE3_TREE_TRAIN_FRACTION:=0.35}"
: "${STAGE3_BALANCE_TREE_TRAIN:=1}"
: "${STAGE3_TREE_NEG_TO_POS_RATIO:=1.0}"

export TEAM_NAME
export STAGE3_MODE
export STAGE3_SEED
export PYSPARK_PYTHON
export STAGE3_NUMERIC_IMPUTE_STRATEGY
export STAGE3_IMPUTER_BATCH_SIZE
export STAGE3_CV_PARALLELISM
export STAGE3_MODEL_ORDER
export STAGE3_TREE_TRAIN_FRACTION
export STAGE3_BALANCE_TREE_TRAIN
export STAGE3_TREE_NEG_TO_POS_RATIO

LOCAL_TMP="${HOME}/tmp/hive"
HDFS_TMP="/user/${TEAM_NAME}/tmp/hive"
HDFS_OUTPUT="/user/${TEAM_NAME}/project/output/stage3"
HDFS_MODELS="/user/${TEAM_NAME}/project/models/stage3"

mkdir -p output/stage3
mkdir -p "${LOCAL_TMP}"

hdfs dfs -mkdir -p "${HDFS_TMP}" 2>/dev/null || true
hdfs dfs -mkdir -p "${HDFS_OUTPUT}" 2>/dev/null || true
hdfs dfs -mkdir -p "${HDFS_MODELS}" 2>/dev/null || true

SPARK_CONF=(
    "--master" "${SPARK_MASTER}"
    "--deploy-mode" "${SPARK_DEPLOY_MODE}"
    "--driver-memory" "${SPARK_DRIVER_MEMORY}"
    "--executor-memory" "${SPARK_EXECUTOR_MEMORY}"
    "--executor-cores" "${SPARK_EXECUTOR_CORES}"
    "--num-executors" "${SPARK_EXECUTOR_INSTANCES}"
    "--conf" "spark.sql.warehouse.dir=/user/${TEAM_NAME}/project/hive/warehouse"
    "--conf" "spark.hadoop.hive.exec.scratchdir=/user/${TEAM_NAME}/tmp/hive/scratch"
    "--conf" "spark.sql.sources.partitionOverwriteMode=dynamic"
    "--conf" "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}"
    "--conf" "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}"
    "--conf" "spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD}"
    "--conf" "spark.driver.extraJavaOptions=-Djava.io.tmpdir=${LOCAL_TMP}"
    "--conf" "spark.executor.extraJavaOptions=-Djava.io.tmpdir=${LOCAL_TMP}"
    "--conf" "spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}"
    "--conf" "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}"
    "--conf" "spark.yarn.appMasterEnv.TEAM_NAME=${TEAM_NAME}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_MODE=${STAGE3_MODE}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_SEED=${STAGE3_SEED}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_NUMERIC_IMPUTE_STRATEGY=${STAGE3_NUMERIC_IMPUTE_STRATEGY}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_IMPUTER_BATCH_SIZE=${STAGE3_IMPUTER_BATCH_SIZE}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_CV_PARALLELISM=${STAGE3_CV_PARALLELISM}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_MODEL_ORDER=${STAGE3_MODEL_ORDER}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_TREE_TRAIN_FRACTION=${STAGE3_TREE_TRAIN_FRACTION}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_BALANCE_TREE_TRAIN=${STAGE3_BALANCE_TREE_TRAIN}"
    "--conf" "spark.yarn.appMasterEnv.STAGE3_TREE_NEG_TO_POS_RATIO=${STAGE3_TREE_NEG_TO_POS_RATIO}"
)

echo "Launching Stage III Spark ML job (mode=${STAGE3_MODE}, master=${SPARK_MASTER}, models=${STAGE3_MODEL_ORDER})..."
echo "Spark resources: driver=${SPARK_DRIVER_MEMORY}, executors=${SPARK_EXECUTOR_INSTANCES} x ${SPARK_EXECUTOR_CORES} cores, executor_memory=${SPARK_EXECUTOR_MEMORY}, overhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD}"
spark-submit \
    "${SPARK_CONF[@]}" \
    scripts/stage3_train.py

echo "Stage III complete."

#!/usr/bin/env bash
set -euo pipefail

: "${TEAM_NAME:=team20}"
: "${SPARK_MASTER:=local[*]}"

python3 scripts/generate_stage2_hql.py
spark-submit \
  --master "$SPARK_MASTER" \
  --conf spark.sql.catalogImplementation=hive \
  --conf hive.metastore.uris=thrift://hadoop-02.uni.innopolis.ru:9883 \
  scripts/stage2_spark.py

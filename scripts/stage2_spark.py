#!/usr/bin/env python3
# pylint: disable=C0301
"""Stage II: create Hive tables and run EDA queries using Spark SQL."""

from __future__ import print_function

import os
import subprocess
import sys

TEAM = os.environ.get("TEAM_NAME", "team20")
HDFS_USER_DIR = "/user/{}/project".format(TEAM)
WAREHOUSE_DIR = "{}/warehouse".format(HDFS_USER_DIR)
AVSC_DIR = "{}/avsc".format(WAREHOUSE_DIR)
HIVE_WAREHOUSE = "{}/hive/warehouse".format(HDFS_USER_DIR)
OUTPUT_HDFS = "{}/output".format(HDFS_USER_DIR)

HIVE_DB = "{}_projectdb".format(TEAM)
LOCAL_OUTPUT = os.path.join("output")
SQL_DIR = os.path.join("sql")
TMP_DIR = os.path.join(os.path.expanduser("~"), "tmp", "hive")

EDA_QUERIES = ["q1", "q2", "q3", "q4", "q5"]


def hdfs(args):
    """Run an hdfs dfs command."""
    cmd = ["hdfs", "dfs"] + args
    ret = subprocess.call(cmd)
    if ret != 0:
        print("WARNING: hdfs {} returned {}".format(" ".join(args), ret))
    return ret


def setup_tmp_dirs():
    """Create local and HDFS temp directories to avoid /tmp permission issues."""
    os.makedirs(TMP_DIR, exist_ok=True)
    hdfs(["-mkdir", "-p", "/user/{}/tmp/hive".format(TEAM)])


def upload_avsc_files():
    """Upload Sqoop-generated .avsc files from local output/ to HDFS avsc/."""
    hdfs(["-mkdir", "-p", AVSC_DIR])
    avsc_files = [
        f for f in os.listdir(LOCAL_OUTPUT)
        if f.endswith(".avsc")
    ]
    if not avsc_files:
        print("WARNING: No .avsc files found in {}. "
              "Sqoop must be run first.".format(LOCAL_OUTPUT))
        return
    for fname in avsc_files:
        local_path = os.path.join(LOCAL_OUTPUT, fname)
        hdfs_path = "{}/{}".format(AVSC_DIR, fname)
        hdfs(["-put", "-f", local_path, hdfs_path])
        print("Uploaded {} -> {}".format(local_path, hdfs_path))


def build_spark_session():
    """Build a SparkSession with Hive support."""
    try:
        from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
    except ImportError:
        print("ERROR: pyspark not available. Run via spark-submit.")
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("Stage2_{}".format(HIVE_DB))
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE)
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
        .config("spark.hadoop.hive.exec.scratchdir",
                "/user/{}/tmp/hive/scratch".format(TEAM))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_hql_file(spark, hql_path):
    """Execute all statements in a .hql file, skipping comments and SET commands."""
    with open(hql_path) as fobj:
        content = fobj.read()

    statements = []
    current = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            current = []

    for stmt in statements:
        upper = stmt.upper().lstrip()
        if upper.startswith("SET "):
            key_val = stmt[4:].strip()
            if "=" in key_val:
                key, val = key_val.split("=", 1)
                spark.conf.set(key.strip(), val.strip())
                print("SET {} = {}".format(key.strip(), val.strip()))
        else:
            first_line = stmt.splitlines()[0] if stmt else ""
            print("Executing: {}...".format(first_line[:80]))
            spark.sql(stmt)


def run_eda_query(spark, query_name):
    """Run an EDA query HQL (DROP+CTAS+SELECT*), then export the results table to CSV."""
    hql_path = os.path.join(SQL_DIR, "{}.hql".format(query_name))
    result_table = "{}_results".format(query_name)
    print("Running EDA query: {} -> {}.{}".format(query_name, HIVE_DB, result_table))

    run_hql_file(spark, hql_path)

    hdfs_csv_path = "{}/{}.csv_dir".format(OUTPUT_HDFS, query_name)
    hdfs(["-rm", "-r", "-f", hdfs_csv_path])
    result_df = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, result_table))
    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(hdfs_csv_path)

    local_csv = os.path.join(LOCAL_OUTPUT, "{}.csv".format(query_name))
    hdfs(["-getmerge", hdfs_csv_path, local_csv])
    print("  CSV exported to {}".format(local_csv))


def main():
    os.makedirs(LOCAL_OUTPUT, exist_ok=True)

    setup_tmp_dirs()
    upload_avsc_files()

    spark = build_spark_session()

    tables_hql = os.path.join(SQL_DIR, "stage2_tables.hql")
    print("Running {}".format(tables_hql))
    run_hql_file(spark, tables_hql)

    spark.sql("USE {}".format(HIVE_DB))

    for qname in EDA_QUERIES:
        run_eda_query(spark, qname)

    print("Verifying table row counts...")
    spark.sql("USE {}".format(HIVE_DB))
    for tname in ["train_transaction_pb"] + ["{}_results".format(q) for q in EDA_QUERIES]:
        try:
            count = spark.sql("SELECT COUNT(*) FROM {}".format(tname)).collect()[0][0]
            print("  {}: {} rows".format(tname, count))
        except Exception as exc:  # pylint: disable=broad-except
            print("  WARNING: could not count {}: {}".format(tname, exc))

    print("Stage II Spark SQL execution complete.")
    spark.stop()


if __name__ == "__main__":
    main()

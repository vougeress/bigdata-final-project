#!/usr/bin/env python3
"""Run Stage II with Spark SQL and the Hive metastore, bypassing HiveServer2."""

import glob
import os
import shutil
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession


TEAM_NAME = os.getenv("TEAM_NAME", "team20")
DATABASE = os.getenv("HIVE_DATABASE", "%s_projectdb" % TEAM_NAME)
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
WAREHOUSE = "/user/%s/project/hive/warehouse" % TEAM_NAME
LOCAL_OUTPUT = Path("output")
HDFS_OUTPUT = "/user/%s/project/output" % TEAM_NAME


QUERIES = ["q1", "q2", "q3", "q4", "q5"]
HEADERS = {
    "q1": "productcd,total_transactions,fraud_transactions,fraud_rate,avg_amount",
    "q2": "amount_band,total_transactions,fraud_transactions,fraud_rate,avg_amount",
    "q3": "card4,card6,total_transactions,fraud_transactions,fraud_rate,avg_amount",
    "q4": "email_domain,total_transactions,fraud_transactions,fraud_rate,avg_amount",
    "q5": "transaction_day,total_transactions,fraud_transactions,fraud_rate,total_amount",
}


def hdfs(*args):
    """Run an HDFS command."""
    subprocess.check_call(["hdfs", "dfs"] + list(args))


def split_sql_file(path):
    """Split a simple semicolon-delimited HQL file into statements."""
    text = Path(path).read_text(encoding="utf-8")
    statements = []
    current = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            current = []
    if current:
        statement = "\n".join(current).strip().rstrip(";").strip()
        if statement:
            statements.append(statement)
    return statements


def run_sql_file(spark, path):
    """Execute all SQL statements in a generated HQL file."""
    for statement in split_sql_file(path):
        if statement.upper().startswith("INSERT OVERWRITE DIRECTORY"):
            continue
        print("Running SQL:", statement.splitlines()[0])
        spark.sql(statement)


def export_result(spark, query):
    """Export a Hive result table to HDFS and a single local CSV."""
    result_table = "%s_results" % query
    spark.table(result_table).coalesce(1).write.mode("overwrite").option("delimiter", ",").csv(
        "%s/%s" % (HDFS_OUTPUT, query)
    )

    output_dir = LOCAL_OUTPUT / ("%s_parts" % query)
    if output_dir.exists():
        shutil.rmtree(str(output_dir))
    hdfs("-get", "%s/%s" % (HDFS_OUTPUT, query), str(output_dir))

    csv_path = LOCAL_OUTPUT / ("%s.csv" % query)
    with csv_path.open("w", encoding="utf-8") as file_obj:
        file_obj.write(HEADERS[query] + "\n")
        for part_path in sorted(glob.glob(str(output_dir / "part-*"))):
            with open(part_path, encoding="utf-8") as part_file:
                shutil.copyfileobj(part_file, file_obj)


def main():
    """Run Stage II Spark SQL pipeline."""
    LOCAL_OUTPUT.mkdir(exist_ok=True)
    hdfs("-mkdir", "-p", "/user/%s/project/warehouse/avsc" % TEAM_NAME, WAREHOUSE, HDFS_OUTPUT)
    avsc_files = glob.glob("output/*.avsc")
    if not avsc_files:
        raise RuntimeError("No Avro schema files found in output/. Run Stage I Sqoop import first.")
    hdfs("-put", "-f", *(avsc_files + ["/user/%s/project/warehouse/avsc/" % TEAM_NAME]))

    spark = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName("Stage II Spark SQL")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS `%s` LOCATION '%s/%s.db'" % (DATABASE, WAREHOUSE, DATABASE))
    spark.sql("USE `%s`" % DATABASE)

    run_sql_file(spark, "sql/stage2_tables.hql")
    for query in QUERIES:
        run_sql_file(spark, "sql/%s.hql" % query)
        export_result(spark, query)

    spark.stop()
    print("Stage II Spark SQL completed.")


if __name__ == "__main__":
    main()

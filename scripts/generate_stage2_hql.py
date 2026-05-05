#!/usr/bin/env python3
# pylint: disable=too-many-locals,too-many-statements,line-too-long,consider-using-f-string,too-few-public-methods,too-many-branches
"""Generate Stage II HiveQL files from project CSV headers."""

import csv
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path


DATA_DIR = Path("data")
SQL_DIR = Path("sql")
SAMPLE_ROWS = int(os.getenv("SCHEMA_SAMPLE_ROWS", "5000"))
TEAM_NAME = os.getenv("TEAM_NAME", "team20")
DATABASE = os.getenv("HIVE_DATABASE", "%s_projectdb" % TEAM_NAME)
HDFS_USER_ROOT = "/user/%s" % TEAM_NAME
HDFS_WAREHOUSE = "%s/project/warehouse" % HDFS_USER_ROOT
HDFS_HIVE_WAREHOUSE = "%s/project/hive/warehouse" % HDFS_USER_ROOT
HDFS_OUTPUT = "%s/project/output" % HDFS_USER_ROOT
AVSC_DIR = "%s/project/warehouse/avsc" % HDFS_USER_ROOT


class TableSpec:
    """A Stage I table imported to HDFS."""

    def __init__(self, name, csv_file):
        self.name = name
        self.csv_file = csv_file


TABLES = [
    TableSpec("train_transaction", "train_transaction.csv"),
    TableSpec("test_transaction", "test_transaction.csv"),
    TableSpec("train_identity", "train_identity.csv"),
    TableSpec("test_identity", "test_identity.csv"),
]


def read_headers(path):
    """Return CSV headers from a data file."""
    with path.open(newline="", encoding="utf-8") as file_obj:
        return next(csv.reader(file_obj))


def infer_types(path, headers):
    """Infer simple Hive types from sampled CSV values."""
    seen_values = {header: 0 for header in headers}
    can_bigint = {header: True for header in headers}
    can_double = {header: True for header in headers}

    with path.open(newline="", encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj)
        for row_index, row in enumerate(reader):
            if row_index >= SAMPLE_ROWS:
                break
            for header in headers:
                value = (row.get(header) or "").strip()
                if not value:
                    continue
                seen_values[header] += 1
                try:
                    int(value)
                except ValueError:
                    can_bigint[header] = False
                try:
                    Decimal(value)
                except InvalidOperation:
                    can_double[header] = False

    hive_types = {}
    for header in headers:
        if header == "TransactionID":
            hive_types[header] = "BIGINT"
        elif header == "isFraud":
            hive_types[header] = "SMALLINT"
        elif seen_values[header] == 0:
            hive_types[header] = "STRING"
        elif can_bigint[header]:
            hive_types[header] = "BIGINT"
        elif can_double[header]:
            hive_types[header] = "DOUBLE"
        else:
            hive_types[header] = "STRING"
    return hive_types


def hive_ident(name):
    """Quote a Hive identifier."""
    return "`%s`" % name.replace("`", "``")


def hive_col(header):
    """Return the normalized Hive column name."""
    return header.lower()


def ddl_columns(headers, hive_types, skip=None):
    """Render Hive column definitions."""
    skip = set(skip or [])
    return ",\n    ".join(
        "%s %s" % (hive_ident(hive_col(header)), hive_types[header])
        for header in headers
        if header not in skip
    )


def select_cast_columns(headers, hive_types, skip=None):
    """Render INSERT SELECT expressions with type casts."""
    skip = set(skip or [])
    expressions = []
    for header in headers:
        if header in skip:
            continue
        expressions.append(
            "CAST(%s AS %s) AS %s" % (
                hive_ident(hive_col(header)),
                hive_types[header],
                hive_ident(hive_col(header)),
            )
        )
    return ",\n    ".join(expressions)


def stage2_tables_hql(headers, hive_types):
    """Build HQL for Hive database, raw Avro tables, optimized table, and checks."""
    lines = [
        "CREATE DATABASE IF NOT EXISTS %s" % hive_ident(DATABASE),
        "LOCATION '%s/%s.db';" % (HDFS_HIVE_WAREHOUSE, DATABASE),
        "",
        "USE %s;" % hive_ident(DATABASE),
        "SET hive.execution.engine=mr;",
        "SET hive.exec.dynamic.partition=true;",
        "SET hive.exec.dynamic.partition.mode=nonstrict;",
        "SET hive.enforce.bucketing=true;",
        "SET hive.vectorized.execution.enabled=false;",
        "",
    ]

    for table in TABLES:
        lines.extend([
            "DROP TABLE IF EXISTS %s;" % hive_ident(table.name),
            "CREATE EXTERNAL TABLE %s (" % hive_ident(table.name),
            "    %s" % ddl_columns(headers[table.name], hive_types[table.name]),
            ")",
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'",
            "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'",
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'",
            "LOCATION '%s/%s'" % (HDFS_WAREHOUSE, table.name),
            "TBLPROPERTIES (",
            "    'avro.schema.url'='hdfs://%s/%s.avsc'",
            ");",
            "DESCRIBE %s;" % hive_ident(table.name),
            "SELECT COUNT(*) AS %s_rows FROM %s;" % (table.name, hive_ident(table.name)),
            "",
        ])
        lines[-5] = lines[-5] % (AVSC_DIR, table.name)

    train_headers = headers["train_transaction"]
    train_types = hive_types["train_transaction"]
    lines.extend([
        "DROP TABLE IF EXISTS train_transaction_pb;",
        "CREATE EXTERNAL TABLE train_transaction_pb (",
        "    %s" % ddl_columns(train_headers, train_types, skip=["isFraud"]),
        ")",
        "PARTITIONED BY (is_fraud SMALLINT)",
        "CLUSTERED BY (`transactionid`) INTO 8 BUCKETS",
        "STORED AS ORC",
        "LOCATION '%s/train_transaction_pb'" % HDFS_HIVE_WAREHOUSE,
        "TBLPROPERTIES ('orc.compress'='SNAPPY');",
        "",
        "INSERT OVERWRITE TABLE train_transaction_pb PARTITION (is_fraud)",
        "SELECT",
        "    %s," % select_cast_columns(train_headers, train_types, skip=["isFraud"]),
        "    CAST(`isfraud` AS SMALLINT) AS is_fraud",
        "FROM train_transaction;",
        "",
        "SELECT COUNT(*) AS train_transaction_pb_rows FROM train_transaction_pb;",
        "SHOW PARTITIONS train_transaction_pb;",
        "",
    ])

    for table in TABLES:
        lines.append("DROP TABLE IF EXISTS %s;" % hive_ident(table.name))

    lines.extend([
        "",
        "SHOW TABLES;",
        "",
    ])
    return "\n".join(lines)


def result_table(name, columns, select_sql):
    """Build an EDA HQL file with result table and HDFS CSV export."""
    ddl = ",\n    ".join("%s %s" % (column, col_type) for column, col_type in columns)
    return "\n".join([
        "USE %s;" % hive_ident(DATABASE),
        "SET hive.execution.engine=mr;",
        "SET hive.resultset.use.unique.column.names=false;",
        "",
        "DROP TABLE IF EXISTS %s_results;" % name,
        "CREATE TABLE %s_results (" % name,
        "    %s" % ddl,
        ")",
        "STORED AS ORC;",
        "",
        "INSERT OVERWRITE TABLE %s_results" % name,
        select_sql.rstrip() + ";",
        "",
        "INSERT OVERWRITE DIRECTORY '%s/%s'" % (HDFS_OUTPUT, name),
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','",
        "SELECT * FROM %s_results;" % name,
        "",
    ])


def write_eda_queries():
    """Write five project-aligned EDA queries."""
    queries = {
        "q1": result_table(
            "q1",
            [
                ("productcd", "STRING"),
                ("total_transactions", "BIGINT"),
                ("fraud_transactions", "BIGINT"),
                ("fraud_rate", "DOUBLE"),
                ("avg_amount", "DOUBLE"),
            ],
            """
SELECT
    `productcd` AS productcd,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY `productcd`
ORDER BY fraud_rate DESC
            """,
        ),
        "q2": result_table(
            "q2",
            [
                ("amount_band", "STRING"),
                ("total_transactions", "BIGINT"),
                ("fraud_transactions", "BIGINT"),
                ("fraud_rate", "DOUBLE"),
                ("avg_amount", "DOUBLE"),
            ],
            """
SELECT
    CASE
        WHEN `transactionamt` < 25 THEN '00_under_25'
        WHEN `transactionamt` < 100 THEN '01_25_99'
        WHEN `transactionamt` < 250 THEN '02_100_249'
        WHEN `transactionamt` < 500 THEN '03_250_499'
        ELSE '04_500_plus'
    END AS amount_band,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY
    CASE
        WHEN `transactionamt` < 25 THEN '00_under_25'
        WHEN `transactionamt` < 100 THEN '01_25_99'
        WHEN `transactionamt` < 250 THEN '02_100_249'
        WHEN `transactionamt` < 500 THEN '03_250_499'
        ELSE '04_500_plus'
    END
ORDER BY amount_band
            """,
        ),
        "q3": result_table(
            "q3",
            [
                ("card4", "STRING"),
                ("card6", "STRING"),
                ("total_transactions", "BIGINT"),
                ("fraud_transactions", "BIGINT"),
                ("fraud_rate", "DOUBLE"),
                ("avg_amount", "DOUBLE"),
            ],
            """
SELECT
    COALESCE(`card4`, 'unknown') AS card4,
    COALESCE(`card6`, 'unknown') AS card6,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY COALESCE(`card4`, 'unknown'), COALESCE(`card6`, 'unknown')
ORDER BY fraud_rate DESC
            """,
        ),
        "q4": result_table(
            "q4",
            [
                ("email_domain", "STRING"),
                ("total_transactions", "BIGINT"),
                ("fraud_transactions", "BIGINT"),
                ("fraud_rate", "DOUBLE"),
                ("avg_amount", "DOUBLE"),
            ],
            """
SELECT
    COALESCE(`p_emaildomain`, 'unknown') AS email_domain,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY COALESCE(`p_emaildomain`, 'unknown')
HAVING COUNT(*) >= 100
ORDER BY fraud_rate DESC
LIMIT 25
            """,
        ),
        "q5": result_table(
            "q5",
            [
                ("transaction_day", "BIGINT"),
                ("total_transactions", "BIGINT"),
                ("fraud_transactions", "BIGINT"),
                ("fraud_rate", "DOUBLE"),
                ("total_amount", "DOUBLE"),
            ],
            """
SELECT
    CAST(FLOOR(`transactiondt` / 86400) AS BIGINT) AS transaction_day,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    SUM(`transactionamt`) AS total_amount
FROM train_transaction_pb
GROUP BY CAST(FLOOR(`transactiondt` / 86400) AS BIGINT)
ORDER BY transaction_day
            """,
        ),
    }
    for name, content in queries.items():
        (SQL_DIR / ("%s.hql" % name)).write_text(content, encoding="utf-8")


def main():
    """Generate Stage II HQL files."""
    SQL_DIR.mkdir(exist_ok=True)
    headers = {}
    hive_types = {}
    for table in TABLES:
        path = DATA_DIR / table.csv_file
        headers[table.name] = read_headers(path)
        hive_types[table.name] = infer_types(path, headers[table.name])

    (SQL_DIR / "stage2_tables.hql").write_text(
        stage2_tables_hql(headers, hive_types),
        encoding="utf-8",
    )
    write_eda_queries()
    print("Generated Stage II HiveQL files in sql/.")


if __name__ == "__main__":
    main()

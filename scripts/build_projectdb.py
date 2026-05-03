#!/usr/bin/env python3
"""Create PostgreSQL tables from project CSV files and load them reproducibly."""

import csv
import os
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import sql


DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
GENERATED_SQL = OUTPUT_DIR / "generated_create_tables.sql"
SAMPLE_ROWS = int(os.getenv("SCHEMA_SAMPLE_ROWS", "5000"))


class TableSpec:
    def __init__(self, name, csv_file, parent=None):
        self.name = name
        self.csv_file = csv_file
        self.parent = parent


TABLES = [
    TableSpec("train_transaction", "train_transaction.csv"),
    TableSpec("test_transaction", "test_transaction.csv"),
    TableSpec("train_identity", "train_identity.csv", "train_transaction"),
    TableSpec("test_identity", "test_identity.csv", "test_transaction"),
]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def validate_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def csv_headers(path):
    if not path.exists() or path.stat().st_size == 0:
        raise FileNotFoundError(f"CSV file is missing or empty: {path}")
    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        try:
            headers = next(reader)
        except StopIteration as exc:
            raise ValueError(f"CSV file has no header: {path}") from exc
    if not headers:
        raise ValueError(f"CSV header is empty: {path}")
    if len(headers) != len(set(headers)):
        raise ValueError(f"CSV header contains duplicate columns: {path}")
    return headers


def infer_column_types(path, headers):
    seen_values = {header: 0 for header in headers}
    can_bigint = {header: True for header in headers}
    can_numeric = {header: True for header in headers}

    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row_index, row in enumerate(reader):
            if row_index >= SAMPLE_ROWS:
                break
            for header in headers:
                value = (row.get(header) or "").strip()
                if value == "":
                    continue
                seen_values[header] += 1
                try:
                    int(value)
                except ValueError:
                    can_bigint[header] = False
                try:
                    Decimal(value)
                except InvalidOperation:
                    can_numeric[header] = False

    types = {}
    for header in headers:
        if header == "TransactionID":
            types[header] = "BIGINT"
        elif header == "isFraud" and path.name == "train_transaction.csv":
            types[header] = "SMALLINT"
        elif seen_values[header] == 0:
            types[header] = "TEXT"
        elif can_bigint[header]:
            types[header] = "BIGINT"
        elif can_numeric[header]:
            types[header] = "NUMERIC"
        else:
            types[header] = "TEXT"
    return types


def create_table_statement(spec, headers, types):
    validate_identifier(spec.name)
    lines = []
    for header in headers:
        column_type = types[header]
        nullable = " NOT NULL" if header == "TransactionID" else ""
        lines.append(f"    {quote_ident(header)} {column_type}{nullable}")
    if "TransactionID" in headers:
        lines.append(f"    CONSTRAINT {quote_ident(spec.name + '_pk')} PRIMARY KEY ({quote_ident('TransactionID')})")
        if spec.parent:
            lines.append(
                "    CONSTRAINT "
                + quote_ident(spec.name + "_transaction_fk")
                + " FOREIGN KEY "
                + f"({quote_ident('TransactionID')}) REFERENCES {quote_ident(spec.parent)} ({quote_ident('TransactionID')})"
            )
    return f"CREATE TABLE {quote_ident(spec.name)} (\n" + ",\n".join(lines) + "\n);"


def build_schema():
    schema = {}
    for spec in TABLES:
        path = DATA_DIR / spec.csv_file
        headers = csv_headers(path)
        types = infer_column_types(path, headers)
        schema[spec.name] = (headers, types, create_table_statement(spec, headers, types))
    return schema


def write_generated_sql(schema):
    OUTPUT_DIR.mkdir(exist_ok=True)
    drop_order = [spec.name for spec in reversed(TABLES)]
    statements = ["BEGIN;"]
    for table in drop_order:
        statements.append(f"DROP TABLE IF EXISTS {quote_ident(table)} CASCADE;")
    for spec in TABLES:
        statements.append(schema[spec.name][2])
    statements.append("COMMIT;")
    GENERATED_SQL.write_text("\n\n".join(statements) + "\n", encoding="utf-8")


def connect():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "hadoop-04.uni.innopolis.ru"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", f"{os.getenv('TEAM_NAME', 'team20')}_projectdb"),
        user=os.getenv("PGUSER", os.getenv("TEAM_NAME", "team20")),
        password=os.getenv("PGPASSWORD"),
    )


def create_tables(cur, schema):
    for spec in reversed(TABLES):
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(spec.name)))
    for spec in TABLES:
        cur.execute(schema[spec.name][2])
    if os.getenv("ENABLE_CITUS", "0") == "1":
        for spec in TABLES:
            cur.execute("SELECT create_distributed_table(%s, %s)", (spec.name, "TransactionID"))


def copy_table(cur, spec, headers):
    path = DATA_DIR / spec.csv_file
    columns = sql.SQL(", ").join(sql.Identifier(header) for header in headers)
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')").format(
        sql.Identifier(spec.name),
        columns,
    )
    with path.open("r", encoding="utf-8", newline="") as file:
        cur.copy_expert(copy_sql.as_string(cur), file)


def test_counts(cur) -> None:
    for spec in TABLES:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(spec.name)))
        count = cur.fetchone()[0]
        print(f"{spec.name}: {count} rows")


def main() -> None:
    schema = build_schema()
    write_generated_sql(schema)

    if "--generate-only" in sys.argv:
        print(f"Generated SQL written to {GENERATED_SQL}")
        return

    with connect() as conn:
        with conn.cursor() as cur:
            create_tables(cur, schema)
            for spec in TABLES:
                headers = schema[spec.name][0]
                copy_table(cur, spec, headers)
            test_counts(cur)

    print(f"Generated SQL written to {GENERATED_SQL}")


if __name__ == "__main__":
    main()

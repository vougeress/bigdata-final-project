#!/usr/bin/env python3
"""Load Stage IV dashboard datasets into PostgreSQL for Apache Superset."""

from __future__ import print_function

import csv
import os
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2 import sql


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TABLES = [
    ("stage4_q1_product_risk", "output/q1.csv"),
    ("stage4_q2_amount_band_risk", "output/q2.csv"),
    ("stage4_q3_card_risk", "output/q3.csv"),
    ("stage4_q4_email_domain_risk", "output/q4.csv"),
    ("stage4_q5_daily_risk", "output/q5.csv"),
    ("stage4_model_evaluation", "output/evaluation.csv"),
    ("stage4_feature_importance_gbt", "output/stage3/feature_importance_gbt.csv"),
    ("stage4_feature_importance_rf", "output/stage3/feature_importance_rf.csv"),
    ("stage4_feature_profile", "output/stage3/feature_profile.csv"),
    ("stage4_model_metrics_long", "output/stage4/model_metrics_long.csv"),
    ("stage4_best_model_summary", "output/stage4/best_model_summary.csv"),
    ("stage4_feature_profile_summary", "output/stage4/feature_profile_summary.csv"),
]


def abs_path(relative_path):
    """Return an absolute repository path."""
    return os.path.join(ROOT, relative_path)


def connect():
    """Connect to the project PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "hadoop-04.uni.innopolis.ru"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv(
            "PGDATABASE",
            "{}_projectdb".format(os.getenv("TEAM_NAME", "team20")),
        ),
        user=os.getenv("PGUSER", os.getenv("TEAM_NAME", "team20")),
        password=os.getenv("PGPASSWORD"),
    )


def read_rows(relative_path):
    """Read a CSV artifact with the same escaping used by Stage III exports."""
    path = abs_path(relative_path)
    if not os.path.isfile(path):
        raise RuntimeError("Missing required CSV: {}".format(relative_path))
    with open(path, "r") as handle:
        reader = csv.DictReader(handle, escapechar="\\")
        rows = list(reader)
        if not reader.fieldnames:
            raise RuntimeError("CSV has no header: {}".format(relative_path))
        if not rows:
            raise RuntimeError("CSV has no rows: {}".format(relative_path))
        return reader.fieldnames, rows


def is_decimal(value):
    """Return True if a non-empty value can be stored as NUMERIC."""
    try:
        Decimal(value)
    except InvalidOperation:
        return False
    return True


def infer_types(headers, rows):
    """Infer conservative PostgreSQL column types from CSV values."""
    types = {}
    for header in headers:
        values = [
            row.get(header)
            for row in rows
            if row.get(header) not in ("", None)
        ]
        if not values:
            types[header] = "TEXT"
        elif header == "included" and set(values) <= set(["True", "False"]):
            types[header] = "BOOLEAN"
        elif all(is_decimal(value) for value in values):
            types[header] = "NUMERIC"
        else:
            types[header] = "TEXT"
    return types


def convert_value(value):
    """Normalize empty strings to NULL for database inserts."""
    if value == "":
        return None
    return value


def create_and_load_table(cursor, table_name, relative_path):
    """Drop, create, and load one PostgreSQL dashboard table."""
    headers, rows = read_rows(relative_path)
    types = infer_types(headers, rows)
    column_defs = [
        sql.SQL("{} {}").format(sql.Identifier(header), sql.SQL(types[header]))
        for header in headers
    ]
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
    cursor.execute(
        sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(column_defs),
        )
    )
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(header) for header in headers),
        sql.SQL(", ").join(sql.Placeholder() for _ in headers),
    )
    values = [
        [convert_value(row.get(header)) for header in headers]
        for row in rows
    ]
    cursor.executemany(insert_sql, values)
    print("{}: {} rows".format(table_name, len(rows)))


def main():
    """Load all Stage IV dashboard tables into PostgreSQL."""
    with connect() as connection:
        with connection.cursor() as cursor:
            for table_name, relative_path in TABLES:
                create_and_load_table(cursor, table_name, relative_path)
    print("OK: Stage IV dashboard tables loaded into PostgreSQL.")


if __name__ == "__main__":
    main()

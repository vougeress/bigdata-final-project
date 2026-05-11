#!/usr/bin/env python3
"""Build a Superset-ready SQLite database from Stage II/III CSV artifacts."""

from __future__ import print_function

import csv
import json
import os
import sqlite3


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(ROOT, "output")
STAGE4_DIR = os.path.join(OUTPUT_DIR, "stage4")
SQLITE_PATH = os.path.join(STAGE4_DIR, "superset_dashboard.sqlite")

SOURCE_TABLES = [
    ("q1_product_risk", "output/q1.csv"),
    ("q2_amount_band_risk", "output/q2.csv"),
    ("q3_card_risk", "output/q3.csv"),
    ("q4_email_domain_risk", "output/q4.csv"),
    ("q5_daily_risk", "output/q5.csv"),
    ("model_evaluation", "output/evaluation.csv"),
    ("feature_importance_gbt", "output/stage3/feature_importance_gbt.csv"),
    ("feature_importance_rf", "output/stage3/feature_importance_rf.csv"),
    ("feature_profile", "output/stage3/feature_profile.csv"),
]

NUMERIC_SUFFIXES = (
    "_rate",
    "_amount",
    "_transactions",
    "_importance",
    "_ratio",
    "_coverage",
    "_non_null",
)

NUMERIC_COLUMNS = set([
    "fraud_rate",
    "avg_amount",
    "total_amount",
    "total_transactions",
    "fraud_transactions",
    "transaction_day",
    "importance",
    "cv_metric_pr",
    "test_accuracy",
    "test_auc_pr",
    "test_auc_roc",
    "test_f1",
    "null_ratio",
    "distinct_non_null",
    "dominant_ratio",
    "top_category_coverage",
])


def abs_path(relative_path):
    """Return an absolute path under the repository root."""
    return os.path.join(ROOT, relative_path)


def fail(message):
    """Raise one formatted failure."""
    raise RuntimeError(message)


def is_numeric_column(column):
    """Return True if a column should be stored as a SQLite numeric value."""
    lower = column.lower()
    if lower in NUMERIC_COLUMNS:
        return True
    return lower.endswith(NUMERIC_SUFFIXES)


def sqlite_type(column):
    """Infer a dashboard-friendly SQLite type from the column name."""
    if is_numeric_column(column):
        return "REAL"
    if column.lower() == "included":
        return "INTEGER"
    return "TEXT"


def read_csv(relative_path):
    """Read a CSV artifact as dictionaries."""
    path = abs_path(relative_path)
    if not os.path.isfile(path):
        fail("Missing required CSV: {}".format(relative_path))
    with open(path, "r") as handle:
        reader = csv.DictReader(handle, escapechar="\\")
        rows = list(reader)
        if not reader.fieldnames:
            fail("CSV has no header: {}".format(relative_path))
        if not rows:
            fail("CSV has no rows: {}".format(relative_path))
        return reader.fieldnames, rows


def convert_value(column, value):
    """Convert one CSV cell to a SQLite value."""
    if value is None or value == "":
        return None
    if column.lower() == "included":
        return 1 if value == "True" else 0
    if is_numeric_column(column):
        return float(value)
    return value


def create_table(connection, table_name, columns, rows):
    """Create and populate one SQLite table."""
    connection.execute('DROP TABLE IF EXISTS "{}"'.format(table_name))
    column_sql = ", ".join(
        ['"{}" {}'.format(column, sqlite_type(column)) for column in columns]
    )
    connection.execute('CREATE TABLE "{}" ({})'.format(table_name, column_sql))
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = 'INSERT INTO "{}" VALUES ({})'.format(table_name, placeholders)
    values = [
        [convert_value(column, row.get(column)) for column in columns]
        for row in rows
    ]
    connection.executemany(insert_sql, values)


def create_model_metrics_long(connection):
    """Create long-form model metrics for grouped Superset bar charts."""
    _, rows = read_csv("output/evaluation.csv")
    connection.execute("DROP TABLE IF EXISTS model_metrics_long")
    connection.execute(
        """
        CREATE TABLE model_metrics_long (
            model_name TEXT,
            metric_name TEXT,
            metric_value REAL
        )
        """
    )
    metrics = ["test_auc_pr", "test_auc_roc", "test_f1", "test_accuracy"]
    values = []
    for row in rows:
        for metric in metrics:
            values.append((row["model_name"], metric, float(row[metric])))
    connection.executemany(
        "INSERT INTO model_metrics_long VALUES (?, ?, ?)",
        values,
    )
    write_rows(
        "model_metrics_long.csv",
        ["model_name", "metric_name", "metric_value"],
        values,
    )


def create_best_model_summary(connection):
    """Create a one-row best-model table for a Big Number or markdown tile."""
    _, rows = read_csv("output/evaluation.csv")
    best = max(rows, key=lambda row: float(row["test_auc_pr"]))
    connection.execute("DROP TABLE IF EXISTS best_model_summary")
    connection.execute(
        """
        CREATE TABLE best_model_summary (
            model_name TEXT,
            best_metric_name TEXT,
            best_metric_value REAL,
            display_text TEXT
        )
        """
    )
    display_text = "Best model: {0}; test AUC-PR: {1:.3f}".format(
        best["model_name"],
        float(best["test_auc_pr"]),
    )
    connection.execute(
        "INSERT INTO best_model_summary VALUES (?, ?, ?, ?)",
        (best["model_name"], "test_auc_pr", float(best["test_auc_pr"]), display_text),
    )
    write_rows(
        "best_model_summary.csv",
        ["model_name", "best_metric_name", "best_metric_value", "display_text"],
        [(best["model_name"], "test_auc_pr", float(best["test_auc_pr"]), display_text)],
    )


def create_feature_profile_summary(connection):
    """Create compact counts by feature inclusion decision."""
    _, rows = read_csv("output/stage3/feature_profile.csv")
    counts = {}
    for row in rows:
        decision = row["inclusion_decision"]
        counts[decision] = counts.get(decision, 0) + 1
    connection.execute("DROP TABLE IF EXISTS feature_profile_summary")
    connection.execute(
        """
        CREATE TABLE feature_profile_summary (
            inclusion_decision TEXT,
            feature_count INTEGER
        )
        """
    )
    values = sorted(counts.items())
    connection.executemany("INSERT INTO feature_profile_summary VALUES (?, ?)", values)
    write_rows(
        "feature_profile_summary.csv",
        ["inclusion_decision", "feature_count"],
        values,
    )


def write_rows(file_name, columns, rows):
    """Write helper rows to a Stage IV CSV file."""
    path = os.path.join(STAGE4_DIR, file_name)
    with open(path, "w") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def create_dashboard_manifest():
    """Write a compact machine-readable manifest for the dashboard datasets."""
    manifest = {
        "database": "team20_stage4_dashboard",
        "sqlite_path": os.path.relpath(SQLITE_PATH, ROOT),
        "dashboard_title": "IEEE-CIS Fraud Risk: EDA to Spark ML",
        "tables": [
            table_name for table_name, _ in SOURCE_TABLES
        ] + [
            "model_metrics_long",
            "best_model_summary",
            "feature_profile_summary",
        ],
        "helper_csvs": [
            "output/stage4/model_metrics_long.csv",
            "output/stage4/best_model_summary.csv",
            "output/stage4/feature_profile_summary.csv",
        ],
    }
    path = os.path.join(STAGE4_DIR, "superset_dashboard_manifest.json")
    with open(path, "w") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main():
    """Create the SQLite database and helper manifest."""
    if not os.path.isdir(STAGE4_DIR):
        os.makedirs(STAGE4_DIR)
    if os.path.exists(SQLITE_PATH):
        os.remove(SQLITE_PATH)

    connection = sqlite3.connect(SQLITE_PATH)
    try:
        for table_name, relative_path in SOURCE_TABLES:
            columns, rows = read_csv(relative_path)
            create_table(connection, table_name, columns, rows)
        create_model_metrics_long(connection)
        create_best_model_summary(connection)
        create_feature_profile_summary(connection)
        connection.commit()
    finally:
        connection.close()

    create_dashboard_manifest()
    print("OK: wrote {}".format(os.path.relpath(SQLITE_PATH, ROOT)))
    print("OK: wrote output/stage4/superset_dashboard_manifest.json")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Validate local assets required to build the Stage IV dashboard."""

from __future__ import print_function

import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

REQUIRED_FILES = [
    "output/q1.csv",
    "output/q1.jpg",
    "output/q2.csv",
    "output/q2.jpg",
    "output/q3.csv",
    "output/q3.jpg",
    "output/q4.csv",
    "output/q4.jpg",
    "output/q5.csv",
    "output/q5.jpg",
    "output/evaluation.csv",
    "output/model1_predictions.csv",
    "output/model2_predictions.csv",
    "output/stage3/model_comparison.json",
    "output/stage3/feature_profile.csv",
    "output/stage3/feature_importance_gbt.csv",
    "output/stage3/feature_importance_rf.csv",
    "output/stage4/superset_dashboard.sqlite",
    "output/stage4/superset_dashboard_manifest.json",
    "output/stage4/model_metrics_long.csv",
    "output/stage4/best_model_summary.csv",
    "output/stage4/feature_profile_summary.csv",
    "reports/dashboard.md",
    "reports/stage3.md",
]


def full_path(relative_path):
    """Return an absolute repository path."""
    return os.path.join(ROOT, relative_path)


def fail(message):
    """Exit with one formatted error."""
    print("FAIL: {}".format(message))
    sys.exit(1)


def check_exists():
    """Check all required files exist and are non-empty."""
    for relative_path in REQUIRED_FILES:
        path = full_path(relative_path)
        if not os.path.isfile(path):
            fail("Missing file {}".format(relative_path))
        if os.path.getsize(path) == 0:
            fail("Empty file {}".format(relative_path))


def check_sqlite_database():
    """Check the generated SQLite database contains dashboard tables."""
    import sqlite3

    expected_tables = set([
        "q1_product_risk",
        "q2_amount_band_risk",
        "q3_card_risk",
        "q4_email_domain_risk",
        "q5_daily_risk",
        "model_evaluation",
        "model_metrics_long",
        "best_model_summary",
        "feature_importance_gbt",
        "feature_importance_rf",
        "feature_profile",
        "feature_profile_summary",
    ])
    path = full_path("output/stage4/superset_dashboard.sqlite")
    connection = sqlite3.connect(path)
    try:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    finally:
        connection.close()
    tables = set(row[0] for row in rows)
    missing = expected_tables - tables
    if missing:
        fail("SQLite dashboard database missing tables: {}".format(sorted(missing)))


def main():
    """Run Stage IV local asset validation."""
    check_exists()
    check_sqlite_database()
    print("OK: Stage IV local dashboard source assets are present.")
    print("OK: Superset-ready SQLite dashboard database is present.")


if __name__ == "__main__":
    main()

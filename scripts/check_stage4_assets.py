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


def main():
    """Run Stage IV local asset validation."""
    check_exists()
    print("OK: Stage IV local dashboard source assets are present.")
    print("Manual Superset work is still required for dashboard creation/publication.")


if __name__ == "__main__":
    main()

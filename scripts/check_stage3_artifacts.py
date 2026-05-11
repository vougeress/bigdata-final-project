#!/usr/bin/env python3
"""Validate required Stage III repository artifacts."""

from __future__ import print_function

import csv
import json
import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

REQUIRED_FILES = [
    "data/train.json",
    "data/test.json",
    "output/model1_predictions.csv",
    "output/model2_predictions.csv",
    "output/evaluation.csv",
    "output/stage3/model_comparison.json",
    "output/stage3/feature_profile.csv",
]

REQUIRED_DIRS = [
    "models/model1",
    "models/model2",
]


def abs_path(relative_path):
    """Return an absolute path from repository root."""
    return os.path.join(ROOT, relative_path)


def fail(message):
    """Print a failure and exit."""
    print("FAIL: {}".format(message))
    sys.exit(1)


def check_exists():
    """Check that required files and directories exist."""
    for relative_path in REQUIRED_FILES:
        path = abs_path(relative_path)
        if not os.path.isfile(path):
            fail("Missing file {}".format(relative_path))
    for relative_path in REQUIRED_DIRS:
        path = abs_path(relative_path)
        if not os.path.isdir(path):
            fail("Missing directory {}".format(relative_path))


def check_json_lines(relative_path):
    """Check that a JSON-lines artifact is not empty."""
    path = abs_path(relative_path)
    with open(path, "r", encoding="utf-8") as handle:
        first_line = handle.readline().strip()
    if not first_line:
        fail("{} is empty".format(relative_path))
    try:
        json.loads(first_line)
    except ValueError:
        fail("{} does not start with valid JSON".format(relative_path))


def check_prediction_csv(relative_path):
    """Check the required prediction CSV format."""
    path = abs_path(relative_path)
    with open(path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != ["label", "prediction"]:
            fail("{} must have header label,prediction".format(relative_path))
        first_row = next(reader, None)
        if first_row is None:
            fail("{} contains no prediction rows".format(relative_path))


def check_evaluation():
    """Check the combined evaluation file contains both model families."""
    path = abs_path("output/evaluation.csv")
    with open(path, "r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle, escapechar="\\"))
    if not rows:
        fail("output/evaluation.csv is empty")
    models = sorted(
        {
            row.get("model_name", "").strip()
            for row in rows
            if row.get("model_name", "").strip()
        }
    )
    if models != ["gbt", "rf"]:
        fail("output/evaluation.csv must contain gbt and rf, found {}".format(models))


def check_model_comparison():
    """Check the comparison JSON matches the combined evaluation."""
    path = abs_path("output/stage3/model_comparison.json")
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    models = sorted(item.get("model_name", "") for item in payload.get("models", []))
    if models != ["gbt", "rf"]:
        fail("output/stage3/model_comparison.json must contain gbt and rf, found {}".format(models))


def main():
    """Run all Stage III artifact checks."""
    check_exists()
    check_json_lines("data/train.json")
    check_json_lines("data/test.json")
    check_prediction_csv("output/model1_predictions.csv")
    check_prediction_csv("output/model2_predictions.csv")
    check_evaluation()
    check_model_comparison()
    print("OK: Stage III required repository artifacts are present and structurally valid.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Create Stage II chart images from exported EDA CSV files."""

import csv
from pathlib import Path

import matplotlib.pyplot as plt


OUTPUT_DIR = Path("output")


def read_csv(name):
    """Read an output CSV file into a list of dictionaries."""
    path = OUTPUT_DIR / ("%s.csv" % name)
    with path.open(newline="", encoding="utf-8") as file_obj:
        return list(csv.DictReader(file_obj))


def save_bar(name, title, xlabel, ylabel, labels, values):
    """Save a horizontal bar chart."""
    plt.figure(figsize=(10, max(4, len(labels) * 0.45)))
    positions = range(len(labels))
    plt.barh(positions, values, color="#2f6f73")
    plt.yticks(positions, labels)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / ("%s.jpg" % name), dpi=160)
    plt.close()


def save_line(name, title, xlabel, ylabel, x_values, y_values):
    """Save a line chart."""
    plt.figure(figsize=(11, 5))
    plt.plot(x_values, y_values, color="#2f6f73", linewidth=2)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / ("%s.jpg" % name), dpi=160)
    plt.close()


def main():
    """Create all Stage II charts."""
    q1 = read_csv("q1")
    save_bar(
        "q1",
        "Fraud rate by product category",
        "Fraud rate",
        "Product category",
        [row["productcd"] for row in q1],
        [float(row["fraud_rate"]) for row in q1],
    )

    q2 = read_csv("q2")
    save_bar(
        "q2",
        "Fraud rate by transaction amount band",
        "Fraud rate",
        "Amount band",
        [row["amount_band"] for row in q2],
        [float(row["fraud_rate"]) for row in q2],
    )

    q3 = read_csv("q3")
    q3_labels = ["%s / %s" % (row["card4"], row["card6"]) for row in q3]
    save_bar(
        "q3",
        "Fraud rate by card network and type",
        "Fraud rate",
        "Card segment",
        q3_labels,
        [float(row["fraud_rate"]) for row in q3],
    )

    q4 = read_csv("q4")
    save_bar(
        "q4",
        "Top email domains by fraud rate",
        "Fraud rate",
        "Email domain",
        [row["email_domain"] for row in q4],
        [float(row["fraud_rate"]) for row in q4],
    )

    q5 = read_csv("q5")
    save_line(
        "q5",
        "Daily fraud rate over transaction timeline",
        "Transaction day",
        "Fraud rate",
        [int(row["transaction_day"]) for row in q5],
        [float(row["fraud_rate"]) for row in q5],
    )

    print("Created output/q1.jpg ... output/q5.jpg")


if __name__ == "__main__":
    main()

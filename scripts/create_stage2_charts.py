from __future__ import print_function

import csv
import os
import re
import sys
import tempfile


OUTPUT_DIR = os.path.join("output")


def configure_matplotlib_cache():
    """Use a project-local writable Matplotlib cache if none was configured."""
    cache_dir = os.path.join(tempfile.gettempdir(), "stage2_matplotlib_cache")
    os.makedirs(cache_dir, exist_ok=True)
    if "MPLCONFIGDIR" not in os.environ:
        os.environ["MPLCONFIGDIR"] = cache_dir
    if "XDG_CACHE_HOME" not in os.environ:
        os.environ["XDG_CACHE_HOME"] = cache_dir


def require_libs():
    """Check that matplotlib is available."""
    configure_matplotlib_cache()
    try:
        import matplotlib  # pylint: disable=import-outside-toplevel
        return matplotlib
    except ImportError as exc:
        print("ERROR: {}. Install matplotlib.".format(exc))
        sys.exit(1)


def load_csv(name):
    """Load output/<name>.csv into a list of dictionaries."""
    path = os.path.join(OUTPUT_DIR, "{}.csv".format(name))
    if not os.path.exists(path):
        print("SKIP: {} not found.".format(path))
        return None
    with open(path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [
            {key.strip(): value for key, value in row.items()}
            for row in reader
        ]


def number(row, column):
    """Return a CSV cell as a float."""
    value = row.get(column, "")
    if value == "":
        return 0.0
    return float(value)


def pct(row):
    """Convert a row's 0-1 fraud_rate value to percentage for display."""
    return number(row, "fraud_rate") * 100.0


def save_chart(fig, name):
    """Save figure to output/<name>.jpg."""
    path = os.path.join(OUTPUT_DIR, "{}.jpg".format(name))
    fig.savefig(path, format="jpeg", dpi=150, bbox_inches="tight")
    print("Saved: {}".format(path))


def chart_q1(rows):
    """Q1: Horizontal bar chart - fraud rate by product category."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    fig, ax = plt.subplots(figsize=(8, 4))
    sorted_rows = sorted(rows, key=lambda row: number(row, "fraud_rate"))
    labels = [row["productcd"] for row in sorted_rows]
    rates = [pct(row) for row in sorted_rows]
    ax.barh(labels, rates, color="#e05c5c")
    ax.set_xlabel("Fraud Rate (%)")
    ax.set_title("Q1: Fraud Rate by Product Category")
    for index, value in enumerate(rates):
        ax.text(value + 0.05, index, "{:.2f}%".format(value), va="center", fontsize=9)
    fig.tight_layout()
    return fig


def chart_q2(rows):
    """Q2: Bar + line chart - transaction volume and fraud rate by amount band."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    labels = [re.sub(r"^\d+_", "", row["amount_band"]) for row in rows]
    x_values = list(range(len(labels)))
    totals = [number(row, "total_transactions") for row in rows]
    rates = [pct(row) for row in rows]

    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(x_values, totals, color="#4c72b0", alpha=0.7, label="Total Txns")
    ax1.set_ylabel("Total Transactions", color="#4c72b0")
    ax1.set_xticks(x_values)
    ax1.set_xticklabels(labels, rotation=30, ha="right")

    ax2 = ax1.twinx()
    ax2.plot(x_values, rates, color="#e05c5c", marker="o", label="Fraud Rate %")
    ax2.set_ylabel("Fraud Rate (%)", color="#e05c5c")
    ax1.set_title("Q2: Transaction Volume and Fraud Rate by Amount Band")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    fig.tight_layout()
    return fig


def chart_q3(rows):
    """Q3: Grouped bar chart - fraud rate by card network and card type."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    networks = sorted(set(row["card4"] for row in rows))
    card_types = sorted(set(row["card6"] for row in rows))
    x_values = list(range(len(networks)))
    width = 0.8 / max(len(card_types), 1)

    fig, ax = plt.subplots(figsize=(11, 5))
    for index, card_type in enumerate(card_types):
        rates = []
        for network in networks:
            match = [
                row for row in rows
                if row["card6"] == card_type and row["card4"] == network
            ]
            rates.append(pct(match[0]) if match else 0.0)
        offsets = [value + index * width for value in x_values]
        ax.bar(offsets, rates, width, label=str(card_type))

    ax.set_xticks([
        value + width * (len(card_types) - 1) / 2
        for value in x_values
    ])
    ax.set_xticklabels(networks, rotation=20, ha="right")
    ax.set_ylabel("Fraud Rate (%)")
    ax.set_title("Q3: Fraud Rate by Card Network and Card Type")
    ax.legend(title="Card Type", bbox_to_anchor=(1.01, 1), loc="upper left")
    fig.tight_layout()
    return fig


def chart_q4(rows):
    """Q4: Horizontal bar chart - email domains by fraud rate."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    fig, ax = plt.subplots(figsize=(9, 8))
    sorted_rows = sorted(rows, key=lambda row: number(row, "fraud_rate"))
    labels = [row["email_domain"] for row in sorted_rows]
    rates = [pct(row) for row in sorted_rows]
    colors = ["#e05c5c" if rate > 5 else "#4c72b0" for rate in rates]
    ax.barh(labels, rates, color=colors)
    ax.set_xlabel("Fraud Rate (%)")
    ax.set_title("Q4: Top Email Domains - Fraud Rate\n(red = >5% fraud rate)")
    fig.tight_layout()
    return fig


def chart_q5(rows):
    """Q5: Line chart - daily transaction volume and fraud rate over time."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    sorted_rows = sorted(rows, key=lambda row: number(row, "transaction_day"))
    days = [number(row, "transaction_day") for row in sorted_rows]
    totals = [number(row, "total_transactions") for row in sorted_rows]
    rates = [pct(row) for row in sorted_rows]

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.fill_between(days, totals, alpha=0.4, color="#4c72b0", label="Total Txns")
    ax1.set_xlabel("Day")
    ax1.set_ylabel("Total Transactions", color="#4c72b0")

    ax2 = ax1.twinx()
    ax2.plot(days, rates, color="#e05c5c", linewidth=1.2, label="Fraud Rate %")
    ax2.set_ylabel("Fraud Rate (%)", color="#e05c5c")
    ax1.set_title("Q5: Daily Transaction Volume and Fraud Rate Over Time")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    fig.tight_layout()
    return fig


def main():
    require_libs()
    import matplotlib  # pylint: disable=import-outside-toplevel
    matplotlib.use("Agg")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    chart_fns = [
        ("q1", chart_q1),
        ("q2", chart_q2),
        ("q3", chart_q3),
        ("q4", chart_q4),
        ("q5", chart_q5),
    ]

    for name, chart_fn in chart_fns:
        rows = load_csv(name)
        if rows is None:
            continue
        try:
            fig = chart_fn(rows)
            save_chart(fig, name)
            import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
            plt.close(fig)
        except Exception as exc:  # pylint: disable=broad-except
            print("ERROR generating {}: {}".format(name, exc))

    print("Chart generation complete.")


if __name__ == "__main__":
    main()

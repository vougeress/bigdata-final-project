from __future__ import print_function

import os
import sys

OUTPUT_DIR = os.path.join("output")


def require_libs():
    """Check that matplotlib and pandas are available."""
    try:
        import matplotlib  # pylint: disable=import-outside-toplevel
        import pandas  # pylint: disable=import-outside-toplevel
        return matplotlib, pandas
    except ImportError as exc:
        print("ERROR: {}. Install matplotlib and pandas.".format(exc))
        sys.exit(1)


def load_csv(name):
    """Load output/<name>.csv into a pandas DataFrame."""
    import pandas as pd  # pylint: disable=import-outside-toplevel
    path = os.path.join(OUTPUT_DIR, "{}.csv".format(name))
    if not os.path.exists(path):
        print("SKIP: {} not found.".format(path))
        return None
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def pct(series):
    """Convert a 0-1 fraud_rate series to percentage for display."""
    return series * 100.0


def save_chart(fig, name):
    """Save figure to output/<name>.jpg."""
    path = os.path.join(OUTPUT_DIR, "{}.jpg".format(name))
    fig.savefig(path, format="jpeg", dpi=150, bbox_inches="tight")
    print("Saved: {}".format(path))


def chart_q1(df):
    """Q1: Horizontal bar chart — fraud rate by product category."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    fig, ax = plt.subplots(figsize=(8, 4))
    df_s = df.sort_values("fraud_rate", ascending=True)
    rates = pct(df_s["fraud_rate"])
    ax.barh(df_s["productcd"], rates, color="#e05c5c")
    ax.set_xlabel("Fraud Rate (%)")
    ax.set_title("Q1: Fraud Rate by Product Category")
    for i, val in enumerate(rates):
        ax.text(val + 0.05, i, "{:.2f}%".format(val), va="center", fontsize=9)
    fig.tight_layout()
    return fig


def chart_q2(df):
    """Q2: Bar + line chart — transaction volume and fraud rate by amount band."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    labels = df["amount_band"].str.replace(r"^\d+_", "", regex=True)
    x = range(len(labels))
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(x, df["total_transactions"], color="#4c72b0", alpha=0.7, label="Total Txns")
    ax1.set_ylabel("Total Transactions", color="#4c72b0")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(labels, rotation=30, ha="right")
    ax2 = ax1.twinx()
    ax2.plot(list(x), pct(df["fraud_rate"]),
             color="#e05c5c", marker="o", label="Fraud Rate %")
    ax2.set_ylabel("Fraud Rate (%)", color="#e05c5c")
    ax1.set_title("Q2: Transaction Volume and Fraud Rate by Amount Band")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    fig.tight_layout()
    return fig


def chart_q3(df):
    """Q3: Grouped bar chart — fraud rate by card network and card type."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    import numpy as np  # pylint: disable=import-outside-toplevel
    df = df.copy()
    df["fraud_rate_pct"] = pct(df["fraud_rate"])
    networks = sorted(df["card4"].unique())
    card_types = sorted(df["card6"].unique())
    x = np.arange(len(networks))
    width = 0.8 / max(len(card_types), 1)
    fig, ax = plt.subplots(figsize=(11, 5))
    for i, ctype in enumerate(card_types):
        subset = df[df["card6"] == ctype]
        rates = []
        for net in networks:
            row = subset[subset["card4"] == net]
            rates.append(float(row["fraud_rate_pct"].values[0]) if len(row) else 0.0)
        ax.bar(x + i * width, rates, width, label=str(ctype))
    ax.set_xticks(x + width * (len(card_types) - 1) / 2)
    ax.set_xticklabels(networks, rotation=20, ha="right")
    ax.set_ylabel("Fraud Rate (%)")
    ax.set_title("Q3: Fraud Rate by Card Network and Card Type")
    ax.legend(title="Card Type", bbox_to_anchor=(1.01, 1), loc="upper left")
    fig.tight_layout()
    return fig


def chart_q4(df):
    """Q4: Horizontal bar chart — email domains by fraud rate."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    fig, ax = plt.subplots(figsize=(9, 8))
    df_s = df.sort_values("fraud_rate", ascending=True)
    rates = pct(df_s["fraud_rate"])
    colors = ["#e05c5c" if r > 5 else "#4c72b0" for r in rates]
    ax.barh(df_s["email_domain"], rates, color=colors)
    ax.set_xlabel("Fraud Rate (%)")
    ax.set_title("Q4: Top Email Domains — Fraud Rate\n(red = >5% fraud rate)")
    fig.tight_layout()
    return fig


def chart_q5(df):
    """Q5: Line chart — daily transaction volume and fraud rate over time."""
    import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.fill_between(df["transaction_day"], df["total_transactions"],
                     alpha=0.4, color="#4c72b0", label="Total Txns")
    ax1.set_xlabel("Day")
    ax1.set_ylabel("Total Transactions", color="#4c72b0")
    ax2 = ax1.twinx()
    ax2.plot(df["transaction_day"], pct(df["fraud_rate"]),
             color="#e05c5c", linewidth=1.2, label="Fraud Rate %")
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

    for name, fn in chart_fns:
        df = load_csv(name)
        if df is None:
            continue
        try:
            fig = fn(df)
            save_chart(fig, name)
            import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
            plt.close(fig)
        except Exception as exc:  # pylint: disable=broad-except
            print("ERROR generating {}: {}".format(name, exc))

    print("Chart generation complete.")


if __name__ == "__main__":
    main()

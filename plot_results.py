"""
plot_results.py - result.md 데이터 시각화
실행: .venv/bin/python plot_results.py
출력: results_plot.png
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── 데이터 ──────────────────────────────────────────────────────────────────
strategies = ["A", "B", "C", "D", "E"]

PG = {
    "ed25519": {
        "insert": [19241, 11230, 16687, 13884, 19945],
        "pq":     [0.175,  0.254,  0.214,  0.160,  0.164],
        "range":  [209.1,  245.1,  197.4,  183.3,  185.6],
        "delete": [0.207,  0.342,  0.271,  0.246,  0.203],
        "table":  [211.2,  120.2,  279.0,  244.1,  211.2],
        "toast":  [0.0,    0.0,    0.0,    0.0,    0.0],
    },
    "haetae-3": {
        "insert": [6873,  5836,  6579,  4540,  6945],
        "pq":     [0.358, 0.896, 0.396, 0.355, 0.707],
        "range":  [11558, 1970,  12130, 10835, 10118],
        "delete": [1.127, 1.289, 1.113, 1.343, 0.969],
        "table":  [1562.5, 120.2, 1953.1, 1953.1, 1562.5],
        "toast":  [2604.2, 0.0, 2604.2, 2604.2, 2604.2],
    },
}

SQ = {
    "ed25519": {
        "insert": [8978,  8193,  11947, 16292, 11520],
        "pq":     [0.044, 0.043, 0.015, 0.015, 0.015],
        "range":  [128.4, 101.8, 143.4, 135.8, 111.3],
        "delete": [0.106, 0.106, 0.107, 0.075, 0.110],
        "table":  [186.5, 91.1,  217.6, 217.6, 122.4],
        "depth":  [3, 3, 3, 3, 3],
        "overflow": [0.0, 0.0, 0.0, 0.0, 0.0],
    },
    "haetae-3": {
        "insert": [5654,  5261,  3371,  4675,  3212],
        "pq":     [0.433, 0.316, 0.391, 0.351, 0.322],
        "range":  [7897,  7633,  23756, 10255, 22269],
        "delete": [0.681, 0.420, 0.672, 0.676, 0.456],
        "table":  [3916.1, 91.1, 3916.1, 3916.1, 122.4],
        "depth":  [4, 3, 4, 4, 3],
        "overflow": [0.0, 0.0, 0.0, 0.0, 0.0],
    },
}

COLORS = {"ed25519": "#4C9BE8", "haetae-3": "#E8674C"}
HATCH  = {"ed25519": "", "haetae-3": "//"}
x = np.arange(len(strategies))
w = 0.35

# ── 그래프 레이아웃 ──────────────────────────────────────────────────────────
fig, axes = plt.subplots(3, 2, figsize=(14, 15))
fig.suptitle("PQC Signature Storage Benchmark\ned25519 (64 B) vs haetae-3 (2,337 B) | 1,000,000 records",
             fontsize=14, fontweight="bold", y=0.98)

def bar_group(ax, data_dict, metric, title, ylabel, db_label,
              log=False, fmt="{:.0f}"):
    for i, (algo, color) in enumerate(COLORS.items()):
        vals = data_dict[algo][metric]
        offset = (i - 0.5) * w
        bars = ax.bar(x + offset, vals, w,
                      label=algo, color=color,
                      hatch=HATCH[algo], edgecolor="white", linewidth=0.5)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() * (1.05 if not log else 1.5),
                    fmt.format(v),
                    ha="center", va="bottom", fontsize=7.5, rotation=45)
    ax.set_title(f"{db_label} — {title}", fontsize=11, fontweight="bold")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels([f"S-{s}" for s in strategies])
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    if log:
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(mticker.ScalarFormatter())


# ── Row 0: INSERT(rps) ───────────────────────────────────────────────────────
bar_group(axes[0][0], PG, "insert", "INSERT Throughput", "rps", "PostgreSQL",
          fmt="{:,.0f}")
bar_group(axes[0][1], SQ, "insert", "INSERT Throughput", "rps", "SQLite",
          fmt="{:,.0f}")

# ── Row 1: Range Scan (ms, log scale) ───────────────────────────────────────
bar_group(axes[1][0], PG, "range", "Range Scan Latency (log)", "ms", "PostgreSQL",
          log=True, fmt="{:.0f}")
bar_group(axes[1][1], SQ, "range", "Range Scan Latency (log)", "ms", "SQLite",
          log=True, fmt="{:.0f}")

# ── Row 2: Table + TOAST/Overflow (stacked) ─────────────────────────────────
def stacked_bar(ax, data_dict, title, db_label):
    for i, (algo, color) in enumerate(COLORS.items()):
        offset = (i - 0.5) * w
        tbl = np.array(data_dict[algo]["table"])
        ext = np.array(data_dict[algo].get("toast", data_dict[algo].get("overflow", [0]*5)))
        ax.bar(x + offset, tbl, w, label=f"{algo} Table",
               color=color, hatch=HATCH[algo], edgecolor="white", linewidth=0.5)
        ax.bar(x + offset, ext, w, bottom=tbl,
               label=f"{algo} TOAST/OVF",
               color=color, alpha=0.4, hatch="xx", edgecolor="white", linewidth=0.5)
    ax.set_title(f"{db_label} — {title}", fontsize=11, fontweight="bold")
    ax.set_ylabel("MB")
    ax.set_xticks(x)
    ax.set_xticklabels([f"S-{s}" for s in strategies])
    ax.legend(fontsize=8, ncol=2)
    ax.grid(axis="y", alpha=0.3)

stacked_bar(axes[2][0], PG, "Table + TOAST Size", "PostgreSQL")
stacked_bar(axes[2][1], SQ, "Table + Overflow Size", "SQLite")

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig("results_plot.png", dpi=150, bbox_inches="tight")
print("저장 완료: results_plot.png")

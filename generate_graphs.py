# =============================================================================
# generate_graphs.py - 기존 체계 vs PQC 비교 그래프 생성
# =============================================================================
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

os.makedirs("results/graphs", exist_ok=True)

df = pd.read_csv("results/results.csv")
df["is_classical"] = df["family"] == "classical"

# 색상 팔레트
COLORS = {
    "classical": "#2196F3",
    "aimer":     "#F44336",
    "haetae":    "#FF9800",
    "ml-dsa":    "#4CAF50",
    "faest":     "#9C27B0",
    "sphincs":   "#795548",
}

STRATEGY_NAMES = {
    "A": "A: Inline",
    "B": "B: Separated",
    "C": "C: Hash Index",
    "D": "D: Compressed",
    "E": "E: TOAST External",
}

# 100만 건, 전략 A 기준 데이터
df_1m_a = df[(df["scale"] == 1_000_000) & (df["strategy"] == "A")].copy()


# =============================================================================
# 그래프 1: INSERT 처리량 비교 (보안 레벨별)
# =============================================================================
def graph_insert_by_level():
    fig, axes = plt.subplots(1, 3, figsize=(18, 7))
    fig.suptitle("INSERT Throughput: Classical vs PQC\n(1M records, Strategy A)", fontsize=14, fontweight="bold")

    for ax, level in zip(axes, [1, 3, 5]):
        sub = df_1m_a[df_1m_a["level"] == level].sort_values("insert_throughput_rps", ascending=False)
        colors = [COLORS.get(f, "#999") for f in sub["family"]]
        bars = ax.barh(sub["algorithm"], sub["insert_throughput_rps"], color=colors)
        ax.set_title(f"Security Level {level}", fontsize=12, fontweight="bold")
        ax.set_xlabel("Throughput (records/sec)")
        ax.axvline(x=sub[sub["family"] == "classical"]["insert_throughput_rps"].mean(),
                   color="blue", linestyle="--", linewidth=1.5, label="Classical avg")
        ax.legend(fontsize=8)
        for bar, val in zip(bars, sub["insert_throughput_rps"]):
            ax.text(bar.get_width() + 100, bar.get_y() + bar.get_height() / 2,
                    f"{val:,.0f}", va="center", fontsize=7)
        ax.tick_params(axis="y", labelsize=8)

    patches = [mpatches.Patch(color=v, label=k) for k, v in COLORS.items()]
    fig.legend(handles=patches, loc="lower center", ncol=6, fontsize=9, bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout(rect=[0, 0.05, 1, 1])
    plt.savefig("results/graphs/01_insert_throughput_by_level.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  그래프 1 완료: INSERT 처리량 보안 레벨별")


# =============================================================================
# 그래프 2: 단건 조회 지연시간 비교
# =============================================================================
def graph_point_query():
    fig, axes = plt.subplots(1, 3, figsize=(18, 7))
    fig.suptitle("Point Query Latency: Classical vs PQC\n(1M records, Strategy A)", fontsize=14, fontweight="bold")

    for ax, level in zip(axes, [1, 3, 5]):
        sub = df_1m_a[df_1m_a["level"] == level].sort_values("pq_avg_ms")
        colors = [COLORS.get(f, "#999") for f in sub["family"]]
        bars = ax.barh(sub["algorithm"], sub["pq_avg_ms"], color=colors)
        ax.set_title(f"Security Level {level}", fontsize=12, fontweight="bold")
        ax.set_xlabel("Avg Latency (ms)")
        for bar, val in zip(bars, sub["pq_avg_ms"]):
            ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height() / 2,
                    f"{val:.3f}", va="center", fontsize=7)
        ax.tick_params(axis="y", labelsize=8)

    patches = [mpatches.Patch(color=v, label=k) for k, v in COLORS.items()]
    fig.legend(handles=patches, loc="lower center", ncol=6, fontsize=9, bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout(rect=[0, 0.05, 1, 1])
    plt.savefig("results/graphs/02_point_query_latency.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  그래프 2 완료: 단건 조회 지연시간")


# =============================================================================
# 그래프 3: 저장 공간 비교 (전략별)
# =============================================================================
def graph_storage():
    df_1m = df[df["scale"] == 1_000_000].copy()
    df_1m["total_size_gb"] = (df_1m["table_size_bytes"] + df_1m["index_size_bytes"] + df_1m["toast_size_bytes"]) / 1024**3

    fig, axes = plt.subplots(1, 5, figsize=(22, 8))
    fig.suptitle("Total Storage Size: Classical vs PQC\n(1M records, by Strategy)", fontsize=14, fontweight="bold")

    for ax, strat in zip(axes, ["A", "B", "C", "D", "E"]):
        sub = df_1m[df_1m["strategy"] == strat].sort_values("total_size_gb", ascending=False).head(20)
        colors = [COLORS.get(f, "#999") for f in sub["family"]]
        ax.barh(sub["algorithm"], sub["total_size_gb"], color=colors)
        ax.set_title(f"Strategy {strat}", fontsize=11, fontweight="bold")
        ax.set_xlabel("Total Size (GB)")
        ax.tick_params(axis="y", labelsize=7)

    patches = [mpatches.Patch(color=v, label=k) for k, v in COLORS.items()]
    fig.legend(handles=patches, loc="lower center", ncol=6, fontsize=9, bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout(rect=[0, 0.05, 1, 1])
    plt.savefig("results/graphs/03_storage_by_strategy.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  그래프 3 완료: 저장 공간 전략별")


# =============================================================================
# 그래프 4: INSERT 처리량 - 전략별 비교 (classical 평균 vs PQC 평균)
# =============================================================================
def graph_strategy_comparison():
    df_1m = df[df["scale"] == 1_000_000].copy()

    classical_avg = df_1m[df_1m["family"] == "classical"].groupby("strategy")["insert_throughput_rps"].mean()
    pqc_avg = df_1m[df_1m["family"] != "classical"].groupby("strategy")["insert_throughput_rps"].mean()

    strategies = ["A", "B", "C", "D", "E"]
    x = np.arange(len(strategies))
    width = 0.35

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Classical vs PQC Performance by Strategy\n(1M records, average)", fontsize=13, fontweight="bold")

    # INSERT
    ax = axes[0]
    ax.bar(x - width/2, [classical_avg.get(s, 0) for s in strategies], width, label="Classical", color="#2196F3")
    ax.bar(x + width/2, [pqc_avg.get(s, 0) for s in strategies], width, label="PQC (avg)", color="#F44336")
    ax.set_title("INSERT Throughput (tps)")
    ax.set_xticks(x)
    ax.set_xticklabels([STRATEGY_NAMES[s] for s in strategies], rotation=15, ha="right")
    ax.set_ylabel("Records/sec")
    ax.legend()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:,.0f}"))

    # 단건 조회
    classical_pq = df_1m[df_1m["family"] == "classical"].groupby("strategy")["pq_avg_ms"].mean()
    pqc_pq = df_1m[df_1m["family"] != "classical"].groupby("strategy")["pq_avg_ms"].mean()

    ax = axes[1]
    ax.bar(x - width/2, [classical_pq.get(s, 0) for s in strategies], width, label="Classical", color="#2196F3")
    ax.bar(x + width/2, [pqc_pq.get(s, 0) for s in strategies], width, label="PQC (avg)", color="#F44336")
    ax.set_title("Point Query Latency (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels([STRATEGY_NAMES[s] for s in strategies], rotation=15, ha="right")
    ax.set_ylabel("Latency (ms)")
    ax.legend()

    plt.tight_layout()
    plt.savefig("results/graphs/04_strategy_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  그래프 4 완료: 전략별 비교")


# =============================================================================
# 그래프 5: 서명 크기 vs INSERT 처리량 산점도
# =============================================================================
def graph_scatter_sig_vs_insert():
    fig, ax = plt.subplots(figsize=(12, 7))

    for family, group in df_1m_a.groupby("family"):
        ax.scatter(group["sig_size"], group["insert_throughput_rps"],
                   c=COLORS.get(family, "#999"), label=family, s=60, alpha=0.8)
        for _, row in group.iterrows():
            ax.annotate(row["algorithm"], (row["sig_size"], row["insert_throughput_rps"]),
                        textcoords="offset points", xytext=(4, 2), fontsize=6, alpha=0.8)

    ax.set_xscale("log")
    ax.set_xlabel("Signature Size (bytes, log scale)", fontsize=11)
    ax.set_ylabel("INSERT Throughput (records/sec)", fontsize=11)
    ax.set_title("Signature Size vs INSERT Throughput\n(1M records, Strategy A)", fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("results/graphs/05_scatter_sig_vs_insert.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  그래프 5 완료: 서명 크기 vs INSERT 산점도")


# =============================================================================
# 그래프 6: 스케일별 성능 변화 (classical vs PQC 대표값)
# =============================================================================
def graph_scale_trend():
    scales = [100_000, 500_000, 1_000_000]
    scale_labels = ["100K", "500K", "1M"]

    representatives = {
        "classical": ["ecdsa-256", "ed25519", "rsa-2048"],
        "pqc":       ["ml-dsa-44", "sphincs-shake-128s", "aimer-128s"],
    }

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Performance by Scale: Classical vs PQC Representatives\n(Strategy A)", fontsize=13, fontweight="bold")

    for group_name, algos in representatives.items():
        color = "#2196F3" if group_name == "classical" else "#F44336"
        ls = "-" if group_name == "classical" else "--"
        for algo in algos:
            vals_ins = [df[(df["algorithm"] == algo) & (df["strategy"] == "A") & (df["scale"] == s)]["insert_throughput_rps"].values for s in scales]
            vals_ins = [v[0] if len(v) > 0 else None for v in vals_ins]
            vals_pq  = [df[(df["algorithm"] == algo) & (df["strategy"] == "A") & (df["scale"] == s)]["pq_avg_ms"].values for s in scales]
            vals_pq  = [v[0] if len(v) > 0 else None for v in vals_pq]

            if all(v is not None for v in vals_ins):
                axes[0].plot(scale_labels, vals_ins, marker="o", label=algo, color=color, linestyle=ls, alpha=0.8)
            if all(v is not None for v in vals_pq):
                axes[1].plot(scale_labels, vals_pq, marker="o", label=algo, color=color, linestyle=ls, alpha=0.8)

    axes[0].set_title("INSERT Throughput (tps)")
    axes[0].set_ylabel("Records/sec")
    axes[0].legend(fontsize=8)
    axes[0].yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:,.0f}"))

    axes[1].set_title("Point Query Latency (ms)")
    axes[1].set_ylabel("Latency (ms)")
    axes[1].legend(fontsize=8)

    plt.tight_layout()
    plt.savefig("results/graphs/06_scale_trend.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  그래프 6 완료: 스케일별 성능 변화")


# =============================================================================
# 실행
# =============================================================================
print("그래프 생성 시작...")
graph_insert_by_level()
graph_point_query()
graph_storage()
graph_strategy_comparison()
graph_scatter_sig_vs_insert()
graph_scale_trend()
print("\n모든 그래프 생성 완료: results/graphs/")

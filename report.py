# =============================================================================
# report.py - 한국어 실험 결과 리포트 생성기
# =============================================================================
# results/results.csv를 읽어서 한국어 마크다운 리포트를 생성합니다.
#
# 실행 방법:
#   python report.py
#
# 출력 파일:
#   results/report.md
# =============================================================================

import os
import csv
from collections import defaultdict
from config import ALGORITHMS, RESULTS_CSV, REPORT_MD, RESULTS_DIR


def load_results() -> list:
    """
    results.csv에서 실험 결과를 읽어옵니다.
    """
    if not os.path.exists(RESULTS_CSV):
        print(f"결과 파일이 없습니다: {RESULTS_CSV}")
        print("먼저 python main.py 를 실행하세요.")
        return []

    results = []
    with open(RESULTS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 숫자형 컬럼 변환
            for key in ["scale", "sig_size", "pk_size", "level",
                        "insert_total_sec", "insert_avg_ms", "insert_throughput_rps",
                        "pq_avg_ms", "pq_min_ms", "pq_max_ms", "pq_throughput_qps",
                        "rs_avg_ms", "rs_min_ms", "rs_max_ms", "rs_avg_results",
                        "del_avg_ms", "del_min_ms", "del_max_ms", "del_throughput_dps",
                        "btree_depth", "index_size_bytes", "table_size_bytes",
                        "toast_size_bytes", "index_page_count",
                        "cache_hit_ratio", "heap_reads", "heap_hits",
                        "idx_reads", "idx_hits"]:
                try:
                    row[key] = float(row[key]) if "." in str(row[key]) else int(row[key])
                except (ValueError, TypeError):
                    row[key] = 0
            results.append(row)

    return results


def fmt_bytes(b: int) -> str:
    """
    바이트를 읽기 쉬운 단위로 변환합니다.
    """
    if b >= 1024 ** 3:
        return f"{b / 1024**3:.2f} GB"
    elif b >= 1024 ** 2:
        return f"{b / 1024**2:.2f} MB"
    elif b >= 1024:
        return f"{b / 1024:.2f} KB"
    return f"{b} B"


def fmt_ms(ms: float) -> str:
    return f"{ms:.3f} ms"


def generate_report(results: list) -> str:
    """
    마크다운 리포트 문자열을 생성합니다.
    """
    if not results:
        return "# 실험 결과 없음\n\n결과 데이터가 없습니다."

    lines = []

    # =========================================================================
    # 제목 및 개요
    # =========================================================================
    lines.append("# PQC 서명 저장이 B+tree 성능에 미치는 영향 분석")
    lines.append("")
    lines.append("## 1. 실험 개요")
    lines.append("")
    lines.append("### 실험 환경")
    lines.append("| 항목 | 내용 |")
    lines.append("|---|---|")
    lines.append("| DB | PostgreSQL (B+tree 인덱스) |")
    lines.append("| 환경 | Apple M2 Mac |")
    lines.append("| 언어 | Python |")
    lines.append(f"| 총 실험 횟수 | {len(results)}회 |")
    lines.append("")

    lines.append("### 실험 구성")
    lines.append("| 항목 | 내용 |")
    lines.append("|---|---|")
    lines.append("| 알고리즘 변형 수 | 27개 |")
    lines.append("| 데이터 규모 | 10만 / 50만 / 100만 건 |")
    lines.append("| DB 저장 전략 | A(Inline) / B(Separated) / C(Hash Index) / D(Compressed) / E(TOAST External) |")
    lines.append("| 측정 연산 | INSERT / 단건 조회 / 범위 조회 / DELETE |")
    lines.append("")

    # =========================================================================
    # 알고리즘별 서명 크기 정리
    # =========================================================================
    lines.append("## 2. PQC 알고리즘 서명 크기")
    lines.append("")
    lines.append("| 알고리즘 | 계열 | 보안 레벨 | 서명 크기 | 공개키 크기 | TOAST 발동 |")
    lines.append("|---|---|---|---|---|---|")

    for name, info in ALGORITHMS.items():
        toast = "✅" if info["sig_size"] > 2040 else "❌"
        lines.append(
            f"| {name} | {info['family']} | {info['level']} | "
            f"{info['sig_size']:,} bytes | {info['pk_size']:,} bytes | {toast} |"
        )
    lines.append("")

    # =========================================================================
    # 전략별 결과 요약 (스케일 100만 기준)
    # =========================================================================
    lines.append("## 3. 전략별 성능 비교 (100만 건 기준)")
    lines.append("")

    for strategy in ["A", "B", "C", "D", "E"]:
        strategy_names = {
            "A": "전략 A: Inline Storage (기준선)",
            "B": "전략 B: Separated Table",
            "C": "전략 C: Hash-based Indexing",
            "D": "전략 D: Compressed Storage",
            "E": "전략 E: TOAST External",
        }
        lines.append(f"### {strategy_names[strategy]}")
        lines.append("")
        lines.append("| 알고리즘 | INSERT 처리량(건/초) | 단건조회(ms) | 범위조회(ms) | DELETE(ms) | B+tree 깊이 | 인덱스 크기 |")
        lines.append("|---|---|---|---|---|---|---|")

        # 해당 전략, 100만 건 데이터 필터
        filtered = [r for r in results if r["strategy"] == strategy and r["scale"] == 1_000_000]
        filtered.sort(key=lambda r: r["sig_size"])

        for r in filtered:
            lines.append(
                f"| {r['algorithm']} "
                f"| {r['insert_throughput_rps']:,.0f} "
                f"| {fmt_ms(r['pq_avg_ms'])} "
                f"| {fmt_ms(r['rs_avg_ms'])} "
                f"| {fmt_ms(r['del_avg_ms'])} "
                f"| {int(r['btree_depth'])} "
                f"| {fmt_bytes(int(r['index_size_bytes']))} |"
            )
        lines.append("")

    # =========================================================================
    # 서명 크기별 B+tree 깊이 분석
    # =========================================================================
    lines.append("## 4. 서명 크기와 B+tree 깊이 상관관계")
    lines.append("")
    lines.append("서명 크기가 커질수록 B+tree 깊이가 어떻게 변하는지 분석합니다.")
    lines.append("")
    lines.append("| 알고리즘 | 서명 크기 | 전략A 깊이 | 전략B 깊이 | 전략C 깊이 | 전략D 깊이 | 전략E 깊이 |")
    lines.append("|---|---|---|---|---|---|---|")

    algo_depth = defaultdict(dict)
    for r in results:
        if r["scale"] == 1_000_000:
            algo_depth[r["algorithm"]][r["strategy"]] = int(r["btree_depth"])

    for algo_name in sorted(ALGORITHMS.keys(), key=lambda a: ALGORITHMS[a]["sig_size"]):
        depths = algo_depth.get(algo_name, {})
        sig_size = ALGORITHMS[algo_name]["sig_size"]
        lines.append(
            f"| {algo_name} | {sig_size:,} bytes "
            f"| {depths.get('A', '-')} "
            f"| {depths.get('B', '-')} "
            f"| {depths.get('C', '-')} "
            f"| {depths.get('D', '-')} "
            f"| {depths.get('E', '-')} |"
        )
    lines.append("")

    # =========================================================================
    # 스케일별 성능 변화 (O(log n) 분석)
    # =========================================================================
    lines.append("## 5. 데이터 규모별 성능 변화 (O(log n) 검증)")
    lines.append("")
    lines.append("레코드 수가 10배 증가할 때 성능이 log(10) ≈ 3.32배 증가하면 O(log n) 특성입니다.")
    lines.append("")

    for strategy in ["A", "B", "C", "D", "E"]:
        lines.append(f"### 전략 {strategy}")
        lines.append("")
        lines.append("| 알고리즘 | 10만-단건(ms) | 50만-단건(ms) | 100만-단건(ms) | 성장 비율 |")
        lines.append("|---|---|---|---|---|")

        for algo_name in list(ALGORITHMS.keys())[:5]:  # 대표 5개만 표시
            pq_by_scale = {}
            for r in results:
                if r["algorithm"] == algo_name and r["strategy"] == strategy:
                    pq_by_scale[r["scale"]] = r["pq_avg_ms"]

            v1 = pq_by_scale.get(100_000, 0)
            v5 = pq_by_scale.get(500_000, 0)
            v10 = pq_by_scale.get(1_000_000, 0)
            ratio = f"{v10/v1:.2f}x" if v1 > 0 else "-"

            lines.append(
                f"| {algo_name} "
                f"| {fmt_ms(v1)} "
                f"| {fmt_ms(v5)} "
                f"| {fmt_ms(v10)} "
                f"| {ratio} |"
            )
        lines.append("")

    # =========================================================================
    # 저장 공간 분석
    # =========================================================================
    lines.append("## 6. 저장 공간 분석")
    lines.append("")
    lines.append("| 알고리즘 | 전략 | 테이블 크기 | 인덱스 크기 | TOAST 크기 | 합계 |")
    lines.append("|---|---|---|---|---|---|")

    space_data = [r for r in results if r["scale"] == 1_000_000]
    space_data.sort(key=lambda r: (r["algorithm"], r["strategy"]))

    for r in space_data:
        total = int(r["table_size_bytes"]) + int(r["index_size_bytes"]) + int(r["toast_size_bytes"])
        lines.append(
            f"| {r['algorithm']} | {r['strategy']} "
            f"| {fmt_bytes(int(r['table_size_bytes']))} "
            f"| {fmt_bytes(int(r['index_size_bytes']))} "
            f"| {fmt_bytes(int(r['toast_size_bytes']))} "
            f"| {fmt_bytes(total)} |"
        )
    lines.append("")

    # =========================================================================
    # 결론 및 권장 전략
    # =========================================================================
    lines.append("## 7. 결론 및 권장 전략")
    lines.append("")
    lines.append("> **참고**: 아래 결론은 실험 데이터를 기반으로 자동 생성되었습니다.")
    lines.append("> 논문 작성 시 상세 분석 및 해석을 추가하세요.")
    lines.append("")

    # 전략별 평균 단건 조회 성능 계산
    strategy_perf = defaultdict(list)
    for r in results:
        if r["scale"] == 1_000_000 and r["pq_avg_ms"] > 0:
            strategy_perf[r["strategy"]].append(r["pq_avg_ms"])

    strategy_avg = {
        s: sum(v)/len(v) for s, v in strategy_perf.items() if v
    }
    best_strategy = min(strategy_avg, key=strategy_avg.get) if strategy_avg else "N/A"

    strategy_desc = {
        "A": "Inline Storage",
        "B": "Separated Table",
        "C": "Hash-based Indexing",
        "D": "Compressed Storage",
        "E": "TOAST External",
    }

    lines.append("### 전략별 평균 단건 조회 성능 순위 (100만 건)")
    lines.append("")
    lines.append("| 순위 | 전략 | 평균 단건 조회 지연시간 |")
    lines.append("|---|---|---|")

    for rank, (s, avg) in enumerate(sorted(strategy_avg.items(), key=lambda x: x[1]), 1):
        lines.append(f"| {rank} | 전략 {s} ({strategy_desc.get(s, '')}) | {fmt_ms(avg)} |")

    lines.append("")
    lines.append(f"### 종합 권장 전략: **전략 {best_strategy}** ({strategy_desc.get(best_strategy, '')})")
    lines.append("")
    lines.append("각 알고리즘 계열별 권장 전략:")
    lines.append("")

    for family in ["aimer", "haetae", "ml-dsa", "faest", "sphincs"]:
        family_data = [r for r in results
                       if r["family"] == family and r["scale"] == 1_000_000]
        if not family_data:
            continue

        family_strategy_perf = defaultdict(list)
        for r in family_data:
            if r["pq_avg_ms"] > 0:
                family_strategy_perf[r["strategy"]].append(r["pq_avg_ms"])

        family_avg = {s: sum(v)/len(v) for s, v in family_strategy_perf.items() if v}
        best = min(family_avg, key=family_avg.get) if family_avg else "N/A"

        lines.append(f"- **{family.upper()}**: 전략 {best} ({strategy_desc.get(best, '')}) 권장")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"*리포트 생성: `python report.py`로 자동 생성됨*")

    return "\n".join(lines)


def main():
    print("리포트 생성 중...")
    results = load_results()

    if not results:
        return

    report_text = generate_report(results)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(REPORT_MD, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"리포트 생성 완료: {REPORT_MD}")
    print(f"총 {len(results)}개 실험 결과 분석됨")


if __name__ == "__main__":
    main()

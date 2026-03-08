# =============================================================================
# main_postgresql.py - PostgreSQL 실험 자동 실행기 (실제 서명 방식)
# =============================================================================
# 알고리즘 × 전략 × 스케일 조합을 순차 실행합니다.
#
# 실행 방법:
#   .venv/bin/python main_postgresql.py            → 전체 실험
#   .venv/bin/python main_postgresql.py --resume   → 중단된 실험 이어서 실행
#   .venv/bin/python main_postgresql.py --algo ml-dsa-44 --strategy A
#                                                  → 단일 실험
#
# 특징:
#   - 서명 풀(pool) 방식: 사전 생성된 서명을 순환 사용 → DB 성능만 측정
#   - 완료된 실험은 CSV에 기록되어 중단 후 재실행 시 건너뜀 (resume 기능)
#   - 각 실험 후 테이블 DROP으로 디스크 공간 자동 반환
# =============================================================================

import os
import sys
import csv
import argparse
from datetime import datetime

from config import ALGORITHMS, STRATEGIES, SCALES, RESULTS_CSV, RESULTS_DIR, SIG_POOL_SIZE
from data.generator import build_sig_pool, generate_records
from db.postgresql.connection import get_connection, setup_extensions, execute
from metrics.pg_collector import collect_all, reset_stats

import db.postgresql.strategy_a as strategy_a
import db.postgresql.strategy_b as strategy_b
import db.postgresql.strategy_c as strategy_c
import db.postgresql.strategy_d as strategy_d
import db.postgresql.strategy_e as strategy_e

from benchmark.insert      import run as run_insert
from benchmark.point_query import run as run_point_query
from benchmark.range_scan  import run as run_range_scan
from benchmark.delete      import run as run_delete

STRATEGY_MODULES = {
    "A": strategy_a, "B": strategy_b, "C": strategy_c,
    "D": strategy_d, "E": strategy_e,
}

# PostgreSQL TOAST 임계값 (2,040B 초과 시 TOAST 발동)
TOAST_THRESHOLD = 2_040

CSV_HEADERS = [
    "db_type", "algorithm", "family", "level", "strategy", "scale",
    "sig_size", "pk_size",
    "overflow_expected", "overflow_pages_est",
    "insert_total_sec", "insert_avg_ms", "insert_throughput_rps",
    "pq_avg_ms", "pq_min_ms", "pq_max_ms", "pq_throughput_qps",
    "rs_avg_ms", "rs_min_ms", "rs_max_ms", "rs_avg_results",
    "del_avg_ms", "del_min_ms", "del_max_ms", "del_throughput_dps",
    "btree_depth", "index_size_bytes", "table_size_bytes",
    "toast_size_bytes", "index_page_count",
    "cache_hit_ratio", "heap_reads", "heap_hits", "idx_reads", "idx_hits",
    "timestamp",
]


def load_completed() -> set:
    completed = set()
    if not os.path.exists(RESULTS_CSV):
        return completed
    with open(RESULTS_CSV, "r") as f:
        for row in csv.DictReader(f):
            completed.add((row["algorithm"], row["strategy"], row["scale"]))
    return completed


def save_result(result: dict):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    file_exists = os.path.exists(RESULTS_CSV)
    with open(RESULTS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(result)


def run_single_experiment(conn, algorithm_name: str, strategy_key: str,
                          scale: int, sig_pool: list) -> dict:
    """
    단일 실험 1회를 실행합니다.
    (테이블 생성 → INSERT → ANALYZE → 측정 → DROP)
    """
    algo_info       = ALGORITHMS[algorithm_name]
    strategy_module = STRATEGY_MODULES[strategy_key]

    print(f"\n{'='*60}")
    print(f"실험: {algorithm_name} | 전략 {strategy_key} | {scale:,}건 | PostgreSQL")
    print(f"{'='*60}")

    # 1. 테이블 생성
    strategy_module.create_table(conn)
    print("  테이블 생성 완료")

    # 2. 통계 초기화
    reset_stats(conn)

    # 3. INSERT 벤치마크 (서명 풀에서 순환 사용)
    records_gen  = generate_records(algorithm_name, scale, sig_pool=sig_pool)
    insert_result = run_insert(conn, strategy_module, algorithm_name, records_gen)
    inserted_ids  = insert_result.pop("inserted_ids")

    # 4. ANALYZE
    main_table = (strategy_module.TABLE_NAME
                  if hasattr(strategy_module, "TABLE_NAME")
                  else strategy_module.MAIN_TABLE)
    execute(conn, f"ANALYZE {main_table};")
    print("  ANALYZE 완료")

    # 5. 메트릭 수집
    index_name = strategy_module.get_index_name(conn)
    metrics    = collect_all(conn, main_table, index_name)

    # 6. Point Query
    pq_result = run_point_query(conn, strategy_module, inserted_ids)

    # 7. Range Scan
    rs_result = run_range_scan(conn, strategy_module, algorithm_name, scale)

    # 8. Delete
    del_result = run_delete(conn, strategy_module, inserted_ids)

    # 9. DROP
    strategy_module.drop_table(conn)
    print("  테이블 삭제 완료")

    # 결과 통합
    sig_size = algo_info["sig_size"]
    overflow_expected  = sig_size > TOAST_THRESHOLD
    overflow_pages_est = (max(0, (sig_size - TOAST_THRESHOLD) // 8192 + 1)
                          if overflow_expected else 0)

    return {
        "db_type":   "PostgreSQL",
        "algorithm": algorithm_name,
        "family":    algo_info["family"],
        "level":     algo_info["level"],
        "strategy":  strategy_key,
        "scale":     scale,
        "sig_size":  sig_size,
        "pk_size":   algo_info["pk_size"],
        "overflow_expected":  overflow_expected,
        "overflow_pages_est": overflow_pages_est,
        "insert_total_sec":      insert_result["total_time_sec"],
        "insert_avg_ms":         insert_result["avg_latency_ms"],
        "insert_throughput_rps": insert_result["throughput_rps"],
        "pq_avg_ms":       pq_result["avg_latency_ms"],
        "pq_min_ms":       pq_result["min_latency_ms"],
        "pq_max_ms":       pq_result["max_latency_ms"],
        "pq_throughput_qps": pq_result["throughput_qps"],
        "rs_avg_ms":      rs_result["avg_latency_ms"],
        "rs_min_ms":      rs_result["min_latency_ms"],
        "rs_max_ms":      rs_result["max_latency_ms"],
        "rs_avg_results": rs_result["avg_result_count"],
        "del_avg_ms":         del_result["avg_latency_ms"],
        "del_min_ms":         del_result["min_latency_ms"],
        "del_max_ms":         del_result["max_latency_ms"],
        "del_throughput_dps": del_result["throughput_dps"],
        "btree_depth":      metrics["btree_depth"],
        "index_size_bytes": metrics["index_size_bytes"],
        "table_size_bytes": metrics["table_size_bytes"],
        "toast_size_bytes": metrics["toast_size_bytes"],
        "index_page_count": metrics["index_page_count"],
        "cache_hit_ratio":  metrics["cache_hit_ratio"],
        "heap_reads":  metrics["heap_reads"],
        "heap_hits":   metrics["heap_hits"],
        "idx_reads":   metrics["idx_reads"],
        "idx_hits":    metrics["idx_hits"],
        "timestamp":   datetime.now().isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description="PQC DB 성능 실험 - PostgreSQL")
    parser.add_argument("--resume",   action="store_true", help="중단된 실험 이어서 실행")
    parser.add_argument("--algo",     type=str, help="단일 알고리즘만 실험")
    parser.add_argument("--family",   type=str, help="계열만 실험 (classical/aimer/haetae/ml-dsa/sphincs)")
    parser.add_argument("--strategy", type=str, help="단일 전략만 실험")
    parser.add_argument("--pool-size", type=int, default=SIG_POOL_SIZE,
                        help=f"서명 풀 크기 (기본값: {SIG_POOL_SIZE})")
    args = parser.parse_args()

    # 실험 대상 결정
    if args.algo:
        algos = [args.algo]
    elif args.family:
        algos = [k for k, v in ALGORITHMS.items() if v["family"] == args.family]
    else:
        algos = list(ALGORITHMS.keys())
    strategies = [args.strategy] if args.strategy else STRATEGIES

    total = len(algos) * len(strategies) * len(SCALES)
    print(f"PQC DB 성능 실험 - PostgreSQL (실제 서명 방식)")
    print(f"총 실험: {len(algos)} 알고리즘 × {len(strategies)} 전략 × {len(SCALES)} 스케일 = {total}회")
    print(f"서명 풀 크기: {args.pool_size}개")

    completed = load_completed() if args.resume else set()
    if completed:
        print(f"이미 완료된 실험: {len(completed)}회 (건너뜀)")

    conn = get_connection()
    setup_extensions(conn)

    done = 0
    skipped = 0

    for algorithm_name in algos:
        # 알고리즘별 서명 풀 생성 (전략/스케일 루프 전에 한 번만)
        sig_pool = build_sig_pool(algorithm_name, pool_size=args.pool_size)

        for strategy_key in strategies:
            for scale in SCALES:
                exp_key = (algorithm_name, strategy_key, str(scale))
                if exp_key in completed:
                    skipped += 1
                    continue

                try:
                    result = run_single_experiment(
                        conn, algorithm_name, strategy_key, scale, sig_pool
                    )
                    save_result(result)
                    done += 1
                    progress = (done + skipped) / total * 100
                    print(f"\n진행률: {progress:.1f}% ({done + skipped}/{total})\n")

                except Exception as e:
                    print(f"\n[오류] {algorithm_name} / 전략{strategy_key} / {scale:,}건: {e}")
                    import traceback; traceback.print_exc()
                    try:
                        conn.rollback()
                        STRATEGY_MODULES[strategy_key].drop_table(conn)
                    except Exception:
                        pass

    conn.close()
    print(f"\n{'='*60}")
    print(f"완료! 결과: {RESULTS_CSV}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

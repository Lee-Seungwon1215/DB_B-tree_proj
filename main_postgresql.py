# =============================================================================
# main_postgresql.py - PostgreSQL 실험 자동 실행기 (실제 서명 방식)
# =============================================================================
# 알고리즘 × 전략 × 스케일 조합을 순차 실행하고 결과를 터미널에 출력합니다.
#
# 실행 방법:
#   .venv/bin/python main_postgresql.py                        → 전체 실험
#   .venv/bin/python main_postgresql.py --algo ml-dsa-44 --strategy A
#   .venv/bin/python main_postgresql.py --family classical
# =============================================================================

import sys
import argparse
from datetime import datetime

from config import ALGORITHMS, STRATEGIES, SCALES, SIG_POOL_SIZE
from data.generator import build_sig_pool, generate_records
from db.postgresql.connection import get_connection, setup_extensions, execute
from metrics.pg_collector import collect_all, reset_stats, get_index_size_bytes

import db.postgresql.strategy_a as strategy_a
import db.postgresql.strategy_b as strategy_b
import db.postgresql.strategy_c as strategy_c
import db.postgresql.strategy_d as strategy_d
import db.postgresql.strategy_e as strategy_e

from benchmark.insert        import run as run_insert
from benchmark.point_query   import run as run_point_query
from benchmark.range_scan    import run as run_range_scan
from benchmark.delete        import run as run_delete
from benchmark.sig_hash_query import run as run_sig_hash_query

STRATEGY_MODULES = {
    "A": strategy_a, "B": strategy_b, "C": strategy_c,
    "D": strategy_d, "E": strategy_e,
}

# PostgreSQL TOAST 임계값 (2,040B 초과 시 TOAST 발동)
TOAST_THRESHOLD = 2_040


def print_result(result: dict):
    """실험 진행 중 단건 결과를 간략히 출력합니다."""
    shq = (f"  SIG-HASH: {result['shq_avg_ms']:.3f}ms\n" if result['shq_avg_ms'] is not None else "")
    print(f"\n{'─'*60}")
    print(f"  결과: {result['algorithm']} | 전략 {result['strategy']} | {result['scale']:,}건")
    print(f"{'─'*60}")
    print(f"  INSERT  : {result['insert_throughput_rps']:>10,.0f} rps  (avg {result['insert_avg_ms']:.3f}ms)")
    print(f"  POINT Q : {result['pq_avg_ms']:.3f}ms")
    print(shq, end="")
    print(f"  RANGE   : {result['rs_avg_ms']:.3f}ms  (avg {result['rs_avg_results']:.0f}건)")
    print(f"  DELETE  : {result['del_avg_ms']:.3f}ms")
    print(f"  B+tree 깊이: {result['btree_depth']}  |  TOAST: {result['toast_size_bytes']:,}B  |  캐시: {result['cache_hit_ratio']:.4f}")


def print_markdown_table(results: list):
    """모든 실험 결과를 마크다운 테이블로 출력합니다."""
    print("\n\n" + "="*60)
    print("## PostgreSQL 실험 결과\n")
    print("| Algorithm | Family | Level | Strategy | Sig(B) | INSERT(rps) | PQ(ms) | SHQ(ms) | Range(ms) | Del(ms) | B+tree Depth | Table(MB) | Index(MB) | SHQ-Idx(MB) | TOAST(MB) | Cache Hit |")
    print("|-----------|--------|-------|----------|--------|-------------|--------|---------|-----------|---------|-------------|-----------|-----------|-------------|-----------|-----------|")
    for r in results:
        shq     = f"{r['shq_avg_ms']:.3f}" if r['shq_avg_ms'] is not None else "-"
        shq_idx = (f"{r['sighash_index_size_bytes']/1024/1024:.1f}"
                   if r['sighash_index_size_bytes'] is not None else "-")
        print(
            f"| {r['algorithm']} "
            f"| {r['family']} "
            f"| {r['level']} "
            f"| {r['strategy']} "
            f"| {r['sig_size']:,} "
            f"| {r['insert_throughput_rps']:,.0f} "
            f"| {r['pq_avg_ms']:.3f} "
            f"| {shq} "
            f"| {r['rs_avg_ms']:.3f} "
            f"| {r['del_avg_ms']:.3f} "
            f"| {r['btree_depth']} "
            f"| {r['table_size_bytes']/1024/1024:.1f} "
            f"| {r['index_size_bytes']/1024/1024:.1f} "
            f"| {shq_idx} "
            f"| {r['toast_size_bytes']/1024/1024:.1f} "
            f"| {r['cache_hit_ratio']:.4f} |"
        )
    print()


def run_single_experiment(conn, algorithm_name: str, strategy_key: str,
                          scale: int, sig_pool: list) -> dict:
    """단일 실험 1회 실행 (테이블 생성 → INSERT → ANALYZE → 측정 → DROP)"""
    algo_info       = ALGORITHMS[algorithm_name]
    strategy_module = STRATEGY_MODULES[strategy_key]

    print(f"\n{'='*60}")
    print(f"실험: {algorithm_name} | 전략 {strategy_key} | {scale:,}건 | PostgreSQL")
    print(f"{'='*60}")

    strategy_module.create_table(conn)
    print("  테이블 생성 완료")

    reset_stats(conn)

    records_gen  = generate_records(algorithm_name, scale, sig_pool=sig_pool)
    insert_result = run_insert(conn, strategy_module, algorithm_name, records_gen)
    inserted_ids  = insert_result.pop("inserted_ids")

    main_table = (strategy_module.TABLE_NAME
                  if hasattr(strategy_module, "TABLE_NAME")
                  else strategy_module.MAIN_TABLE)
    execute(conn, f"ANALYZE {main_table};")
    print("  ANALYZE 완료")

    index_name = strategy_module.get_index_name(conn)
    metrics    = collect_all(conn, main_table, index_name)

    sighash_index_size = (get_index_size_bytes(conn, strategy_c.INDEX_SIGHASH)
                          if strategy_key == "C" else None)

    pq_result = run_point_query(conn, strategy_module, inserted_ids)

    if strategy_key == "C":
        shq_result = run_sig_hash_query(conn, strategy_module, inserted_ids)
    else:
        shq_result = {"avg_latency_ms": None, "min_latency_ms": None,
                      "max_latency_ms": None, "throughput_qps": None}

    rs_result  = run_range_scan(conn, strategy_module, algorithm_name, scale)
    del_result = run_delete(conn, strategy_module, inserted_ids)

    strategy_module.drop_table(conn)
    print("  테이블 삭제 완료")

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
        "pq_avg_ms":         pq_result["avg_latency_ms"],
        "pq_min_ms":         pq_result["min_latency_ms"],
        "pq_max_ms":         pq_result["max_latency_ms"],
        "pq_throughput_qps": pq_result["throughput_qps"],
        "shq_avg_ms":         shq_result["avg_latency_ms"],
        "shq_min_ms":         shq_result["min_latency_ms"],
        "shq_max_ms":         shq_result["max_latency_ms"],
        "shq_throughput_qps": shq_result["throughput_qps"],
        "rs_avg_ms":      rs_result["avg_latency_ms"],
        "rs_min_ms":      rs_result["min_latency_ms"],
        "rs_max_ms":      rs_result["max_latency_ms"],
        "rs_avg_results": rs_result["avg_result_count"],
        "del_avg_ms":         del_result["avg_latency_ms"],
        "del_min_ms":         del_result["min_latency_ms"],
        "del_max_ms":         del_result["max_latency_ms"],
        "del_throughput_dps": del_result["throughput_dps"],
        "btree_depth":      metrics["btree_depth"],
        "index_size_bytes":        metrics["index_size_bytes"],
        "sighash_index_size_bytes": sighash_index_size,
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
    parser.add_argument("--algo",      type=str, help="단일 알고리즘만 실험")
    parser.add_argument("--family",    type=str, help="계열만 실험 (classical/aimer/haetae/ml-dsa/sphincs)")
    parser.add_argument("--strategy",  type=str, help="단일 전략만 실험")
    parser.add_argument("--pool-size", type=int, default=SIG_POOL_SIZE,
                        help=f"서명 풀 크기 (기본값: {SIG_POOL_SIZE})")
    args = parser.parse_args()

    if args.algo:
        algos = [args.algo]
    elif args.family:
        algos = [k for k, v in ALGORITHMS.items() if v["family"] == args.family]
    else:
        algos = list(ALGORITHMS.keys())
    def get_strategies(algo_name: str) -> list:
        if args.strategy:
            return [args.strategy]
        if ALGORITHMS[algo_name]["family"] == "classical":
            return ["A"]
        return STRATEGIES

    total = sum(len(get_strategies(a)) * len(SCALES) for a in algos)
    print(f"PQC DB 성능 실험 - PostgreSQL (실제 서명 방식)")
    print(f"총 실험: {total}회 (classical=전략A만, PQC=전략A~E)")
    print(f"서명 풀 크기: {args.pool_size}개")

    conn = get_connection()
    setup_extensions(conn)

    done = 0
    all_results = []

    for algorithm_name in algos:
        sig_pool = build_sig_pool(algorithm_name, pool_size=args.pool_size)

        for strategy_key in get_strategies(algorithm_name):
            for scale in SCALES:
                try:
                    result = run_single_experiment(
                        conn, algorithm_name, strategy_key, scale, sig_pool
                    )
                    print_result(result)
                    all_results.append(result)
                    done += 1
                    print(f"\n진행률: {done/total*100:.1f}% ({done}/{total})\n")

                except Exception as e:
                    print(f"\n[오류] {algorithm_name} / 전략{strategy_key} / {scale:,}건: {e}")
                    import traceback; traceback.print_exc()
                    try:
                        conn.rollback()
                        STRATEGY_MODULES[strategy_key].drop_table(conn)
                    except Exception:
                        pass

    conn.close()
    print_markdown_table(all_results)
    print(f"{'='*60}")
    print(f"완료! 총 {done}회 실험")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

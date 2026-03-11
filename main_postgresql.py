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

from config import ALGORITHMS, SCALES, SIG_POOL_SIZE
from data.generator import build_sig_pool, generate_records
from db.postgresql.connection import get_connection, setup_extensions, execute
from metrics.pg_collector import collect_all, reset_stats

import db.postgresql.strategy_a as strategy_a
import db.postgresql.strategy_b as strategy_b #수직 파티셔닝 추가햇어요

from benchmark.insert        import run as run_insert
from benchmark.single_insert import run as run_single_insert
from benchmark.point_query   import run as run_point_query
from benchmark.range_scan    import run as run_range_scan
from benchmark.update        import run as run_update
from benchmark.delete        import run as run_delete
from benchmark.range_delete  import run as run_range_delete

STRATEGY_MODULES = {"A": strategy_a, "B": strategy_b} #B전략 추가

# PostgreSQL TOAST 임계값 (2,040B 초과 시 TOAST 발동)
TOAST_THRESHOLD = 2_040

# 유효한 벤치마크 이름
VALID_BENCHES    = {"si", "bulk", "pq", "range", "update", "del", "rdel"}
# 1M 데이터가 필요한 벤치마크
BENCH_NEEDS_DATA = {"pq", "range", "update", "del", "rdel"}


def print_result(result: dict):
    """실험 진행 중 단건 결과를 간략히 출력합니다."""
    print(f"\n{'─'*60}")
    print(f"  결과: {result['algorithm']} | 전략 {result['strategy']} | {result['scale']:,}건")
    print(f"{'─'*60}")
    print(f"  범위INS : {result['insert_throughput_rps']:>10,.0f} rps  (avg {result['insert_avg_ms']:.3f}ms)")
    print(f"  단건INS : {result['si_avg_ms']:.3f}ms")
    print(f"  단건 PQ : {result['pq_avg_ms']:.3f}ms")
    print(f"  RANGE   : {result['rs_avg_ms']:.3f}ms  (avg {result['rs_avg_results']:.0f}건)")
    print(f"  UPDATE  : {result['upd_avg_ms']:.3f}ms")
    print(f"  단건DEL : {result['del_avg_ms']:.3f}ms")
    print(f"  범위DEL : {result['rd_per_record_ms']:.3f}ms/건")
    print(f"  B+tree 깊이: {result['btree_depth']}  |  TOAST: {result['toast_size_bytes']:,}B  |  캐시: {result['cache_hit_ratio']:.4f}")


def print_markdown_table(results: list):
    """모든 실험 결과를 마크다운 테이블로 출력합니다."""
    print("\n\n" + "="*60)
    print("## PostgreSQL 실험 결과\n")
    print("| Algorithm | Family | Level | Strategy | Sig(B) | 범위INS(rps) | 단건INS(ms) | PQ(ms) | Range(ms) | Update(ms) | 단건DEL(ms) | 범위DEL(ms/건) | B+tree Depth | Table(MB) | TOAST(MB) | Cache Hit |")
    print("|-----------|--------|-------|----------|--------|-------------|------------|--------|-----------|------------|------------|--------------|-------------|-----------|-----------|-----------|")
    for r in results:
        print(
            f"| {r['algorithm']} "
            f"| {r['family']} "
            f"| {r['level']} "
            f"| {r['strategy']} "
            f"| {r['sig_size']:,} "
            f"| {r['insert_throughput_rps']:,.0f} "
            f"| {r['si_avg_ms']:.3f} "
            f"| {r['pq_avg_ms']:.3f} "
            f"| {r['rs_avg_ms']:.3f} "
            f"| {r['upd_avg_ms']:.3f} "
            f"| {r['del_avg_ms']:.3f} "
            f"| {r['rd_per_record_ms']:.3f} "
            f"| {r['btree_depth']} "
            f"| {r['table_size_bytes']/1024/1024:.1f} "
            f"| {r['toast_size_bytes']/1024/1024:.1f} "
            f"| {r['cache_hit_ratio']:.4f} |"
        )
    print()


def run_single_experiment(conn, algorithm_name: str, strategy_key: str,
                          scale: int, sig_pool: list,
                          benches: set = None) -> dict:
    """단일 실험 1회 실행 (테이블 생성 → INSERT → ANALYZE → 측정 → DROP)"""
    if benches is None:
        benches = VALID_BENCHES

    needs_data = bool(benches & BENCH_NEEDS_DATA)
    needs_bulk = "bulk" in benches or needs_data

    algo_info       = ALGORITHMS[algorithm_name]
    strategy_module = STRATEGY_MODULES[strategy_key]

    print(f"\n{'='*60}")
    print(f"실험: {algorithm_name} | 전략 {strategy_key} | {scale:,}건 | PostgreSQL")
    print(f"벤치마크: {', '.join(sorted(benches))}")
    print(f"{'='*60}")

    strategy_module.create_table(conn)
    print("  테이블 생성 완료")

    reset_stats(conn)

    # 단건INS: 빈 테이블에서 먼저 측정 후 정리
    if "si" in benches:
        si_result = run_single_insert(conn, strategy_module, algorithm_name, sig_pool)
    else:
        si_result = {"avg_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0, "throughput_rps": 0}

    inserted_ids = []
    if needs_bulk:
        records_gen   = generate_records(algorithm_name, scale, sig_pool=sig_pool)
        insert_result = run_insert(conn, strategy_module, algorithm_name, records_gen)
        inserted_ids  = insert_result.pop("inserted_ids")
    else:
        insert_result = {"total_time_sec": 0, "avg_latency_ms": 0, "throughput_rps": 0}

    if needs_data:
        main_table = strategy_module.TABLE_NAME
        execute(conn, f"ANALYZE {main_table};")
        print("  ANALYZE 완료")
        index_name = strategy_module.get_index_name(conn)
        metrics    = collect_all(conn, main_table, index_name)
    else:
        metrics = {"btree_depth": 0, "index_size_bytes": 0, "table_size_bytes": 0,
                   "toast_size_bytes": 0, "index_page_count": 0, "cache_hit_ratio": 0,
                   "heap_reads": 0, "heap_hits": 0, "idx_reads": 0, "idx_hits": 0}

    pq_result  = run_point_query(conn, strategy_module, inserted_ids)  if "pq"     in benches and inserted_ids else {"avg_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0, "throughput_qps": 0}
    upd_result = run_update(conn, strategy_module, inserted_ids, sig_pool) if "update" in benches and inserted_ids else {"avg_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0, "throughput_ups": 0}
    rs_result  = run_range_scan(conn, strategy_module, algorithm_name, scale) if "range"  in benches and inserted_ids else {"avg_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0, "avg_result_count": 0}
    del_result = run_delete(conn, strategy_module, inserted_ids)         if "del"    in benches and inserted_ids else {"avg_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0, "throughput_dps": 0}
    rd_result  = run_range_delete(conn, strategy_module, inserted_ids)   if "rdel"   in benches and inserted_ids else {"per_record_ms": 0, "throughput_dps": 0}

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
        "si_avg_ms":         si_result["avg_latency_ms"],
        "si_min_ms":         si_result["min_latency_ms"],
        "si_max_ms":         si_result["max_latency_ms"],
        "si_throughput_rps": si_result["throughput_rps"],
        "pq_avg_ms":         pq_result["avg_latency_ms"],
        "pq_min_ms":         pq_result["min_latency_ms"],
        "pq_max_ms":         pq_result["max_latency_ms"],
        "pq_throughput_qps": pq_result["throughput_qps"],
        "upd_avg_ms":         upd_result["avg_latency_ms"],
        "upd_min_ms":         upd_result["min_latency_ms"],
        "upd_max_ms":         upd_result["max_latency_ms"],
        "upd_throughput_ups": upd_result["throughput_ups"],
        "rs_avg_ms":      rs_result["avg_latency_ms"],
        "rs_min_ms":      rs_result["min_latency_ms"],
        "rs_max_ms":      rs_result["max_latency_ms"],
        "rs_avg_results": rs_result["avg_result_count"],
        "del_avg_ms":         del_result["avg_latency_ms"],
        "del_min_ms":         del_result["min_latency_ms"],
        "del_max_ms":         del_result["max_latency_ms"],
        "del_throughput_dps": del_result["throughput_dps"],
        "rd_per_record_ms":   rd_result["per_record_ms"],
        "rd_throughput_dps":  rd_result["throughput_dps"],
        "btree_depth":      metrics["btree_depth"],
        "index_size_bytes":  metrics["index_size_bytes"],
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
    parser.add_argument("--level",     type=int, help="보안 레벨만 실험 (1/3/5)")
    parser.add_argument("--strategy",  type=str, help="단일 전략만 실험")
    parser.add_argument("--pool-size", type=int, default=SIG_POOL_SIZE,
                        help=f"서명 풀 크기 (기본값: {SIG_POOL_SIZE})")
    parser.add_argument("--bench", type=str, default=None,
                        help="실행할 벤치마크 (쉼표 구분, 기본=전체). "
                             "선택: si,bulk,pq,range,update,del,rdel")
    args = parser.parse_args()

    if args.bench:
        benches = set(b.strip() for b in args.bench.split(","))
        invalid = benches - VALID_BENCHES
        if invalid:
            print(f"[오류] 유효하지 않은 벤치마크: {invalid}")
            print(f"       사용 가능: {', '.join(sorted(VALID_BENCHES))}")
            sys.exit(1)
    else:
        benches = VALID_BENCHES

    if args.algo:
        algos = [args.algo]
    elif args.family and args.level:
        algos = [k for k, v in ALGORITHMS.items() if v["family"] == args.family and v["level"] == args.level]
    elif args.family:
        algos = [k for k, v in ALGORITHMS.items() if v["family"] == args.family]
    elif args.level:
        algos = [k for k, v in ALGORITHMS.items() if v["level"] == args.level]
    else:
        algos = list(ALGORITHMS.keys())
    def get_strategies(_) -> list:
        if args.strategy:
            return [args.strategy]
        return list(STRATEGY_MODULES.keys())

    total = sum(len(get_strategies(a)) * len(SCALES) for a in algos)
    print(f"PQC DB 성능 실험 - PostgreSQL (실제 서명 방식)")
    print(f"총 실험: {total}회 (전략 A)")
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
                        conn, algorithm_name, strategy_key, scale, sig_pool, benches
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

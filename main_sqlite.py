# =============================================================================
# main_sqlite.py - SQLite 실험 자동 실행기 (실제 서명 방식)
# =============================================================================
# 알고리즘 × 전략 × 스케일 조합을 SQLite에서 실행하고 결과를 터미널에 출력합니다.
#
# 실행 방법:
#   .venv/bin/python main_sqlite.py                        → 전체 실험
#   .venv/bin/python main_sqlite.py --algo ecdsa-256 --strategy A
#   .venv/bin/python main_sqlite.py --family classical
# =============================================================================

import os
import time
import random
import hashlib
import sqlite3
import argparse
from datetime import datetime

from config import (
    ALGORITHMS, SCALES, SIG_POOL_SIZE,
    POINT_QUERY_COUNT, RANGE_SCAN_COUNT, DELETE_COUNT, UPDATE_COUNT,
    SINGLE_INSERT_COUNT, RANGE_DELETE_COUNT, RANGE_DELETE_REPEAT, RANGE_RATIO,
)
from data.generator import build_sig_pool

import db.sqlite.strategy_a as strategy_a
import db.sqlite.strategy_b as strategy_b
import db.sqlite.strategy_c as strategy_c
import db.sqlite.strategy_d as strategy_d

# =============================================================================
# 설정
# =============================================================================

# 전략별 모듈: A=4KB 인라인, B=64KB 인라인, C=4KB 수직 파티셔닝
STRATEGY_MODULES = {
    "A": strategy_a,
    "B": strategy_b,
    "C": strategy_c,
    "D": strategy_d,
}

SQLITE_DB_PATH    = "sqlite_benchmark.db"
INSERT_BATCH_SIZE = 500

VALID_BENCHES    = {"si", "bulk", "pq", "range", "update", "del", "rdel"}
BENCH_NEEDS_DATA = {"pq", "range", "update", "del", "rdel"}

# =============================================================================
# 테이블 이름
# =============================================================================

def tname(algo: str, strategy: str) -> str:
    safe = algo.replace("-", "_").replace("+", "p")
    return f"sq_{safe}_{strategy}"

# =============================================================================
# 벤치마크
# =============================================================================

def measure_point_query(conn, table: str, mod, entity_ids: list) -> dict:
    sample = random.choices(entity_ids, k=POINT_QUERY_COUNT)
    latencies = []

    for eid in sample:
        t0 = time.perf_counter()
        mod.point_query(conn, table, eid)
        latencies.append((time.perf_counter() - t0) * 1000)

    avg = sum(latencies) / len(latencies)
    return {
        "pq_avg_ms":         round(avg, 4),
        "pq_min_ms":         round(min(latencies), 4),
        "pq_max_ms":         round(max(latencies), 4),
        "pq_throughput_qps": round(1000 / avg, 2) if avg > 0 else 0,
    }


def measure_range_scan(conn, table: str, mod, scale: int) -> dict:
    limit = int(scale * RANGE_RATIO)
    latencies = []
    result_counts = []

    for _ in range(RANGE_SCAN_COUNT):
        offset = random.randint(0, max(0, scale - limit))
        t0 = time.perf_counter()
        rows = mod.range_scan(conn, table, limit, offset)
        latencies.append((time.perf_counter() - t0) * 1000)
        result_counts.append(len(rows))

    return {
        "rs_avg_ms":      round(sum(latencies) / len(latencies), 4),
        "rs_min_ms":      round(min(latencies), 4),
        "rs_max_ms":      round(max(latencies), 4),
        "rs_avg_results": round(sum(result_counts) / len(result_counts), 2),
    }


def measure_update(conn, table: str, mod, entity_ids: list, sig_pool: list) -> dict:
    targets = random.sample(entity_ids, min(UPDATE_COUNT, len(entity_ids)))
    pool_sz  = len(sig_pool)
    latencies = []

    print(f"  [UPDATE] {len(targets)}건 서명 갱신 시작...")

    for i, eid in enumerate(targets):
        pk, sig, _ = sig_pool[i % pool_sz]
        t0 = time.perf_counter()
        mod.update_record(conn, table, eid, pk, sig)
        latencies.append((time.perf_counter() - t0) * 1000)

    avg = sum(latencies) / len(latencies)
    print(f"  [UPDATE] 완료: 평균 {avg:.3f}ms")

    return {
        "upd_avg_ms":         round(avg, 4),
        "upd_min_ms":         round(min(latencies), 4),
        "upd_max_ms":         round(max(latencies), 4),
        "upd_throughput_ups": round(len(targets) / (sum(latencies) / 1000), 2),
    }


def measure_single_insert(conn, table: str, mod, sig_pool: list) -> dict:
    count   = min(SINGLE_INSERT_COUNT, len(sig_pool))
    pool_sz = len(sig_pool)
    latencies = []

    print(f"  [SINGLE INSERT] {count}건 단건 삽입 시작...")

    for i in range(count):
        pk, sig, _ = sig_pool[i % pool_sz]
        entity_id  = f"__si_{i}__"
        t0 = time.perf_counter()
        mod.insert_single(conn, table, entity_id, pk, sig)
        latencies.append((time.perf_counter() - t0) * 1000)

    avg = sum(latencies) / len(latencies)
    print(f"  [SINGLE INSERT] 완료: 평균 {avg:.3f}ms")

    # 측정용 레코드 정리
    main_table = mod.get_main_table(table)
    conn.execute(f"DELETE FROM {main_table} WHERE entity_id LIKE '__si_%'")
    conn.commit()
    print(f"  [SINGLE INSERT] 임시 레코드 {count}건 정리 완료")

    return {
        "si_avg_ms":         round(avg, 4),
        "si_min_ms":         round(min(latencies), 4),
        "si_max_ms":         round(max(latencies), 4),
        "si_throughput_rps": round(1000 / avg, 2) if avg > 0 else 0,
    }


def measure_range_delete(conn, table: str, mod, entity_ids: list) -> dict:
    needed = RANGE_DELETE_COUNT * RANGE_DELETE_REPEAT
    repeat = RANGE_DELETE_REPEAT if len(entity_ids) >= needed else max(1, len(entity_ids) // RANGE_DELETE_COUNT)
    count  = RANGE_DELETE_COUNT

    pool = random.sample(entity_ids, min(count * repeat, len(entity_ids)))

    print(f"  [RANGE DELETE] {count}건 × {repeat}회 일괄 삭제 시작...")

    per_record_latencies = []

    for i in range(repeat):
        targets = pool[i * count : (i + 1) * count]
        t0 = time.perf_counter()
        mod.range_delete_records(conn, table, targets)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        per_record_latencies.append(elapsed_ms / len(targets))

    avg_per_record_ms = sum(per_record_latencies) / len(per_record_latencies)
    throughput_dps    = 1000 / avg_per_record_ms if avg_per_record_ms > 0 else 0

    print(f"  [RANGE DELETE] 완료: 건당 평균 {avg_per_record_ms:.4f}ms ({repeat}회 평균)")

    return {
        "rd_per_record_ms":  round(avg_per_record_ms, 4),
        "rd_throughput_dps": round(throughput_dps, 2),
    }


def measure_delete(conn, table: str, mod, entity_ids: list) -> dict:
    targets = random.sample(entity_ids, min(DELETE_COUNT, len(entity_ids)))
    latencies = []

    for eid in targets:
        t0 = time.perf_counter()
        mod.delete_record(conn, table, eid)
        latencies.append((time.perf_counter() - t0) * 1000)

    total_ms = sum(latencies)
    avg = total_ms / len(latencies)
    return {
        "del_avg_ms":         round(avg, 4),
        "del_min_ms":         round(min(latencies), 4),
        "del_max_ms":         round(max(latencies), 4),
        "del_throughput_dps": round(len(targets) / (total_ms / 1000), 2) if total_ms > 0 else 0,
    }

# =============================================================================
# 결과 출력
# =============================================================================

def print_result(result: dict):
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
    print(f"  B+tree 깊이: {result['btree_depth']}  |  overflow: {result['toast_size_bytes']:,}B  |  리프: {result['leaf_page_count']}페이지")


def print_markdown_table(results: list):
    print("\n\n" + "="*60)
    print("## SQLite 실험 결과\n")
    print("| Algorithm | Family | Level | Strategy | Sig(B) | 범위INS(rps) | 단건INS(ms) | PQ(ms) | Range(ms) | Update(ms) | 단건DEL(ms) | 범위DEL(ms/건) | B+tree Depth | Table(MB) | Overflow(MB) | Leaf Pages | Overflow Pages |")
    print("|-----------|--------|-------|----------|--------|-------------|------------|--------|-----------|------------|------------|--------------|-------------|-----------|--------------|------------|----------------|")
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
            f"| {r['leaf_page_count']} "
            f"| {r['overflow_page_count']} |"
        )
    print()

# =============================================================================
# 단일 실험
# =============================================================================

def run_one(algo_name: str, algo_info: dict, strategy: str, scale: int,
            sig_pool: list, benches: set = None) -> dict:
    """단일 실험 실행 (테이블 생성 → INSERT → 측정 → DROP)"""
    if benches is None:
        benches = VALID_BENCHES

    needs_data = bool(benches & BENCH_NEEDS_DATA)
    needs_bulk = "bulk" in benches or needs_data

    mod      = STRATEGY_MODULES[strategy]
    table    = tname(algo_name, strategy)
    pool_sz  = len(sig_pool)
    sig_size = algo_info["sig_size"]

    page_size          = mod.PAGE_SIZE
    overflow_threshold = page_size // 4
    cache_kib          = (page_size // 4096) * 65536   # 4KB→64MB, 64KB→256MB

    print(f"\n{'='*60}")
    print(f"실험: {algo_name} | 전략 {strategy} | {scale:,}건 | SQLite (page={page_size}B)")
    print(f"벤치마크: {', '.join(sorted(benches))}")
    print(f"{'='*60}")

    # page_size는 DB 파일 생성 시에만 적용되므로 매 실험 전 삭제 (WAL/SHM 포함)
    for ext in ["", "-wal", "-shm"]:
        path = SQLITE_DB_PATH + ext
        if os.path.exists(path):
            os.remove(path)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute(f"PRAGMA page_size={page_size}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA cache_size=-{cache_kib}")

    _zero_si  = {"si_avg_ms": 0, "si_min_ms": 0, "si_max_ms": 0, "si_throughput_rps": 0}
    _zero_pq  = {"pq_avg_ms": 0, "pq_min_ms": 0, "pq_max_ms": 0, "pq_throughput_qps": 0}
    _zero_rs  = {"rs_avg_ms": 0, "rs_min_ms": 0, "rs_max_ms": 0, "rs_avg_results": 0}
    _zero_upd = {"upd_avg_ms": 0, "upd_min_ms": 0, "upd_max_ms": 0, "upd_throughput_ups": 0}
    _zero_dl  = {"del_avg_ms": 0, "del_min_ms": 0, "del_max_ms": 0, "del_throughput_dps": 0}
    _zero_rd  = {"rd_per_record_ms": 0, "rd_throughput_dps": 0}
    _zero_m   = {"btree_depth": 0, "index_size_bytes": 0, "table_size_bytes": 0,
                 "toast_size_bytes": 0, "leaf_page_count": 0,
                 "internal_page_count": 0, "overflow_page_count": 0,
                 "page_size": page_size}

    try:
        mod.create_table(conn, table)

        # 단건INS: 빈 테이블에서 먼저 측정 후 정리
        si = measure_single_insert(conn, table, mod, sig_pool) if "si" in benches else _zero_si

        # 범위INS: 1M 레코드 적재
        entity_ids = []
        insert_sec = 0.0
        if needs_bulk:
            t0 = time.perf_counter()
            batch = []
            for i in range(scale):
                pk, sig, _ = sig_pool[i % pool_sz]
                entity_id = hashlib.sha256(f"{algo_name}_{i}".encode()).hexdigest()
                batch.append((entity_id, pk, sig))
                entity_ids.append(entity_id)
                if len(batch) == INSERT_BATCH_SIZE:
                    mod.insert_batch(conn, table, batch)
                    batch = []
            if batch:
                mod.insert_batch(conn, table, batch)
            insert_sec = time.perf_counter() - t0
            print(f"  [INSERT] 완료: {scale:,}건 / {insert_sec:.2f}초")

        if needs_data:
            mod.analyze(conn, table)
            print("  ANALYZE 완료")
            metrics = mod.collect_metrics(conn, table)
        else:
            metrics = _zero_m

        pq  = measure_point_query(conn, table, mod, entity_ids)          if "pq"     in benches and entity_ids else _zero_pq
        rs  = measure_range_scan(conn, table, mod, scale)                if "range"  in benches and entity_ids else _zero_rs
        upd = measure_update(conn, table, mod, entity_ids, sig_pool)     if "update" in benches and entity_ids else _zero_upd
        dl  = measure_delete(conn, table, mod, entity_ids)               if "del"    in benches and entity_ids else _zero_dl
        rd  = measure_range_delete(conn, table, mod, entity_ids)         if "rdel"   in benches and entity_ids else _zero_rd

        mod.drop_table(conn, table)
        print("  테이블 삭제 완료")

    except Exception as e:
        import traceback
        print(f"[오류] {e}")
        traceback.print_exc()
        try:
            mod.drop_table(conn, table)
        except Exception:
            pass
        conn.close()
        raise

    conn.close()

    overflow = sig_size > overflow_threshold
    ov_pages = (max(0, (sig_size - overflow_threshold) // page_size + 1)
                if overflow else 0)

    return {
        "db_type":   "SQLite",
        "algorithm": algo_name,
        "family":    algo_info["family"],
        "level":     algo_info["level"],
        "strategy":  strategy,
        "scale":     scale,
        "sig_size":  sig_size,
        "pk_size":   algo_info["pk_size"],
        "overflow_expected":  overflow,
        "overflow_pages_est": ov_pages,
        "insert_total_sec":      round(insert_sec, 4),
        "insert_avg_ms":         round(insert_sec / scale * 1000, 4) if insert_sec > 0 else 0,
        "insert_throughput_rps": round(scale / insert_sec, 2)        if insert_sec > 0 else 0,
        "si_avg_ms":         si["si_avg_ms"],
        "si_min_ms":         si["si_min_ms"],
        "si_max_ms":         si["si_max_ms"],
        "si_throughput_rps": si["si_throughput_rps"],
        "pq_avg_ms":         pq["pq_avg_ms"],
        "pq_min_ms":         pq["pq_min_ms"],
        "pq_max_ms":         pq["pq_max_ms"],
        "pq_throughput_qps": pq["pq_throughput_qps"],
        "rs_avg_ms":      rs["rs_avg_ms"],
        "rs_min_ms":      rs["rs_min_ms"],
        "rs_max_ms":      rs["rs_max_ms"],
        "rs_avg_results": rs["rs_avg_results"],
        "upd_avg_ms":         upd["upd_avg_ms"],
        "upd_min_ms":         upd["upd_min_ms"],
        "upd_max_ms":         upd["upd_max_ms"],
        "upd_throughput_ups": upd["upd_throughput_ups"],
        "del_avg_ms":         dl["del_avg_ms"],
        "del_min_ms":         dl["del_min_ms"],
        "del_max_ms":         dl["del_max_ms"],
        "del_throughput_dps": dl["del_throughput_dps"],
        "rd_per_record_ms":   rd["rd_per_record_ms"],
        "rd_throughput_dps":  rd["rd_throughput_dps"],
        "btree_depth":           metrics["btree_depth"],
        "index_size_bytes":      metrics["index_size_bytes"],
        "table_size_bytes":      metrics["table_size_bytes"],
        "toast_size_bytes":      metrics["toast_size_bytes"],
        "leaf_page_count":       metrics["leaf_page_count"],
        "internal_page_count":   metrics["internal_page_count"],
        "overflow_page_count":   metrics["overflow_page_count"],
        "page_size":             metrics["page_size"],
        "timestamp":             datetime.now().isoformat(),
    }

# =============================================================================
# 메인
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="PQC DB 성능 실험 - SQLite (실제 서명)")
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
            import sys; sys.exit(1)
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
    print(f"PQC DB 성능 실험 - SQLite (실제 서명 방식)")
    print(f"총 실험: {total}회")
    print(f"서명 풀 크기: {args.pool_size}개")

    done = 0
    start_t = time.perf_counter()
    all_results = []

    for algo_name in algos:
        algo_info = ALGORITHMS[algo_name]
        print(f"\n[알고리즘] {algo_name} (level={algo_info['level']}, sig={algo_info['sig_size']}B)")
        sig_pool = build_sig_pool(algo_name, pool_size=args.pool_size)

        for strategy in get_strategies(algo_name):
            for scale in SCALES:
                try:
                    result = run_one(algo_name, algo_info, strategy, scale, sig_pool, benches)
                    print_result(result)
                    all_results.append(result)
                    done += 1

                    elapsed = time.perf_counter() - start_t
                    eta = (elapsed / done) * (total - done) if done > 0 else 0
                    print(f"\n진행률: {done/total*100:.1f}% ({done}/{total}) | ETA {int(eta//60)}m{int(eta%60)}s\n")

                except Exception as e:
                    print(f"[오류] {algo_name}/{strategy}/{scale}: {e}")

    print_markdown_table(all_results)
    print(f"{'='*60}")
    print(f"완료! 총 {done}회 실험")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

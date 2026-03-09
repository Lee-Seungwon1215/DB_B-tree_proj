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

import time
import random
import hashlib
import zlib
import sqlite3
import argparse
from datetime import datetime

from config import (
    ALGORITHMS, STRATEGIES, SCALES, SIG_POOL_SIZE,
    POINT_QUERY_COUNT, RANGE_SCAN_COUNT, DELETE_COUNT, RANGE_RATIO,
)
from data.generator import build_sig_pool
from metrics.sqlite_collector import collect_all as sqlite_collect_all, get_index_size_bytes as sqlite_get_index_size_bytes

# =============================================================================
# 설정
# =============================================================================

SQLITE_PAGE_SIZE   = 4096
OVERFLOW_THRESHOLD = SQLITE_PAGE_SIZE // 4   # 1,024B
SQLITE_DB_PATH     = "sqlite_benchmark.db"
INSERT_BATCH_SIZE  = 500

# =============================================================================
# 테이블 이름
# =============================================================================

def tname(algo: str, strategy: str) -> str:
    safe = algo.replace("-", "_").replace("+", "p")
    return f"sq_{safe}_{strategy}"

# =============================================================================
# 전략별 테이블 생성 / 삭제
# =============================================================================

def create_table(conn, table: str, strategy: str):
    if strategy == "A":
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id  TEXT NOT NULL,
                public_key BLOB NOT NULL,
                signature  BLOB NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_e ON {table}(entity_id)")

    elif strategy == "B":
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table}_meta (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id  TEXT NOT NULL UNIQUE,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table}_crypto (
                id         INTEGER PRIMARY KEY REFERENCES {table}_meta(id),
                public_key BLOB NOT NULL,
                signature  BLOB NOT NULL
            )""")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_me ON {table}_meta(entity_id)")

    elif strategy == "C":
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id  TEXT NOT NULL,
                sig_hash   BLOB NOT NULL,
                public_key BLOB NOT NULL,
                signature  BLOB NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_h ON {table}(sig_hash)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_e ON {table}(entity_id)")

    elif strategy == "D":
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id     TEXT NOT NULL,
                public_key_c  BLOB NOT NULL,
                signature_c   BLOB NOT NULL,
                sig_orig_size INTEGER NOT NULL,
                created_at    TEXT DEFAULT (datetime('now'))
            )""")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_e ON {table}(entity_id)")

    elif strategy == "E":
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table}_main (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id  TEXT NOT NULL,
                sig_hash   BLOB NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table}_blob (
                id         INTEGER PRIMARY KEY REFERENCES {table}_main(id),
                public_key BLOB NOT NULL,
                signature  BLOB NOT NULL
            )""")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_me ON {table}_main(entity_id)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_sh ON {table}_main(sig_hash)")

    conn.commit()


def drop_table(conn, table: str, strategy: str):
    if strategy == "B":
        conn.execute(f"DROP TABLE IF EXISTS {table}_crypto")
        conn.execute(f"DROP TABLE IF EXISTS {table}_meta")
    elif strategy == "E":
        conn.execute(f"DROP TABLE IF EXISTS {table}_blob")
        conn.execute(f"DROP TABLE IF EXISTS {table}_main")
    else:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()

# =============================================================================
# 전략별 배치 INSERT
# =============================================================================

def insert_batch(conn, table: str, strategy: str, batch: list):
    """batch: list of (entity_id, public_key, signature)"""
    if strategy == "A":
        conn.executemany(
            f"INSERT INTO {table}(entity_id,public_key,signature) VALUES(?,?,?)", batch)

    elif strategy == "B":
        cur = conn.cursor()
        for entity_id, pk, sig in batch:
            cur.execute(f"INSERT INTO {table}_meta(entity_id) VALUES(?)", (entity_id,))
            rid = cur.lastrowid
            cur.execute(
                f"INSERT INTO {table}_crypto(id,public_key,signature) VALUES(?,?,?)",
                (rid, pk, sig))

    elif strategy == "C":
        hashed = [(e, hashlib.sha256(s).digest(), pk, s) for e, pk, s in batch]
        conn.executemany(
            f"INSERT INTO {table}(entity_id,sig_hash,public_key,signature) VALUES(?,?,?,?)",
            hashed)

    elif strategy == "D":
        compressed = [(e, zlib.compress(pk, 1), zlib.compress(s, 1), len(s))
                      for e, pk, s in batch]
        conn.executemany(
            f"INSERT INTO {table}(entity_id,public_key_c,signature_c,sig_orig_size) VALUES(?,?,?,?)",
            compressed)

    elif strategy == "E":
        cur = conn.cursor()
        for entity_id, pk, sig in batch:
            sig_hash = hashlib.sha256(sig).digest()
            cur.execute(
                f"INSERT INTO {table}_main(entity_id,sig_hash) VALUES(?,?)",
                (entity_id, sig_hash))
            rid = cur.lastrowid
            cur.execute(
                f"INSERT INTO {table}_blob(id,public_key,signature) VALUES(?,?,?)",
                (rid, pk, sig))

    conn.commit()

# =============================================================================
# 벤치마크
# =============================================================================

def measure_point_query(conn, table: str, strategy: str, entity_ids: list) -> dict:
    sample = random.choices(entity_ids, k=POINT_QUERY_COUNT)
    latencies = []
    cur = conn.cursor()

    for eid in sample:
        if strategy == "B":
            sql = (f"SELECT m.entity_id,c.public_key,c.signature "
                   f"FROM {table}_meta m JOIN {table}_crypto c ON m.id=c.id "
                   f"WHERE m.entity_id=?")
        elif strategy == "E":
            sql = (f"SELECT m.entity_id,b.public_key,b.signature "
                   f"FROM {table}_main m JOIN {table}_blob b ON m.id=b.id "
                   f"WHERE m.entity_id=?")
        else:
            sql = f"SELECT * FROM {table} WHERE entity_id=?"

        t0 = time.perf_counter()
        cur.execute(sql, (eid,))
        cur.fetchone()
        latencies.append((time.perf_counter() - t0) * 1000)

    avg = sum(latencies) / len(latencies)
    return {
        "pq_avg_ms":         round(avg, 4),
        "pq_min_ms":         round(min(latencies), 4),
        "pq_max_ms":         round(max(latencies), 4),
        "pq_throughput_qps": round(1000 / avg, 2) if avg > 0 else 0,
    }


def measure_range_scan(conn, table: str, strategy: str, scale: int) -> dict:
    limit = int(scale * RANGE_RATIO)
    latencies = []
    result_counts = []
    cur = conn.cursor()

    for _ in range(RANGE_SCAN_COUNT):
        offset = random.randint(0, max(0, scale - limit))
        if strategy == "B":
            sql = (f"SELECT m.entity_id,c.public_key,c.signature "
                   f"FROM {table}_meta m JOIN {table}_crypto c ON m.id=c.id "
                   f"ORDER BY m.id LIMIT ? OFFSET ?")
        elif strategy == "E":
            sql = (f"SELECT m.entity_id,b.public_key,b.signature "
                   f"FROM {table}_main m JOIN {table}_blob b ON m.id=b.id "
                   f"ORDER BY m.id LIMIT ? OFFSET ?")
        else:
            sql = f"SELECT * FROM {table} ORDER BY id LIMIT ? OFFSET ?"

        t0 = time.perf_counter()
        cur.execute(sql, (limit, offset))
        rows = cur.fetchall()
        latencies.append((time.perf_counter() - t0) * 1000)
        result_counts.append(len(rows))

    return {
        "rs_avg_ms":      round(sum(latencies) / len(latencies), 4),
        "rs_min_ms":      round(min(latencies), 4),
        "rs_max_ms":      round(max(latencies), 4),
        "rs_avg_results": round(sum(result_counts) / len(result_counts), 2),
    }


def measure_delete(conn, table: str, strategy: str, entity_ids: list) -> dict:
    targets = random.sample(entity_ids, min(DELETE_COUNT, len(entity_ids)))
    latencies = []

    for eid in targets:
        tgt = (f"{table}_meta" if strategy == "B"
               else f"{table}_main" if strategy == "E"
               else table)
        t0 = time.perf_counter()
        conn.execute(f"DELETE FROM {tgt} WHERE entity_id=?", (eid,))
        conn.commit()
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
    """실험 진행 중 단건 결과를 간략히 출력합니다."""
    print(f"\n{'─'*60}")
    print(f"  결과: {result['algorithm']} | 전략 {result['strategy']} | {result['scale']:,}건")
    print(f"{'─'*60}")
    print(f"  INSERT  : {result['insert_throughput_rps']:>10,.0f} rps  (avg {result['insert_avg_ms']:.3f}ms)")
    print(f"  POINT Q : {result['pq_avg_ms']:.3f}ms")
    print(f"  RANGE   : {result['rs_avg_ms']:.3f}ms  (avg {result['rs_avg_results']:.0f}건)")
    print(f"  DELETE  : {result['del_avg_ms']:.3f}ms")
    print(f"  B+tree 깊이: {result['btree_depth']}  |  overflow: {result['toast_size_bytes']:,}B  |  리프: {result['leaf_page_count']}페이지")


def print_markdown_table(results: list):
    """모든 실험 결과를 마크다운 테이블로 출력합니다."""
    print("\n\n" + "="*60)
    print("## SQLite 실험 결과\n")
    print("| Algorithm | Family | Level | Strategy | Sig(B) | INSERT(rps) | PQ(ms) | Range(ms) | Del(ms) | B+tree Depth | Table(MB) | Index(MB) | SHQ-Idx(MB) | Overflow(MB) | Leaf Pages | Overflow Pages |")
    print("|-----------|--------|-------|----------|--------|-------------|--------|-----------|---------|-------------|-----------|-----------|-------------|--------------|------------|----------------|")
    for r in results:
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
            f"| {r['rs_avg_ms']:.3f} "
            f"| {r['del_avg_ms']:.3f} "
            f"| {r['btree_depth']} "
            f"| {r['table_size_bytes']/1024/1024:.1f} "
            f"| {r['index_size_bytes']/1024/1024:.1f} "
            f"| {shq_idx} "
            f"| {r['toast_size_bytes']/1024/1024:.1f} "
            f"| {r['leaf_page_count']} "
            f"| {r['overflow_page_count']} |"
        )
    print()

# =============================================================================
# 단일 실험
# =============================================================================

def run_one(algo_name: str, algo_info: dict, strategy: str, scale: int,
            sig_pool: list) -> dict:
    """단일 실험 실행 (테이블 생성 → INSERT → 측정 → DROP)"""
    table   = tname(algo_name, strategy)
    pool_sz = len(sig_pool)
    sig_size = algo_info["sig_size"]

    print(f"\n{'='*60}")
    print(f"실험: {algo_name} | 전략 {strategy} | {scale:,}건 | SQLite")
    print(f"{'='*60}")

    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA page_size={SQLITE_PAGE_SIZE}")
    conn.execute("PRAGMA cache_size=-65536")

    try:
        create_table(conn, table, strategy)

        # INSERT (배치, 서명 풀 순환)
        entity_ids = []
        t0 = time.perf_counter()
        batch = []

        for i in range(scale):
            pk, sig, msg_hash = sig_pool[i % pool_sz]
            entity_id = hashlib.sha256(f"{algo_name}_{i}".encode()).hexdigest()
            batch.append((entity_id, pk, sig))
            entity_ids.append(entity_id)

            if len(batch) == INSERT_BATCH_SIZE:
                insert_batch(conn, table, strategy, batch)
                batch = []

        if batch:
            insert_batch(conn, table, strategy, batch)

        insert_sec = time.perf_counter() - t0
        print(f"  [INSERT] 완료: {scale:,}건 / {insert_sec:.2f}초")

        # ANALYZE
        analyze_tbl = (f"{table}_meta" if strategy == "B"
                       else f"{table}_main" if strategy == "E"
                       else table)
        conn.execute(f"ANALYZE {analyze_tbl}")
        conn.commit()
        print("  ANALYZE 완료")

        # 메트릭 수집
        index_name = (f"idx_{table}_me" if strategy in ("B", "E") else f"idx_{table}_e")
        metrics = sqlite_collect_all(conn, analyze_tbl, index_name)

        sighash_index_size = (sqlite_get_index_size_bytes(conn, f"idx_{table}_h")
                              if strategy == "C" else None)

        # 벤치마크
        pq = measure_point_query(conn, table, strategy, entity_ids)
        rs = measure_range_scan(conn, table, strategy, scale)
        dl = measure_delete(conn, table, strategy, entity_ids)

        drop_table(conn, table, strategy)
        print("  테이블 삭제 완료")

    except Exception as e:
        import traceback
        print(f"[오류] {e}")
        traceback.print_exc()
        try:
            drop_table(conn, table, strategy)
        except Exception:
            pass
        conn.close()
        raise

    conn.close()

    overflow = sig_size > OVERFLOW_THRESHOLD
    ov_pages = (max(0, (sig_size - OVERFLOW_THRESHOLD) // SQLITE_PAGE_SIZE + 1)
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
        "insert_avg_ms":         round(insert_sec / scale * 1000, 4),
        "insert_throughput_rps": round(scale / insert_sec, 2),
        "pq_avg_ms":         pq["pq_avg_ms"],
        "pq_min_ms":         pq["pq_min_ms"],
        "pq_max_ms":         pq["pq_max_ms"],
        "pq_throughput_qps": pq["pq_throughput_qps"],
        "rs_avg_ms":      rs["rs_avg_ms"],
        "rs_min_ms":      rs["rs_min_ms"],
        "rs_max_ms":      rs["rs_max_ms"],
        "rs_avg_results": rs["rs_avg_results"],
        "del_avg_ms":         dl["del_avg_ms"],
        "del_min_ms":         dl["del_min_ms"],
        "del_max_ms":         dl["del_max_ms"],
        "del_throughput_dps": dl["del_throughput_dps"],
        "btree_depth":             metrics["btree_depth"],
        "index_size_bytes":        metrics["index_size_bytes"],
        "sighash_index_size_bytes": sighash_index_size,
        "table_size_bytes":        metrics["table_size_bytes"],
        "toast_size_bytes":  metrics["toast_size_bytes"],
        "leaf_page_count":   metrics["leaf_page_count"],
        "internal_page_count": metrics["internal_page_count"],
        "overflow_page_count": metrics["overflow_page_count"],
        "page_size":         metrics["page_size"],
        "timestamp":         datetime.now().isoformat(),
    }

# =============================================================================
# 메인
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="PQC DB 성능 실험 - SQLite (실제 서명)")
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
    print(f"PQC DB 성능 실험 - SQLite (실제 서명 방식)")
    print(f"총 실험: {total}회 (classical=전략A만, PQC=전략A~E)")
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
                    result = run_one(algo_name, algo_info, strategy, scale, sig_pool)
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

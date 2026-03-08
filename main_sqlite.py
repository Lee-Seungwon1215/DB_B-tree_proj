# =============================================================================
# main_sqlite.py - SQLite 실험 자동 실행기 (실제 서명 방식)
# =============================================================================
# PostgreSQL과 동일한 알고리즘 × 전략 × 스케일 조합을 SQLite에서 실행합니다.
#
# 실행 방법:
#   .venv/bin/python main_sqlite.py            → 전체 실험
#   .venv/bin/python main_sqlite.py --resume   → 중단된 실험 이어서 실행
#   .venv/bin/python main_sqlite.py --algo ecdsa-256 --strategy A
# =============================================================================

import os
import sys
import csv
import math
import time
import random
import hashlib
import zlib
import sqlite3
import argparse
import logging
from datetime import datetime

from config import (
    ALGORITHMS, STRATEGIES, SCALES, SQLITE_CSV,
    RESULTS_DIR, SIG_POOL_SIZE,
    POINT_QUERY_COUNT, RANGE_SCAN_COUNT, DELETE_COUNT, RANGE_RATIO,
)
from data.generator import build_sig_pool

# =============================================================================
# 설정
# =============================================================================

SQLITE_PAGE_SIZE   = 4096
OVERFLOW_THRESHOLD = SQLITE_PAGE_SIZE // 4   # 1,024B
SQLITE_DB_PATH     = "results/sqlite_benchmark.db"
INSERT_BATCH_SIZE  = 500

# =============================================================================
# 로깅
# =============================================================================

os.makedirs("logs", exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

log_file = f"logs/sqlite_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# =============================================================================
# CSV 헤더 (PostgreSQL과 통일)
# =============================================================================

CSV_FIELDS = [
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
# 메트릭 측정
# =============================================================================

def measure_structure(sig_size: int, scale: int, conn, table: str, strategy: str) -> dict:
    cur = conn.cursor()
    cur.execute("PRAGMA page_count")
    total_pages = cur.fetchone()[0] or 0

    index_pages = 0
    try:
        idx_tbl = (f"{table}_meta" if strategy == "B"
                   else f"{table}_main" if strategy == "E"
                   else table)
        cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
                    (idx_tbl,))
        idx_names = [r[0] for r in cur.fetchall()]
        if idx_names:
            conn.execute("ANALYZE")
            cur.execute("SELECT stat FROM sqlite_stat1 WHERE idx=?", (idx_names[0],))
            row = cur.fetchone()
            if row:
                stats = row[0].split()
                index_pages = int(stats[0]) if stats else 0
    except Exception:
        pass

    table_size_bytes = (total_pages - index_pages) * SQLITE_PAGE_SIZE
    overflow = sig_size > OVERFLOW_THRESHOLD
    ov_pages = (max(0, (sig_size - OVERFLOW_THRESHOLD) // SQLITE_PAGE_SIZE + 1)
                if overflow else 0)
    toast_size_bytes = ov_pages * SQLITE_PAGE_SIZE * scale if overflow else 0

    fan_out   = max(2, SQLITE_PAGE_SIZE // 40)
    depth_est = round(math.log(max(scale, 2)) / math.log(fan_out), 2)

    return {
        "btree_depth":       depth_est,
        "index_size_bytes":  index_pages * SQLITE_PAGE_SIZE,
        "table_size_bytes":  table_size_bytes,
        "toast_size_bytes":  toast_size_bytes,
        "index_page_count":  index_pages,
        "overflow_expected":  overflow,
        "overflow_pages_est": ov_pages,
    }


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
# 단일 실험
# =============================================================================

def run_one(algo_name: str, algo_info: dict, strategy: str, scale: int,
            sig_pool: list) -> dict:
    """
    단일 실험 실행 (테이블 생성 → INSERT → 측정 → DROP)
    sig_pool: build_sig_pool() 반환값 [(pk, sig, msg_hash), ...]
    """
    table    = tname(algo_name, strategy)
    pool_sz  = len(sig_pool)
    sig_size = algo_info["sig_size"]

    log.info(f"  실험: {algo_name} | 전략 {strategy} | {scale:,}건 | SQLite")

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

        # ANALYZE
        analyze_tbl = (f"{table}_meta" if strategy == "B"
                       else f"{table}_main" if strategy == "E"
                       else table)
        conn.execute(f"ANALYZE {analyze_tbl}")
        conn.commit()

        # 구조 지표
        m = measure_structure(sig_size, scale, conn, table, strategy)

        # Point Query
        pq = measure_point_query(conn, table, strategy, entity_ids)

        # Range Scan
        rs = measure_range_scan(conn, table, strategy, scale)

        # Delete
        dl = measure_delete(conn, table, strategy, entity_ids)

        drop_table(conn, table, strategy)

    except Exception as e:
        import traceback
        log.error(f"오류: {e}")
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
        "pq_avg_ms":       pq["pq_avg_ms"],
        "pq_min_ms":       pq["pq_min_ms"],
        "pq_max_ms":       pq["pq_max_ms"],
        "pq_throughput_qps": pq["pq_throughput_qps"],
        "rs_avg_ms":      rs["rs_avg_ms"],
        "rs_min_ms":      rs["rs_min_ms"],
        "rs_max_ms":      rs["rs_max_ms"],
        "rs_avg_results": rs["rs_avg_results"],
        "del_avg_ms":         dl["del_avg_ms"],
        "del_min_ms":         dl["del_min_ms"],
        "del_max_ms":         dl["del_max_ms"],
        "del_throughput_dps": dl["del_throughput_dps"],
        "btree_depth":      m["btree_depth"],
        "index_size_bytes": m["index_size_bytes"],
        "table_size_bytes": m["table_size_bytes"],
        "toast_size_bytes": m["toast_size_bytes"],
        "index_page_count": m["index_page_count"],
        # SQLite는 buffer pool 통계 미지원
        "cache_hit_ratio": -1,
        "heap_reads": -1, "heap_hits": -1,
        "idx_reads":  -1, "idx_hits":  -1,
        "timestamp": datetime.now().isoformat(),
    }

# =============================================================================
# CSV 저장
# =============================================================================

def save_result(result: dict):
    exists = os.path.exists(SQLITE_CSV)
    with open(SQLITE_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow(result)


def load_completed() -> set:
    if not os.path.exists(SQLITE_CSV):
        return set()
    with open(SQLITE_CSV, "r", encoding="utf-8") as f:
        return {(r["algorithm"], r["strategy"], r["scale"]) for r in csv.DictReader(f)}

# =============================================================================
# 메인
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="PQC DB 성능 실험 - SQLite (실제 서명)")
    parser.add_argument("--resume",    action="store_true", help="중단된 실험 이어서 실행")
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
    strategies = [args.strategy] if args.strategy else STRATEGIES

    total = len(algos) * len(strategies) * len(SCALES)
    completed = load_completed() if args.resume else set()

    log.info("=" * 65)
    log.info("PQC DB 성능 실험 - SQLite (실제 서명 방식)")
    log.info(f"총 실험: {len(algos)} × {len(strategies)} × {len(SCALES)} = {total}회")
    log.info(f"서명 풀 크기: {args.pool_size}개")
    log.info("=" * 65)

    done = 0
    skipped = 0
    start_t = time.perf_counter()

    for algo_name in algos:
        algo_info = ALGORITHMS[algo_name]

        # 알고리즘별 서명 풀 생성 (전략/스케일 루프 전 한 번만)
        log.info(f"\n[알고리즘] {algo_name} (level={algo_info['level']}, "
                 f"sig={algo_info['sig_size']}B)")
        sig_pool = build_sig_pool(algo_name, pool_size=args.pool_size)

        for strategy in strategies:
            for scale in SCALES:
                exp_key = (algo_name, strategy, str(scale))
                if exp_key in completed:
                    skipped += 1
                    continue

                try:
                    result = run_one(algo_name, algo_info, strategy, scale, sig_pool)
                    save_result(result)
                    done += 1

                    elapsed = time.perf_counter() - start_t
                    eta = (elapsed / (done + skipped)) * (total - done - skipped) if (done + skipped) > 0 else 0
                    log.info(f"  완료 | {done+skipped}/{total} | ETA {int(eta//60)}m{int(eta%60)}s | "
                             f"TPS: {result['insert_throughput_rps']:>10,.0f} | "
                             f"PQ: {result['pq_avg_ms']:.4f}ms")

                except Exception as e:
                    log.error(f"오류: {algo_name}/{strategy}/{scale}: {e}")

    log.info(f"\n{'='*65}")
    log.info(f"완료! 결과: {SQLITE_CSV}")
    log.info(f"{'='*65}")


if __name__ == "__main__":
    main()

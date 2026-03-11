# =============================================================================
# db/sqlite/strategy_c.py - 전략 C: 4KB 페이지, 수직 파티셔닝 (기준선 비교)
# =============================================================================
# 메타데이터(id, entity_id, created_at)와 바이너리(public_key, signature)를
# 별도 테이블로 분리합니다. meta B+tree는 항상 작게 유지되고,
# 조회 시 JOIN으로 A/B와 동일한 데이터를 반환하여 공평한 비교를 보장합니다.
#
# 테이블 구조:
#   {table}_meta: id, entity_id, created_at
#   {table}_blob: entity_id (PK), public_key, signature
#
# 참고: Navathe et al. 1984 (수직 파티셔닝)
# =============================================================================

from metrics.sqlite_collector import (
    get_btree_depth, get_table_size_bytes, get_overflow_size_bytes,
    get_overflow_page_count, get_index_size_bytes,
    get_leaf_page_count, get_internal_page_count, get_page_size,
)

PAGE_SIZE = 4096


def _meta(table: str) -> str:
    return f"{table}_meta"


def _blob(table: str) -> str:
    return f"{table}_blob"


def create_table(conn, table: str):
    conn.execute("PRAGMA foreign_keys = ON")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_meta(table)} (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id  TEXT    NOT NULL UNIQUE,
            created_at TEXT    DEFAULT (datetime('now'))
        )""")
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table}_meta_e"
        f" ON {_meta(table)}(entity_id)")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_blob(table)} (
            entity_id  TEXT  PRIMARY KEY
                             REFERENCES {_meta(table)}(entity_id)
                             ON DELETE CASCADE,
            public_key BLOB  NOT NULL,
            signature  BLOB  NOT NULL
        )""")
    conn.commit()


def drop_table(conn, table: str):
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"DROP TABLE IF EXISTS {_blob(table)}")
    conn.execute(f"DROP TABLE IF EXISTS {_meta(table)}")
    conn.commit()


def insert_batch(conn, table: str, batch: list):
    """batch: list of (entity_id, public_key, signature)"""
    meta_rows = [(entity_id,)                  for entity_id, pk, sig in batch]
    blob_rows = [(entity_id, pk, sig)          for entity_id, pk, sig in batch]
    conn.executemany(
        f"INSERT INTO {_meta(table)}(entity_id) VALUES(?)", meta_rows)
    conn.executemany(
        f"INSERT INTO {_blob(table)}(entity_id, public_key, signature) VALUES(?,?,?)",
        blob_rows)
    conn.commit()


def insert_single(conn, table: str, entity_id: str, pk: bytes, sig: bytes):
    conn.execute(
        f"INSERT INTO {_meta(table)}(entity_id) VALUES(?)", (entity_id,))
    conn.execute(
        f"INSERT INTO {_blob(table)}(entity_id, public_key, signature) VALUES(?,?,?)",
        (entity_id, pk, sig))
    conn.commit()


def point_query(conn, table: str, entity_id: str):
    """A/B와 동일한 컬럼을 반환 (JOIN)"""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT m.id, m.entity_id, m.created_at, b.public_key, b.signature
        FROM {_meta(table)} m
        JOIN {_blob(table)} b ON m.entity_id = b.entity_id
        WHERE m.entity_id = ?
    """, (entity_id,))
    return cur.fetchone()


def range_scan(conn, table: str, limit: int, offset: int) -> list:
    """A/B와 동일한 컬럼을 반환 (JOIN)"""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT m.id, m.entity_id, m.created_at, b.public_key, b.signature
        FROM {_meta(table)} m
        JOIN {_blob(table)} b ON m.entity_id = b.entity_id
        ORDER BY m.id LIMIT ? OFFSET ?
    """, (limit, offset))
    return cur.fetchall()


def update_record(conn, table: str, entity_id: str, pk: bytes, sig: bytes):
    """blob 테이블만 UPDATE (meta B+tree 변경 없음)"""
    conn.execute(
        f"UPDATE {_blob(table)} SET public_key=?, signature=? WHERE entity_id=?",
        (pk, sig, entity_id))
    conn.commit()


def delete_record(conn, table: str, entity_id: str):
    """meta 삭제 → ON DELETE CASCADE로 blob 자동 삭제"""
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        f"DELETE FROM {_meta(table)} WHERE entity_id=?", (entity_id,))
    conn.commit()


def range_delete_records(conn, table: str, entity_ids: list):
    if not entity_ids:
        return
    conn.execute("PRAGMA foreign_keys = ON")
    placeholders = ",".join(["?"] * len(entity_ids))
    conn.execute(
        f"DELETE FROM {_meta(table)} WHERE entity_id IN ({placeholders})",
        entity_ids)
    conn.commit()


def get_main_table(table: str) -> str:
    return _meta(table)


def get_index_name(table: str) -> str:
    return f"idx_{table}_meta_e"


def analyze(conn, table: str):
    conn.execute(f"ANALYZE {_meta(table)}")
    conn.execute(f"ANALYZE {_blob(table)}")
    conn.commit()


def collect_metrics(conn, table: str) -> dict:
    """
    meta + blob 두 테이블을 합산한 메트릭을 반환합니다.
    A/B와 공평한 비교를 위해 전체 저장 공간을 반영합니다.

    btree_depth    : meta 테이블 기준 (탐색 경로)
    table_size_bytes: meta + blob 합산 (overflow 제외)
    toast_size_bytes: blob overflow 크기 (overflow 발생 시)
    leaf_page_count : meta + blob 합산
    """
    page_size = get_page_size(conn)

    meta = _meta(table)
    blob = _blob(table)
    idx  = get_index_name(table)

    return {
        "btree_depth":         get_btree_depth(conn, meta),
        "table_size_bytes":    get_table_size_bytes(conn, meta)
                             + get_table_size_bytes(conn, blob),
        "index_size_bytes":    get_index_size_bytes(conn, idx),
        "toast_size_bytes":    get_overflow_size_bytes(conn, meta)
                             + get_overflow_size_bytes(conn, blob),
        "leaf_page_count":     get_leaf_page_count(conn, meta)
                             + get_leaf_page_count(conn, blob),
        "internal_page_count": get_internal_page_count(conn, meta)
                             + get_internal_page_count(conn, blob),
        "overflow_page_count": get_overflow_page_count(conn, meta)
                             + get_overflow_page_count(conn, blob),
        "page_size":           page_size,
        "overflow_threshold":  page_size // 4,
    }

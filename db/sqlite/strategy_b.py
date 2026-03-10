# =============================================================================
# db/sqlite/strategy_b.py - 전략 B: Vertical Partitioning (수직 파티셔닝)
# =============================================================================
# 메타 데이터와 바이너리 데이터를 별도 테이블로 분리합니다.
# 자주 조회되는 컬럼(메타)과 크기가 큰 컬럼(서명/공개키)을 분리하여
# 메타 테이블이 항상 Inline 상태를 유지하도록 합니다.
#
# 참고: Navathe et al. 1984 (수직 파티셔닝)
#
# 테이블 구조:
#   <table>_meta (
#     id, entity_id, created_at
#   )
#   <table>_blob (
#     entity_id, public_key, signature
#   )
# =============================================================================


def _meta(table: str) -> str:
    return f"{table}_meta"


def _blob(table: str) -> str:
    return f"{table}_blob"


def create_table(conn, table: str):
    """
    전략 B 테이블과 인덱스를 생성합니다.
    """
    # 외래키 제약 활성화
    conn.execute("PRAGMA foreign_keys = ON")

    # 메타 테이블: 자주 조회되는 컬럼만 저장 → 항상 Inline 유지
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_meta(table)} (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id  TEXT    NOT NULL UNIQUE,
            created_at TEXT    DEFAULT (datetime('now'))
        )""")
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table}_meta_e"
        f" ON {_meta(table)}(entity_id)")

    # 블롭 테이블: 크기가 큰 바이너리 컬럼만 저장 → Overflow 허용
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
    """
    전략 B 테이블을 삭제합니다.
    블롭 테이블은 CASCADE로 메타 테이블에 종속되어 있으므로 함께 삭제됩니다.
    """
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"DROP TABLE IF EXISTS {_blob(table)}")
    conn.execute(f"DROP TABLE IF EXISTS {_meta(table)}")
    conn.commit()


def insert_batch(conn, table: str, batch: list):
    """
    레코드를 배치로 삽입합니다. 메타와 블롭 테이블에 각각 executemany합니다.

    Args:
        batch: (entity_id, public_key, signature) 튜플의 리스트
    """
    meta_rows = [(entity_id,)           for entity_id, pk, sig in batch]
    blob_rows = [(entity_id, pk, sig)   for entity_id, pk, sig in batch]

    conn.executemany(
        f"INSERT INTO {_meta(table)}(entity_id) VALUES(?)",
        meta_rows)
    conn.executemany(
        f"INSERT INTO {_blob(table)}(entity_id, public_key, signature) VALUES(?,?,?)",
        blob_rows)
    conn.commit()


def insert_single(conn, table: str, entity_id: str, public_key: bytes, signature: bytes):
    """
    레코드 1건을 삽입합니다. (단건 INSERT 벤치마크용)
    """
    conn.execute(
        f"INSERT INTO {_meta(table)}(entity_id) VALUES(?)",
        (entity_id,))
    conn.execute(
        f"INSERT INTO {_blob(table)}(entity_id, public_key, signature) VALUES(?,?,?)",
        (entity_id, public_key, signature))
    conn.commit()


def point_query(conn, table: str, entity_id: str):
    """
    entity_id로 단건 조회합니다.
    메타 테이블만 조회하므로 Inline 데이터만 읽습니다.
    """
    cur = conn.cursor()
    cur.execute(
        f"SELECT id, entity_id, created_at FROM {_meta(table)} WHERE entity_id=?",
        (entity_id,))
    return cur.fetchone()


def range_scan(conn, table: str, limit: int, offset: int) -> list:
    """
    id 순서 기준 범위 조회합니다.
    메타 테이블만 조회하므로 Overflow I/O가 발생하지 않습니다.
    """
    cur = conn.cursor()
    cur.execute(
        f"SELECT id, entity_id, created_at FROM {_meta(table)}"
        f" ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset))
    return cur.fetchall()


def update_record(conn, table: str, entity_id: str, public_key: bytes, signature: bytes):
    """
    entity_id로 공개키와 서명을 갱신합니다. (키 교체 시나리오)
    블롭 테이블만 UPDATE하므로 메타 테이블은 변경되지 않습니다.
    """
    conn.execute(
        f"UPDATE {_blob(table)} SET public_key=?, signature=? WHERE entity_id=?",
        (public_key, signature, entity_id))
    conn.commit()


def delete_record(conn, table: str, entity_id: str):
    """
    entity_id로 레코드 1건을 삭제합니다.
    메타 테이블 삭제 시 ON DELETE CASCADE로 블롭도 함께 삭제됩니다.
    """
    conn.execute(
        f"DELETE FROM {_meta(table)} WHERE entity_id=?",
        (entity_id,))
    conn.commit()


def range_delete_records(conn, table: str, entity_ids: list):
    """
    entity_id 목록에 해당하는 레코드를 단일 쿼리로 일괄 삭제합니다.
    ON DELETE CASCADE로 블롭도 함께 삭제됩니다.
    """
    if not entity_ids:
        return
    placeholders = ",".join(["?"] * len(entity_ids))
    conn.execute(
        f"DELETE FROM {_meta(table)} WHERE entity_id IN ({placeholders})",
        entity_ids)
    conn.commit()


def get_index_name(table: str) -> str:
    """
    entity_id 인덱스 이름을 반환합니다. (메트릭 수집에 사용)
    메타 테이블의 인덱스를 기준으로 합니다.
    """
    return f"idx_{table}_meta_e"


def get_main_table(table: str) -> str:
    """
    메트릭 수집 기준 테이블 이름을 반환합니다.
    strategy_b는 메타 테이블을 기준으로 합니다.
    """
    return _meta(table)


def analyze(conn, table: str):
    """
    메타/블롭 테이블 통계를 모두 갱신합니다.
    """
    conn.execute(f"ANALYZE {_meta(table)}")
    conn.execute(f"ANALYZE {_blob(table)}")
    conn.commit()

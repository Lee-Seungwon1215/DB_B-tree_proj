# =============================================================================
# db/sqlite/strategy_a.py - 전략 A: Inline Storage (기준선)
# =============================================================================
# 모든 데이터를 하나의 테이블에 그대로 저장합니다.
# 서명과 공개키가 크면 SQLite가 자동으로 Overflow 페이지를 사용합니다.
# 다른 전략들과 비교하기 위한 기준선(baseline) 역할을 합니다.
#
# 참고: Comer 1979 (B+tree 원조)
#
# 테이블 구조:
#   <table> (
#     id, entity_id, public_key, signature, created_at
#   )
# =============================================================================


def create_table(conn, table: str):
    """
    전략 A 테이블과 인덱스를 생성합니다.
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id  TEXT    NOT NULL,
            public_key BLOB    NOT NULL,
            signature  BLOB    NOT NULL,
            created_at TEXT    DEFAULT (datetime('now'))
        )""")
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table}_e ON {table}(entity_id)")
    conn.commit()


def drop_table(conn, table: str):
    """
    전략 A 테이블을 삭제합니다. (실험 종료 후 공간 반환)
    """
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()


def insert_batch(conn, table: str, batch: list):
    """
    레코드를 배치로 삽입합니다.

    Args:
        batch: (entity_id, public_key, signature) 튜플의 리스트
    """
    conn.executemany(
        f"INSERT INTO {table}(entity_id, public_key, signature) VALUES(?,?,?)",
        batch)
    conn.commit()


def insert_single(conn, table: str, entity_id: str, public_key: bytes, signature: bytes):
    """
    레코드 1건을 삽입합니다. (단건 INSERT 벤치마크용)
    """
    conn.execute(
        f"INSERT INTO {table}(entity_id, public_key, signature) VALUES(?,?,?)",
        (entity_id, public_key, signature))
    conn.commit()


def point_query(conn, table: str, entity_id: str):
    """
    entity_id로 단건 조회합니다.
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE entity_id=?", (entity_id,))
    return cur.fetchone()


def range_scan(conn, table: str, limit: int, offset: int) -> list:
    """
    id 순서 기준 범위 조회합니다.
    """
    cur = conn.cursor()
    cur.execute(
        f"SELECT * FROM {table} ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset))
    return cur.fetchall()


def update_record(conn, table: str, entity_id: str, public_key: bytes, signature: bytes):
    """
    entity_id로 공개키와 서명을 갱신합니다. (키 교체 시나리오)
    """
    conn.execute(
        f"UPDATE {table} SET public_key=?, signature=? WHERE entity_id=?",
        (public_key, signature, entity_id))
    conn.commit()


def delete_record(conn, table: str, entity_id: str):
    """
    entity_id로 레코드 1건을 삭제합니다.
    """
    conn.execute(f"DELETE FROM {table} WHERE entity_id=?", (entity_id,))
    conn.commit()


def range_delete_records(conn, table: str, entity_ids: list):
    """
    entity_id 목록에 해당하는 레코드를 단일 쿼리로 일괄 삭제합니다.
    """
    if not entity_ids:
        return
    placeholders = ",".join(["?"] * len(entity_ids))
    conn.execute(
        f"DELETE FROM {table} WHERE entity_id IN ({placeholders})",
        entity_ids)
    conn.commit()


def get_index_name(table: str) -> str:
    """
    entity_id 인덱스 이름을 반환합니다. (메트릭 수집에 사용)
    """
    return f"idx_{table}_e"


def get_main_table(table: str) -> str:
    """
    메트릭 수집 기준 테이블 이름을 반환합니다.
    """
    return table


def analyze(conn, table: str):
    """
    테이블 통계를 갱신합니다.
    """
    conn.execute(f"ANALYZE {table}")
    conn.commit()

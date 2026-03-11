# =============================================================================
# db/sqlite/strategy_d.py - 전략 D: 4KB 페이지, 선택적 외부화(TOAST-like)
# =============================================================================
# 목적:
#   - 작은 값은 인라인 저장
#   - 큰 값만 head/tail로 분리 저장
#   - A(전부 인라인)와 C(전부 분리)의 중간 전략
#
# 테이블 구조:
#   {table}_main:
#       id, entity_id, public_key_head, signature_head, has_pk_tail, has_sig_tail, created_at
#
#   {table}_tail:
#       entity_id, public_key_tail, signature_tail
#
# 조회 시:
#   - main에서 먼저 읽고
#   - has_*_tail 플래그가 있으면 tail을 붙여서 원본 복원
#
# 반환 형식:
#   A/B와 맞추기 위해 (id, entity_id, public_key, signature, created_at) 순서로 반환
# =============================================================================

from metrics.sqlite_collector import (
    get_btree_depth, get_table_size_bytes, get_overflow_size_bytes,
    get_overflow_page_count, get_index_size_bytes,
    get_leaf_page_count, get_internal_page_count, get_page_size,
)

PAGE_SIZE = 4096

# 조절 가능한 실험 파라미터
HEAD_SIZE = 128
PK_THRESHOLD = 256
SIG_THRESHOLD = 512


def _main(table: str) -> str:
    return f"{table}_main"


def _tail(table: str) -> str:
    return f"{table}_tail"


def _split_value(value: bytes, threshold: int):
    """
    threshold 이하: 전부 head에 저장, tail 없음
    threshold 초과: 앞 HEAD_SIZE는 head, 나머지는 tail
    반환: (head_bytes, tail_bytes_or_none, has_tail_int)
    """
    if len(value) <= threshold:
        return value, None, 0

    head = value[:HEAD_SIZE]
    tail = value[HEAD_SIZE:]
    return head, tail, 1


def _merge_value(head: bytes, tail: bytes | None, has_tail: int) -> bytes:
    if not has_tail:
        return head
    return (head or b"") + (tail or b"")


def create_table(conn, table: str):
    conn.execute("PRAGMA foreign_keys = ON")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_main(table)} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id       TEXT NOT NULL UNIQUE,
            public_key_head BLOB NOT NULL,
            signature_head  BLOB NOT NULL,
            has_pk_tail     INTEGER NOT NULL DEFAULT 0,
            has_sig_tail    INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now'))
        )""")

    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table}_main_e "
        f"ON {_main(table)}(entity_id)"
    )

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_tail(table)} (
            entity_id       TEXT PRIMARY KEY
                            REFERENCES {_main(table)}(entity_id)
                            ON DELETE CASCADE,
            public_key_tail BLOB,
            signature_tail  BLOB
        )""")

    conn.commit()


def drop_table(conn, table: str):
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"DROP TABLE IF EXISTS {_tail(table)}")
    conn.execute(f"DROP TABLE IF EXISTS {_main(table)}")
    conn.commit()


def insert_batch(conn, table: str, batch: list):
    """batch: list of (entity_id, public_key, signature)"""
    main_rows = []
    tail_rows = []

    for entity_id, pk, sig in batch:
        pk_head, pk_tail, has_pk_tail = _split_value(pk, PK_THRESHOLD)
        sig_head, sig_tail, has_sig_tail = _split_value(sig, SIG_THRESHOLD)

        main_rows.append((
            entity_id,
            pk_head,
            sig_head,
            has_pk_tail,
            has_sig_tail,
        ))

        if has_pk_tail or has_sig_tail:
            tail_rows.append((
                entity_id,
                pk_tail,
                sig_tail,
            ))

    conn.executemany(
        f"""
        INSERT INTO {_main(table)}
        (entity_id, public_key_head, signature_head, has_pk_tail, has_sig_tail)
        VALUES (?, ?, ?, ?, ?)
        """,
        main_rows
    )

    if tail_rows:
        conn.executemany(
            f"""
            INSERT INTO {_tail(table)}
            (entity_id, public_key_tail, signature_tail)
            VALUES (?, ?, ?)
            """,
            tail_rows
        )

    conn.commit()


def insert_single(conn, table: str, entity_id: str, pk: bytes, sig: bytes):
    pk_head, pk_tail, has_pk_tail = _split_value(pk, PK_THRESHOLD)
    sig_head, sig_tail, has_sig_tail = _split_value(sig, SIG_THRESHOLD)

    conn.execute(
        f"""
        INSERT INTO {_main(table)}
        (entity_id, public_key_head, signature_head, has_pk_tail, has_sig_tail)
        VALUES (?, ?, ?, ?, ?)
        """,
        (entity_id, pk_head, sig_head, has_pk_tail, has_sig_tail)
    )

    if has_pk_tail or has_sig_tail:
        conn.execute(
            f"""
            INSERT INTO {_tail(table)}
            (entity_id, public_key_tail, signature_tail)
            VALUES (?, ?, ?)
            """,
            (entity_id, pk_tail, sig_tail)
        )

    conn.commit()


def point_query(conn, table: str, entity_id: str):
    """
    A/B와 같은 형태로 반환:
    (id, entity_id, public_key, signature, created_at)
    """
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            m.id,
            m.entity_id,
            m.public_key_head,
            t.public_key_tail,
            m.has_pk_tail,
            m.signature_head,
            t.signature_tail,
            m.has_sig_tail,
            m.created_at
        FROM {_main(table)} m
        LEFT JOIN {_tail(table)} t
          ON m.entity_id = t.entity_id
        WHERE m.entity_id = ?
    """, (entity_id,))

    row = cur.fetchone()
    if row is None:
        return None

    (
        row_id,
        row_entity_id,
        pk_head,
        pk_tail,
        has_pk_tail,
        sig_head,
        sig_tail,
        has_sig_tail,
        created_at,
    ) = row

    public_key = _merge_value(pk_head, pk_tail, has_pk_tail)
    signature = _merge_value(sig_head, sig_tail, has_sig_tail)

    return (row_id, row_entity_id, public_key, signature, created_at)


def range_scan(conn, table: str, limit: int, offset: int) -> list:
    """
    A/B와 같은 형태로 반환:
    [(id, entity_id, public_key, signature, created_at), ...]
    """
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            m.id,
            m.entity_id,
            m.public_key_head,
            t.public_key_tail,
            m.has_pk_tail,
            m.signature_head,
            t.signature_tail,
            m.has_sig_tail,
            m.created_at
        FROM {_main(table)} m
        LEFT JOIN {_tail(table)} t
          ON m.entity_id = t.entity_id
        ORDER BY m.id
        LIMIT ? OFFSET ?
    """, (limit, offset))

    rows = cur.fetchall()
    result = []

    for row in rows:
        (
            row_id,
            row_entity_id,
            pk_head,
            pk_tail,
            has_pk_tail,
            sig_head,
            sig_tail,
            has_sig_tail,
            created_at,
        ) = row

        public_key = _merge_value(pk_head, pk_tail, has_pk_tail)
        signature = _merge_value(sig_head, sig_tail, has_sig_tail)

        result.append((row_id, row_entity_id, public_key, signature, created_at))

    return result


def update_record(conn, table: str, entity_id: str, pk: bytes, sig: bytes):
    pk_head, pk_tail, has_pk_tail = _split_value(pk, PK_THRESHOLD)
    sig_head, sig_tail, has_sig_tail = _split_value(sig, SIG_THRESHOLD)

    conn.execute(
        f"""
        UPDATE {_main(table)}
        SET public_key_head = ?,
            signature_head  = ?,
            has_pk_tail     = ?,
            has_sig_tail    = ?
        WHERE entity_id = ?
        """,
        (pk_head, sig_head, has_pk_tail, has_sig_tail, entity_id)
    )

    if has_pk_tail or has_sig_tail:
        conn.execute(
            f"""
            INSERT INTO {_tail(table)} (entity_id, public_key_tail, signature_tail)
            VALUES (?, ?, ?)
            ON CONFLICT(entity_id) DO UPDATE SET
                public_key_tail = excluded.public_key_tail,
                signature_tail  = excluded.signature_tail
            """,
            (entity_id, pk_tail, sig_tail)
        )
    else:
        conn.execute(
            f"DELETE FROM {_tail(table)} WHERE entity_id = ?",
            (entity_id,)
        )

    conn.commit()


def delete_record(conn, table: str, entity_id: str):
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        f"DELETE FROM {_main(table)} WHERE entity_id = ?",
        (entity_id,)
    )
    conn.commit()


def range_delete_records(conn, table: str, entity_ids: list):
    if not entity_ids:
        return

    conn.execute("PRAGMA foreign_keys = ON")
    placeholders = ",".join(["?"] * len(entity_ids))
    conn.execute(
        f"DELETE FROM {_main(table)} WHERE entity_id IN ({placeholders})",
        entity_ids
    )
    conn.commit()


def get_main_table(table: str) -> str:
    return _main(table)


def get_index_name(table: str) -> str:
    return f"idx_{table}_main_e"


def analyze(conn, table: str):
    conn.execute(f"ANALYZE {_main(table)}")
    conn.execute(f"ANALYZE {_tail(table)}")
    conn.commit()


def collect_metrics(conn, table: str) -> dict:
    """
    main + tail 합산 메트릭
    btree_depth는 main 기준
    """
    page_size = get_page_size(conn)

    main = _main(table)
    tail = _tail(table)
    idx = get_index_name(table)

    return {
        "btree_depth":         get_btree_depth(conn, main),
        "table_size_bytes":    get_table_size_bytes(conn, main)
                             + get_table_size_bytes(conn, tail),
        "index_size_bytes":    get_index_size_bytes(conn, idx),
        "toast_size_bytes":    get_overflow_size_bytes(conn, main)
                             + get_overflow_size_bytes(conn, tail),
        "leaf_page_count":     get_leaf_page_count(conn, main)
                             + get_leaf_page_count(conn, tail),
        "internal_page_count": get_internal_page_count(conn, main)
                             + get_internal_page_count(conn, tail),
        "overflow_page_count": get_overflow_page_count(conn, main)
                             + get_overflow_page_count(conn, tail),
        "page_size":           page_size,
        "overflow_threshold":  page_size // 4,
    }
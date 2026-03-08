# =============================================================================
# db/strategy_c.py - 전략 C: Hash-based Indexing (해시 인덱싱)
# =============================================================================
# 서명 전체 대신 서명의 64바이트 SHA-256 해시값만 인덱싱합니다.
# B+tree 인덱스는 항상 64바이트 고정 크기만 관리하므로 fanout이 최대화됩니다.
#
# 테이블 구조:
#   records_c (
#     id, created_at, signer_id, message_hash, algorithm,
#     public_key,       ← 인덱스 없음
#     sig_hash CHAR(64) ← 서명 해시 (인덱싱)
#     signature         ← 원본 서명 (인덱스 없음)
#   )
# =============================================================================

import hashlib
from db.postgresql.connection import execute


TABLE_NAME    = "records_c"
INDEX_TIME    = "idx_c_created"
INDEX_SID     = "idx_c_signer"
INDEX_SIGHASH = "idx_c_sig_hash"   # 서명 해시 인덱스 (64바이트 고정)


def create_table(conn):
    """
    전략 C 테이블과 인덱스를 생성합니다.
    """
    drop_table(conn)

    execute(conn, f"""
        CREATE TABLE {TABLE_NAME} (
            id           BIGINT       PRIMARY KEY,   -- 고유번호
            created_at   TIMESTAMPTZ  NOT NULL,       -- 생성시간 (범위 조회 기준)
            signer_id    INTEGER      NOT NULL,        -- 서명자 ID
            message_hash CHAR(64)     NOT NULL,        -- 메시지 해시
            algorithm    VARCHAR(30)  NOT NULL,        -- 알고리즘 이름
            public_key   BYTEA        NOT NULL,        -- PQC 공개키 (인덱스 없음)
            sig_hash     CHAR(64)     NOT NULL,        -- 서명의 SHA-256 해시 (인덱싱)
            signature    BYTEA        NOT NULL         -- 원본 PQC 서명 (인덱스 없음)
        );
    """)

    # 범위 조회 / 단건 조회 인덱스
    execute(conn, f"CREATE INDEX {INDEX_TIME}    ON {TABLE_NAME}(created_at);")
    execute(conn, f"CREATE INDEX {INDEX_SID}     ON {TABLE_NAME}(signer_id);")

    # 서명 해시 인덱스: 항상 64바이트 고정 → B+tree fanout 최대화
    execute(conn, f"CREATE INDEX {INDEX_SIGHASH} ON {TABLE_NAME}(sig_hash);")


def drop_table(conn):
    """
    전략 C 테이블을 삭제합니다.
    """
    execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;")


def compute_sig_hash(signature: bytes) -> str:
    """
    서명 바이트를 SHA-256 해시(64자리 16진수 문자열)로 변환합니다.
    이 해시값이 B+tree에 인덱싱됩니다.
    """
    return hashlib.sha256(signature).hexdigest()


def insert_record(conn, record: dict):
    """
    레코드 1건을 삽입합니다.
    서명의 SHA-256 해시를 자동으로 계산하여 함께 저장합니다.
    """
    sig_hash = compute_sig_hash(record["signature"])

    execute(conn, f"""
        INSERT INTO {TABLE_NAME}
            (id, created_at, signer_id, message_hash, algorithm,
             public_key, sig_hash, signature)
        VALUES
            (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s,
             %(public_key)s, %(sig_hash)s, %(signature)s);
    """, {**record, "sig_hash": sig_hash})


def insert_batch(conn, records: list):
    """
    레코드를 배치로 삽입합니다.
    각 레코드의 서명 해시를 미리 계산합니다.
    """
    # 각 레코드에 sig_hash 추가
    records_with_hash = [
        {**r, "sig_hash": compute_sig_hash(r["signature"])}
        for r in records
    ]

    with conn.cursor() as cur:
        cur.executemany(f"""
            INSERT INTO {TABLE_NAME}
                (id, created_at, signer_id, message_hash, algorithm,
                 public_key, sig_hash, signature)
            VALUES
                (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s,
                 %(public_key)s, %(sig_hash)s, %(signature)s);
        """, records_with_hash)
    conn.commit()


def point_query(conn, record_id: int) -> list:
    """
    고유번호(id)로 단건 조회합니다.
    """
    return execute(conn, f"""
        SELECT id, created_at, signer_id, algorithm
        FROM {TABLE_NAME}
        WHERE id = %s;
    """, (record_id,), fetch=True)


def range_scan(conn, start_time, end_time) -> list:
    """
    생성시간 범위로 조회합니다.
    """
    return execute(conn, f"""
        SELECT id, created_at, signer_id, algorithm
        FROM {TABLE_NAME}
        WHERE created_at BETWEEN %s AND %s;
    """, (start_time, end_time), fetch=True)


def delete_record(conn, record_id: int):
    """
    고유번호(id)로 레코드를 삭제합니다.
    """
    execute(conn, f"""
        DELETE FROM {TABLE_NAME} WHERE id = %s;
    """, (record_id,))


def get_index_name(conn) -> str:
    return f"{TABLE_NAME}_pkey"

# =============================================================================
# db/strategy_b.py - 전략 B: Vertical Partitioning (수직 파티셔닝)
# =============================================================================
# 메타 데이터와 바이너리 데이터를 별도 테이블로 분리합니다.
# 자주 조회되는 컬럼(메타)과 크기가 큰 컬럼(서명/공개키)을 분리하여
# 메타 테이블이 항상 Inline 상태를 유지하도록 합니다.
#
# 참고: Navathe et al. 1984 (수직 파티셔닝)
#
# 테이블 구조:
#   records_b_meta (
#     id, created_at, signer_id, message_hash, algorithm
#   )
#   records_b_blob (
#     id, public_key, signature
#   )
# =============================================================================

from db.postgresql.connection import execute


TABLE_NAME      = "records_b_meta"   # 메트릭 수집 기준 테이블 (메타)
TABLE_BLOB      = "records_b_blob"   # 바이너리 데이터 테이블
INDEX_TIME      = "idx_b_created"    # 생성시간 인덱스 (범위 조회용)
INDEX_SID       = "idx_b_signer"     # 서명자ID 인덱스 (단건 조회용)


def create_table(conn):
    """
    전략 B 테이블과 인덱스를 생성합니다.
    이미 존재하면 먼저 삭제 후 새로 만듭니다.
    """
    drop_table(conn)

    # 메타 테이블: 자주 조회되는 컬럼만 저장 → 항상 Inline 유지
    execute(conn, f"""
        CREATE TABLE {TABLE_NAME} (
            id           BIGINT       PRIMARY KEY,
            created_at   TIMESTAMPTZ  NOT NULL,
            signer_id    INTEGER      NOT NULL,
            message_hash CHAR(64)     NOT NULL,
            algorithm    VARCHAR(30)  NOT NULL
        );
    """)

    # 블롭 테이블: 크기가 큰 바이너리 컬럼만 저장 → TOAST 허용
    execute(conn, f"""
        CREATE TABLE {TABLE_BLOB} (
            id         BIGINT  PRIMARY KEY
                                REFERENCES {TABLE_NAME}(id)
                                ON DELETE CASCADE,
            public_key BYTEA   NOT NULL,
            signature  BYTEA   NOT NULL
        );
    """)

    # 범위 조회를 위한 생성시간 인덱스 (메타 테이블)
    execute(conn, f"CREATE INDEX {INDEX_TIME} ON {TABLE_NAME}(created_at);")

    # 단건 조회를 위한 서명자ID 인덱스 (메타 테이블)
    execute(conn, f"CREATE INDEX {INDEX_SID} ON {TABLE_NAME}(signer_id);")


def drop_table(conn):
    """
    전략 B 테이블을 삭제합니다. (실험 종료 후 공간 반환)
    블롭 테이블은 CASCADE로 메타 테이블에 종속되어 있으므로 함께 삭제됩니다.
    """
    execute(conn, f"DROP TABLE IF EXISTS {TABLE_BLOB} CASCADE;")
    execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;")


def insert_record(conn, record: dict):
    """
    레코드 1건을 삽입합니다. 메타와 블롭 테이블에 각각 INSERT합니다.

    Args:
        record: {id, created_at, signer_id, message_hash, algorithm, public_key, signature}
    """
    execute(conn, f"""
        INSERT INTO {TABLE_NAME}
            (id, created_at, signer_id, message_hash, algorithm)
        VALUES
            (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s);
    """, record)

    execute(conn, f"""
        INSERT INTO {TABLE_BLOB}
            (id, public_key, signature)
        VALUES
            (%(id)s, %(public_key)s, %(signature)s);
    """, record)


def insert_batch(conn, records: list):
    """
    레코드를 배치로 삽입합니다. 메타와 블롭 테이블에 각각 executemany합니다.

    Args:
        records: record dict의 리스트
    """
    with conn.cursor() as cur:
        cur.executemany(f"""
            INSERT INTO {TABLE_NAME}
                (id, created_at, signer_id, message_hash, algorithm)
            VALUES
                (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s);
        """, records)

        cur.executemany(f"""
            INSERT INTO {TABLE_BLOB}
                (id, public_key, signature)
            VALUES
                (%(id)s, %(public_key)s, %(signature)s);
        """, records)

    conn.commit()


def point_query(conn, record_id: int) -> list:
    """
    고유번호(id)로 단건 조회합니다.
    메타 테이블만 조회하므로 Inline 데이터만 읽습니다.
    """
    return execute(conn, f"""
        SELECT id, created_at, signer_id, algorithm
        FROM {TABLE_NAME}
        WHERE id = %s;
    """, (record_id,), fetch=True)


def range_scan(conn, start_time, end_time) -> list:
    """
    생성시간 범위로 조회합니다.
    메타 테이블만 조회하므로 TOAST I/O가 발생하지 않습니다.
    """
    return execute(conn, f"""
        SELECT id, created_at, signer_id, algorithm
        FROM {TABLE_NAME}
        WHERE created_at BETWEEN %s AND %s;
    """, (start_time, end_time), fetch=True)


def update_record(conn, record_id: int, public_key: bytes, signature: bytes):
    """
    고유번호(id)로 공개키와 서명을 갱신합니다. (키 교체 시나리오)
    블롭 테이블만 UPDATE하므로 메타 테이블은 변경되지 않습니다.
    """
    execute(conn, f"""
        UPDATE {TABLE_BLOB}
        SET public_key = %s, signature = %s
        WHERE id = %s;
    """, (public_key, signature, record_id))


def delete_record(conn, record_id: int):
    """
    고유번호(id)로 레코드 1건을 삭제합니다.
    메타 테이블 삭제 시 ON DELETE CASCADE로 블롭도 함께 삭제됩니다.
    """
    execute(conn, f"""
        DELETE FROM {TABLE_NAME} WHERE id = %s;
    """, (record_id,))


def range_delete_records(conn, record_ids: list):
    """
    고유번호(id) 목록에 해당하는 레코드를 단일 쿼리로 일괄 삭제합니다.
    ON DELETE CASCADE로 블롭도 함께 삭제됩니다.
    """
    if not record_ids:
        return
    placeholders = ",".join(["%s"] * len(record_ids))
    execute(conn, f"DELETE FROM {TABLE_NAME} WHERE id IN ({placeholders});",
            tuple(record_ids))


def get_index_name(conn) -> str:
    """
    기본 키 인덱스 이름을 반환합니다. (메트릭 수집에 사용)
    메타 테이블의 PK 인덱스를 기준으로 합니다.
    """
    return f"{TABLE_NAME}_pkey"

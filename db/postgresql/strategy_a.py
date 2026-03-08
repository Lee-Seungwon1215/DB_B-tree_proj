# =============================================================================
# db/strategy_a.py - 전략 A: Inline Storage (기준선)
# =============================================================================
# 모든 데이터를 하나의 테이블에 그대로 저장합니다.
# 서명과 공개키가 크면 PostgreSQL이 자동으로 TOAST(EXTENDED)를 적용합니다.
# 다른 전략들과 비교하기 위한 기준선(baseline) 역할을 합니다.
#
# 테이블 구조:
#   records (
#     id, created_at, signer_id, message_hash, algorithm, public_key, signature
#   )
# =============================================================================

from db.postgresql.connection import execute


TABLE_NAME = "records_a"
INDEX_PK   = "idx_a_pk"        # 고유번호 인덱스 (B+tree 기본)
INDEX_TIME = "idx_a_created"   # 생성시간 인덱스 (범위 조회용)
INDEX_SID  = "idx_a_signer"    # 서명자ID 인덱스 (단건 조회용)


def create_table(conn):
    """
    전략 A 테이블과 인덱스를 생성합니다.
    이미 존재하면 먼저 삭제 후 새로 만듭니다.
    """
    drop_table(conn)

    # 테이블 생성: 모든 컬럼을 하나의 테이블에 저장
    execute(conn, f"""
        CREATE TABLE {TABLE_NAME} (
            id           BIGINT       PRIMARY KEY,   -- 고유번호 (B+tree 기본 인덱스)
            created_at   TIMESTAMPTZ  NOT NULL,       -- 생성시간 (범위 조회 기준)
            signer_id    INTEGER      NOT NULL,        -- 서명자 ID (단건 조회 기준)
            message_hash CHAR(64)     NOT NULL,        -- 원본 메시지의 SHA-256 해시
            algorithm    VARCHAR(30)  NOT NULL,        -- PQC 알고리즘 이름
            public_key   BYTEA        NOT NULL,        -- PQC 공개키 (크기 가변)
            signature    BYTEA        NOT NULL         -- PQC 서명 (크기 가변, TOAST 자동)
        );
    """)

    # 범위 조회를 위한 생성시간 인덱스
    execute(conn, f"CREATE INDEX {INDEX_TIME} ON {TABLE_NAME}(created_at);")

    # 단건 조회를 위한 서명자ID 인덱스
    execute(conn, f"CREATE INDEX {INDEX_SID} ON {TABLE_NAME}(signer_id);")


def drop_table(conn):
    """
    전략 A 테이블을 삭제합니다. (실험 종료 후 공간 반환)
    """
    execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;")


def insert_record(conn, record: dict):
    """
    레코드 1건을 삽입합니다.

    Args:
        record: {id, created_at, signer_id, message_hash, algorithm, public_key, signature}
    """
    execute(conn, f"""
        INSERT INTO {TABLE_NAME}
            (id, created_at, signer_id, message_hash, algorithm, public_key, signature)
        VALUES
            (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s,
             %(algorithm)s, %(public_key)s, %(signature)s);
    """, record)


def insert_batch(conn, records: list):
    """
    레코드를 배치로 삽입합니다. (단건보다 훨씬 빠름)

    Args:
        records: record dict의 리스트
    """
    with conn.cursor() as cur:
        cur.executemany(f"""
            INSERT INTO {TABLE_NAME}
                (id, created_at, signer_id, message_hash, algorithm, public_key, signature)
            VALUES
                (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s,
                 %(algorithm)s, %(public_key)s, %(signature)s);
        """, records)
    conn.commit()


def point_query(conn, record_id: int) -> list:
    """
    고유번호(id)로 단건 조회합니다. B+tree 인덱스를 사용합니다.
    """
    return execute(conn, f"""
        SELECT id, created_at, signer_id, algorithm
        FROM {TABLE_NAME}
        WHERE id = %s;
    """, (record_id,), fetch=True)


def range_scan(conn, start_time, end_time) -> list:
    """
    생성시간 범위로 조회합니다. B+tree 범위 스캔을 사용합니다.
    """
    return execute(conn, f"""
        SELECT id, created_at, signer_id, algorithm
        FROM {TABLE_NAME}
        WHERE created_at BETWEEN %s AND %s;
    """, (start_time, end_time), fetch=True)


def delete_record(conn, record_id: int):
    """
    고유번호(id)로 레코드 1건을 삭제합니다.
    """
    execute(conn, f"""
        DELETE FROM {TABLE_NAME} WHERE id = %s;
    """, (record_id,))


def get_index_name(conn) -> str:
    """
    기본 키 인덱스 이름을 반환합니다. (메트릭 수집에 사용)
    """
    return f"{TABLE_NAME}_pkey"

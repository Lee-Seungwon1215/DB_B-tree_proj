# =============================================================================
# db/strategy_e.py - 전략 E: TOAST EXTERNAL (명시적 외부 저장)
# =============================================================================
# 공개키와 서명 컬럼에 TOAST 모드를 EXTERNAL로 명시 설정합니다.
#
# 전략 A(기본 EXTENDED)와의 차이:
#   EXTENDED : PostgreSQL이 압축 시도 → 그래도 크면 out-of-row 저장
#   EXTERNAL : 압축 시도 없이 바로 out-of-row 저장
#
# PQC 서명은 랜덤 바이트라 압축이 불가능합니다.
# EXTENDED는 압축 시도 자체가 CPU 낭비 → EXTERNAL이 유리할 수 있습니다.
# 이 가설을 실험으로 검증합니다.
#
# 테이블 구조: 전략 A와 동일 (TOAST 설정만 다름)
# =============================================================================

from db.postgresql.connection import execute


TABLE_NAME = "records_e"
INDEX_TIME = "idx_e_created"
INDEX_SID  = "idx_e_signer"


def create_table(conn):
    """
    전략 E 테이블을 생성하고 TOAST 모드를 EXTERNAL로 설정합니다.
    """
    drop_table(conn)

    # 테이블 구조는 전략 A와 동일
    execute(conn, f"""
        CREATE TABLE {TABLE_NAME} (
            id           BIGINT       PRIMARY KEY,   -- 고유번호
            created_at   TIMESTAMPTZ  NOT NULL,       -- 생성시간 (범위 조회 기준)
            signer_id    INTEGER      NOT NULL,        -- 서명자 ID
            message_hash CHAR(64)     NOT NULL,        -- 메시지 해시
            algorithm    VARCHAR(30)  NOT NULL,        -- 알고리즘 이름
            public_key   BYTEA        NOT NULL,        -- PQC 공개키
            signature    BYTEA        NOT NULL         -- PQC 서명
        );
    """)

    # 핵심 설정: TOAST 모드를 EXTERNAL로 변경
    # EXTERNAL = 압축 없이 바로 out-of-row 저장 (압축 CPU 낭비 방지)
    execute(conn, f"""
        ALTER TABLE {TABLE_NAME}
        ALTER COLUMN public_key SET STORAGE EXTERNAL;
    """)
    execute(conn, f"""
        ALTER TABLE {TABLE_NAME}
        ALTER COLUMN signature SET STORAGE EXTERNAL;
    """)

    execute(conn, f"CREATE INDEX {INDEX_TIME} ON {TABLE_NAME}(created_at);")
    execute(conn, f"CREATE INDEX {INDEX_SID}  ON {TABLE_NAME}(signer_id);")


def drop_table(conn):
    """
    전략 E 테이블을 삭제합니다.
    """
    execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;")


def insert_record(conn, record: dict):
    """
    레코드 1건을 삽입합니다. (압축 없이 바로 TOAST 외부 저장됨)
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
    레코드를 배치로 삽입합니다.
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

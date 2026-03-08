# =============================================================================
# db/strategy_b.py - 전략 B: Separated Table (분리 저장)
# =============================================================================
# 큰 데이터(공개키, 서명)를 별도 테이블로 분리합니다.
# 메인 테이블의 B+tree는 작은 데이터만 관리하므로 fanout이 높아집니다.
#
# 테이블 구조:
#   records_b   (id, created_at, signer_id, message_hash, algorithm)
#   crypto_b    (id, record_id [FK], public_key, signature)
# =============================================================================

from db.postgresql.connection import execute


MAIN_TABLE   = "records_b"
CRYPTO_TABLE = "crypto_b"
INDEX_PK     = "idx_b_pk"
INDEX_TIME   = "idx_b_created"
INDEX_SID    = "idx_b_signer"
INDEX_FK     = "idx_b_record_id"   # FK 조인 성능을 위한 인덱스


def create_table(conn):
    """
    전략 B 테이블 2개와 인덱스를 생성합니다.
    """
    drop_table(conn)

    # 메인 테이블: 작은 데이터만 저장 → B+tree 가벼움
    execute(conn, f"""
        CREATE TABLE {MAIN_TABLE} (
            id           BIGINT       PRIMARY KEY,   -- 고유번호
            created_at   TIMESTAMPTZ  NOT NULL,       -- 생성시간 (범위 조회 기준)
            signer_id    INTEGER      NOT NULL,        -- 서명자 ID
            message_hash CHAR(64)     NOT NULL,        -- 메시지 해시
            algorithm    VARCHAR(30)  NOT NULL         -- 알고리즘 이름
        );
    """)

    # 암호 데이터 테이블: 큰 데이터(공개키, 서명)를 별도 저장
    execute(conn, f"""
        CREATE TABLE {CRYPTO_TABLE} (
            id         BIGINT  PRIMARY KEY,                          -- 고유번호
            record_id  BIGINT  NOT NULL REFERENCES {MAIN_TABLE}(id) ON DELETE CASCADE, -- 메인 테이블 참조
            public_key BYTEA   NOT NULL,                             -- PQC 공개키
            signature  BYTEA   NOT NULL                              -- PQC 서명
        );
    """)

    # 인덱스 생성
    execute(conn, f"CREATE INDEX {INDEX_TIME} ON {MAIN_TABLE}(created_at);")
    execute(conn, f"CREATE INDEX {INDEX_SID}  ON {MAIN_TABLE}(signer_id);")
    execute(conn, f"CREATE INDEX {INDEX_FK}   ON {CRYPTO_TABLE}(record_id);")


def drop_table(conn):
    """
    전략 B 테이블 2개를 삭제합니다.
    CRYPTO 테이블 먼저 삭제 (FK 제약 때문에 순서 중요)
    """
    execute(conn, f"DROP TABLE IF EXISTS {CRYPTO_TABLE} CASCADE;")
    execute(conn, f"DROP TABLE IF EXISTS {MAIN_TABLE}   CASCADE;")


def insert_record(conn, record: dict):
    """
    레코드 1건을 메인/크립토 테이블에 나눠서 삽입합니다.
    """
    # 메인 테이블에 기본 정보 삽입
    execute(conn, f"""
        INSERT INTO {MAIN_TABLE}
            (id, created_at, signer_id, message_hash, algorithm)
        VALUES
            (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s);
    """, record)

    # 크립토 테이블에 공개키/서명 삽입
    execute(conn, f"""
        INSERT INTO {CRYPTO_TABLE}
            (id, record_id, public_key, signature)
        VALUES
            (%(id)s, %(id)s, %(public_key)s, %(signature)s);
    """, record)


def insert_batch(conn, records: list):
    """
    레코드를 배치로 삽입합니다.
    메인 테이블 전체 → 크립토 테이블 전체 순서로 삽입합니다.
    """
    with conn.cursor() as cur:
        # 메인 테이블 배치 삽입
        cur.executemany(f"""
            INSERT INTO {MAIN_TABLE}
                (id, created_at, signer_id, message_hash, algorithm)
            VALUES
                (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s);
        """, records)

        # 크립토 테이블 배치 삽입
        cur.executemany(f"""
            INSERT INTO {CRYPTO_TABLE}
                (id, record_id, public_key, signature)
            VALUES
                (%(id)s, %(id)s, %(public_key)s, %(signature)s);
        """, records)
    conn.commit()


def point_query(conn, record_id: int) -> list:
    """
    고유번호(id)로 단건 조회합니다.
    메인 테이블과 크립토 테이블을 JOIN합니다.
    """
    return execute(conn, f"""
        SELECT r.id, r.created_at, r.signer_id, r.algorithm
        FROM {MAIN_TABLE} r
        JOIN {CRYPTO_TABLE} c ON r.id = c.record_id
        WHERE r.id = %s;
    """, (record_id,), fetch=True)


def range_scan(conn, start_time, end_time) -> list:
    """
    생성시간 범위로 조회합니다.
    메인 테이블 범위 스캔 후 크립토 테이블 JOIN합니다.
    """
    return execute(conn, f"""
        SELECT r.id, r.created_at, r.signer_id, r.algorithm
        FROM {MAIN_TABLE} r
        JOIN {CRYPTO_TABLE} c ON r.id = c.record_id
        WHERE r.created_at BETWEEN %s AND %s;
    """, (start_time, end_time), fetch=True)


def delete_record(conn, record_id: int):
    """
    고유번호(id)로 레코드를 삭제합니다.
    FK CASCADE 설정으로 크립토 테이블도 자동 삭제됩니다.
    """
    execute(conn, f"""
        DELETE FROM {MAIN_TABLE} WHERE id = %s;
    """, (record_id,))


def get_index_name(conn) -> str:
    """
    기본 키 인덱스 이름 반환 (메트릭 수집용)
    """
    return f"{MAIN_TABLE}_pkey"

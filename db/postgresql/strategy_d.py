# =============================================================================
# db/strategy_d.py - 전략 D: Compressed Storage (압축 저장)
# =============================================================================
# Python zlib으로 공개키와 서명을 압축한 후 저장합니다.
# PQC 서명은 랜덤 바이트에 가까워 압축률이 낮을 수 있습니다.
# 이 자체가 논문의 분석 포인트 중 하나입니다.
#
# 테이블 구조:
#   records_d (
#     id, created_at, signer_id, message_hash, algorithm,
#     public_key        ← zlib 압축 후 저장
#     pk_original_size  ← 압축 전 공개키 크기 (바이트)
#     signature         ← zlib 압축 후 저장
#     sig_original_size ← 압축 전 서명 크기 (바이트)
#   )
# =============================================================================

import zlib
from db.postgresql.connection import execute


TABLE_NAME = "records_d"
INDEX_TIME = "idx_d_created"
INDEX_SID  = "idx_d_signer"

# zlib 압축 레벨: 1(빠름/압축률낮음) ~ 9(느림/압축률높음), 기본값 6
COMPRESSION_LEVEL = 6


def create_table(conn):
    """
    전략 D 테이블과 인덱스를 생성합니다.
    """
    drop_table(conn)

    execute(conn, f"""
        CREATE TABLE {TABLE_NAME} (
            id                BIGINT       PRIMARY KEY,   -- 고유번호
            created_at        TIMESTAMPTZ  NOT NULL,       -- 생성시간 (범위 조회 기준)
            signer_id         INTEGER      NOT NULL,        -- 서명자 ID
            message_hash      CHAR(64)     NOT NULL,        -- 메시지 해시
            algorithm         VARCHAR(30)  NOT NULL,        -- 알고리즘 이름
            public_key        BYTEA        NOT NULL,        -- zlib 압축된 공개키
            pk_original_size  INTEGER      NOT NULL,        -- 압축 전 공개키 크기 (분석용)
            signature         BYTEA        NOT NULL,        -- zlib 압축된 서명
            sig_original_size INTEGER      NOT NULL         -- 압축 전 서명 크기 (분석용)
        );
    """)

    execute(conn, f"CREATE INDEX {INDEX_TIME} ON {TABLE_NAME}(created_at);")
    execute(conn, f"CREATE INDEX {INDEX_SID}  ON {TABLE_NAME}(signer_id);")


def drop_table(conn):
    """
    전략 D 테이블을 삭제합니다.
    """
    execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;")


def compress(data: bytes) -> bytes:
    """
    바이트 데이터를 zlib으로 압축합니다.
    PQC 서명처럼 랜덤 바이트는 압축이 거의 안 될 수 있습니다.
    """
    return zlib.compress(data, level=COMPRESSION_LEVEL)


def decompress(data: bytes) -> bytes:
    """
    zlib 압축된 바이트 데이터를 복원합니다.
    """
    return zlib.decompress(data)


def insert_record(conn, record: dict):
    """
    레코드 1건을 압축 후 삽입합니다.
    """
    pk_compressed  = compress(record["public_key"])
    sig_compressed = compress(record["signature"])

    execute(conn, f"""
        INSERT INTO {TABLE_NAME}
            (id, created_at, signer_id, message_hash, algorithm,
             public_key, pk_original_size, signature, sig_original_size)
        VALUES
            (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s,
             %(public_key)s, %(pk_original_size)s,
             %(signature)s, %(sig_original_size)s);
    """, {
        **record,
        "public_key":        pk_compressed,
        "pk_original_size":  len(record["public_key"]),
        "signature":         sig_compressed,
        "sig_original_size": len(record["signature"]),
    })


def insert_batch(conn, records: list):
    """
    레코드를 배치로 압축 후 삽입합니다.
    """
    compressed_records = []
    for r in records:
        pk_compressed  = compress(r["public_key"])
        sig_compressed = compress(r["signature"])
        compressed_records.append({
            **r,
            "public_key":        pk_compressed,
            "pk_original_size":  len(r["public_key"]),
            "signature":         sig_compressed,
            "sig_original_size": len(r["signature"]),
        })

    with conn.cursor() as cur:
        cur.executemany(f"""
            INSERT INTO {TABLE_NAME}
                (id, created_at, signer_id, message_hash, algorithm,
                 public_key, pk_original_size, signature, sig_original_size)
            VALUES
                (%(id)s, %(created_at)s, %(signer_id)s, %(message_hash)s, %(algorithm)s,
                 %(public_key)s, %(pk_original_size)s,
                 %(signature)s, %(sig_original_size)s);
        """, compressed_records)
    conn.commit()


def point_query(conn, record_id: int) -> list:
    """
    고유번호(id)로 단건 조회합니다.
    (압축 해제는 애플리케이션에서 별도 처리)
    """
    return execute(conn, f"""
        SELECT id, created_at, signer_id, algorithm,
               sig_original_size, pk_original_size
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

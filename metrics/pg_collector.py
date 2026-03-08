# =============================================================================
# metrics/collector.py - PostgreSQL 지표 수집
# =============================================================================
# PostgreSQL 시스템 테이블을 쿼리하여 B+tree 관련 지표를 수집합니다.
#
# 수집 지표:
#   - B+tree 깊이       : pageinspect 확장 사용
#   - 인덱스 크기        : pg_relation_size()
#   - 테이블 크기        : pg_relation_size()
#   - TOAST 크기        : pg_total_relation_size() - pg_relation_size()
#   - Page split 수     : 인덱스 페이지 수 변화로 추정
#   - 캐시 히트율        : pg_statio_user_tables
#   - I/O 횟수          : pg_statio_user_tables
# =============================================================================

from db.postgresql.connection import execute


def get_btree_depth(conn, index_name: str) -> int:
    """
    B+tree 인덱스의 깊이(레벨 수)를 반환합니다.
    pageinspect 확장의 bt_metap() 함수를 사용합니다.

    Args:
        index_name : 인덱스 이름 (예: 'records_a_pkey')

    Returns:
        B+tree 깊이 (루트=1, 리프까지의 레벨 수)
        인덱스가 없거나 오류 시 -1 반환
    """
    try:
        result = execute(conn, f"""
            SELECT level FROM bt_metap('{index_name}');
        """, fetch=True)
        return result[0][0] if result else -1
    except Exception as e:
        print(f"  [경고] B+tree 깊이 측정 실패 ({index_name}): {e}")
        return -1


def get_index_size_bytes(conn, index_name: str) -> int:
    """
    인덱스 크기를 바이트로 반환합니다.

    Args:
        index_name : 인덱스 이름

    Returns:
        인덱스 크기 (bytes)
    """
    try:
        result = execute(conn, f"""
            SELECT pg_relation_size('{index_name}');
        """, fetch=True)
        return result[0][0] if result else 0
    except Exception:
        return 0


def get_table_size_bytes(conn, table_name: str) -> int:
    """
    테이블 크기를 바이트로 반환합니다. (TOAST 제외)

    Args:
        table_name : 테이블 이름

    Returns:
        테이블 크기 (bytes)
    """
    try:
        result = execute(conn, f"""
            SELECT pg_relation_size('{table_name}');
        """, fetch=True)
        return result[0][0] if result else 0
    except Exception:
        return 0


def get_toast_size_bytes(conn, table_name: str) -> int:
    """
    TOAST 테이블 크기를 바이트로 반환합니다.
    (총 크기 - 메인 테이블 크기 = TOAST 크기)

    Args:
        table_name : 메인 테이블 이름

    Returns:
        TOAST 크기 (bytes). TOAST가 없으면 0.
    """
    try:
        result = execute(conn, f"""
            SELECT
                pg_total_relation_size('{table_name}') - pg_relation_size('{table_name}')
            AS toast_size;
        """, fetch=True)
        return result[0][0] if result else 0
    except Exception:
        return 0


def get_index_page_count(conn, index_name: str) -> int:
    """
    인덱스의 총 페이지 수를 반환합니다.
    Page split 추정에 사용합니다.

    Args:
        index_name : 인덱스 이름

    Returns:
        페이지 수 (relpages)
    """
    try:
        result = execute(conn, f"""
            SELECT relpages FROM pg_class WHERE relname = '{index_name}';
        """, fetch=True)
        return result[0][0] if result else 0
    except Exception:
        return 0


def get_cache_hit_ratio(conn, table_name: str) -> float:
    """
    테이블의 캐시 히트율을 반환합니다.
    히트율 = 캐시에서 읽은 횟수 / (캐시 + 디스크 읽기 횟수)

    Args:
        table_name : 테이블 이름

    Returns:
        캐시 히트율 (0.0 ~ 1.0). 데이터 없으면 -1 반환.
    """
    try:
        result = execute(conn, f"""
            SELECT
                heap_blks_hit,
                heap_blks_read,
                CASE
                    WHEN (heap_blks_hit + heap_blks_read) = 0 THEN -1
                    ELSE ROUND(heap_blks_hit::numeric /
                               (heap_blks_hit + heap_blks_read), 4)
                END AS hit_ratio
            FROM pg_statio_user_tables
            WHERE relname = '{table_name}';
        """, fetch=True)
        return float(result[0][2]) if result else -1.0
    except Exception:
        return -1.0


def get_io_counts(conn, table_name: str) -> dict:
    """
    테이블의 I/O 횟수를 반환합니다.

    Args:
        table_name : 테이블 이름

    Returns:
        {'heap_read': 디스크읽기, 'heap_hit': 캐시읽기, 'idx_read': 인덱스디스크읽기}
    """
    try:
        result = execute(conn, f"""
            SELECT
                heap_blks_read AS heap_read,
                heap_blks_hit  AS heap_hit,
                idx_blks_read  AS idx_read,
                idx_blks_hit   AS idx_hit
            FROM pg_statio_user_tables
            WHERE relname = '{table_name}';
        """, fetch=True)

        if result:
            return {
                "heap_read": result[0][0] or 0,
                "heap_hit":  result[0][1] or 0,
                "idx_read":  result[0][2] or 0,
                "idx_hit":   result[0][3] or 0,
            }
        return {"heap_read": 0, "heap_hit": 0, "idx_read": 0, "idx_hit": 0}
    except Exception:
        return {"heap_read": 0, "heap_hit": 0, "idx_read": 0, "idx_hit": 0}


def reset_stats(conn):
    """
    PostgreSQL 통계를 초기화합니다.
    각 실험 시작 전에 호출하여 이전 실험 데이터의 영향을 제거합니다.
    """
    execute(conn, "SELECT pg_stat_reset();")


def collect_all(conn, table_name: str, index_name: str) -> dict:
    """
    모든 지표를 한 번에 수집하여 dict로 반환합니다.

    Args:
        table_name : 테이블 이름
        index_name : 기본 키 인덱스 이름

    Returns:
        모든 지표를 담은 dict
    """
    io = get_io_counts(conn, table_name)

    return {
        "btree_depth":       get_btree_depth(conn, index_name),
        "index_size_bytes":  get_index_size_bytes(conn, index_name),
        "table_size_bytes":  get_table_size_bytes(conn, table_name),
        "toast_size_bytes":  get_toast_size_bytes(conn, table_name),
        "index_page_count":  get_index_page_count(conn, index_name),
        "cache_hit_ratio":   get_cache_hit_ratio(conn, table_name),
        "heap_reads":        io["heap_read"],
        "heap_hits":         io["heap_hit"],
        "idx_reads":         io["idx_read"],
        "idx_hits":          io["idx_hit"],
    }

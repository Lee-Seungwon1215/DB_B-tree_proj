# =============================================================================
# metrics/sqlite_collector.py - SQLite B+tree 지표 수집
# =============================================================================
# SQLite의 dbstat 가상 테이블을 사용하여 B+tree 관련 지표를 수집합니다.
#
# SQLite B+tree 구조:
#   - 테이블 자체가 rowid B+tree (heap-organized가 아님)
#   - 서명 데이터가 리프 노드에 직접 저장됨
#   - 1024B(page_size/4) 초과 시 overflow page로 자동 분리
#
# 수집 지표:
#   - B+tree 깊이       : dbstat.path 슬래시 수로 계산
#   - 테이블 크기        : dbstat (non-overflow 페이지)
#   - overflow 크기     : dbstat (overflow 페이지, PostgreSQL TOAST 대응)
#   - overflow 페이지 수 : dbstat
#   - 인덱스 크기        : dbstat (인덱스 이름 필터)
#   - 리프/내부 페이지 수 : dbstat pagetype 구분
# =============================================================================


def get_btree_depth(conn, table_name: str) -> int:
    """
    B+tree 깊이를 반환합니다.
    dbstat.path의 '/' 개수로 계산합니다.
      path='/'      → depth 1 (루트=리프, 소규모)
      path='/001/'  → depth 2
      path='/001/001/' → depth 3

    Args:
        conn       : SQLite 연결 객체
        table_name : 테이블 이름

    Returns:
        B+tree 깊이. 실패 시 -1.
    """
    try:
        cursor = conn.execute("""
            SELECT MAX(length(path) - length(replace(path, '/', '')))
            FROM dbstat
            WHERE name = ? AND pagetype = 'leaf';
        """, (table_name,))
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else -1
    except Exception as e:
        print(f"  [경고] B+tree 깊이 측정 실패 ({table_name}): {e}")
        return -1


def get_table_size_bytes(conn, table_name: str) -> int:
    """
    테이블의 B+tree 페이지 총 크기를 반환합니다. (overflow 제외)

    Args:
        conn       : SQLite 연결 객체
        table_name : 테이블 이름

    Returns:
        크기 (bytes)
    """
    try:
        cursor = conn.execute("""
            SELECT COALESCE(SUM(pgsize), 0)
            FROM dbstat
            WHERE name = ? AND pagetype != 'overflow';
        """, (table_name,))
        result = cursor.fetchone()
        return result[0] or 0
    except Exception:
        return 0


def get_overflow_size_bytes(conn, table_name: str) -> int:
    """
    overflow 페이지의 총 크기를 반환합니다. (PostgreSQL TOAST에 대응)
    서명이 page_size/4 초과 시 overflow page에 저장됩니다.

    Args:
        conn       : SQLite 연결 객체
        table_name : 테이블 이름

    Returns:
        overflow 크기 (bytes). overflow 없으면 0.
    """
    try:
        cursor = conn.execute("""
            SELECT COALESCE(SUM(pgsize), 0)
            FROM dbstat
            WHERE name = ? AND pagetype = 'overflow';
        """, (table_name,))
        result = cursor.fetchone()
        return result[0] or 0
    except Exception:
        return 0


def get_overflow_page_count(conn, table_name: str) -> int:
    """
    overflow 페이지 수를 반환합니다.

    Args:
        conn       : SQLite 연결 객체
        table_name : 테이블 이름

    Returns:
        overflow 페이지 수
    """
    try:
        cursor = conn.execute("""
            SELECT COUNT(*)
            FROM dbstat
            WHERE name = ? AND pagetype = 'overflow';
        """, (table_name,))
        result = cursor.fetchone()
        return result[0] or 0
    except Exception:
        return 0


def get_index_size_bytes(conn, index_name: str) -> int:
    """
    인덱스의 총 크기를 반환합니다.

    Args:
        conn       : SQLite 연결 객체
        index_name : 인덱스 이름

    Returns:
        인덱스 크기 (bytes)
    """
    try:
        cursor = conn.execute("""
            SELECT COALESCE(SUM(pgsize), 0)
            FROM dbstat
            WHERE name = ?;
        """, (index_name,))
        result = cursor.fetchone()
        return result[0] or 0
    except Exception:
        return 0


def get_leaf_page_count(conn, table_name: str) -> int:
    """
    B+tree 리프 페이지 수를 반환합니다.
    리프 페이지에 실제 행 데이터(서명 포함)가 저장됩니다.

    Args:
        conn       : SQLite 연결 객체
        table_name : 테이블 이름

    Returns:
        리프 페이지 수
    """
    try:
        cursor = conn.execute("""
            SELECT COUNT(*)
            FROM dbstat
            WHERE name = ? AND pagetype = 'leaf';
        """, (table_name,))
        result = cursor.fetchone()
        return result[0] or 0
    except Exception:
        return 0


def get_internal_page_count(conn, table_name: str) -> int:
    """
    B+tree 내부 노드(internal page) 수를 반환합니다.

    Args:
        conn       : SQLite 연결 객체
        table_name : 테이블 이름

    Returns:
        내부 노드 수
    """
    try:
        cursor = conn.execute("""
            SELECT COUNT(*)
            FROM dbstat
            WHERE name = ? AND pagetype = 'internal';
        """, (table_name,))
        result = cursor.fetchone()
        return result[0] or 0
    except Exception:
        return 0


def get_page_size(conn) -> int:
    """
    SQLite 페이지 크기를 반환합니다. (기본 4096B)
    overflow 임계값 = page_size / 4

    Returns:
        페이지 크기 (bytes)
    """
    try:
        cursor = conn.execute("PRAGMA page_size;")
        result = cursor.fetchone()
        return result[0] if result else 4096
    except Exception:
        return 4096


def collect_all(conn, table_name: str, index_name: str) -> dict:
    """
    B+tree 지표를 수집하여 dict로 반환합니다.

    공통 지표 (PostgreSQL pg_collector와 동일한 키):
      - btree_depth, table_size_bytes, index_size_bytes, toast_size_bytes

    SQLite 전용 지표:
      - leaf_page_count, internal_page_count, overflow_page_count, page_size

    Args:
        conn       : SQLite 연결 객체
        table_name : 테이블 이름
        index_name : 인덱스 이름 (없으면 None)

    Returns:
        지표 dict
    """
    page_size = get_page_size(conn)

    return {
        # ── 공통 지표 (PostgreSQL pg_collector와 동일한 키) ──────────────
        "btree_depth":         get_btree_depth(conn, table_name),
        "table_size_bytes":    get_table_size_bytes(conn, table_name),
        "index_size_bytes":    get_index_size_bytes(conn, index_name) if index_name else 0,
        "toast_size_bytes":    get_overflow_size_bytes(conn, table_name),  # overflow = TOAST 대응

        # ── SQLite 전용 지표 ──────────────────────────────────────────────
        "leaf_page_count":     get_leaf_page_count(conn, table_name),
        "internal_page_count": get_internal_page_count(conn, table_name),
        "overflow_page_count": get_overflow_page_count(conn, table_name),
        "page_size":           page_size,
        "overflow_threshold":  page_size // 4,
    }

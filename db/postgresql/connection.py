# =============================================================================
# db/connection.py - PostgreSQL 연결 관리
# =============================================================================
# PostgreSQL에 연결하고 연결을 끊는 함수를 제공합니다.
# 모든 DB 작업은 이 모듈을 통해 연결을 가져옵니다.
# =============================================================================

import psycopg2
from config import DB_CONFIG


def get_connection():
    """
    PostgreSQL 연결 객체를 반환합니다.
    config.py의 DB_CONFIG 설정을 사용합니다.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def execute(conn, sql, params=None, fetch=False):
    """
    SQL 쿼리를 실행하는 헬퍼 함수입니다.

    Args:
        conn   : psycopg2 연결 객체
        sql    : 실행할 SQL 문자열
        params : SQL 파라미터 (옵션)
        fetch  : True면 결과를 반환, False면 반환 없음

    Returns:
        fetch=True일 때 쿼리 결과 리스트 반환
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if fetch:
            return cur.fetchall()
    conn.commit()


def setup_extensions(conn):
    """
    실험에 필요한 PostgreSQL 확장(extension)을 설치합니다.
    - pageinspect : B+tree 깊이 측정에 필요
    - pg_stat_statements : 쿼리 통계 수집에 필요
    """
    execute(conn, "CREATE EXTENSION IF NOT EXISTS pageinspect;")
    execute(conn, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
    print("PostgreSQL 확장 설치 완료 (pageinspect, pg_stat_statements)")

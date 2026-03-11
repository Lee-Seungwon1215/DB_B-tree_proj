# =============================================================================
# db/sqlite/strategy_a.py - 전략 A: 4KB 페이지, 단일 테이블 인라인 (기준선)
# =============================================================================

from metrics.sqlite_collector import collect_all as _collect_all

PAGE_SIZE = 4096


def create_table(conn, table: str):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id  TEXT NOT NULL,
            public_key BLOB NOT NULL,
            signature  BLOB NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )""")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_e ON {table}(entity_id)")
    conn.commit()


def drop_table(conn, table: str):
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()


def insert_batch(conn, table: str, batch: list):
    """batch: list of (entity_id, public_key, signature)"""
    conn.executemany(
        f"INSERT INTO {table}(entity_id,public_key,signature) VALUES(?,?,?)", batch)
    conn.commit()


def insert_single(conn, table: str, entity_id: str, pk: bytes, sig: bytes):
    conn.execute(
        f"INSERT INTO {table}(entity_id, public_key, signature) VALUES(?, ?, ?)",
        (entity_id, pk, sig))
    conn.commit()


def point_query(conn, table: str, entity_id: str):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE entity_id=?", (entity_id,))
    return cur.fetchone()


def range_scan(conn, table: str, limit: int, offset: int) -> list:
    cur = conn.cursor()
    cur.execute(
        f"SELECT * FROM {table} ORDER BY id LIMIT ? OFFSET ?", (limit, offset))
    return cur.fetchall()


def update_record(conn, table: str, entity_id: str, pk: bytes, sig: bytes):
    conn.execute(
        f"UPDATE {table} SET public_key=?, signature=? WHERE entity_id=?",
        (pk, sig, entity_id))
    conn.commit()


def delete_record(conn, table: str, entity_id: str):
    conn.execute(f"DELETE FROM {table} WHERE entity_id=?", (entity_id,))
    conn.commit()


def range_delete_records(conn, table: str, entity_ids: list):
    if not entity_ids:
        return
    placeholders = ",".join(["?"] * len(entity_ids))
    conn.execute(
        f"DELETE FROM {table} WHERE entity_id IN ({placeholders})", entity_ids)
    conn.commit()


def get_main_table(table: str) -> str:
    return table


def get_index_name(table: str) -> str:
    return f"idx_{table}_e"


def analyze(conn, table: str):
    conn.execute(f"ANALYZE {table}")
    conn.commit()


def collect_metrics(conn, table: str) -> dict:
    return _collect_all(conn, table, get_index_name(table))

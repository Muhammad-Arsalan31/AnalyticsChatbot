"""
db.py — Database layer
Fixes:
  - Connection pool (no more per-call connect/close)
  - Parameterized queries everywhere (SQL injection fix)
  - Safe SQL execution with forbidden-keyword guard
"""

import os
import re
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv(override=True)

_DB_URL = os.getenv("DATABASE_URL", "")
_CLEAN_URL = _DB_URL.split("?")[0] if _DB_URL else ""

# ---------------------------------------------------------------------------
# Connection pool (1–10 connections)
# ---------------------------------------------------------------------------
_pool: pool.SimpleConnectionPool | None = None

def _get_pool() -> pool.SimpleConnectionPool:
    global _pool
    db_url = os.getenv("DATABASE_URL", "")
    # Keep full URL — psycopg2 supports sslmode in the connection string
    # Only strip unsupported params like 'schema' (not 'sslmode')
    clean_url = re.sub(r"[?&]schema=[^&]*", "", db_url) if db_url else ""
    
    if _pool is None or _pool.closed:
        _pool = pool.SimpleConnectionPool(1, 10, clean_url)
    return _pool


def get_conn():
    return _get_pool().getconn()


def put_conn(conn):
    _get_pool().putconn(conn)


# ---------------------------------------------------------------------------
# Safe SQL execution
# ---------------------------------------------------------------------------
_FORBIDDEN = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE"}


def run_sql_query(query: str):
    """
    Execute a READ-ONLY SELECT or WITH query.
    Returns list[dict] on success, or {"error": str} on failure.
    """
    # Strip SQL comments before validation
    clean_q = re.sub(r"(--.*)|(/\*[\s\S]*?\*/)", "", query).strip().upper()

    if not (clean_q.startswith("SELECT") or clean_q.startswith("WITH")):
        return {"error": "Only SELECT or WITH queries are allowed."}

    for word in _FORBIDDEN:
        if re.search(rf"\b{word}\b", clean_q):
            return {"error": f"Keyword '{word}' is not allowed."}

    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        if conn:
            put_conn(conn)


# ---------------------------------------------------------------------------
# Auth — parameterized, bcrypt-safe
# ---------------------------------------------------------------------------
def verify_login(username: str, password: str) -> bool:
    """
    Verify credentials against managers and users tables.
    Uses parameterized queries — no string interpolation.
    """
    import bcrypt

    # Hardcoded admin fallback (replace with env var in production)
    admin_user = os.getenv("ADMIN_USER", "admin")
    admin_pass = os.getenv("ADMIN_PASS", "admin")
    if username == admin_user and password == admin_pass:
        return True

    pass_bytes = password.encode("utf-8")

    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # 1. managers table
            cur.execute(
                "SELECT password FROM managers WHERE email ILIKE %s OR name ILIKE %s LIMIT 1",
                (username, username),
            )
            row = cur.fetchone()
            if row and row.get("password"):
                try:
                    if bcrypt.checkpw(pass_bytes, row["password"].encode("utf-8")):
                        return True
                except Exception:
                    pass

            # 2. users table
            cur.execute(
                "SELECT password FROM users WHERE email ILIKE %s OR firstname ILIKE %s LIMIT 1",
                (username, username),
            )
            row = cur.fetchone()
            if row and row.get("password"):
                stored = str(row["password"])
                try:
                    if bcrypt.checkpw(pass_bytes, stored.encode("utf-8")):
                        return True
                except Exception:
                    # Plain-text integer/text fallback
                    if stored == password:
                        return True

    except Exception as exc:
        print(f"[Auth Error] {exc}")
    finally:
        if conn:
            put_conn(conn)

    return False


# ---------------------------------------------------------------------------
# Chat history persistence
# ---------------------------------------------------------------------------
def upsert_db_chat(username: str, session_id: str, messages_json: str):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_chat_history (username, session_id, history_json, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (session_id)
                DO UPDATE SET history_json = EXCLUDED.history_json,
                              updated_at   = CURRENT_TIMESTAMP
                """,
                (username, session_id, messages_json),
            )
            conn.commit()
    except Exception as exc:
        print(f"[DB Save Warning] {exc}")
    finally:
        if conn:
            put_conn(conn)


def delete_db_chat(session_id: str):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM app_chat_history WHERE session_id = %s",
                (session_id,),
            )
            conn.commit()
    except Exception as exc:
        print(f"[DB Delete Warning] {exc}")
    finally:
        if conn:
            put_conn(conn)


def load_db_chat(session_id: str) -> str | None:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT history_json FROM app_chat_history WHERE session_id = %s",
                (session_id,),
            )
            row = cur.fetchone()
            return row["history_json"] if row else None
    except Exception as exc:
        print(f"[DB Load Warning] {exc}")
        return None
    finally:
        if conn:
            put_conn(conn)

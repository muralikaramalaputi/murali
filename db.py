# db.py

import os
import math
from typing import Dict, Any, Iterable, List, Sequence
from urllib.parse import urlparse
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values


# ==================================================
# DATABASE CONNECTION
# ==================================================
def get_connection():
    database_url = os.environ.get("DATABASE_URL")

    if database_url:
        parsed = urlparse(database_url)
        return psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:],
            user=parsed.username,
            password=parsed.password,
            sslmode="require",
        )
    else:
        from config import DB_CONFIG
        return psycopg2.connect(**DB_CONFIG)


# ==================================================
# INIT DB
# ==================================================
def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS part_master (
            id SERIAL PRIMARY KEY,
            part_number TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


# ==================================================
# COLUMN HELPERS
# ==================================================
def _get_existing_columns(cur) -> List[str]:
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'part_master';
    """)
    return [r[0] for r in cur.fetchall()]


def ensure_columns(columns: Iterable[str]) -> None:
    cols = set(columns) - {"id", "part_number", "updated_at"}
    if not cols:
        return

    conn = get_connection()
    cur = conn.cursor()

    existing = set(_get_existing_columns(cur))
    for c in cols:
        if c not in existing:
            cur.execute(f'ALTER TABLE part_master ADD COLUMN "{c}" TEXT;')

    conn.commit()
    cur.close()
    conn.close()


# ==================================================
# SANITIZE
# ==================================================
def _sanitize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


# ==================================================
# 🔥 UPSERT PART MASTER (FULL FIX)
# ==================================================
def upsert_part_master(records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return

    # Collect all keys
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())

    if "part_number" not in all_keys:
        return

    # Ensure DB has all columns
    ensure_columns(all_keys)

    conn = get_connection()
    cur = conn.cursor()

    existing_cols = set(_get_existing_columns(cur))

    # Columns used for insert/update
    data_cols = [
        c for c in all_keys
        if c in existing_cols and c not in {"id", "updated_at"}
    ]

    # Ensure order
    data_cols = ["part_number"] + [c for c in data_cols if c != "part_number"]

    insert_cols = ["part_number", "updated_at"] + [
        c for c in data_cols if c != "part_number"
    ]

    insert_sql_cols = ", ".join(f'"{c}"' for c in insert_cols)

    update_sql = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in insert_cols
        if c != "part_number"
    )

    sql = f"""
        INSERT INTO part_master ({insert_sql_cols})
        VALUES %s
        ON CONFLICT (part_number)
        DO UPDATE SET {update_sql};
    """

    now = datetime.utcnow()
    values = []

    for r in records:
        pn = r.get("part_number")
        if not pn:
            continue

        row = [pn, now]
        for c in data_cols:
            if c == "part_number":
                continue
            row.append(_sanitize_value(r.get(c)))

        values.append(tuple(row))

    if not values:
        cur.close()
        conn.close()
        return

    execute_values(cur, sql, values)
    conn.commit()
    cur.close()
    conn.close()


# ==================================================
# FETCH
# ==================================================
def fetch_part_by_number(part_number: str) -> Dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM part_master WHERE part_number = %s;",
        (part_number,)
    )

    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None

    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return dict(zip(cols, row))

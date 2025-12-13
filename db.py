# db.py

import os
import math
from typing import Dict, Any, Iterable, List, Sequence
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values


def get_connection():
    """
    Get database connection from DATABASE_URL environment variable (Render).
    Falls back to config.DB_CONFIG for local development.
    """
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Parse DATABASE_URL for Render/production
        parsed = urlparse(database_url)
        return psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:],  # Remove leading '/'
            user=parsed.username,
            password=parsed.password,
            sslmode='require'  # Render requires SSL
        )
    else:
        # Fall back to local config for development
        try:
            from config import DB_CONFIG
            return psycopg2.connect(**DB_CONFIG)
        except ImportError:
            raise Exception("DATABASE_URL not set and config.DB_CONFIG not available")


def init_db():
    """
    Create single flat table part_master if not exists.
    Only id, part_number, updated_at here.
    Other columns will be added dynamically.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS part_master (
            id SERIAL PRIMARY KEY,
            part_number TEXT UNIQUE,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    conn.commit()
    cur.close()
    conn.close()


def _get_existing_columns(cur) -> List[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'part_master'
          AND table_schema = 'public';
        """
    )
    return [r[0] for r in cur.fetchall()]


def ensure_columns(columns: Iterable[str]) -> None:
    """
    Add missing columns (TEXT) to part_master.
    """
    cols = set(columns) - {"id", "part_number", "updated_at"}
    if not cols:
        return

    conn = get_connection()
    cur = conn.cursor()

    existing = set(_get_existing_columns(cur))
    new_cols = [c for c in cols if c not in existing]

    for c in new_cols:
        cur.execute(f'ALTER TABLE part_master ADD COLUMN "{c}" TEXT;')

    conn.commit()
    cur.close()
    conn.close()


def _sanitize_value(v: Any) -> Any:
    """
    Convert values to DB-safe (None or string).
    """
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


def upsert_part_master(records: Sequence[Dict[str, Any]]) -> None:
    """
    Upsert records into part_master.
    - Dynamic columns (TEXT) added as needed.
    - ON CONFLICT(part_number) -> UPDATE all other columns + updated_at.
    """
    if not records:
        return

    # union of all keys
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())

    if "part_number" not in all_keys:
        return

    # ensure columns in DB
    ensure_columns(all_keys)

    conn = get_connection()
    cur = conn.cursor()

    existing_cols = set(_get_existing_columns(cur))
    # Only use columns that actually exist in DB
    used_cols = [c for c in all_keys if c in existing_cols and c != "id" and c != "updated_at"]

    # make sure part_number is first
    used_cols = ["part_number"] + [c for c in used_cols if c != "part_number"]

    cols_sql = ", ".join(f'"{c}"' for c in used_cols)

    update_assignments = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in used_cols if c != "part_number"
    )

    sql = f"""
        INSERT INTO part_master ({cols_sql})
        VALUES %s
        ON CONFLICT (part_number) DO UPDATE SET
            {update_assignments},
            updated_at = NOW();
    """

    values = []
    for r in records:
        pn = r.get("part_number")
        if not pn:
            continue
        row_vals = [_sanitize_value(r.get(c)) for c in used_cols]
        values.append(row_vals)

    if not values:
        cur.close()
        conn.close()
        return

    execute_values(cur, sql, values)
    conn.commit()
    cur.close()
    conn.close()


def fetch_part_by_number(part_number: str) -> Dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM part_master WHERE part_number = %s;', (part_number,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None

    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return dict(zip(cols, row))
# fix_database.py
import os
from urllib.parse import urlparse
import psycopg2

def fix_database():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL not set")
        return

    parsed = urlparse(database_url)
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path[1:],
        user=parsed.username,
        password=parsed.password,
        sslmode="require"
    )

    cur = conn.cursor()

    try:
        print("🔧 Ensuring UNIQUE constraint on part_number...", flush=True)

        cur.execute("""
            DELETE FROM part_master
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM part_master
                GROUP BY part_number
            );
        """)

        cur.execute("""
            ALTER TABLE part_master
            DROP CONSTRAINT IF EXISTS part_master_part_number_key;
        """)

        cur.execute("""
            ALTER TABLE part_master
            ADD CONSTRAINT part_master_part_number_key UNIQUE (part_number);
        """)

        conn.commit()
        print("✅ Constraint ensured", flush=True)

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}", flush=True)

    finally:
        cur.close()
        conn.close()

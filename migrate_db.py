"""
Portable Railway → Render Postgres migrator.
No pg_dump needed — uses psycopg2 + COPY so it works anywhere psycopg2 does.

Usage:
  python migrate_db.py dump                     # Reads DATABASE_URL from .env, writes ./db_backup/
  python migrate_db.py restore <TARGET_DB_URL>  # Loads ./db_backup/ into the target database

Handles:
  • pgvector: dumped/loaded as text literals, registered on restore
  • Extensions listed in pg_extension (except plpgsql which is built in)
  • All user tables in the public schema
  • Sequences re-synced to max(id) after restore

Does NOT handle: views, triggers, custom types, non-public schemas.
This app's schema fits within those limits (just tables + vector columns).
"""
import os
import sys
import pathlib
import json
import time
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

BACKUP_DIR = pathlib.Path(__file__).parent / "db_backup"


def _connect(url: str):
    return psycopg2.connect(url)


def _get_tables(cur) -> list:
    cur.execute("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """)
    return [r[0] for r in cur.fetchall()]


def _get_extensions(cur) -> list:
    cur.execute("SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname")
    return [r[0] for r in cur.fetchall()]


def _get_ddl(cur, table: str) -> str:
    """Build a CREATE TABLE statement from information_schema + pg_indexes."""
    cur.execute("""
        SELECT column_name, data_type, udt_name, is_nullable, column_default,
               character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    cols = cur.fetchall()

    col_defs = []
    for name, dtype, udt, nullable, default, char_max, num_prec, num_scale in cols:
        # Map to a portable type string
        if udt == "vector":
            # pgvector — dimension stored in atttypmod
            cur.execute("""
                SELECT atttypmod FROM pg_attribute
                WHERE attrelid = %s::regclass AND attname = %s
            """, (f'public.{table}', name))
            mod = cur.fetchone()[0]
            dim = mod if mod > 0 else 1024
            type_str = f"vector({dim})"
        elif dtype == "character varying":
            type_str = f"varchar({char_max})" if char_max else "text"
        elif dtype == "numeric":
            type_str = (
                f"numeric({num_prec},{num_scale})"
                if num_prec is not None and num_scale is not None else "numeric"
            )
        elif dtype == "ARRAY":
            # udt like "_text" → text[]
            base = udt[1:] if udt.startswith("_") else udt
            type_str = f"{base}[]"
        elif dtype == "USER-DEFINED":
            type_str = udt
        else:
            type_str = dtype

        parts = [f'  "{name}" {type_str}']
        if default is not None:
            parts.append(f"DEFAULT {default}")
        if nullable == "NO":
            parts.append("NOT NULL")
        col_defs.append(" ".join(parts))

    # Primary key
    cur.execute("""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
    """, (f'public.{table}',))
    pk = [r[0] for r in cur.fetchall()]
    if pk:
        col_defs.append(f'  PRIMARY KEY ("{ chr(34).join(pk) }")'.replace(chr(34), '"'))

    return f'CREATE TABLE IF NOT EXISTS "{table}" (\n' + ",\n".join(col_defs) + "\n);"


def _get_indexes(cur, table: str) -> list:
    cur.execute("""
        SELECT indexdef FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
          AND indexname NOT IN (
            SELECT conname FROM pg_constraint
            WHERE conrelid = %s::regclass AND contype IN ('p','u')
          )
    """, (table, f'public.{table}'))
    return [r[0] for r in cur.fetchall()]


def dump(url: str) -> None:
    BACKUP_DIR.mkdir(exist_ok=True)
    start = time.time()
    meta = {"dumped_at": time.time(), "tables": {}, "extensions": []}

    with _connect(url) as conn, conn.cursor() as cur:
        meta["extensions"] = _get_extensions(cur)
        tables = _get_tables(cur)
        print(f"Found {len(tables)} tables: {', '.join(tables)}")

        schema_sql_parts = []
        for ext in meta["extensions"]:
            schema_sql_parts.append(f"CREATE EXTENSION IF NOT EXISTS {ext};")
        schema_sql_parts.append("")

        for table in tables:
            schema_sql_parts.append(_get_ddl(cur, table))
            for idx in _get_indexes(cur, table):
                schema_sql_parts.append(idx + ";")
            schema_sql_parts.append("")

            # Dump data as CSV
            data_file = BACKUP_DIR / f"{table}.csv"
            with data_file.open("w", encoding="utf-8", newline="") as f:
                cur.copy_expert(
                    f'COPY "{table}" TO STDOUT WITH (FORMAT CSV, HEADER, FORCE_QUOTE *)',
                    f,
                )

            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            n = cur.fetchone()[0]
            size = data_file.stat().st_size
            meta["tables"][table] = {"rows": n, "csv_bytes": size}
            print(f"  dumped {table:<24} {n:>6} rows  {size/1024:>8.1f} KB")

        (BACKUP_DIR / "schema.sql").write_text("\n".join(schema_sql_parts), encoding="utf-8")
        (BACKUP_DIR / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    elapsed = time.time() - start
    total_rows = sum(t["rows"] for t in meta["tables"].values())
    print(f"\nDump complete: {total_rows} rows across {len(meta['tables'])} tables in {elapsed:.1f}s")
    print(f"Backup at: {BACKUP_DIR}")
    print(f"\nNext: python migrate_db.py restore <RENDER_DATABASE_URL>")


def restore(url: str) -> None:
    if not BACKUP_DIR.exists():
        print(f"ERROR: {BACKUP_DIR} not found. Run 'python migrate_db.py dump' first.")
        sys.exit(1)

    meta = json.loads((BACKUP_DIR / "meta.json").read_text(encoding="utf-8"))
    schema_sql = (BACKUP_DIR / "schema.sql").read_text(encoding="utf-8")

    start = time.time()
    with _connect(url) as conn:
        # Register pgvector if the backup uses it. The extension MUST be enabled
        # on the server so the CREATE TABLE ... vector(N) works. Client-side
        # register_vector is only needed for typed query results — our COPY-from-CSV
        # path sends/receives vectors as text literals, which the server parses
        # natively, so the Python pgvector package is optional here.
        if "vector" in meta["extensions"]:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            conn.commit()
            try:
                from pgvector.psycopg2 import register_vector
                register_vector(conn)
            except ImportError:
                print("  (pgvector Python pkg not installed — continuing; CSV path does not need it)")

        # Pre-create any sequences referenced by column defaults — CREATE TABLE
        # fails if a nextval() target doesn't exist yet. (The dump doesn't emit
        # these separately; parsing them out of schema.sql is cheaper than re-dumping.)
        import re
        seqs = set(re.findall(r"nextval\('([^']+)'", schema_sql))
        if seqs:
            with conn.cursor() as cur:
                for s in seqs:
                    cur.execute(f'CREATE SEQUENCE IF NOT EXISTS "{s}"')
            conn.commit()
            print(f"Pre-created {len(seqs)} sequence(s): {', '.join(sorted(seqs))}")

        # Create schema
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print(f"Schema applied ({len(meta['tables'])} tables)")

        # Load data in order, wiping any existing rows first
        for table, info in meta["tables"].items():
            data_file = BACKUP_DIR / f"{table}.csv"
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE "{table}" RESTART IDENTITY CASCADE')
                with data_file.open("r", encoding="utf-8") as f:
                    cur.copy_expert(
                        f'COPY "{table}" FROM STDIN WITH (FORMAT CSV, HEADER)',
                        f,
                    )
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                n = cur.fetchone()[0]
                conn.commit()
                status = "ok" if n == info["rows"] else f"MISMATCH (expected {info['rows']})"
                print(f"  restored {table:<24} {n:>6} rows  [{status}]")

        # Re-sync sequences so future INSERTs don't collide with restored IDs
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sequencename, schemaname
                FROM pg_sequences WHERE schemaname = 'public'
            """)
            for seq_name, schema in cur.fetchall():
                # Find which table/column owns this sequence
                cur.execute("""
                    SELECT tbl.relname AS table_name, att.attname AS column_name
                    FROM pg_class seq
                    JOIN pg_depend dep ON dep.objid = seq.oid
                    JOIN pg_class tbl ON tbl.oid = dep.refobjid
                    JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = dep.refobjsubid
                    WHERE seq.relname = %s AND seq.relkind = 'S'
                """, (seq_name,))
                owner = cur.fetchone()
                if owner:
                    tbl, col = owner
                    cur.execute(
                        f'SELECT setval(%s, COALESCE((SELECT MAX("{col}") FROM "{tbl}"), 1))',
                        (f"{schema}.{seq_name}",),
                    )
            conn.commit()

    elapsed = time.time() - start
    total_rows = sum(t["rows"] for t in meta["tables"].values())
    print(f"\nRestore complete: {total_rows} rows in {elapsed:.1f}s")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("dump", "restore"):
        print(__doc__)
        sys.exit(1)

    load_dotenv(pathlib.Path(__file__).parent / ".env")

    if sys.argv[1] == "dump":
        url = os.environ.get("DATABASE_URL")
        if not url:
            print("ERROR: DATABASE_URL not set in .env"); sys.exit(1)
        dump(url)
    else:
        if len(sys.argv) < 3:
            print("ERROR: restore needs the target URL as arg 2"); sys.exit(1)
        restore(sys.argv[2])

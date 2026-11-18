"""Land the CSV extracts in PostgreSQL schema ``raw`` (the "EL" of ELT).

Every ``data/raw/*.csv`` file becomes one table, ``raw.<file stem>``, exactly as
``docs/data-contract.md`` describes:

* every source column is ``text`` so the load never fails on bad values;
  typing and cleaning are the dbt staging layer's job;
* empty CSV fields are stored as NULL;
* two metadata columns are appended: ``_source_file`` (the file name) and
  ``_loaded_at`` (one UTC timestamp shared by the whole run);
* each file is a full refresh: drop, create, ``COPY ... FROM STDIN`` and an
  audit row in ``raw._load_audit`` all happen in a single transaction, so a
  failed load leaves the previous version of the table untouched.

The drop uses ``CASCADE`` because dbt builds its staging layer as views on these
tables; those views are recreated by the next ``dbt build``.

Connection settings come from the standard libpq variables (PGHOST, PGPORT,
PGUSER, PGPASSWORD, PGDATABASE) with local development defaults.

Usage::

    python pipeline/load_raw.py [--data-dir data/raw]
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql

RAW_SCHEMA = "raw"
AUDIT_TABLE = "_load_audit"
SOURCE_FILE_COLUMN = "_source_file"
LOADED_AT_COLUMN = "_loaded_at"
METADATA_COLUMNS = (SOURCE_FILE_COLUMN, LOADED_AT_COLUMN)
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
CSV_GLOB = "*.csv"
CSV_ENCODING = "utf-8-sig"  # tolerates a byte-order mark written by spreadsheet tools
IDENTIFIER_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")
CONNECTION_DEFAULTS = {
    "PGHOST": "localhost",
    "PGPORT": "5432",
    "PGUSER": "postgres",
    "PGPASSWORD": "postgres",
    "PGDATABASE": "retail",
}
CONNECT_TIMEOUT_SECONDS = 10
APPLICATION_NAME = "retail-load-raw"

log = logging.getLogger("load_raw")


class LoadError(Exception):
    """A source file cannot be loaded as the data contract requires."""


# --------------------------------------------------------------------------- #
# Pure helpers (no database needed)
# --------------------------------------------------------------------------- #
def connection_params(env: Mapping[str, str]) -> dict[str, str | int]:
    """Build psycopg connection arguments from libpq variables, using contract defaults."""
    setting = {key: env.get(key) or default for key, default in CONNECTION_DEFAULTS.items()}
    return {
        "host": setting["PGHOST"],
        "port": setting["PGPORT"],
        "user": setting["PGUSER"],
        "password": setting["PGPASSWORD"],
        "dbname": setting["PGDATABASE"],
        "connect_timeout": CONNECT_TIMEOUT_SECONDS,
        "application_name": APPLICATION_NAME,
    }


def validate_identifier(name: str, kind: str) -> str:
    """Accept only lower snake_case names so tables and columns never need quoting in SQL."""
    if not IDENTIFIER_PATTERN.fullmatch(name):
        raise LoadError(f"{kind} name {name!r} is not lower snake_case")
    return name


def table_name_for(path: Path) -> str:
    """Map ``data/raw/orders.csv`` to the table name ``orders``."""
    return validate_identifier(path.stem, "table")


def parse_header(header: Sequence[str] | None, file_name: str) -> list[str]:
    """Validate a CSV header row and return its column names."""
    if not header:
        raise LoadError(f"{file_name} has no header row")
    columns = [validate_identifier(column.strip(), f"{file_name}: column") for column in header]
    if len(set(columns)) != len(columns):
        raise LoadError(f"{file_name} has duplicate column names: {columns}")
    reserved = set(columns) & set(METADATA_COLUMNS)
    if reserved:
        raise LoadError(f"{file_name} uses reserved metadata column(s) {sorted(reserved)}")
    return columns


def qualified(table: str) -> sql.Identifier:
    return sql.Identifier(RAW_SCHEMA, table)


def build_create_table(table: str, columns: Sequence[str]) -> sql.Composed:
    """CREATE TABLE statement with every source column as text plus the metadata columns."""
    definitions = [sql.SQL("{} text").format(sql.Identifier(column)) for column in columns]
    definitions += [
        sql.SQL("{} text NOT NULL").format(sql.Identifier(SOURCE_FILE_COLUMN)),
        sql.SQL("{} timestamptz NOT NULL").format(sql.Identifier(LOADED_AT_COLUMN)),
    ]
    return sql.SQL("CREATE TABLE {} ({})").format(qualified(table), sql.SQL(", ").join(definitions))


def build_copy_statement(table: str, columns: Sequence[str]) -> sql.Composed:
    """COPY statement that streams source and metadata columns from the client."""
    target_columns = [*columns, *METADATA_COLUMNS]
    return sql.SQL("COPY {} ({}) FROM STDIN").format(
        qualified(table), sql.SQL(", ").join(map(sql.Identifier, target_columns))
    )


def to_copy_row(
    fields: Sequence[str], source_file: str, loaded_at: datetime
) -> list[str | datetime | None]:
    """Turn one CSV record into a COPY row: empty strings become NULL, metadata appended."""
    return [field if field != "" else None for field in fields] + [source_file, loaded_at]


# --------------------------------------------------------------------------- #
# Database work
# --------------------------------------------------------------------------- #
def ensure_schema(conn: psycopg.Connection) -> None:
    """Create the raw schema and the audit table if this is the first run."""
    with conn.transaction():
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(RAW_SCHEMA)))
        conn.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ("
            "file_name text NOT NULL, row_count integer NOT NULL, loaded_at timestamptz NOT NULL)"
        ).format(qualified(AUDIT_TABLE)))


def copy_rows(
    cursor: psycopg.Cursor,
    table: str,
    columns: Sequence[str],
    records: Iterable[list[str]],
    source_file: str,
    loaded_at: datetime,
) -> int:
    """Stream CSV records into ``raw.<table>`` with COPY; return the number of rows."""
    row_count = 0
    with cursor.copy(build_copy_statement(table, columns)) as copy:
        for record_number, record in enumerate(records, start=1):
            if not record:
                continue  # blank line
            if len(record) != len(columns):
                raise LoadError(
                    f"{source_file} data row {record_number}: expected {len(columns)} fields, "
                    f"got {len(record)}"
                )
            copy.write_row(to_copy_row(record, source_file, loaded_at))
            row_count += 1
    return row_count


def load_file(conn: psycopg.Connection, path: Path, loaded_at: datetime) -> int:
    """Fully refresh ``raw.<file stem>`` from one CSV file inside one transaction."""
    table = table_name_for(path)
    started = time.perf_counter()
    with path.open(encoding=CSV_ENCODING, newline="") as handle:
        reader = csv.reader(handle)
        columns = parse_header(next(reader, None), path.name)
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(qualified(table)))
            cursor.execute(build_create_table(table, columns))
            row_count = copy_rows(cursor, table, columns, reader, path.name, loaded_at)
            cursor.execute(
                sql.SQL("INSERT INTO {} (file_name, row_count, loaded_at) VALUES (%s, %s, %s)")
                .format(qualified(AUDIT_TABLE)),
                (path.name, row_count, loaded_at),
            )
    log.info(
        "Loaded %-16s -> %s.%-12s %8s rows in %.2fs",
        path.name, RAW_SCHEMA, table, f"{row_count:,}", time.perf_counter() - started,
    )
    return row_count


def find_csv_files(data_dir: Path) -> list[Path]:
    if not data_dir.is_dir():
        raise LoadError(f"data directory {data_dir} does not exist")
    files = sorted(data_dir.glob(CSV_GLOB))
    if not files:
        raise LoadError(f"no {CSV_GLOB} files found in {data_dir}")
    return files


def load_all(data_dir: Path, env: Mapping[str, str]) -> int:
    """Load every CSV in ``data_dir``; stop at the first failure. Return total rows."""
    files = find_csv_files(data_dir)
    params = connection_params(env)
    loaded_at = datetime.now(timezone.utc)
    log.info(
        "Connecting to %s:%s/%s as %s",
        params["host"], params["port"], params["dbname"], params["user"],
    )
    with psycopg.connect(autocommit=True, **params) as conn:
        ensure_schema(conn)
        return sum(load_file(conn, path, loaded_at) for path in files)


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #
def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load data/raw/*.csv into PostgreSQL schema 'raw' (full refresh).",
        epilog="Connection: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE "
               "(defaults: localhost, 5432, postgres, postgres, retail).",
    )
    parser.add_argument(
        "--data-dir", type=Path, default=DEFAULT_DATA_DIR,
        help="directory containing the CSV extracts (default: data/raw in the repository)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    started = time.perf_counter()
    try:
        total_rows = load_all(args.data_dir, os.environ)
    except (LoadError, OSError, UnicodeDecodeError, csv.Error, psycopg.Error) as error:
        log.error("Load failed: %s", error)
        return 1
    log.info("Done: %s rows loaded in %.2fs", f"{total_rows:,}", time.perf_counter() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())

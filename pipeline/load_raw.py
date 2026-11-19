"""Land the CSV extracts in PostgreSQL schema ``raw`` (the "EL" of ELT).

Every CSV file becomes one table, ``raw.<file stem>``, exactly as
``docs/data-contract.md`` describes:

* every source column is ``text`` so the load never fails on bad values;
  typing and cleaning are the dbt staging layer's job;
* empty CSV fields are stored as NULL;
* three metadata columns are appended: ``_source_file`` (the file name),
  ``_loaded_at`` (one UTC timestamp shared by the whole run) and ``_run_date``
  (the batch day; NULL in full mode);
* each file is loaded in a single transaction together with its audit row in
  ``raw._load_audit``, so a failed load leaves the previous data untouched.

Two modes:

* ``full`` (default, v1): reads ``data/raw/*.csv`` and fully refreshes each table
  (drop, create, ``COPY ... FROM STDIN``). The drop uses ``CASCADE`` because dbt
  builds its staging layer as views on these tables; those views are recreated
  by the next ``dbt build``.
* ``append`` (v2 daily batches): reads ``data/raw/<run-date>/*.csv``, creates
  missing tables and columns, then deletes the rows of that run date and copies
  the batch in. Re-running a day therefore replaces it instead of duplicating it.

Connection settings come from the standard libpq variables (PGHOST, PGPORT,
PGUSER, PGPASSWORD, PGDATABASE) with local development defaults.

Usage::

    python pipeline/load_raw.py [--data-dir data/raw]
    python pipeline/load_raw.py --mode append --run-date 2025-07-01 [--data-dir data/raw]
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
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql

RAW_SCHEMA = "raw"
AUDIT_TABLE = "_load_audit"
SOURCE_FILE_COLUMN = "_source_file"
LOADED_AT_COLUMN = "_loaded_at"
RUN_DATE_COLUMN = "_run_date"
METADATA_COLUMNS = (SOURCE_FILE_COLUMN, LOADED_AT_COLUMN, RUN_DATE_COLUMN)
FULL_MODE = "full"
APPEND_MODE = "append"
MODES = (FULL_MODE, APPEND_MODE)
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


def batch_dir(data_dir: Path, run_date: date) -> Path:
    """Folder of one daily batch, e.g. ``data/raw/2025-07-01``."""
    return data_dir / run_date.isoformat()


def source_name(path: Path, run_date: date | None) -> str:
    """Value of ``_source_file``: the file name, prefixed by the batch folder in append mode."""
    return path.name if run_date is None else f"{run_date.isoformat()}/{path.name}"


def qualified(table: str) -> sql.Identifier:
    return sql.Identifier(RAW_SCHEMA, table)


def build_create_table(
    table: str, columns: Sequence[str], if_not_exists: bool = False
) -> sql.Composed:
    """CREATE TABLE statement with every source column as text plus the metadata columns."""
    definitions = [sql.SQL("{} text").format(sql.Identifier(column)) for column in columns]
    definitions += [
        sql.SQL("{} text NOT NULL").format(sql.Identifier(SOURCE_FILE_COLUMN)),
        sql.SQL("{} timestamptz NOT NULL").format(sql.Identifier(LOADED_AT_COLUMN)),
        sql.SQL("{} date").format(sql.Identifier(RUN_DATE_COLUMN)),
    ]
    command = sql.SQL("CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE")
    return sql.SQL("{} {} ({})").format(command, qualified(table), sql.SQL(", ").join(definitions))


def build_add_missing_columns(table: str, columns: Sequence[str]) -> sql.Composed:
    """ALTER TABLE adding any source column (as text) or ``_run_date`` the table lacks.

    Lets append mode extend a table created by an older load, for example a v1
    full refresh that had no ``_run_date`` column yet.
    """
    additions = [
        sql.SQL("ADD COLUMN IF NOT EXISTS {} text").format(sql.Identifier(column))
        for column in columns
    ]
    additions.append(
        sql.SQL("ADD COLUMN IF NOT EXISTS {} date").format(sql.Identifier(RUN_DATE_COLUMN))
    )
    return sql.SQL("ALTER TABLE {} {}").format(qualified(table), sql.SQL(", ").join(additions))


def build_delete_run_date(table: str) -> sql.Composed:
    """DELETE of one run date's rows; the date is passed as the single parameter."""
    return sql.SQL("DELETE FROM {} WHERE {} = %s").format(
        qualified(table), sql.Identifier(RUN_DATE_COLUMN)
    )


def build_copy_statement(
    table: str, columns: Sequence[str], with_run_date: bool = False
) -> sql.Composed:
    """COPY statement that streams source and metadata columns from the client.

    Full mode leaves ``_run_date`` out so it keeps its NULL default.
    """
    metadata = METADATA_COLUMNS if with_run_date else (SOURCE_FILE_COLUMN, LOADED_AT_COLUMN)
    return sql.SQL("COPY {} ({}) FROM STDIN").format(
        qualified(table), sql.SQL(", ").join(map(sql.Identifier, [*columns, *metadata]))
    )


def to_copy_row(
    fields: Sequence[str], source_file: str, loaded_at: datetime, run_date: date | None = None
) -> list[str | datetime | date | None]:
    """Turn one CSV record into a COPY row: empty strings become NULL, metadata appended.

    ``run_date`` is appended only when given, matching ``build_copy_statement``.
    """
    row: list[str | datetime | date | None] = [field if field != "" else None for field in fields]
    row += [source_file, loaded_at]
    if run_date is not None:
        row.append(run_date)
    return row


def build_audit_statements() -> list[sql.Composed]:
    """Create ``raw._load_audit`` if needed and upgrade a v1 audit table in place."""
    return [
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ("
            "file_name text NOT NULL, row_count integer NOT NULL, loaded_at timestamptz NOT NULL, "
            "run_date date, mode text)"
        ).format(qualified(AUDIT_TABLE)),
        sql.SQL(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS run_date date, "
            "ADD COLUMN IF NOT EXISTS mode text"
        ).format(qualified(AUDIT_TABLE)),
    ]


def build_audit_insert() -> sql.Composed:
    return sql.SQL(
        "INSERT INTO {} (file_name, row_count, loaded_at, run_date, mode) "
        "VALUES (%s, %s, %s, %s, %s)"
    ).format(qualified(AUDIT_TABLE))


def build_prepare_statements(
    table: str, columns: Sequence[str], run_date: date | None
) -> list[tuple[sql.Composed, tuple[date] | None]]:
    """Statements that make ``raw.<table>`` ready for the COPY, as (statement, params) pairs.

    Full mode (no ``run_date``) drops and recreates the table. Append mode creates
    the table or adds missing columns, and ends by deleting the run date's rows.
    """
    if run_date is None:
        return [
            (sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(qualified(table)), None),
            (build_create_table(table, columns), None),
        ]
    return [
        (build_create_table(table, columns, if_not_exists=True), None),
        (build_add_missing_columns(table, columns), None),
        (build_delete_run_date(table), (run_date,)),
    ]


# --------------------------------------------------------------------------- #
# Database work
# --------------------------------------------------------------------------- #
def ensure_schema(conn: psycopg.Connection) -> None:
    """Create the raw schema and the audit table if this is the first run."""
    with conn.transaction():
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(RAW_SCHEMA)))
        for statement in build_audit_statements():
            conn.execute(statement)


def copy_rows(
    cursor: psycopg.Cursor,
    table: str,
    columns: Sequence[str],
    records: Iterable[list[str]],
    source_file: str,
    loaded_at: datetime,
    run_date: date | None = None,
) -> int:
    """Stream CSV records into ``raw.<table>`` with COPY; return the number of rows."""
    row_count = 0
    statement = build_copy_statement(table, columns, with_run_date=run_date is not None)
    with cursor.copy(statement) as copy:
        for record_number, record in enumerate(records, start=1):
            if not record:
                continue  # blank line
            if len(record) != len(columns):
                raise LoadError(
                    f"{source_file} data row {record_number}: expected {len(columns)} fields, "
                    f"got {len(record)}"
                )
            copy.write_row(to_copy_row(record, source_file, loaded_at, run_date))
            row_count += 1
    return row_count


def load_file(
    conn: psycopg.Connection, path: Path, loaded_at: datetime, run_date: date | None = None
) -> int:
    """Load one CSV file into ``raw.<file stem>`` inside one transaction.

    Without ``run_date`` the table is fully refreshed; with it, the file replaces
    that run date's rows (append mode).
    """
    table = table_name_for(path)
    mode = FULL_MODE if run_date is None else APPEND_MODE
    source_file = source_name(path, run_date)
    started = time.perf_counter()
    with path.open(encoding=CSV_ENCODING, newline="") as handle:
        reader = csv.reader(handle)
        columns = parse_header(next(reader, None), path.name)
        with conn.transaction(), conn.cursor() as cursor:
            for statement, params in build_prepare_statements(table, columns, run_date):
                cursor.execute(statement, params)
            # In append mode the last statement was the DELETE of this run date.
            replaced = cursor.rowcount if mode == APPEND_MODE else 0
            row_count = copy_rows(cursor, table, columns, reader, source_file, loaded_at, run_date)
            cursor.execute(build_audit_insert(), (path.name, row_count, loaded_at, run_date, mode))
    log.info(
        "Loaded %-16s -> %s.%-12s %8s rows in %.2fs%s",
        path.name, RAW_SCHEMA, table, f"{row_count:,}", time.perf_counter() - started,
        f" ({replaced:,} rows of {run_date} replaced)" if replaced > 0 else "",
    )
    return row_count


def find_csv_files(data_dir: Path) -> list[Path]:
    if not data_dir.is_dir():
        raise LoadError(f"data directory {data_dir} does not exist")
    files = sorted(data_dir.glob(CSV_GLOB))
    if not files:
        raise LoadError(f"no {CSV_GLOB} files found in {data_dir}")
    return files


def load_all(data_dir: Path, env: Mapping[str, str], run_date: date | None = None) -> int:
    """Load every CSV of the full extract, or of one daily batch when ``run_date`` is given.

    Stops at the first failure. Returns the total number of rows loaded.
    """
    source_dir = data_dir if run_date is None else batch_dir(data_dir, run_date)
    files = find_csv_files(source_dir)
    params = connection_params(env)
    loaded_at = datetime.now(timezone.utc)
    log.info(
        "Connecting to %s:%s/%s as %s",
        params["host"], params["port"], params["dbname"], params["user"],
    )
    log.info(
        "Mode %s: loading %d files from %s",
        FULL_MODE if run_date is None else f"{APPEND_MODE} ({run_date})", len(files), source_dir,
    )
    with psycopg.connect(autocommit=True, **params) as conn:
        ensure_schema(conn)
        return sum(load_file(conn, path, loaded_at, run_date) for path in files)


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #
def iso_date(value: str) -> date:
    """argparse type for a ``YYYY-MM-DD`` date."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a YYYY-MM-DD date, got {value!r}") from None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load CSV extracts into PostgreSQL schema 'raw': a full refresh from "
                    "data/raw/*.csv, or one daily batch from data/raw/<run-date>/*.csv.",
        epilog="Connection: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE "
               "(defaults: localhost, 5432, postgres, postgres, retail).",
    )
    parser.add_argument(
        "--data-dir", type=Path, default=DEFAULT_DATA_DIR,
        help="directory containing the CSV extracts (default: data/raw in the repository); "
             "append mode reads <data-dir>/<run-date>/",
    )
    parser.add_argument(
        "--mode", choices=MODES, default=FULL_MODE,
        help="full: drop and reload every table (default); "
             "append: replace one run date's rows",
    )
    parser.add_argument(
        "--run-date", type=iso_date, metavar="D",
        help="batch day to load, YYYY-MM-DD (required with --mode append)",
    )
    args = parser.parse_args(argv)
    if args.mode == APPEND_MODE and args.run_date is None:
        parser.error("--mode append requires --run-date")
    if args.mode == FULL_MODE and args.run_date is not None:
        parser.error("--run-date only applies to --mode append")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    started = time.perf_counter()
    try:
        total_rows = load_all(args.data_dir, os.environ, args.run_date)
    except (LoadError, OSError, UnicodeDecodeError, csv.Error, psycopg.Error) as error:
        log.error("Load failed: %s", error)
        return 1
    log.info("Done: %s rows loaded in %.2fs", f"{total_rows:,}", time.perf_counter() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())

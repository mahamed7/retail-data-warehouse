"""Tests for the loader's append mode (data contract, "Daily batches (v2)").

Everything here runs without a database: the SQL is checked through the composed
statements' ``as_string()``, and the command line and file lookup are checked
before any connection would be opened.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))  # make `pipeline` importable when pytest runs from anywhere

from pipeline import load_raw  # noqa: E402

RUN_DATE = date(2025, 7, 1)
LOADED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def statements(table: str, columns: list[str], run_date: date | None) -> list[tuple[str, object]]:
    return [
        (statement.as_string(), params)
        for statement, params in load_raw.build_prepare_statements(table, columns, run_date)
    ]


# --------------------------------------------------------------------------- #
# SQL builders
# --------------------------------------------------------------------------- #
def test_create_table_if_not_exists_for_append_mode() -> None:
    statement = load_raw.build_create_table("stores", ["store_id"], if_not_exists=True).as_string()
    assert statement == (
        'CREATE TABLE IF NOT EXISTS "raw"."stores" ("store_id" text, '
        '"_source_file" text NOT NULL, "_loaded_at" timestamptz NOT NULL, "_run_date" date)'
    )


def test_add_missing_columns_covers_source_columns_and_run_date() -> None:
    statement = load_raw.build_add_missing_columns("orders", ["order_id", "status"]).as_string()
    assert statement == (
        'ALTER TABLE "raw"."orders" ADD COLUMN IF NOT EXISTS "order_id" text, '
        'ADD COLUMN IF NOT EXISTS "status" text, ADD COLUMN IF NOT EXISTS "_run_date" date'
    )


def test_delete_run_date_binds_the_date_as_a_parameter() -> None:
    statement = load_raw.build_delete_run_date("order_items").as_string()
    assert statement == 'DELETE FROM "raw"."order_items" WHERE "_run_date" = %s'


def test_copy_statement_with_run_date() -> None:
    statement = load_raw.build_copy_statement("stores", ["store_id"], with_run_date=True)
    statement = statement.as_string()
    assert statement == (
        'COPY "raw"."stores" ("store_id", "_source_file", "_loaded_at", "_run_date") FROM STDIN'
    )


def test_copy_row_carries_run_date_only_in_append_mode() -> None:
    appended = load_raw.to_copy_row(["1", ""], "2025-07-01/stores.csv", LOADED_AT, RUN_DATE)
    assert appended == ["1", None, "2025-07-01/stores.csv", LOADED_AT, RUN_DATE]
    full = load_raw.to_copy_row(["1", ""], "stores.csv", LOADED_AT)
    assert full == ["1", None, "stores.csv", LOADED_AT]


def test_full_mode_drops_and_recreates() -> None:
    assert statements("stores", ["store_id"], None) == [
        ('DROP TABLE IF EXISTS "raw"."stores" CASCADE', None),
        (
            'CREATE TABLE "raw"."stores" ("store_id" text, "_source_file" text NOT NULL, '
            '"_loaded_at" timestamptz NOT NULL, "_run_date" date)',
            None,
        ),
    ]


def test_append_mode_creates_extends_then_deletes_the_run_date() -> None:
    steps = statements("stores", ["store_id"], RUN_DATE)
    assert [sql_text.split(" ")[0] for sql_text, _ in steps] == ["CREATE", "ALTER", "DELETE"]
    assert steps[0][0].startswith('CREATE TABLE IF NOT EXISTS "raw"."stores"')
    assert steps[2] == ('DELETE FROM "raw"."stores" WHERE "_run_date" = %s', (RUN_DATE,))
    assert all(params is None for _, params in steps[:2])
    assert not any("DROP" in sql_text for sql_text, _ in steps)


def test_audit_table_is_created_or_upgraded_from_v1() -> None:
    create, upgrade = (statement.as_string() for statement in load_raw.build_audit_statements())
    assert create == (
        'CREATE TABLE IF NOT EXISTS "raw"."_load_audit" (file_name text NOT NULL, '
        "row_count integer NOT NULL, loaded_at timestamptz NOT NULL, run_date date, mode text)"
    )
    assert upgrade == (
        'ALTER TABLE "raw"."_load_audit" ADD COLUMN IF NOT EXISTS run_date date, '
        "ADD COLUMN IF NOT EXISTS mode text"
    )


def test_audit_insert_records_run_date_and_mode() -> None:
    assert load_raw.build_audit_insert().as_string() == (
        'INSERT INTO "raw"."_load_audit" (file_name, row_count, loaded_at, run_date, mode) '
        "VALUES (%s, %s, %s, %s, %s)"
    )


# --------------------------------------------------------------------------- #
# Paths, headers and command line
# --------------------------------------------------------------------------- #
def test_batch_dir_and_source_name() -> None:
    assert load_raw.batch_dir(Path("data/raw"), RUN_DATE) == Path("data/raw/2025-07-01")
    assert load_raw.source_name(Path("data/raw/2025-07-01/orders.csv"), RUN_DATE) == (
        "2025-07-01/orders.csv"
    )
    assert load_raw.source_name(Path("data/raw/orders.csv"), None) == "orders.csv"


def test_parse_header_reserves_run_date() -> None:
    with pytest.raises(load_raw.LoadError):
        load_raw.parse_header(["order_id", "_run_date"], "orders.csv")


def test_parse_args_defaults_to_full_mode() -> None:
    args = load_raw.parse_args([])
    assert (args.mode, args.run_date) == ("full", None)


def test_parse_args_append_mode() -> None:
    args = load_raw.parse_args(["--mode", "append", "--run-date", "2025-07-01"])
    assert (args.mode, args.run_date) == ("append", RUN_DATE)


@pytest.mark.parametrize("argv", [
    ["--mode", "append"],
    ["--run-date", "2025-07-01"],
    ["--mode", "append", "--run-date", "01/07/2025"],
    ["--mode", "upsert", "--run-date", "2025-07-01"],
])
def test_parse_args_rejects_inconsistent_modes(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        load_raw.parse_args(argv)


def test_append_mode_reads_the_run_date_folder(tmp_path: Path) -> None:
    """A missing batch folder fails before any connection is attempted."""
    (tmp_path / "orders.csv").write_text("order_id\n1\n", encoding="utf-8")
    with pytest.raises(load_raw.LoadError, match="2025-07-01"):
        load_raw.load_all(tmp_path, {}, RUN_DATE)

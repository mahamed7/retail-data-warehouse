"""Unit tests for the raw-layer quality gate. No database needed: these cover the check
catalogue, the generated SQL, scoping, result statuses, the report and the CLI. The SQL
itself runs against PostgreSQL 16 in CI, including a deliberately corrupted batch."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import check_raw  # noqa: E402

RUN_DATE = date(2025, 7, 1)


def as_text(composed) -> str:
    return composed.as_string(None)


def test_check_ids_are_unique_and_severities_valid() -> None:
    ids = [check.check_id for check in check_raw.CHECKS]
    assert len(ids) == len(set(ids))
    assert {check.severity for check in check_raw.CHECKS} <= {check_raw.ERROR, check_raw.WARN}


def test_every_check_has_exactly_one_kind_of_sql() -> None:
    for check in check_raw.CHECKS:
        assert (check.predicate is None) != (check.aggregate_sql is None), check.check_id


def test_every_source_table_is_covered() -> None:
    assert {check.table for check in check_raw.CHECKS} == set(check_raw.REQUIRED_COLUMNS)


def test_known_source_issues_are_warnings_not_errors() -> None:
    by_id = {check.check_id: check for check in check_raw.CHECKS}
    assert by_id["order_items.product_exists"].severity == check_raw.WARN
    assert by_id["customers.email_format"].severity == check_raw.WARN
    assert by_id["orders.status_in_contract"].severity == check_raw.ERROR


def test_scope_filters() -> None:
    assert as_text(check_raw.Scope("batch", RUN_DATE).where()) == "_run_date = '2025-07-01'::date"
    assert as_text(check_raw.Scope("full").where()) == "_run_date is null"
    assert as_text(check_raw.Scope("all").where()) == "true"


def test_batch_only_checks_are_skipped_outside_a_batch() -> None:
    batch = {c.check_id for c in check_raw.applicable(check_raw.CHECKS, check_raw.Scope("batch", RUN_DATE))}
    full = {c.check_id for c in check_raw.applicable(check_raw.CHECKS, check_raw.Scope("full"))}
    assert "orders.updated_on_batch_day" in batch
    assert "orders.updated_on_batch_day" not in full
    assert batch - full == {"orders.updated_on_batch_day"}


def test_row_check_query_counts_and_samples_bad_rows() -> None:
    check = next(c for c in check_raw.CHECKS if c.check_id == "orders.status_in_contract")
    text = as_text(check_raw.build_query(check, check_raw.Scope("batch", RUN_DATE)))
    assert text.startswith('with batch as (select * from "raw"."orders" where _run_date = \'2025-07-01\'::date)')
    assert "count(*) as failing" in text
    assert "limit 3" in text
    assert "'completed', 'cancelled', 'returned'" in text


def test_aggregate_query_wraps_the_aggregate() -> None:
    check = next(c for c in check_raw.CHECKS if c.check_id == "order_items.order_item_id_unique")
    text = as_text(check_raw.build_query(check, check_raw.Scope("all")))
    assert '"raw"."order_items" where true' in text
    assert "group by trim(order_item_id)" in text


def test_casts_are_always_guarded() -> None:
    # Every ::numeric / ::integer / ::timestamp cast must sit behind pg_input_is_valid.
    for check in check_raw.CHECKS:
        if check.predicate and "::" in check.predicate.replace("::date", ""):
            assert "pg_input_is_valid" in check.predicate, check.check_id


def test_not_in_escapes_quotes() -> None:
    assert "'it''s'" in check_raw.not_in("x", ["it's"])


def result(severity: str, failing: int) -> check_raw.CheckResult:
    return check_raw.CheckResult(
        check_id=f"t.{severity}{failing}", table="t", severity=severity,
        description="", failing_rows=failing,
    )


def test_status_and_exit_code() -> None:
    assert result(check_raw.ERROR, 0).status == "pass"
    assert result(check_raw.WARN, 5).status == "warn"
    assert result(check_raw.ERROR, 1).status == "fail"
    assert check_raw.exit_code([result(check_raw.WARN, 44), result(check_raw.ERROR, 0)]) == 0
    assert check_raw.exit_code([result(check_raw.ERROR, 1)]) == 1


def test_report_and_table() -> None:
    results = [result(check_raw.ERROR, 0), result(check_raw.WARN, 44), result(check_raw.ERROR, 2)]
    report = check_raw.build_report(
        check_raw.Scope("batch", RUN_DATE), results, datetime(2025, 7, 1, tzinfo=timezone.utc),
    )
    assert report["scope"] == "2025-07-01"
    assert report["passed"] is False
    assert report["summary"] == {"pass": 1, "warn": 1, "fail": 1}
    table = check_raw.format_table(results)
    assert "FAIL" in table and "WARN" in table and "44" in table


def test_missing_columns_become_schema_failures() -> None:
    results = check_raw.schema_results({"orders": ["status", "channel"]})
    assert [r.check_id for r in results] == ["orders.required_columns"]
    assert results[0].status == "fail" and "status, channel" in results[0].description


def test_cli_scopes() -> None:
    assert check_raw.scope_from_args(check_raw.parse_args([])) == check_raw.Scope("full")
    assert check_raw.scope_from_args(check_raw.parse_args(["--all"])) == check_raw.Scope("all")
    assert check_raw.scope_from_args(
        check_raw.parse_args(["--run-date", "2025-07-01"])
    ) == check_raw.Scope("batch", RUN_DATE)
    with pytest.raises(SystemExit):
        check_raw.parse_args(["--all", "--run-date", "2025-07-01"])


def test_unreachable_database_exits_2(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PGHOST", "127.0.0.1")
    monkeypatch.setenv("PGPORT", "1")  # nothing listens here
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "2")
    assert check_raw.main(["--report-dir", str(tmp_path)]) == 2

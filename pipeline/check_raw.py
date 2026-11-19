"""Quality gate for the raw layer: validate a loaded batch before dbt touches it.

The loader lands every source column as text so a load never fails on bad data. That
makes this gate the place where bad data is caught: it runs declarative checks against
the rows of one batch (or the full extract) and fails fast, so a broken export never
reaches the warehouse.

Each check is a SQL predicate that selects *bad* rows, evaluated inside PostgreSQL.
Type checks use ``pg_input_is_valid`` (PostgreSQL 16+), and every cast is guarded by it,
so a malformed value is reported instead of crashing the query.

Severity:
    error  - the batch breaks the data contract; the gate exits 1 and dbt must not run.
    warn   - a known, tolerated issue (for example orphan product ids); logged only.

Results are printed as a table, written to ``reports/quality/<scope>.json`` and
appended to ``raw._quality_results`` so the history of every gate run is queryable.

Usage:
    python pipeline/check_raw.py                      # the full extract (_run_date is null)
    python pipeline/check_raw.py --run-date 2025-07-01
    python pipeline/check_raw.py --all                # every row in raw, any batch

Exit codes: 0 passed (warnings allowed), 1 at least one error check failed,
2 the gate could not run (connection, missing tables).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT_DIR = REPO_ROOT / "reports" / "quality"
RAW_SCHEMA = "raw"
RESULTS_TABLE = "_quality_results"
SAMPLE_SIZE = 3

SALES_START = date(2024, 1, 1)
SALES_END = date(2025, 12, 31)

CONNECTION_DEFAULTS = {
    "PGHOST": "localhost",
    "PGPORT": "5432",
    "PGUSER": "postgres",
    "PGPASSWORD": "postgres",
    "PGDATABASE": "retail",
}

# Columns each source table must have (docs/data-contract.md).
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "stores": ("store_id", "store_name", "city", "region", "store_type", "opened_date"),
    "products": (
        "product_id", "sku", "product_name", "category", "subcategory",
        "unit_cost", "unit_price", "is_active",
    ),
    "customers": (
        "customer_id", "first_name", "last_name", "email", "phone", "city", "signup_date",
    ),
    "orders": (
        "order_id", "customer_id", "store_id", "order_ts", "channel",
        "payment_method", "status", "updated_at",
    ),
    "order_items": (
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount",
    ),
}

STORE_TYPES = ("Supermarket", "Mini-mart", "Wholesale")
CATEGORIES = (
    "Groceries", "Beverages", "Household", "Personal Care", "Electronics", "Fresh Produce",
)
CHANNELS = ("in_store", "mobile_app", "phone")
PAYMENT_METHODS = ("cash", "zaad", "edahab", "card")
STATUSES = ("completed", "cancelled", "returned")

ERROR = "error"
WARN = "warn"

log = logging.getLogger("check_raw")


# --------------------------------------------------------------------------- #
# Predicate helpers: each returns SQL text that is true for a BAD row.
# Every cast is guarded, because Postgres may evaluate predicates in any order.
# --------------------------------------------------------------------------- #
def missing(column: str) -> str:
    return f"nullif(trim({column}), '') is null"


def not_of_type(column: str, pg_type: str) -> str:
    """Present but not parseable as ``pg_type``."""
    return f"(not {missing(column)} and not pg_input_is_valid(trim({column}), '{pg_type}'))"


def bad_key(column: str) -> str:
    """A required integer key that is missing or not an integer."""
    return f"({missing(column)} or not pg_input_is_valid(trim({column}), 'integer'))"


def guarded(column: str, pg_type: str, condition: str) -> str:
    """``condition`` evaluated on ``column::pg_type`` only when the value parses."""
    value = f"trim({column})::{pg_type}"
    return (
        f"(case when pg_input_is_valid(trim({column}), '{pg_type}') "
        f"then {condition.format(v=value)} else false end)"
    )


def not_in(column: str, allowed: Sequence[str]) -> str:
    values = ", ".join("'" + value.replace("'", "''") + "'" for value in allowed)
    return f"({missing(column)} or trim({column}) not in ({values}))"


def outside_period(column: str, pg_type: str) -> str:
    return guarded(
        column, pg_type,
        f"({{v}})::date not between date '{SALES_START}' and date '{SALES_END}'",
    )


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Check:
    """One expectation about one raw table.

    ``predicate`` selects bad rows from the batch. Aggregate checks set ``aggregate_sql``
    instead: a query over the scoped rows (CTE ``batch``) returning one integer, the
    number of violations.
    """

    name: str
    table: str
    severity: str
    description: str
    predicate: str | None = None
    aggregate_sql: str | None = None
    batch_only: bool = False  # only meaningful for a single daily batch

    @property
    def check_id(self) -> str:
        return f"{self.table}.{self.name}"


def row_count_check(table: str, minimum: int, severity: str) -> Check:
    return Check(
        name="has_rows", table=table, severity=severity,
        description=f"At least {minimum} row(s) in the batch.",
        aggregate_sql=f"select case when count(*) >= {minimum} then 0 else 1 end from batch",
    )


def unique_check(table: str, column: str) -> Check:
    return Check(
        name=f"{column}_unique", table=table, severity=ERROR,
        description=f"{column} appears at most once in the batch.",
        aggregate_sql=(
            f"select coalesce(sum(n - 1), 0)::bigint from "
            f"(select count(*) as n from batch group by trim({column})) as counts"
        ),
    )


CHECKS: tuple[Check, ...] = (
    # stores: a full snapshot every batch
    row_count_check("stores", 1, ERROR),
    Check("store_id_valid", "stores", ERROR, "store_id is a present integer.", bad_key("store_id")),
    Check("store_type_in_contract", "stores", ERROR, "store_type is a contract value.",
          not_in("store_type", STORE_TYPES)),
    Check("opened_date_valid", "stores", ERROR, "opened_date parses as a date.",
          not_of_type("opened_date", "date")),

    # products: a full snapshot every batch
    row_count_check("products", 1, ERROR),
    Check("product_id_valid", "products", ERROR, "product_id is a present integer.",
          bad_key("product_id")),
    Check("prices_are_numeric", "products", ERROR, "unit_cost and unit_price parse as numbers.",
          f"({missing('unit_cost')} or {missing('unit_price')} or "
          f"{not_of_type('unit_cost', 'numeric')} or {not_of_type('unit_price', 'numeric')})"),
    Check("price_above_cost", "products", ERROR, "unit_price is greater than unit_cost.",
          "(case when pg_input_is_valid(trim(unit_cost), 'numeric') "
          "and pg_input_is_valid(trim(unit_price), 'numeric') "
          "then trim(unit_price)::numeric <= trim(unit_cost)::numeric else false end)"),
    Check("is_active_boolean", "products", ERROR, "is_active is true or false.",
          not_in("lower(is_active)", ("true", "false"))),
    Check("category_in_contract", "products", ERROR, "category is a contract value.",
          not_in("category", CATEGORIES)),

    # customers
    Check("customer_id_valid", "customers", ERROR, "customer_id is a present integer.",
          bad_key("customer_id")),
    Check("signup_date_valid", "customers", ERROR, "signup_date parses as a date.",
          f"({missing('signup_date')} or {not_of_type('signup_date', 'date')})"),
    Check("email_format", "customers", WARN,
          "Non-empty emails look like an address (case and spaces are fixed in staging).",
          f"(not {missing('email')} and trim(email) !~ '^[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+$')"),

    # orders
    row_count_check("orders", 1, WARN),
    Check("ids_valid", "orders", ERROR,
          "order_id and store_id are present integers; customer_id is an integer or empty.",
          f"({bad_key('order_id')} or {bad_key('store_id')} or "
          f"{not_of_type('customer_id', 'integer')})"),
    Check("timestamps_valid", "orders", ERROR, "order_ts and updated_at parse as timestamps.",
          f"({missing('order_ts')} or {missing('updated_at')} or "
          f"{not_of_type('order_ts', 'timestamp')} or {not_of_type('updated_at', 'timestamp')})"),
    Check("order_ts_in_sales_period", "orders", ERROR,
          f"order_ts falls between {SALES_START} and {SALES_END}.",
          outside_period("order_ts", "timestamp")),
    Check("updated_after_order", "orders", ERROR, "updated_at is not before order_ts.",
          f"(case when pg_input_is_valid(trim(order_ts), 'timestamp') "
          f"and pg_input_is_valid(trim(updated_at), 'timestamp') "
          f"then trim(updated_at)::timestamp < trim(order_ts)::timestamp else false end)"),
    Check("updated_on_batch_day", "orders", ERROR,
          "Every order row in a daily batch was updated on the batch day.",
          guarded("updated_at", "timestamp", "({v})::date <> _run_date"),
          batch_only=True),
    Check("status_in_contract", "orders", ERROR, "status is a contract value.",
          not_in("lower(status)", STATUSES)),
    Check("channel_in_contract", "orders", ERROR, "channel is a contract value.",
          not_in("lower(channel)", CHANNELS)),
    Check("payment_method_in_contract", "orders", ERROR, "payment_method is a contract value.",
          not_in("lower(payment_method)", PAYMENT_METHODS)),
    Check("store_exists", "orders", ERROR, "store_id is a known store.",
          f"(pg_input_is_valid(trim(store_id), 'integer') and not exists ("
          f"select 1 from {RAW_SCHEMA}.stores as s where trim(s.store_id) = trim(batch.store_id)))"),
    Check("customer_exists", "orders", ERROR,
          "A registered customer_id belongs to a customer already loaded.",
          f"(not {missing('customer_id')} and not exists ("
          f"select 1 from {RAW_SCHEMA}.customers as c "
          f"where trim(c.customer_id) = trim(batch.customer_id)))"),

    # order_items
    row_count_check("order_items", 1, WARN),
    unique_check("order_items", "order_item_id"),
    Check("ids_valid", "order_items", ERROR,
          "order_item_id, order_id and product_id are present integers.",
          f"({bad_key('order_item_id')} or {bad_key('order_id')} or {bad_key('product_id')})"),
    Check("quantity_positive_integer", "order_items", ERROR, "quantity is an integer of at least 1.",
          f"({bad_key('quantity')} or {guarded('quantity', 'integer', '{v} < 1')})"),
    Check("amounts_valid", "order_items", ERROR,
          "unit_price is a non-negative number; discount_amount is empty or a non-negative number.",
          f"({missing('unit_price')} or {not_of_type('unit_price', 'numeric')} or "
          f"{not_of_type('discount_amount', 'numeric')} or "
          f"{guarded('unit_price', 'numeric', '{v} < 0')} or "
          f"{guarded('discount_amount', 'numeric', '{v} < 0')})"),
    Check("discount_within_line_total", "order_items", ERROR,
          "discount_amount never exceeds quantity * unit_price.",
          "(case when pg_input_is_valid(trim(discount_amount), 'numeric') "
          "and pg_input_is_valid(trim(quantity), 'integer') "
          "and pg_input_is_valid(trim(unit_price), 'numeric') "
          "then trim(discount_amount)::numeric > trim(quantity)::integer * trim(unit_price)::numeric "
          "else false end)"),
    Check("order_exists", "order_items", ERROR, "Every line belongs to a loaded order.",
          f"(not exists (select 1 from {RAW_SCHEMA}.orders as o "
          f"where trim(o.order_id) = trim(batch.order_id)))"),
    Check("product_exists", "order_items", WARN,
          "product_id is in the catalogue (a few orphans are a known, tolerated source issue).",
          f"(not exists (select 1 from {RAW_SCHEMA}.products as p "
          f"where trim(p.product_id) = trim(batch.product_id)))"),
)


# --------------------------------------------------------------------------- #
# Scope: which rows of each raw table the gate looks at
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Scope:
    kind: str  # "batch", "full" or "all"
    run_date: date | None = None

    @property
    def label(self) -> str:
        return str(self.run_date) if self.kind == "batch" else self.kind

    def where(self) -> sql.Composable:
        if self.kind == "batch":
            return sql.SQL("_run_date = {}").format(sql.Literal(self.run_date))
        if self.kind == "full":
            return sql.SQL("_run_date is null")
        return sql.SQL("true")


def applicable(checks: Sequence[Check], scope: Scope) -> list[Check]:
    return [check for check in checks if scope.kind == "batch" or not check.batch_only]


def build_query(check: Check, scope: Scope) -> sql.Composed:
    """Query returning (failing_rows, sample_json) for one check."""
    batch = sql.SQL("with batch as (select * from {table} where {where}) ").format(
        table=sql.Identifier(RAW_SCHEMA, check.table), where=scope.where(),
    )
    if check.aggregate_sql is not None:
        return batch + sql.SQL("select ({}) as failing, null::json as sample").format(
            sql.SQL(check.aggregate_sql)
        )
    return batch + sql.SQL(
        "select count(*) as failing, "
        "(select json_agg(sample_rows) from (select * from batch where {pred} limit {n}) as sample_rows) "
        "from batch where {pred}"
    ).format(pred=sql.SQL(check.predicate), n=sql.Literal(SAMPLE_SIZE))


# --------------------------------------------------------------------------- #
# Results and report
# --------------------------------------------------------------------------- #
@dataclass
class CheckResult:
    check_id: str
    table: str
    severity: str
    description: str
    failing_rows: int
    status: str = field(init=False)
    sample: list[dict[str, object]] | None = None

    def __post_init__(self) -> None:
        if self.failing_rows == 0:
            self.status = "pass"
        else:
            self.status = "fail" if self.severity == ERROR else "warn"


def exit_code(results: Sequence[CheckResult]) -> int:
    return 1 if any(result.status == "fail" for result in results) else 0


def summarise(results: Sequence[CheckResult]) -> dict[str, int]:
    return {status: sum(r.status == status for r in results) for status in ("pass", "warn", "fail")}


def build_report(scope: Scope, results: Sequence[CheckResult], checked_at: datetime) -> dict:
    return {
        "scope": scope.label,
        "checked_at": checked_at.isoformat(timespec="seconds"),
        "passed": exit_code(results) == 0,
        "summary": summarise(results),
        "checks": [asdict(result) for result in results],
    }


def format_table(results: Sequence[CheckResult]) -> str:
    width = max(len(result.check_id) for result in results)
    lines = [f"{'STATUS':<6}  {'CHECK':<{width}}  BAD ROWS"]
    for result in results:
        lines.append(f"{result.status.upper():<6}  {result.check_id:<{width}}  {result.failing_rows:>8,}")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
def connection_params(env: Mapping[str, str]) -> dict[str, str]:
    setting = {key: env.get(key) or default for key, default in CONNECTION_DEFAULTS.items()}
    return {
        "host": setting["PGHOST"], "port": setting["PGPORT"], "user": setting["PGUSER"],
        "password": setting["PGPASSWORD"], "dbname": setting["PGDATABASE"],
        "connect_timeout": env.get("PGCONNECT_TIMEOUT") or "10",
        "application_name": "dukaan-quality-gate",
    }


def missing_columns(conn: psycopg.Connection) -> dict[str, list[str]]:
    """Required columns absent from each raw table (a missing table lists all of them)."""
    rows = conn.execute(
        "select table_name, column_name from information_schema.columns where table_schema = %s",
        (RAW_SCHEMA,),
    ).fetchall()
    present: dict[str, set[str]] = {}
    for table, column in rows:
        present.setdefault(table, set()).add(column)
    return {
        table: [column for column in columns if column not in present.get(table, set())]
        for table, columns in REQUIRED_COLUMNS.items()
        if any(column not in present.get(table, set()) for column in columns)
    }


def run_checks(conn: psycopg.Connection, scope: Scope) -> list[CheckResult]:
    results = []
    for check in applicable(CHECKS, scope):
        failing, sample = conn.execute(build_query(check, scope)).fetchone()
        results.append(CheckResult(
            check_id=check.check_id, table=check.table, severity=check.severity,
            description=check.description, failing_rows=int(failing or 0), sample=sample,
        ))
    return results


def record_results(
    conn: psycopg.Connection, scope: Scope, results: Sequence[CheckResult], checked_at: datetime,
) -> None:
    conn.execute(sql.SQL(
        "create table if not exists {} (checked_at timestamptz, scope text, run_date date, "
        "check_id text, severity text, status text, failing_rows bigint)"
    ).format(sql.Identifier(RAW_SCHEMA, RESULTS_TABLE)))
    with conn.cursor() as cursor:
        cursor.executemany(
            sql.SQL("insert into {} values (%s, %s, %s, %s, %s, %s, %s)").format(
                sql.Identifier(RAW_SCHEMA, RESULTS_TABLE)
            ),
            [
                (checked_at, scope.label, scope.run_date, r.check_id, r.severity, r.status, r.failing_rows)
                for r in results
            ],
        )


def schema_results(gaps: Mapping[str, list[str]]) -> list[CheckResult]:
    return [
        CheckResult(
            check_id=f"{table}.required_columns", table=table, severity=ERROR,
            description=f"Missing required columns: {', '.join(columns)}", failing_rows=len(columns),
        )
        for table, columns in gaps.items()
    ]


def gate(scope: Scope, env: Mapping[str, str], report_dir: Path) -> int:
    checked_at = datetime.now(timezone.utc)
    with psycopg.connect(autocommit=True, **connection_params(env)) as conn:
        gaps = missing_columns(conn)
        if gaps:
            # Row checks would fail on missing columns; report the schema problem alone.
            results = schema_results(gaps)
        else:
            results = run_checks(conn, scope)
        record_results(conn, scope, results, checked_at)

    print(format_table(results))
    report = build_report(scope, results, checked_at)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{scope.label}.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    counts = report["summary"]
    log.info(
        "Quality gate %s for %s: %d passed, %d warnings, %d failed. Report: %s",
        "PASSED" if report["passed"] else "FAILED", scope.label,
        counts["pass"], counts["warn"], counts["fail"], report_path,
    )
    for result in results:
        if result.status == "fail":
            log.error("%s: %d bad rows. %s Sample: %s",
                      result.check_id, result.failing_rows, result.description, result.sample)
    return exit_code(results)


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #
def iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a YYYY-MM-DD date, got {value!r}") from None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate raw data against the data contract before dbt runs.",
        epilog="Exit codes: 0 passed, 1 a check failed, 2 the gate could not run.",
    )
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--run-date", type=iso_date, metavar="D",
                        help="check one daily batch (rows where _run_date = D)")
    target.add_argument("--all", action="store_true", help="check every row in raw")
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR,
                        help="where to write <scope>.json (default: reports/quality)")
    return parser.parse_args(argv)


def scope_from_args(args: argparse.Namespace) -> Scope:
    if args.run_date is not None:
        return Scope("batch", args.run_date)
    return Scope("all") if args.all else Scope("full")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    try:
        return gate(scope_from_args(args), os.environ, args.report_dir)
    except psycopg.Error as error:
        log.error("Quality gate could not run: %s", error)
        return 2


if __name__ == "__main__":
    sys.exit(main())

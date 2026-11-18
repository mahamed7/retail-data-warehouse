"""Tests for the extract layer: the CSV generator and the loader's pure helpers.

The generator runs once per session into a temporary directory; the checks below
assert that its output honours ``docs/data-contract.md``, including the data
quality issues the dbt staging layer is expected to clean up. Expected headers
and value sets are written out here on purpose rather than imported from the
generator, so the tests compare the output against the contract, not against itself.
"""

from __future__ import annotations

import csv
import sys
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))  # make `pipeline` importable when pytest runs from anywhere

from pipeline import generate_data, load_raw  # noqa: E402

PERIOD_START = date(2024, 1, 1)
PERIOD_END = date(2025, 12, 31)

CONTRACT_HEADERS = {
    "stores.csv": ["store_id", "store_name", "city", "region", "store_type", "opened_date"],
    "products.csv": [
        "product_id", "sku", "product_name", "category", "subcategory",
        "unit_cost", "unit_price", "is_active",
    ],
    "customers.csv": [
        "customer_id", "first_name", "last_name", "email", "phone", "city", "signup_date",
    ],
    "orders.csv": [
        "order_id", "customer_id", "store_id", "order_ts", "channel",
        "payment_method", "status", "updated_at",
    ],
    "order_items.csv": [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount",
    ],
}
STORE_CITIES = {"Hargeisa", "Borama", "Berbera", "Burao", "Gabiley", "Erigavo", "Sheikh", "Wajaale"}
STORE_TYPES = {"Supermarket", "Mini-mart", "Wholesale"}
CATEGORIES = {
    "Groceries", "Beverages", "Household", "Personal Care", "Electronics", "Fresh Produce",
}
CHANNELS = {"in_store", "mobile_app", "phone"}
PAYMENT_METHODS = {"cash", "zaad", "edahab", "card"}
STATUSES = {"completed", "cancelled", "returned"}

Rows = list[dict[str, str]]


def read_rows(path: Path) -> Rows:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def latest_orders(orders: Rows) -> Rows:
    """Deduplicate orders the way the contract says: latest updated_at wins."""
    latest: dict[str, dict[str, str]] = {}
    for order in orders:
        current = latest.get(order["order_id"])
        if current is None or order["updated_at"] > current["updated_at"]:
            latest[order["order_id"]] = order
    return list(latest.values())


@pytest.fixture(scope="session")
def raw_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out_dir = tmp_path_factory.mktemp("raw")
    assert generate_data.main(["--out", str(out_dir)]) == 0
    return out_dir


@pytest.fixture(scope="session")
def data(raw_dir: Path) -> dict[str, Rows]:
    return {name: read_rows(raw_dir / name) for name in CONTRACT_HEADERS}


# --------------------------------------------------------------------------- #
# Structure and determinism
# --------------------------------------------------------------------------- #
def test_output_is_byte_identical_across_runs(raw_dir: Path, tmp_path: Path) -> None:
    generate_data.generate(tmp_path)
    for name in CONTRACT_HEADERS:
        assert (tmp_path / name).read_bytes() == (raw_dir / name).read_bytes(), name


@pytest.mark.parametrize("name", CONTRACT_HEADERS)
def test_headers_match_contract(raw_dir: Path, name: str) -> None:
    with (raw_dir / name).open(encoding="utf-8", newline="") as handle:
        assert next(csv.reader(handle)) == CONTRACT_HEADERS[name]


def test_row_counts_near_contract_targets(data: dict[str, Rows]) -> None:
    assert len(data["stores.csv"]) == 8
    assert 100 <= len(data["products.csv"]) <= 140
    assert 1_900 <= len({row["customer_id"] for row in data["customers.csv"]}) <= 2_100
    assert 24_000 <= len({row["order_id"] for row in data["orders.csv"]}) <= 26_000
    assert 55_000 <= len(data["order_items.csv"]) <= 65_000


# --------------------------------------------------------------------------- #
# Value sets, keys and dates
# --------------------------------------------------------------------------- #
def test_stores(data: dict[str, Rows]) -> None:
    stores = data["stores.csv"]
    assert {row["city"] for row in stores} == STORE_CITIES
    assert {row["store_type"] for row in stores} <= STORE_TYPES
    assert len({row["store_id"] for row in stores}) == len(stores)
    assert all(date.fromisoformat(row["opened_date"]) < PERIOD_START for row in stores)


def test_products(data: dict[str, Rows]) -> None:
    products = data["products.csv"]
    assert {row["category"] for row in products} == CATEGORIES
    assert len({row["product_id"] for row in products}) == len(products)
    assert len({row["sku"] for row in products}) == len(products)
    assert {row["is_active"] for row in products} == {"true", "false"}
    assert all(Decimal(row["unit_price"]) > Decimal(row["unit_cost"]) for row in products)


def test_order_value_sets(data: dict[str, Rows]) -> None:
    orders = data["orders.csv"]
    store_ids = {row["store_id"] for row in data["stores.csv"]}
    assert {row["channel"] for row in orders} == CHANNELS
    assert {row["payment_method"] for row in orders} == PAYMENT_METHODS
    assert {row["status"] for row in orders} == STATUSES
    assert {row["store_id"] for row in orders} <= store_ids


def test_order_timestamps_within_sales_period(data: dict[str, Rows]) -> None:
    for order in data["orders.csv"]:
        order_ts = datetime.fromisoformat(order["order_ts"])
        assert PERIOD_START <= order_ts.date() <= PERIOD_END
        assert datetime.fromisoformat(order["updated_at"]) >= order_ts


def test_status_mix_after_deduplication(data: dict[str, Rows]) -> None:
    orders = latest_orders(data["orders.csv"])
    shares = {status: count / len(orders)
              for status, count in Counter(row["status"] for row in orders).items()}
    assert shares["completed"] == pytest.approx(0.92, abs=0.015)
    assert shares["cancelled"] == pytest.approx(0.05, abs=0.01)
    assert shares["returned"] == pytest.approx(0.03, abs=0.01)


def test_order_items_reference_orders_and_have_valid_amounts(data: dict[str, Rows]) -> None:
    order_ids = {row["order_id"] for row in data["orders.csv"]}
    items = data["order_items.csv"]
    assert len({row["order_item_id"] for row in items}) == len(items)
    for item in items:
        assert item["order_id"] in order_ids
        quantity = int(item["quantity"])
        assert quantity >= 1
        discount = Decimal(item["discount_amount"] or "0")
        assert Decimal("0") <= discount <= quantity * Decimal(item["unit_price"])


def test_lines_per_order_between_one_and_five(data: dict[str, Rows]) -> None:
    lines = Counter(row["order_id"] for row in data["order_items.csv"])
    assert set(lines.values()) == {1, 2, 3, 4, 5}
    assert len(lines) == len({row["order_id"] for row in data["orders.csv"]})


def test_sale_price_sometimes_differs_from_list_price(data: dict[str, Rows]) -> None:
    list_price = {row["product_id"]: row["unit_price"] for row in data["products.csv"]}
    items = [row for row in data["order_items.csv"] if row["product_id"] in list_price]
    differs = sum(row["unit_price"] != list_price[row["product_id"]] for row in items)
    assert 0 < differs < len(items)


# --------------------------------------------------------------------------- #
# Injected data quality issues
# --------------------------------------------------------------------------- #
def test_customer_exact_duplicates_about_two_percent(data: dict[str, Rows]) -> None:
    rows = [tuple(row.values()) for row in data["customers.csv"]]
    duplicate_share = (len(rows) - len(set(rows))) / len(rows)
    assert duplicate_share == pytest.approx(0.02, abs=0.005)
    ids = [row["customer_id"] for row in data["customers.csv"]]
    assert len(set(ids)) == len(set(rows)), "a duplicated id must be an exact duplicate row"


def test_customer_duplicates_are_shuffled_in(data: dict[str, Rows]) -> None:
    rows = data["customers.csv"]
    seen: set[str] = set()
    repeat_positions = []
    for position, row in enumerate(rows):
        if row["customer_id"] in seen:
            repeat_positions.append(position)
        seen.add(row["customer_id"])
    first_appended_slot = len(rows) - len(repeat_positions)
    assert min(repeat_positions) < first_appended_slot, "duplicates must not all sit at the end"


def test_customer_emails_are_messy(data: dict[str, Rows]) -> None:
    customers = list({row["customer_id"]: row for row in data["customers.csv"]}.values())
    emails = [row["email"] for row in customers]
    empty_share = sum(email == "" for email in emails) / len(emails)
    assert empty_share == pytest.approx(0.03, abs=0.015)
    assert any(email != email.strip() for email in emails)
    assert any(email != email.lower() for email in emails)
    assert all("@" in email for email in emails if email)


def test_customer_name_casing_is_inconsistent(data: dict[str, Rows]) -> None:
    names = [row["first_name"] for row in data["customers.csv"]]
    names += [row["last_name"] for row in data["customers.csv"]]
    assert any(name.isupper() for name in names)
    assert any(name.islower() for name in names)
    assert any(name.istitle() for name in names)


def test_resent_orders_about_one_percent_with_later_update(data: dict[str, Rows]) -> None:
    by_id: dict[str, Rows] = {}
    for order in data["orders.csv"]:
        by_id.setdefault(order["order_id"], []).append(order)
    resent = [versions for versions in by_id.values() if len(versions) > 1]
    assert len(resent) / len(by_id) == pytest.approx(0.01, abs=0.003)
    for versions in resent:
        assert len(versions) == 2
        first, second = sorted(versions, key=lambda row: row["updated_at"])
        assert second["updated_at"] > first["updated_at"]
        assert second["status"] != first["status"]
        assert second["order_ts"] == first["order_ts"]


def test_resent_orders_not_ordered_by_file_position(data: dict[str, Rows]) -> None:
    """The winner must be chosen by updated_at, so the stale row is sometimes last."""
    last_row: dict[str, dict[str, str]] = {}
    for order in data["orders.csv"]:
        last_row[order["order_id"]] = order
    winners = {row["order_id"]: row for row in latest_orders(data["orders.csv"])}
    assert any(last_row[order_id] is not winners[order_id] for order_id in winners)


def test_walk_in_orders_about_thirty_percent(data: dict[str, Rows]) -> None:
    orders = latest_orders(data["orders.csv"])
    walk_in_share = sum(row["customer_id"] == "" for row in orders) / len(orders)
    assert walk_in_share == pytest.approx(0.30, abs=0.02)


def test_registered_customers_exist(data: dict[str, Rows]) -> None:
    customer_ids = {row["customer_id"] for row in data["customers.csv"]}
    referenced = {row["customer_id"] for row in data["orders.csv"] if row["customer_id"]}
    assert referenced <= customer_ids


def test_orphan_product_ids_exist_but_are_rare(data: dict[str, Rows]) -> None:
    product_ids = {row["product_id"] for row in data["products.csv"]}
    items = data["order_items.csv"]
    orphans = sum(row["product_id"] not in product_ids for row in items)
    assert 0 < orphans / len(items) < 0.003


def test_empty_discount_means_no_discount(data: dict[str, Rows]) -> None:
    discounts = [row["discount_amount"] for row in data["order_items.csv"]]
    assert "" in discounts
    assert all(Decimal(value) > 0 for value in discounts if value)


# --------------------------------------------------------------------------- #
# Loader helpers (no database required)
# --------------------------------------------------------------------------- #
def test_build_create_table_types_every_column_as_text() -> None:
    statement = load_raw.build_create_table("orders", ["order_id", "status"]).as_string()
    assert statement == (
        'CREATE TABLE "raw"."orders" ("order_id" text, "status" text, '
        '"_source_file" text NOT NULL, "_loaded_at" timestamptz NOT NULL)'
    )


def test_build_copy_statement_lists_source_and_metadata_columns() -> None:
    statement = load_raw.build_copy_statement("stores", ["store_id", "city"]).as_string()
    assert statement == (
        'COPY "raw"."stores" ("store_id", "city", "_source_file", "_loaded_at") FROM STDIN'
    )


def test_to_copy_row_turns_empty_strings_into_null() -> None:
    loaded_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = load_raw.to_copy_row(["100001", "", " a@b.com "], "orders.csv", loaded_at)
    assert row == ["100001", None, " a@b.com ", "orders.csv", loaded_at]


@pytest.mark.parametrize("header", [
    ["order_id", "Order Id"],
    ["order_id", "order_id"],
    ["order_id", "_loaded_at"],
    [],
    None,
])
def test_parse_header_rejects_unsafe_headers(header: list[str] | None) -> None:
    with pytest.raises(load_raw.LoadError):
        load_raw.parse_header(header, "orders.csv")


def test_parse_header_accepts_contract_headers() -> None:
    for name, header in CONTRACT_HEADERS.items():
        assert load_raw.parse_header(header, name) == header


def test_table_name_for_uses_file_stem() -> None:
    assert load_raw.table_name_for(Path("data/raw/order_items.csv")) == "order_items"
    with pytest.raises(load_raw.LoadError):
        load_raw.table_name_for(Path("data/raw/Order Items.csv"))


def test_connection_params_use_contract_defaults_and_env_overrides() -> None:
    defaults = load_raw.connection_params({})
    assert (defaults["host"], defaults["port"], defaults["user"], defaults["dbname"]) == (
        "localhost", "5432", "postgres", "retail",
    )
    assert defaults["password"] == "postgres"
    overridden = load_raw.connection_params({"PGHOST": "db", "PGDATABASE": "retail_ci"})
    assert (overridden["host"], overridden["dbname"]) == ("db", "retail_ci")

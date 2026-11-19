"""Tests for the generator's daily batch mode (data contract, "Daily batches (v2)").

A daily batch is a slice of the same deterministic dataset as the full extract.
These tests check the slicing rules on a short backfill written to disk through
the command line, and the union property (all daily batches together reproduce
the full extract) on the whole period in memory, which takes about a second.
"""

from __future__ import annotations

import csv
import hashlib
import sys
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))  # make `pipeline` importable when pytest runs from anywhere

from pipeline import generate_data  # noqa: E402

FILES = ("stores.csv", "products.csv", "customers.csv", "orders.csv", "order_items.csv")
PERIOD_START = date(2024, 1, 1)
BACKFILL_START = date(2025, 6, 28)
BACKFILL_END = date(2025, 7, 3)
BEFORE_PRICE_RISE = date(2025, 6, 30)
PRICE_RISE = date(2025, 7, 1)
LAST_DAY_BEFORE_DISCONTINUATION = date(2024, 12, 31)
DISCONTINUED_FROM = date(2025, 1, 1)
EMPTY_DAY = date(2030, 1, 1)

# SHA-256 of the v1 full extract (seed 42), recorded before daily batch mode was added.
V1_FULL_EXTRACT_SHA256 = {
    "stores.csv": "44d2fd20b9f881af312369b915ddbb254997958880ab79520f49d1a1df13e922",
    "products.csv": "4c28317ab386aa7a0bc6ec9d69bdcd36cc609e4f8d5f36a66fd1ecbf2a4bd5d6",
    "customers.csv": "bdb3134564532ac492924665684cb0a10dff645176dde005fcc0951afe83d55c",
    "orders.csv": "4be50b61b965d89e3dea182039fee950b14af3d8f5e62126dce98d9752c8dddc",
    "order_items.csv": "3680a8d1722c1fe6b99a08674eccddc687ca43eb6e737d79163a7b0a6b3c6f3c",
}

Rows = list[dict[str, str]]


def read_rows(path: Path) -> Rows:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_batch(folder: Path) -> dict[str, Rows]:
    return {name: read_rows(folder / name) for name in FILES}


def as_tuples(rows: Rows) -> list[tuple[str, ...]]:
    return [tuple(row.values()) for row in rows]


def day_of(timestamp: str) -> date:
    return date.fromisoformat(timestamp[:10])


def backfill_days() -> list[date]:
    return [BACKFILL_START + timedelta(days=offset)
            for offset in range((BACKFILL_END - BACKFILL_START).days + 1)]


@pytest.fixture(scope="session")
def full_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out_dir = tmp_path_factory.mktemp("full")
    assert generate_data.main(["--out", str(out_dir)]) == 0
    return out_dir


@pytest.fixture(scope="session")
def full(full_dir: Path) -> dict[str, Rows]:
    return read_batch(full_dir)


@pytest.fixture(scope="session")
def backfill_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out_dir = tmp_path_factory.mktemp("daily")
    argv = ["--backfill", BACKFILL_START.isoformat(), BACKFILL_END.isoformat()]
    assert generate_data.main([*argv, "--out", str(out_dir)]) == 0
    return out_dir


@pytest.fixture(scope="session")
def batches(backfill_dir: Path) -> dict[date, dict[str, Rows]]:
    return {day: read_batch(backfill_dir / day.isoformat()) for day in backfill_days()}


@pytest.fixture(scope="session")
def first_export(full: dict[str, Rows]) -> dict[str, date]:
    """Day each order is first exported: its earliest updated_at, from the full extract."""
    first: dict[str, date] = {}
    for order in full["orders.csv"]:
        day = day_of(order["updated_at"])
        first[order["order_id"]] = min(day, first.get(order["order_id"], day))
    return first


@pytest.fixture(scope="session")
def dataset() -> generate_data.Dataset:
    return generate_data.build_dataset()


@pytest.fixture(scope="session")
def slicer(dataset: generate_data.Dataset) -> generate_data.DailySlicer:
    return generate_data.DailySlicer(dataset)


# --------------------------------------------------------------------------- #
# Full mode is untouched
# --------------------------------------------------------------------------- #
def test_full_mode_is_byte_identical_to_v1(full_dir: Path) -> None:
    for name, expected in V1_FULL_EXTRACT_SHA256.items():
        assert hashlib.sha256((full_dir / name).read_bytes()).hexdigest() == expected, name


def test_full_mode_writes_no_batch_folders(full_dir: Path) -> None:
    assert sorted(path.name for path in full_dir.iterdir()) == sorted(FILES)


# --------------------------------------------------------------------------- #
# Layout of a backfill
# --------------------------------------------------------------------------- #
def test_backfill_writes_one_folder_per_day_with_all_files(backfill_dir: Path) -> None:
    assert sorted(path.name for path in backfill_dir.iterdir()) == [
        day.isoformat() for day in backfill_days()
    ]
    for day in backfill_days():
        folder = backfill_dir / day.isoformat()
        assert sorted(path.name for path in folder.iterdir()) == sorted(FILES)


def test_batch_headers_match_full_extract(backfill_dir: Path, full_dir: Path) -> None:
    for name in FILES:
        full_header = (full_dir / name).read_text(encoding="utf-8").splitlines()[0]
        for day in backfill_days():
            batch_text = (backfill_dir / day.isoformat() / name).read_text(encoding="utf-8")
            assert batch_text.splitlines()[0] == full_header, (day, name)


def test_empty_day_still_writes_every_file_with_a_header(tmp_path: Path) -> None:
    assert generate_data.main(["--run-date", EMPTY_DAY.isoformat(), "--out", str(tmp_path)]) == 0
    batch = read_batch(tmp_path / EMPTY_DAY.isoformat())
    assert batch["orders.csv"] == batch["order_items.csv"] == batch["customers.csv"] == []
    assert len(batch["stores.csv"]) == 8
    assert len(batch["products.csv"]) == 120


def test_rerunning_a_day_is_byte_identical(backfill_dir: Path, tmp_path: Path) -> None:
    """--run-date D reproduces the backfill's folder for D (not the first day of the backfill)."""
    day = PRICE_RISE
    assert generate_data.main(["--run-date", day.isoformat(), "--out", str(tmp_path)]) == 0
    for name in FILES:
        rerun = (tmp_path / day.isoformat() / name).read_bytes()
        assert rerun == (backfill_dir / day.isoformat() / name).read_bytes(), name


def test_run_date_with_history_matches_first_backfill_batch(
    backfill_dir: Path, tmp_path: Path
) -> None:
    argv = ["--run-date", BACKFILL_START.isoformat(), "--include-history", "--out", str(tmp_path)]
    assert generate_data.main(argv) == 0
    for name in FILES:
        rerun = (tmp_path / BACKFILL_START.isoformat() / name).read_bytes()
        assert rerun == (backfill_dir / BACKFILL_START.isoformat() / name).read_bytes(), name


@pytest.mark.parametrize("argv", [
    ["--backfill", "2025-07-03", "2025-06-28"],
    ["--run-date", "2025-07-01", "--backfill", "2025-07-01", "2025-07-02"],
    ["--include-history"],
    ["--run-date", "2025-13-01"],
])
def test_invalid_command_lines_are_rejected(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        generate_data.parse_args(argv)


# --------------------------------------------------------------------------- #
# Slicing rules
# --------------------------------------------------------------------------- #
def test_orders_are_sliced_by_updated_at_day(batches: dict[date, dict[str, Rows]]) -> None:
    for day, batch in batches.items():
        assert batch["orders.csv"], day
        assert {day_of(order["updated_at"]) for order in batch["orders.csv"]} == {day}


def test_each_order_row_lands_in_exactly_its_updated_at_batch(
    batches: dict[date, dict[str, Rows]], full: dict[str, Rows]
) -> None:
    expected = Counter(
        tuple(order.values()) for order in full["orders.csv"]
        if BACKFILL_START <= day_of(order["updated_at"]) <= BACKFILL_END
    )
    actual = Counter(row for batch in batches.values() for row in as_tuples(batch["orders.csv"]))
    assert actual == expected


def test_order_items_only_in_the_batch_of_the_first_export(
    batches: dict[date, dict[str, Rows]], full: dict[str, Rows], first_export: dict[str, date]
) -> None:
    for day, batch in batches.items():
        item_orders = {item["order_id"] for item in batch["order_items.csv"]}
        first_exported_today = {
            order["order_id"] for order in batch["orders.csv"]
            if first_export[order["order_id"]] == day
        }
        assert item_orders == first_exported_today, day
    full_items = Counter(
        tuple(item.values()) for item in full["order_items.csv"]
        if BACKFILL_START <= first_export[item["order_id"]] <= BACKFILL_END
    )
    batch_items = Counter(
        row for batch in batches.values() for row in as_tuples(batch["order_items.csv"])
    )
    assert batch_items == full_items


def test_resent_orders_split_across_batches(
    dataset: generate_data.Dataset, slicer: generate_data.DailySlicer
) -> None:
    """The stale version ships on its own day with the lines; the later version without them."""
    order_id_at, updated_at = generate_data.ORDER_ID_INDEX, generate_data.ORDER_UPDATED_AT_INDEX
    versions: dict[str, list[list[str]]] = {}
    for row in dataset.order_rows:
        versions.setdefault(row[order_id_at], []).append(row)
    resent = [rows for rows in versions.values() if len(rows) == 2]
    assert resent
    split = 0
    for rows in resent:
        stale, latest = sorted(rows, key=lambda row: row[updated_at])
        stale_day, latest_day = day_of(stale[updated_at]), day_of(latest[updated_at])
        if stale_day == latest_day:
            continue
        split += 1
        stale_batch, latest_batch = slicer.batch(stale_day), slicer.batch(latest_day)
        stale_orders, latest_orders = stale_batch["orders.csv"][1], latest_batch["orders.csv"][1]
        assert stale in stale_orders and latest not in stale_orders
        assert latest in latest_orders and stale not in latest_orders
        order_id = stale[order_id_at]
        assert any(item[1] == order_id for item in stale_batch["order_items.csv"][1])
        assert not any(item[1] == order_id for item in latest_batch["order_items.csv"][1])
    assert split > 0


def test_customers_first_batch_has_history_later_batches_only_signups(
    batches: dict[date, dict[str, Rows]], full: dict[str, Rows]
) -> None:
    first = batches[BACKFILL_START]["customers.csv"]
    expected_first = [row for row in full["customers.csv"]
                      if date.fromisoformat(row["signup_date"]) <= BACKFILL_START]
    assert as_tuples(first) == as_tuples(expected_first)
    for day in backfill_days()[1:]:
        expected = [row for row in full["customers.csv"] if row["signup_date"] == day.isoformat()]
        assert as_tuples(batches[day]["customers.csv"]) == as_tuples(expected), day


def test_registered_customers_of_backfill_orders_are_in_the_backfill(
    batches: dict[date, dict[str, Rows]]
) -> None:
    customer_ids = {
        row["customer_id"] for batch in batches.values() for row in batch["customers.csv"]
    }
    for day, batch in batches.items():
        referenced = {order["customer_id"] for order in batch["orders.csv"] if order["customer_id"]}
        assert referenced <= customer_ids, day


def test_stores_snapshot_equals_full_extract(
    batches: dict[date, dict[str, Rows]], full: dict[str, Rows]
) -> None:
    for batch in batches.values():
        assert batch["stores.csv"] == full["stores.csv"]


# --------------------------------------------------------------------------- #
# Product snapshots as of the batch day
# --------------------------------------------------------------------------- #
def test_price_rise_shows_up_between_june_30_and_july_1(
    batches: dict[date, dict[str, Rows]]
) -> None:
    before = {row["product_id"]: row for row in batches[BEFORE_PRICE_RISE]["products.csv"]}
    after = {row["product_id"]: row for row in batches[PRICE_RISE]["products.csv"]}
    assert before.keys() == after.keys()
    for product_id, new in after.items():
        old = before[product_id]
        if new["category"] == "Electronics":  # exempt from the 2025 increase
            assert (old["unit_price"], old["unit_cost"]) == (new["unit_price"], new["unit_cost"])
        else:
            assert Decimal(old["unit_price"]) < Decimal(new["unit_price"]), product_id
            assert Decimal(old["unit_cost"]) <= Decimal(new["unit_cost"]), product_id
        assert old["is_active"] == new["is_active"]


def test_snapshot_after_price_rise_equals_full_extract(
    batches: dict[date, dict[str, Rows]], full: dict[str, Rows]
) -> None:
    assert batches[PRICE_RISE]["products.csv"] == full["products.csv"]


def test_snapshot_prices_match_sales_of_that_day(batches: dict[date, dict[str, Rows]]) -> None:
    """A line sold on day D is priced at D's snapshot price (orphan products aside)."""
    for day in (BEFORE_PRICE_RISE, PRICE_RISE):
        price = {row["product_id"]: row["unit_price"] for row in batches[day]["products.csv"]}
        orders_today = {order["order_id"] for order in batches[day]["orders.csv"]
                        if day_of(order["order_ts"]) == day}
        lines = [item for item in batches[day]["order_items.csv"]
                 if item["order_id"] in orders_today and item["product_id"] in price]
        assert lines
        assert all(item["unit_price"] == price[item["product_id"]] for item in lines)


def test_inactive_products_are_active_until_discontinued(slicer: generate_data.DailySlicer) -> None:
    before = slicer.batch(LAST_DAY_BEFORE_DISCONTINUATION)["products.csv"][1]
    after = slicer.batch(DISCONTINUED_FROM)["products.csv"][1]
    assert {row[7] for row in before} == {"true"}
    assert Counter(row[7] for row in after) == Counter({"true": 114, "false": 6})


def test_snapshots_keep_price_above_cost(slicer: generate_data.DailySlicer) -> None:
    for day in (PERIOD_START, BEFORE_PRICE_RISE, PRICE_RISE):
        for row in slicer.batch(day)["products.csv"][1]:
            assert Decimal(row[6]) > Decimal(row[5]), (day, row[0])


# --------------------------------------------------------------------------- #
# Union of every daily batch == the full extract (whole period, in memory)
# --------------------------------------------------------------------------- #
def test_union_of_all_daily_batches_reproduces_full_extract(
    slicer: generate_data.DailySlicer, full: dict[str, Rows]
) -> None:
    """Each order, order line and customer row ships in exactly one batch of the period.

    Runs from the first day of sales (which carries all earlier signups) to the last
    export day, which lies after the sales period because of late returns.
    """
    days = generate_data.days_between(PERIOD_START, slicer.last_export_day)
    assert slicer.last_export_day > date(2025, 12, 31)
    union: dict[str, Counter[tuple[str, ...]]] = {
        name: Counter() for name in ("customers.csv", "orders.csv", "order_items.csv")
    }
    for day in days:
        batch = slicer.batch(day)
        for name, counter in union.items():
            counter.update(tuple(row) for row in batch[name][1])
    for name, counter in union.items():
        assert counter == Counter(as_tuples(full[name])), name
    last_products = slicer.batch(slicer.last_export_day)["products.csv"][1]
    assert [tuple(row) for row in last_products] == as_tuples(full["products.csv"])

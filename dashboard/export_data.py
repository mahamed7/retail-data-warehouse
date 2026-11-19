"""Export the warehouse marts to the JSON files the static dashboard reads.

Runs a handful of read-only queries against the ``marts`` schema (plus ``raw`` and
``staging`` for the data-quality counts) and writes compact JSON to
``dashboard/data/``:

* ``summary.json``    headline KPIs for all years and for each year, plus YoY growth
* ``monthly.json``    revenue, gross margin and orders per calendar month
* ``stores.json``     store league table per period, with a monthly revenue sparkline
* ``categories.json`` revenue and margin per product category per period
* ``channels.json``   revenue and orders per sales channel x payment method per period
* ``quality.json``    how many rows each data-quality rule in the warehouse handled
* ``meta.json``       when and from which commit the files were built

Revenue, margin, units and average order value count completed orders only
(``fct_sales.is_revenue``). Margin % uses only revenue from lines with a known
product cost, exactly like ``mart_monthly_store_performance``. Money is rounded
to 2 decimal places and ratios (fractions, 0.25 = 25%) to 4.

Connection settings come from the standard libpq environment variables, with the
same local-dev defaults as ``warehouse/profiles.yml``.

Usage::

    python dashboard/export_data.py [--out dashboard/data]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

log = logging.getLogger("export_data")

DEFAULT_OUT_DIR = Path(__file__).resolve().parent / "data"

Row = dict[str, Any]
Payload = dict[str, Any]

# Period label used for "every year combined" in the per-period payloads.
ALL_PERIODS = "all"

# --------------------------------------------------------------------------- SQL
# Every query groups by `grouping sets ((year), ())` where a per-year breakdown is
# needed: the `()` set is the all-years total and shows up with a null year, which
# `coalesce(year::text, 'all')` labels. Ratios use nullif() so an empty slice gives
# null, never a division error.

SUMMARY_SQL = """
with lines as (

    select
        dim_date.year,
        fct_sales.order_id,
        fct_sales.order_status,
        fct_sales.is_revenue,
        fct_sales.is_walk_in,
        fct_sales.quantity,
        fct_sales.net_amount,
        fct_sales.gross_margin
    from marts.fct_sales as fct_sales
    inner join marts.dim_date as dim_date
        on fct_sales.date_key = dim_date.date_key

),

totals as (

    select
        coalesce(year::text, 'all') as period,
        count(distinct order_id) as total_orders,
        count(distinct order_id) filter (where is_revenue) as completed_orders,
        count(distinct order_id) filter (where order_status = 'cancelled') as cancelled_orders,
        count(distinct order_id) filter (where order_status = 'returned') as returned_orders,
        count(distinct order_id) filter (where is_revenue and is_walk_in) as walk_in_orders,
        coalesce(sum(quantity) filter (where is_revenue), 0) as units_sold,
        coalesce(sum(net_amount) filter (where is_revenue), 0) as revenue,
        coalesce(sum(gross_margin) filter (where is_revenue), 0) as gross_margin,
        sum(net_amount) filter (where is_revenue and gross_margin is not null) as revenue_with_known_cost
    from lines
    group by grouping sets ((year), ())

)

select
    period,
    total_orders,
    completed_orders,
    cancelled_orders,
    returned_orders,
    walk_in_orders,
    units_sold,
    round(revenue, 2) as revenue,
    round(gross_margin, 2) as gross_margin,
    round(gross_margin / nullif(revenue_with_known_cost, 0), 4) as margin_pct,
    round(revenue / nullif(completed_orders, 0), 2) as avg_order_value,
    round(returned_orders::numeric / nullif(completed_orders + returned_orders, 0), 4) as returned_rate,
    round(walk_in_orders::numeric / nullif(completed_orders, 0), 4) as walk_in_share
from totals
order by period
"""

# dim_date drives the query so a month without sales still appears, with zeros.
MONTHLY_SQL = """
with months as (

    select distinct
        month_start_date,
        year_month,
        year,
        month,
        month_name_short
    from marts.dim_date

),

monthly as (

    select
        dim_date.month_start_date,
        count(distinct fct_sales.order_id) as total_orders,
        count(distinct fct_sales.order_id) filter (where fct_sales.is_revenue) as completed_orders,
        sum(fct_sales.net_amount) filter (where fct_sales.is_revenue) as revenue,
        sum(fct_sales.gross_margin) filter (where fct_sales.is_revenue) as gross_margin,
        sum(fct_sales.net_amount) filter (
            where fct_sales.is_revenue and fct_sales.gross_margin is not null
        ) as revenue_with_known_cost
    from marts.fct_sales as fct_sales
    inner join marts.dim_date as dim_date
        on fct_sales.date_key = dim_date.date_key
    group by dim_date.month_start_date

)

select
    months.year_month,
    months.year,
    months.month,
    months.month_name_short,
    coalesce(monthly.total_orders, 0) as total_orders,
    coalesce(monthly.completed_orders, 0) as completed_orders,
    round(coalesce(monthly.revenue, 0), 2) as revenue,
    round(coalesce(monthly.gross_margin, 0), 2) as gross_margin,
    round(monthly.gross_margin / nullif(monthly.revenue_with_known_cost, 0), 4) as margin_pct,
    round(monthly.revenue / nullif(monthly.completed_orders, 0), 2) as avg_order_value
from months
left join monthly
    on months.month_start_date = monthly.month_start_date
order by months.month_start_date
"""

STORES_SQL = """
with lines as (

    select
        fct_sales.store_key,
        dim_date.year,
        fct_sales.order_id,
        fct_sales.order_status,
        fct_sales.is_revenue,
        fct_sales.net_amount,
        fct_sales.gross_margin
    from marts.fct_sales as fct_sales
    inner join marts.dim_date as dim_date
        on fct_sales.date_key = dim_date.date_key

),

store_periods as (

    select
        store_key,
        coalesce(year::text, 'all') as period,
        count(distinct order_id) filter (where is_revenue) as completed_orders,
        count(distinct order_id) filter (where order_status = 'returned') as returned_orders,
        coalesce(sum(net_amount) filter (where is_revenue), 0) as revenue,
        coalesce(sum(gross_margin) filter (where is_revenue), 0) as gross_margin,
        sum(net_amount) filter (where is_revenue and gross_margin is not null) as revenue_with_known_cost
    from lines
    group by grouping sets ((store_key, year), (store_key))

)

select
    dim_store.store_key,
    dim_store.store_name,
    dim_store.city,
    dim_store.region,
    dim_store.store_type,
    store_periods.period,
    store_periods.completed_orders,
    round(store_periods.revenue, 2) as revenue,
    round(store_periods.gross_margin, 2) as gross_margin,
    round(store_periods.gross_margin / nullif(store_periods.revenue_with_known_cost, 0), 4) as margin_pct,
    round(store_periods.revenue / nullif(store_periods.completed_orders, 0), 2) as avg_order_value,
    round(
        store_periods.returned_orders::numeric
        / nullif(store_periods.completed_orders + store_periods.returned_orders, 0),
        4
    ) as returned_rate
from marts.dim_store as dim_store
inner join store_periods
    on dim_store.store_key = store_periods.store_key
order by dim_store.store_key, store_periods.period
"""

# The reporting mart already has one row per store per month, zero months included.
SPARKLINE_SQL = """
select
    store_key,
    year_month,
    revenue
from marts.mart_monthly_store_performance
order by store_key, month_start_date
"""

CATEGORIES_SQL = """
with lines as (

    select
        dim_date.year,
        dim_product.category,
        fct_sales.quantity,
        fct_sales.net_amount,
        fct_sales.gross_margin
    from marts.fct_sales as fct_sales
    inner join marts.dim_date as dim_date
        on fct_sales.date_key = dim_date.date_key
    inner join marts.dim_product as dim_product
        on fct_sales.product_key = dim_product.product_key
    where fct_sales.is_revenue

),

category_periods as (

    select
        coalesce(year::text, 'all') as period,
        category,
        sum(quantity) as units_sold,
        sum(net_amount) as revenue,
        sum(gross_margin) as gross_margin,
        sum(net_amount) filter (where gross_margin is not null) as revenue_with_known_cost
    from lines
    group by grouping sets ((year, category), (category))

)

select
    period,
    category,
    units_sold,
    round(revenue, 2) as revenue,
    round(coalesce(gross_margin, 0), 2) as gross_margin,
    round(gross_margin / nullif(revenue_with_known_cost, 0), 4) as margin_pct,
    round(revenue / nullif(sum(revenue) over (partition by period), 0), 4) as revenue_share
from category_periods
order by period, revenue desc
"""

CHANNELS_SQL = """
with lines as (

    select
        dim_date.year,
        fct_sales.channel,
        fct_sales.payment_method,
        fct_sales.order_id,
        fct_sales.net_amount
    from marts.fct_sales as fct_sales
    inner join marts.dim_date as dim_date
        on fct_sales.date_key = dim_date.date_key
    where fct_sales.is_revenue

)

select
    coalesce(year::text, 'all') as period,
    channel,
    payment_method,
    count(distinct order_id) as completed_orders,
    round(sum(net_amount), 2) as revenue
from lines
group by grouping sets ((year, channel, payment_method), (channel, payment_method))
order by period, channel, payment_method
"""

# Each count compares a raw table with the cleaned model that fixes it, so the
# numbers are exactly what the warehouse handled on this run.
QUALITY_SQL = """
select
    (select count(*) from raw.customers) as raw_customer_rows,
    (select count(*) from staging.stg_customers) as staged_customers,
    (select count(*) from raw.orders) as raw_order_rows,
    (select count(*) from staging.stg_orders) as staged_orders,
    (select count(*) from staging.stg_orders where is_walk_in) as walk_in_orders,
    (select count(*) from marts.fct_sales) as order_lines,
    (select count(*) from marts.fct_sales where product_key = -1) as orphan_lines,
    (select count(distinct source_product_id) from marts.fct_sales where product_key = -1)
        as orphan_product_ids,
    (select coalesce(round(sum(net_amount), 2), 0) from marts.fct_sales
        where product_key = -1 and is_revenue) as orphan_revenue,
    (select count(*) from raw.order_items
        where nullif(trim(discount_amount), '') is null) as blank_discounts,
    (select count(*) from raw.customers
        where nullif(trim(email), '') is not null
          and email <> lower(trim(email))) as emails_normalised,
    (select count(*) from raw.customers
        where nullif(trim(email), '') is null) as blank_emails
"""

QUERIES: dict[str, str] = {
    "summary": SUMMARY_SQL,
    "monthly": MONTHLY_SQL,
    "stores": STORES_SQL,
    "sparklines": SPARKLINE_SQL,
    "categories": CATEGORIES_SQL,
    "channels": CHANNELS_SQL,
    "quality": QUALITY_SQL,
}


# ----------------------------------------------------------------------- helpers
def to_number(value: Any, places: int | None = None) -> float | int | None:
    """Turn a database value into a JSON number, rounding Decimals to ``places``."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal) and places is not None:
        # Round as Decimal, half away from zero like Postgres, before going to float.
        value = value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    number = float(value)
    return round(number, places) if places is not None else number


def money(value: Any) -> float | None:
    """USD amount rounded to cents."""
    result = to_number(value, 2)
    return None if result is None else float(result)


def ratio(value: Any) -> float | None:
    """Fraction rounded to 4 dp (0.2512 = 25.12%)."""
    result = to_number(value, 4)
    return None if result is None else float(result)


def count(value: Any) -> int:
    """Row or order count; null becomes 0."""
    return 0 if value is None else int(value)


def growth(current: float | None, previous: float | None) -> float | None:
    """Relative change from ``previous`` to ``current`` as a fraction."""
    if current is None or previous in (None, 0):
        return None
    return round((current - previous) / previous, 4)


def period_sort_key(period: str) -> tuple[int, str]:
    """'all' first, then years in ascending order."""
    return (0, "") if period == ALL_PERIODS else (1, period)


def periods_in(rows: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted({str(row["period"]) for row in rows}, key=period_sort_key)


# ---------------------------------------------------------------------- payloads
def kpis(row: Mapping[str, Any]) -> Payload:
    return {
        "revenue": money(row["revenue"]),
        "gross_margin": money(row["gross_margin"]),
        "margin_pct": ratio(row["margin_pct"]),
        "total_orders": count(row["total_orders"]),
        "completed_orders": count(row["completed_orders"]),
        "cancelled_orders": count(row["cancelled_orders"]),
        "returned_orders": count(row["returned_orders"]),
        "walk_in_orders": count(row["walk_in_orders"]),
        "units_sold": count(row["units_sold"]),
        "avg_order_value": money(row["avg_order_value"]),
        "returned_rate": ratio(row["returned_rate"]),
        "walk_in_share": ratio(row["walk_in_share"]),
    }


def build_summary(rows: Sequence[Mapping[str, Any]]) -> Payload:
    """Headline KPIs per period, plus growth of the latest year over the one before."""
    periods = {str(row["period"]): kpis(row) for row in rows}
    years = sorted(p for p in periods if p != ALL_PERIODS)
    yoy: Payload | None = None
    if len(years) >= 2:
        prev, curr = periods[years[-2]], periods[years[-1]]
        margin_change = (
            round(curr["margin_pct"] - prev["margin_pct"], 4)
            if curr["margin_pct"] is not None and prev["margin_pct"] is not None
            else None
        )
        yoy = {
            "current_year": int(years[-1]),
            "previous_year": int(years[-2]),
            "revenue_growth": growth(curr["revenue"], prev["revenue"]),
            "orders_growth": growth(curr["completed_orders"], prev["completed_orders"]),
            "aov_growth": growth(curr["avg_order_value"], prev["avg_order_value"]),
            "margin_pct_change": margin_change,
        }
    ordered = {p: periods[p] for p in sorted(periods, key=period_sort_key)}
    return {"years": [int(y) for y in years], "periods": ordered, "yoy": yoy}


def build_monthly(rows: Sequence[Mapping[str, Any]]) -> Payload:
    return {
        "months": [
            {
                "month": str(row["year_month"]),
                "year": count(row["year"]),
                "month_num": count(row["month"]),
                "label": str(row["month_name_short"]),
                "revenue": money(row["revenue"]),
                "gross_margin": money(row["gross_margin"]),
                "margin_pct": ratio(row["margin_pct"]),
                "completed_orders": count(row["completed_orders"]),
                "total_orders": count(row["total_orders"]),
                "avg_order_value": money(row["avg_order_value"]),
            }
            for row in rows
        ]
    }


def build_stores(
    store_rows: Sequence[Mapping[str, Any]], spark_rows: Sequence[Mapping[str, Any]]
) -> Payload:
    """One entry per store with metrics per period and a monthly revenue series."""
    months: list[str] = []
    series: dict[int, list[float | None]] = {}
    for row in spark_rows:
        month = str(row["year_month"])
        if month not in months:
            months.append(month)
        series.setdefault(count(row["store_key"]), []).append(money(row["revenue"]))

    stores: dict[int, Payload] = {}
    for row in store_rows:
        key = count(row["store_key"])
        store = stores.setdefault(
            key,
            {
                "store_key": key,
                "store_name": row["store_name"],
                "city": row["city"],
                "region": row["region"],
                "store_type": row["store_type"],
                "periods": {},
                "sparkline": series.get(key, []),
            },
        )
        store["periods"][str(row["period"])] = {
            "revenue": money(row["revenue"]),
            "gross_margin": money(row["gross_margin"]),
            "margin_pct": ratio(row["margin_pct"]),
            "completed_orders": count(row["completed_orders"]),
            "avg_order_value": money(row["avg_order_value"]),
            "returned_rate": ratio(row["returned_rate"]),
        }
    return {"months": months, "stores": list(stores.values())}


def build_categories(rows: Sequence[Mapping[str, Any]]) -> Payload:
    periods: dict[str, list[Payload]] = {p: [] for p in periods_in(rows)}
    for row in rows:
        periods[str(row["period"])].append(
            {
                "category": row["category"],
                "revenue": money(row["revenue"]),
                "gross_margin": money(row["gross_margin"]),
                "margin_pct": ratio(row["margin_pct"]),
                "units_sold": count(row["units_sold"]),
                "revenue_share": ratio(row["revenue_share"]),
            }
        )
    for items in periods.values():
        items.sort(key=lambda item: item["revenue"] or 0, reverse=True)
    return {"periods": periods}


def build_channels(rows: Sequence[Mapping[str, Any]]) -> Payload:
    channels = sorted({str(row["channel"]) for row in rows})
    payments = sorted({str(row["payment_method"]) for row in rows})
    periods: dict[str, list[Payload]] = {p: [] for p in periods_in(rows)}
    for row in rows:
        periods[str(row["period"])].append(
            {
                "channel": row["channel"],
                "payment_method": row["payment_method"],
                "revenue": money(row["revenue"]),
                "completed_orders": count(row["completed_orders"]),
            }
        )
    return {"channels": channels, "payment_methods": payments, "periods": periods}


def build_quality(row: Mapping[str, Any]) -> Payload:
    raw_customers = count(row["raw_customer_rows"])
    staged_customers = count(row["staged_customers"])
    raw_orders = count(row["raw_order_rows"])
    staged_orders = count(row["staged_orders"])
    walk_ins = count(row["walk_in_orders"])
    return {
        "duplicate_customers_removed": {
            "count": raw_customers - staged_customers,
            "raw_rows": raw_customers,
            "clean_rows": staged_customers,
        },
        "resent_orders_collapsed": {
            "count": raw_orders - staged_orders,
            "raw_rows": raw_orders,
            "clean_rows": staged_orders,
        },
        "walk_in_orders": {
            "count": walk_ins,
            "share": round(walk_ins / staged_orders, 4) if staged_orders else None,
        },
        "orphan_product_lines": {
            "count": count(row["orphan_lines"]),
            "order_lines": count(row["order_lines"]),
            "product_ids": count(row["orphan_product_ids"]),
            "revenue": money(row["orphan_revenue"]),
        },
        "blank_discounts_zeroed": {"count": count(row["blank_discounts"])},
        "emails_normalised": {
            "count": count(row["emails_normalised"]),
            "blank_to_null": count(row["blank_emails"]),
        },
    }


def build_meta(source: str) -> Payload:
    sha = os.environ.get("GITHUB_SHA") or None
    repo = os.environ.get("GITHUB_REPOSITORY") or "mahamed7/retail-data-warehouse"
    run_id = os.environ.get("GITHUB_RUN_ID")
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "git_sha": sha,
        "repository": repo,
        "run_url": f"https://github.com/{repo}/actions/runs/{run_id}" if run_id else None,
        "source": source,
        "business": "Dukaan Retail",
        "currency": "USD",
        "period_start": "2024-01-01",
        "period_end": "2025-12-31",
    }


def build_payloads(results: Mapping[str, Sequence[Mapping[str, Any]]], source: str) -> dict[str, Payload]:
    """Shape query results into one payload per output file (name without .json)."""
    if not results["quality"]:
        raise ValueError("quality query returned no row")
    return {
        "summary": build_summary(results["summary"]),
        "monthly": build_monthly(results["monthly"]),
        "stores": build_stores(results["stores"], results["sparklines"]),
        "categories": build_categories(results["categories"]),
        "channels": build_channels(results["channels"]),
        "quality": build_quality(results["quality"][0]),
        "meta": build_meta(source),
    }


def write_payloads(payloads: Mapping[str, Payload], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, payload in payloads.items():
        path = out_dir / f"{name}.json"
        text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        path.write_text(text + "\n", encoding="utf-8")
        log.info("wrote %-16s %6s bytes", path.name, f"{len(text):,}")


# -------------------------------------------------------------------- database
def connection_kwargs() -> dict[str, str]:
    """libpq settings from the environment, with the project's local-dev defaults."""
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": os.environ.get("PGPORT", "5432"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
        "dbname": os.environ.get("PGDATABASE", "retail"),
    }


def run_queries() -> dict[str, list[Row]]:
    import psycopg  # imported here so the payload builders work without a driver
    from psycopg.rows import dict_row

    settings = connection_kwargs()
    log.info("connecting to %s:%s/%s as %s", settings["host"], settings["port"], settings["dbname"], settings["user"])
    results: dict[str, list[Row]] = {}
    with psycopg.connect(**settings, row_factory=dict_row, connect_timeout=10) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            for name, sql in QUERIES.items():
                cur.execute(sql)
                results[name] = cur.fetchall()
                log.info("query %-12s %5d rows", name, len(results[name]))
    return results


# ------------------------------------------------------------------------- CLI
def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export the warehouse marts to JSON for the dashboard.")
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT_DIR,
        help="output directory (default: dashboard/data next to this script)",
    )
    parser.add_argument(
        "--source", default="warehouse",
        help='label written to meta.json "source" (default: warehouse)',
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    try:
        payloads = build_payloads(run_queries(), source=args.source)
        write_payloads(payloads, args.out)
    except Exception:
        log.exception("dashboard export failed")
        return 1
    summary = payloads["summary"]["periods"].get(ALL_PERIODS, {})
    log.info(
        "done: revenue %s USD, %s completed orders -> %s",
        f"{summary.get('revenue') or 0:,.2f}", f"{summary.get('completed_orders') or 0:,}", args.out,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

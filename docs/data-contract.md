# Data contract: source extracts → `raw` schema

This file is the agreement between the **extract/load** layer (`pipeline/`) and the
**transform** layer (`warehouse/`, dbt). Change it first, then change both sides.

## The business

**Dukaan Retail** is a fictional chain of shops in Somaliland. Its point-of-sale
system exports five CSV extracts every night. All data is synthetic and generated
deterministically (`random.Random(42)`), so every run produces identical files.

- Sales period: **2024-01-01 to 2025-12-31** (inclusive)
- Currency: **USD**, two decimals
- Timestamps: ISO 8601 without timezone, local time (`2024-03-05T14:22:10`)
- Dates: `YYYY-MM-DD`

## Files (`data/raw/*.csv`, UTF-8, comma-separated, header row)

### `stores.csv` (~8 rows)

| column | example | notes |
| --- | --- | --- |
| store_id | `1` | integer, unique |
| store_name | `Dukaan Hargeisa Central` | |
| city | `Hargeisa` | Hargeisa, Borama, Berbera, Burao, Gabiley, Erigavo, Sheikh, Wajaale |
| region | `Maroodi Jeex` | |
| store_type | `Supermarket` | `Supermarket`, `Mini-mart` or `Wholesale` |
| opened_date | `2019-06-01` | before 2024-01-01 |

### `products.csv` (~120 rows)

| column | example | notes |
| --- | --- | --- |
| product_id | `101` | integer, unique |
| sku | `GRC-0101` | unique |
| product_name | `Basmati Rice 5kg` | |
| category | `Groceries` | Groceries, Beverages, Household, Personal Care, Electronics, Fresh Produce |
| subcategory | `Rice & Grains` | |
| unit_cost | `4.10` | current cost price |
| unit_price | `6.25` | current list price, always > unit_cost |
| is_active | `true` | `true` / `false` |

### `customers.csv` (~2,000 distinct customers)

| column | example | notes |
| --- | --- | --- |
| customer_id | `5001` | integer |
| first_name | `Hodan` | casing is inconsistent (`HODAN`, `hodan`) |
| last_name | `Warsame` | casing is inconsistent |
| email | ` Hodan.Warsame@Example.com ` | may have whitespace/uppercase; ~3% empty |
| phone | `+252634123456` | |
| city | `Borama` | |
| signup_date | `2023-11-14` | |

**Known quality issues:** ~2% of rows are exact duplicates of another row.

### `orders.csv` (~25,000 distinct orders)

| column | example | notes |
| --- | --- | --- |
| order_id | `100001` | integer |
| customer_id | `5001` | **empty = walk-in customer** (~30% of orders) |
| store_id | `3` | always a valid store |
| order_ts | `2024-03-05T14:22:10` | within the sales period |
| channel | `in_store` | `in_store`, `mobile_app`, `phone` |
| payment_method | `zaad` | `cash`, `zaad`, `edahab`, `card` |
| status | `completed` | `completed`, `cancelled`, `returned` |
| updated_at | `2024-03-05T14:22:10` | last change in the source system |

**Known quality issues:** ~1% of `order_id`s appear twice: the source re-sent the
order after a status change. The row with the **latest `updated_at` wins**.

### `order_items.csv` (~60,000 rows)

| column | example | notes |
| --- | --- | --- |
| order_item_id | `1` | integer, unique |
| order_id | `100001` | every item belongs to an order in `orders.csv` |
| product_id | `101` | **~0.1% reference a product missing from `products.csv`** |
| quantity | `2` | integer ≥ 1 |
| unit_price | `6.25` | price **at time of sale** (may differ from today's list price) |
| discount_amount | `0.50` | line-level discount in USD; **empty means 0**; never more than quantity × unit_price |

## How the loader lands it (`raw` schema in PostgreSQL)

- One table per file: `raw.stores`, `raw.products`, `raw.customers`, `raw.orders`, `raw.order_items`.
- **Every source column is `text`**, so the load never fails on bad data. Typing and cleaning happen in dbt staging.
- Empty CSV fields are loaded as **NULL**.
- Two metadata columns are appended: `_source_file text`, `_loaded_at timestamptz`.
- The load is a full refresh: the table is dropped and recreated in one transaction.
- Each run appends one row per file to `raw._load_audit(file_name, row_count, loaded_at)`.

## Connection

Both layers read the standard libpq variables, defaulting to a local dev database:
`PGHOST=localhost`, `PGPORT=5432`, `PGUSER=postgres`, `PGPASSWORD=postgres`, `PGDATABASE=retail`.

## Daily batches (v2)

From v2 the source system also exports **one batch per day**. Full mode above stays
supported and unchanged (CI's full-refresh path keeps using it).

### Generator

| Command | Writes |
| --- | --- |
| `python pipeline/generate_data.py` | full extract to `data/raw/*.csv` (v1, byte-identical) |
| `python pipeline/generate_data.py --run-date 2025-07-01` | that day's batch to `data/raw/2025-07-01/*.csv` |
| `python pipeline/generate_data.py --backfill 2025-06-28 2025-07-03` | one folder per day, inclusive |

A day's batch is a slice of the **same** deterministic dataset as the full extract:

| File | Rows in the batch for day `D` |
| --- | --- |
| `orders.csv` | every order row whose `updated_at` date is `D` (new orders, and re-sent versions) |
| `order_items.csv` | lines of the orders whose **first** export is on `D` (lines are never re-sent) |
| `customers.csv` | customers whose `signup_date` is `D`. The first batch of a backfill also includes everyone who signed up before it, so the batch is self-contained |
| `products.csv` | **full snapshot** of all products with `unit_price` / `unit_cost` / `is_active` **as of `D`** (so the 2025-07-01 price rise shows up as a change) |
| `stores.csv` | full snapshot of all stores |

Same columns and quality issues as the full extract. An empty day still writes all five
files (header only where there are no rows).

### Loader

| Command | Behaviour |
| --- | --- |
| `python pipeline/load_raw.py` | full refresh from `data/raw/*.csv` (v1) |
| `python pipeline/load_raw.py --mode append --run-date 2025-07-01` | appends `data/raw/2025-07-01/*.csv` |

- Every raw table gets a third metadata column, `_run_date date` (`null` in full mode).
- Append mode creates missing tables, then in **one transaction per file** deletes
  `where _run_date = D` and inserts the batch, so re-running a day never creates duplicates.
- `raw._load_audit` gains `run_date date` and `mode text`.

### Warehouse

- `stg_products` and `stg_stores` keep the **latest snapshot per id**, ordered by
  `_run_date desc nulls last, _loaded_at desc`.
- `stg_orders` already keeps the latest `updated_at` per order; `stg_customers` already
  deduplicates.

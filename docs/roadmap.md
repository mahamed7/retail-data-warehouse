# Roadmap: v2

v1 rebuilds the whole warehouse from one full extract. v2 makes it behave like a production
pipeline: **daily batches**, **history**, **incremental models**, **orchestration**, **raw-layer
quality gates** and a **live dashboard**.

Each phase ships as its own commits on `main`, with CI green before the next phase builds on it.

| Phase | Theme | Depends on | Status |
| --- | --- | --- | --- |
| 1 | Daily batch mode | – | ✅ done |
| 2 | History and incremental models | 1 | ⚪ planned |
| 3 | Orchestration with Airflow | 1 | 🟡 DAG done; local stack not yet run |
| 4 | Raw-layer quality gate | 1 | ⚪ planned |
| 5 | Live dashboard on GitHub Pages | – | 🟡 built; publishing pending |

---

## Phase 1: Daily batch mode

The shop system exports one batch per day, not one big file. This is the foundation for
phases 2 to 4. The interface is fixed in [data-contract.md § Daily batches](data-contract.md#daily-batches-v2).

- [x] **1.1 Generator `--run-date`.** `generate_data.py --run-date 2025-07-01` writes only that day's export to `data/raw/2025-07-01/`. It contains the orders whose `updated_at` falls on that day (new orders and re-sends), their order lines (first export only), customers who signed up that day, and full snapshots of `stores` and `products` with prices as of that date. Full mode (no flag) stays byte-identical to v1.
- [x] **1.2 Generator `--backfill START END`.** Writes one folder per day by calling the same slice logic.
- [x] **1.3 Loader `--mode append --run-date D`.** Appends a day's folder to the raw tables and adds a `_run_date` column. It's **idempotent**: rows for `D` are deleted and reloaded in the same transaction, so a re-run never duplicates. Full mode is unchanged apart from the new column.
- [x] **1.4 Staging handles snapshots.** `stg_products` and `stg_stores` keep the latest snapshot per id. `stg_customers` and `stg_orders` already deduplicate.
- [x] **1.5 Tests.**
  - pytest: the union of all daily slices equals the full extract (after dedupe), and re-running a day changes nothing.
  - CI: a `daily-batches` job loads 2025-06-28 to 2025-07-03 one day at a time, runs `dbt build` after each day, then runs one day again to prove idempotency.

**Done when:** both CI jobs (full refresh and daily batches) are green and the revenue reconciliation test passes in both.

> ✅ **Shipped.** CI loads 2025-06-28 to 2025-07-03 one day at a time; `dbt build` passes after every day
> (159 of 159). Replaying 2025-07-01 replaces its rows in place: row counts and revenue are identical
> before and after. The products snapshot shows the price rise (5.79 to 6.25) on 2025-07-01.

## Phase 2: History and incremental models

- [ ] **2.1 SCD type 2 products.** `snapshots/snap_products.sql` (check strategy on price, cost and active flag) feeds `dim_product_history` with `valid_from`, `valid_to` and `is_current`. The 2025-07-01 price rise must create a second version.
- [ ] **2.2 Point-in-time joins.** `fct_sales` gets a `product_version_key`, so each sale uses the cost in force on the day it happened, not today's cost.
- [ ] **2.3 Incremental `fct_sales`.** `materialized='incremental'`, `unique_key='order_item_id'`, `merge` strategy, with a 3-day lookback on `_loaded_at` so late re-sent orders still update.
- [ ] **2.4 Tests.** No overlapping SCD intervals per product; exactly one current row per product; `dbt build --full-refresh` and the incremental run give identical totals.

## Phase 3: Orchestration with Airflow

- [x] **3.1 DAG `dukaan_daily_elt`.** Daily schedule with catchup, so backfills work. Tasks: `extract` (generator `--run-date {{ ds }}`) → `load` (append) → `dbt_source_freshness` → `dbt_build` → `publish_dashboard_data`. Retries, timeouts and an `on_failure_callback` that logs a clear alert.
- [ ] **3.2 Local stack** *(written, not yet run end to end)*. `orchestration/docker-compose.airflow.yml`: Airflow standalone plus the warehouse Postgres, with the repo mounted.
- [x] **3.3 DAG integrity test in CI** *(28 tests pass)*. The DAG imports with no errors, has the expected task order, and its schedule and catchup settings are as designed.

## Phase 4: Raw-layer quality gate

- [ ] **4.1 Checks before transform.** Row counts per batch, required columns present, numeric columns parse, and no future dates. Implemented with Great Expectations or Python checks plus a JSON report.
- [ ] **4.2 Fail fast.** A failing batch stops the run before `dbt build`, in both CI and Airflow.

## Phase 5: Live dashboard on GitHub Pages

- [x] **5.1 Export.** `dashboard/export_data.py` reads the marts and writes `dashboard/data/*.json`: monthly revenue, store league table, category mix, channel and payment mix, and data-quality stats from the run.
- [x] **5.2 Static dashboard.** `dashboard/index.html` with KPI cards, charts and filters. No build step; works on phones; light and dark themes.
- [ ] **5.3 Publish** *(next: enable GitHub Pages with the GitHub Actions source)*. CI builds the warehouse, exports the JSON and deploys `dashboard/` to GitHub Pages on every push to `main`.

**Done when:** the README links to a live dashboard built from the latest CI run.

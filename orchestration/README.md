# Orchestration: Airflow DAG `dukaan_daily_elt`

Phase 3 of the [v2 roadmap](../docs/roadmap.md#phase-3-orchestration-with-airflow). Airflow
runs the warehouse **one business day at a time**, using the same command-line entry points
as CI and local development ([data contract, "Daily batches (v2)"](../docs/data-contract.md#daily-batches-v2)).

| File | Purpose |
| --- | --- |
| [`dags/dukaan_daily_elt.py`](dags/dukaan_daily_elt.py) | The DAG |
| [`docker-compose.airflow.yml`](docker-compose.airflow.yml) | Local stack: Airflow standalone, warehouse Postgres, Airflow metadata Postgres |
| [`Dockerfile`](Dockerfile) | Airflow 2.10.5 image with the pipeline and dbt in a separate virtualenv |
| [`requirements-airflow.txt`](requirements-airflow.txt) | Airflow pinned with the official constraints, for the CI integrity test |
| [`tests/test_dag_integrity.py`](tests/test_dag_integrity.py) | DAG integrity test (no database or network needed) |

## What the DAG does

```mermaid
flowchart LR
    extract["extract<br/><code>generate_data.py --run-date ds</code>"]
    load["load<br/><code>load_raw.py --mode append --run-date ds</code>"]
    gate["quality_gate<br/><code>check_raw.py --run-date ds</code><br/>(Phase 4 placeholder)"]
    fresh["dbt_source_freshness<br/><code>dbt source freshness</code>"]
    build["dbt_build<br/><code>dbt build</code>"]
    publish["publish_dashboard_data<br/><code>export_data.py --out dashboard/data</code>"]
    extract --> load --> gate --> fresh --> build --> publish
```

| Task | Runs (in the repository; dbt tasks in `warehouse/`) | Effect |
| --- | --- | --- |
| `extract` | `python pipeline/generate_data.py --run-date {{ ds }}`, plus `--include-history` when no earlier run has succeeded | Writes the day's batch to `data/raw/{{ ds }}/`. The history flag adds customers who signed up before that day, so the first day of a backfill window on an empty warehouse passes `stg_orders`' relationship test |
| `load` | `python pipeline/load_raw.py --mode append --run-date {{ ds }}` | Deletes and reloads that day's rows in `raw.*` |
| `quality_gate` | `python pipeline/check_raw.py --run-date {{ ds }}` | Phase 4. Until that script exists the task logs `quality gate not configured yet` and exits 0. Once it exists, a non-zero exit fails the task and stops the run before dbt |
| `dbt_source_freshness` | `dbt source freshness --profiles-dir .` | Fails if `raw` was not just loaded (`_loaded_at`) |
| `dbt_build` | `dbt build --profiles-dir . --vars '{"run_date": "{{ ds }}"}'` | Builds and tests every model. `run_date` is available to date-aware models later on |
| `publish_dashboard_data` | `python dashboard/export_data.py --out dashboard/data` | Refreshes the dashboard JSON |

Schedule: `@daily` from **2024-01-01** to **2025-12-31** (the synthetic sales period), so the
DAG has exactly 731 runs, one per day of 2024 and 2025.

### Configuration

The DAG reads environment variables when it is parsed. It uses no Airflow Variables or
Connections, so parsing never needs the metadata database.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DUKAAN_REPO_DIR` | `/opt/dukaan` | Repository checkout the tasks run in |
| `DUKAAN_PYTHON` | `python` | Interpreter with `requirements.txt` installed |
| `DUKAAN_DBT` | `dbt` | dbt executable |
| `PGHOST` `PGPORT` `PGUSER` `PGPASSWORD` `PGDATABASE` | `localhost` `5432` `postgres` `postgres` `retail` | Warehouse connection, passed to every task |

### Alerts

When a task has failed for the last time, after its retries, `on_failure_callback` logs one line to the
task log (logger `dukaan.alerts`):

```text
DUKAAN_ALERT {"dag_id": "dukaan_daily_elt", "error": "AirflowException: Bash command failed. The command returned a non-zero exit code 1.", "event": "task_failed", "log_url": "http://localhost:8080/dags/dukaan_daily_elt/grid?...", "max_tries": 2, "run_date": "2025-07-01", "run_id": "scheduled__2025-07-01T00:00:00+00:00", "task_id": "dbt_build", "try_number": 3}
```

Each retry logs a matching `DUKAAN_RETRY {...}` warning. The payload is JSON, so forwarding it to Slack or
e-mail later only means changing `notify_failure`, not the DAG.

## Run it locally

You need Docker with Compose v2. From the **repository root**:

```bash
# Linux only: let the container write to the mounted repo as you
export AIRFLOW_UID=$(id -u)

docker compose -f orchestration/docker-compose.airflow.yml up --build
```

| Service | What | Host port |
| --- | --- | --- |
| `airflow` | `airflow standalone` (scheduler, webserver, triggerer) with the LocalExecutor | [localhost:8080](http://localhost:8080), user `admin`, password `admin` (or `$AIRFLOW_ADMIN_PASSWORD`) |
| `warehouse-db` | Postgres 16, database `retail` | `5433` (`$WAREHOUSE_HOST_PORT`), so it can run next to the root `docker-compose.yml` on 5432 |
| `airflow-db` | Postgres 16 for Airflow's metadata | not published |

The repository is mounted at `/opt/dukaan` and `orchestration/dags` at `/opt/airflow/dags`,
so edits to the DAG or the pipeline show up without a rebuild. Rebuild (`--build`) only after
changing `requirements.txt`.

Connect to the warehouse from the host with
`psql "postgresql://postgres:postgres@localhost:5433/retail"`.

Stop with `docker compose -f orchestration/docker-compose.airflow.yml down`. Add `-v` to also delete
the warehouse and the Airflow history.

### Trigger runs

The DAG starts **paused**. Unpausing it in the UI schedules every day of the sales period
(731 runs, one after another). For a demo, backfill a short window instead:

```bash
docker compose -f orchestration/docker-compose.airflow.yml exec airflow \
  airflow dags backfill -s 2025-06-28 -e 2025-07-03 dukaan_daily_elt
```

This runs 2025-06-28 to 2025-07-03 in date order, one day at a time. It is the same window
as the CI `daily-batches` job and includes the 2025-07-01 price rise. To re-run a single
day, clear it in the UI (or `airflow tasks clear -s 2025-07-01 -e 2025-07-01 dukaan_daily_elt`). Because
every task is keyed on `--run-date`, this replaces that day's data instead of duplicating it.

### Where the logs go

- **Task logs:** `orchestration/logs/` on the host (mounted at `/opt/airflow/logs`; git-ignored), laid
  out as `dag_id=dukaan_daily_elt/run_id=.../task_id=.../attempt=N.log`. They are also in the UI
  under each task instance. Search them for `DUKAAN_ALERT`.
- **Scheduler and webserver logs:** `docker compose -f orchestration/docker-compose.airflow.yml logs -f airflow`.
- **dbt logs:** `warehouse/logs/dbt.log` in the mounted repository (and the task log).

## DAG integrity test

`tests/test_dag_integrity.py` loads the DAG folder with `DagBag` and checks that:

- the DAG imports without errors and `dukaan_daily_elt` exists;
- the task ids match and the dependency chain is exactly
  `extract → load → quality_gate → dbt_source_freshness → dbt_build → publish_dashboard_data`;
- the schedule is daily, the first run covers 2024-01-01, and no run is scheduled after 2025-12-31;
- `catchup` is on and `max_active_runs` is 1;
- every task has retries, a 5-minute retry delay, an execution timeout, `depends_on_past` and the
  failure callback, and the callback logs a structured alert;
- every command renders with `ds=2025-07-01` (`StrictUndefined`, so typos fail) and contains
  the expected script and `--run-date 2025-07-01`, and `extract` adds `--include-history` only
  when there is no earlier successful run;
- the quality-gate placeholder passes when `pipeline/check_raw.py` is missing and returns its
  exit code when it exists (runs the rendered bash, no Airflow runtime needed).

It needs Airflow but no database, warehouse or network. Airflow 2.10.5 supports Python
3.9 to 3.12, so use Python 3.12:

```bash
python3.12 -m venv .venv-airflow && . .venv-airflow/bin/activate
pip install -r orchestration/requirements-airflow.txt
# equivalent: pip install "apache-airflow==2.10.5" pytest \
#   --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
AIRFLOW_HOME=$(mktemp -d) DUKAAN_REQUIRE_AIRFLOW=1 pytest orchestration/tests -q
```

Without Airflow installed the module is skipped, so the repository-wide `pytest -q` keeps
working in the main CI job.

## Design choices

- **BashOperator, not PythonOperator or provider operators.** The tasks call the same CLIs as
  CI and the README, so there is one way to run each step, and the DAG works with core Airflow
  only. It also keeps the pipeline's dependencies (dbt, psycopg) out of the scheduler's Python
  environment. They live in `/opt/dukaan-venv` in the image, because dbt-core 1.10 needs
  `protobuf>=6` and Airflow 2.10.5's constraints pin `protobuf` 4.25.
- **Idempotent tasks keyed on `--run-date`.** The generator is deterministic and the loader
  deletes and reinserts `where _run_date = D` in one transaction per file. Retries, clears and
  backfills therefore converge on the same state. The DAG passes `{{ ds }}` and never uses
  "today".
- **`catchup=True` with a fixed `start_date` and `end_date`.** Each run's logical date is the
  business day it processes, so history can be (re)built just by scheduling, and
  `airflow dags backfill` reuses the same path. `end_date` stops the DAG at the end of the
  synthetic data.
- **`max_active_runs=1` and `depends_on_past=True`.** Days are built strictly in order and a
  failed day blocks the days after it. Phase 2 needs this: SCD snapshots and the incremental
  `fct_sales` assume batches arrive chronologically, and two dbt runs must never write to the
  same schemas at the same time.
- **Retries and timeouts.** Two retries five minutes apart absorb transient database hiccups. A
  30-minute `execution_timeout` per task and a 2-hour `dagrun_timeout` make a hung run fail
  visibly instead of blocking every later day.
- **Postgres for Airflow metadata, not SQLite.** SQLite would force the SequentialExecutor and
  lock under the scheduler and webserver together. A second small Postgres container keeps
  `standalone` simple and allows the LocalExecutor. It is also separate from the warehouse, so
  `down -v` on one does not affect the other's history.

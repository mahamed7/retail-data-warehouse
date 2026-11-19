"""Airflow DAG ``dukaan_daily_elt``: the Dukaan Retail warehouse, one day at a time.

Each DAG run processes a single business day, ``{{ ds }}``, end to end:

    extract -> load -> quality_gate -> dbt_source_freshness -> dbt_build -> publish_dashboard_data

Every task is a ``BashOperator`` that calls the repository's own command-line entry
points, exactly as documented in ``docs/data-contract.md`` ("Daily batches (v2)").
The DAG holds no business logic, so the same commands work from a laptop, in CI and
here.

Runtime configuration comes from environment variables read when the file is parsed.
No Airflow Variables or Connections are used, so parsing never touches the metadata
database:

==================  =====================  ===========================================
Variable            Default                Meaning
==================  =====================  ===========================================
DUKAAN_REPO_DIR     /opt/dukaan            Repository checkout the commands run in
DUKAAN_PYTHON       python                 Interpreter with the pipeline requirements
DUKAAN_DBT          dbt                    dbt executable (dbt-postgres installed)
PGHOST              localhost              Warehouse host (libpq variables, as in the
PGPORT              5432                   data contract; passed to every task)
PGUSER              postgres
PGPASSWORD          postgres
PGDATABASE          retail
==================  =====================  ===========================================
"""

from __future__ import annotations

import json
import logging
import os
import posixpath
import shlex
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from jinja2 import StrictUndefined

if TYPE_CHECKING:
    from airflow.utils.context import Context

# A stable logger name (not __name__, which DagBag mangles) so alerts can be routed by
# name. It propagates to the root logger, which Airflow points at the task log while a
# task runs.
log = logging.getLogger("dukaan.alerts")

DAG_ID = "dukaan_daily_elt"

# The synthetic sales period from docs/data-contract.md. One DAG run per day.
SALES_START = pendulum.datetime(2024, 1, 1, tz="UTC")
SALES_END = pendulum.datetime(2025, 12, 31, tz="UTC")

REPO_DIR: str = os.environ.get("DUKAAN_REPO_DIR", "/opt/dukaan")
WAREHOUSE_DIR: str = posixpath.join(REPO_DIR, "warehouse")
PYTHON: str = shlex.quote(os.environ.get("DUKAAN_PYTHON", "python"))
DBT: str = shlex.quote(os.environ.get("DUKAAN_DBT", "dbt"))

# Warehouse connection, handed to every task. append_env=True below keeps the worker's
# own environment (PATH, HOME, ...) and lays these on top.
WAREHOUSE_ENV: dict[str, str] = {
    "PGHOST": os.environ.get("PGHOST", "localhost"),
    "PGPORT": os.environ.get("PGPORT", "5432"),
    "PGUSER": os.environ.get("PGUSER", "postgres"),
    "PGPASSWORD": os.environ.get("PGPASSWORD", "postgres"),
    "PGDATABASE": os.environ.get("PGDATABASE", "retail"),
}


def build_alert(context: Context | dict[str, Any], event: str) -> dict[str, Any]:
    """Collect the fields an on-call engineer needs from a task callback context."""
    ti = context.get("task_instance") or context.get("ti")
    exception = context.get("exception")
    return {
        "event": event,
        "dag_id": getattr(ti, "dag_id", DAG_ID),
        "task_id": getattr(ti, "task_id", None),
        "run_date": context.get("ds"),
        "run_id": getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
        "max_tries": getattr(ti, "max_tries", None),
        "log_url": getattr(ti, "log_url", None),
        "error": f"{type(exception).__name__}: {exception}" if exception else None,
    }


def notify_failure(context: Context) -> None:
    """Task failure callback: log one structured, greppable alert line.

    Fires once a task has used up all its retries. Searching the logs for
    ``DUKAAN_ALERT`` finds every failure; the JSON payload is easy to forward to
    Slack, PagerDuty or e-mail later without changing the DAG.
    """
    alert = build_alert(context, event="task_failed")
    log.error("DUKAAN_ALERT %s", json.dumps(alert, sort_keys=True, default=str))


def notify_retry(context: Context) -> None:
    """Task retry callback: same structure as the failure alert, logged as a warning."""
    alert = build_alert(context, event="task_retry")
    log.warning("DUKAAN_RETRY %s", json.dumps(alert, sort_keys=True, default=str))


default_args: dict[str, Any] = {
    "owner": "dukaan-data",
    # A day is only processed after the previous day succeeded, so history (SCD
    # snapshots, incremental facts) is always built in date order.
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
}

DOC_MD = """
### Dukaan Retail: daily ELT

One run per business day (`ds`) of the synthetic sales period, 2024-01-01 to 2025-12-31.

| Task | Command (run from the repository) |
| --- | --- |
| `extract` | `python pipeline/generate_data.py --run-date {{ ds }}` writes `data/raw/{{ ds }}/` (adds `--include-history` when no earlier run has succeeded) |
| `load` | `python pipeline/load_raw.py --mode append --run-date {{ ds }}` replaces that day's rows in `raw.*` |
| `quality_gate` | `python pipeline/check_raw.py --run-date {{ ds }}` runs 32 data-contract checks on the batch; any error-level failure stops the run before dbt |
| `dbt_source_freshness` | `dbt source freshness` fails if the raw tables were not just loaded |
| `dbt_build` | `dbt build` runs and tests staging, intermediate and marts |
| `publish_dashboard_data` | `python dashboard/export_data.py --out dashboard/data` refreshes the dashboard JSON |

**Idempotent:** every task is keyed on `--run-date`, so clearing and re-running a day
replaces that day's data instead of duplicating it.

**Ordering:** `catchup=True`, `max_active_runs=1` and `depends_on_past=True` make
runs go strictly one day after another; a failed day stops the days after it.

**Alerts:** a task that fails after its retries logs a `DUKAAN_ALERT {...}` JSON line
with task id, run date, try number and log URL.

Backfill a window: `airflow dags backfill -s 2025-06-28 -e 2025-07-03 dukaan_daily_elt`.
See `orchestration/README.md`.
"""


def repo_task(task_id: str, bash_command: str, doc_md: str, cwd: str = REPO_DIR) -> BashOperator:
    """A BashOperator that runs one repository command with the warehouse env."""
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        cwd=cwd,
        env=WAREHOUSE_ENV,
        append_env=True,
        doc_md=doc_md,
    )


with DAG(
    dag_id=DAG_ID,
    description="Daily extract, load, quality gate, dbt build and dashboard export for Dukaan Retail.",
    schedule="@daily",
    start_date=SALES_START,
    end_date=SALES_END,
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    # A misspelt template variable fails the task instead of rendering as "".
    template_undefined=StrictUndefined,
    default_args=default_args,
    doc_md=DOC_MD,
    tags=["dukaan", "elt", "dbt", "postgres"],
) as dag:
    # A day's batch only carries the customers who signed up that day. The first run
    # with no earlier successful run (a fresh warehouse, or the start of a backfill
    # window) adds --include-history so earlier customers are loaded too; otherwise
    # stg_orders' relationship test to stg_customers would fail.
    extract = repo_task(
        "extract",
        f"{PYTHON} pipeline/generate_data.py --run-date {{{{ ds }}}}"
        "{{ '' if prev_data_interval_start_success else ' --include-history' }}",
        "Write the day's source batch to `data/raw/{{ ds }}/` (deterministic, safe to re-run). "
        "Adds `--include-history` when no earlier run has succeeded.",
    )

    load = repo_task(
        "load",
        f"{PYTHON} pipeline/load_raw.py --mode append --run-date {{{{ ds }}}}",
        "Delete-and-insert the day's rows in the `raw` schema, one transaction per file.",
    )

    # Phase 4 quality gate: pipeline/check_raw.py validates the day's raw batch against
    # the data contract. Exit 1 (a contract breach) or 2 (could not run) fails this task
    # and stops the run before dbt. The fallback branch only matters on a checkout from
    # before Phase 4, where the script does not exist yet.
    quality_gate = repo_task(
        "quality_gate",
        "\n".join(
            [
                "set -euo pipefail",
                "if [ -f pipeline/check_raw.py ]; then",
                f"  {PYTHON} pipeline/check_raw.py --run-date {{{{ ds }}}}",
                "else",
                '  echo "quality gate not configured yet (pipeline/check_raw.py not found);'
                ' passing batch {{ ds }}"',
                "fi",
            ]
        ),
        "Validate the day's raw batch against the data contract (`pipeline/check_raw.py`). "
        "Writes `reports/quality/{{ ds }}.json` and appends to `raw._quality_results`.",
    )

    dbt_source_freshness = repo_task(
        "dbt_source_freshness",
        f"{DBT} source freshness --profiles-dir .",
        "Fail if the raw tables were not loaded recently (`_loaded_at`, see `_sources.yml`).",
        cwd=WAREHOUSE_DIR,
    )

    dbt_build = repo_task(
        "dbt_build",
        f"{DBT} build --profiles-dir . --vars '{{\"run_date\": \"{{{{ ds }}}}\"}}'",
        "Run and test every dbt model. `run_date` is passed as a var for date-aware models.",
        cwd=WAREHOUSE_DIR,
    )

    publish_dashboard_data = repo_task(
        "publish_dashboard_data",
        f"{PYTHON} dashboard/export_data.py --out dashboard/data",
        "Export the marts to `dashboard/data/*.json` for the static dashboard.",
    )

    extract >> load >> quality_gate >> dbt_source_freshness >> dbt_build >> publish_dashboard_data

"""Integrity tests for the ``dukaan_daily_elt`` Airflow DAG.

They parse the DAG folder with ``DagBag`` and inspect the result, so they need
``apache-airflow`` installed (``orchestration/requirements-airflow.txt``) but no
metadata database, no warehouse and no network.

Without Airflow the module is skipped, so the repository-wide ``pytest -q`` in the
main CI job still passes. The Airflow CI job sets ``DUKAAN_REQUIRE_AIRFLOW=1`` so a
broken install fails loudly instead of being skipped.
"""

from __future__ import annotations

import logging
import os
import posixpath
import shutil
import subprocess
import sys
import tempfile
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

# Configure Airflow before it is imported: a throwaway home, no example DAGs, and the
# test's own interpreter for the DAG's `python` commands.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-home-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("DUKAAN_PYTHON", sys.executable)

if os.environ.get("DUKAAN_REQUIRE_AIRFLOW") == "1":
    import airflow  # noqa: F401  (fail, do not skip, when the CI job expects Airflow)
else:
    pytest.importorskip("airflow", reason="apache-airflow is not installed")

import pendulum  # noqa: E402  (installed with Airflow)
from airflow.models import DAG, DagBag  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402
from airflow.timetables.base import DataInterval  # noqa: E402
from airflow.timetables.interval import CronDataIntervalTimetable  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "dukaan_daily_elt"
RUN_DATE = "2025-07-01"

EXPECTED_CHAIN = [
    "extract",
    "load",
    "quality_gate",
    "dbt_source_freshness",
    "dbt_build",
    "publish_dashboard_data",
]

REPO_DIR = os.environ.get("DUKAAN_REPO_DIR", "/opt/dukaan")
WAREHOUSE_DIR = posixpath.join(REPO_DIR, "warehouse")

# What each task must run, after rendering with ds=2025-07-01.
EXPECTED_COMMAND_PARTS: dict[str, list[str]] = {
    "extract": ["pipeline/generate_data.py", f"--run-date {RUN_DATE}"],
    "load": ["pipeline/load_raw.py", "--mode append", f"--run-date {RUN_DATE}"],
    "quality_gate": [
        "pipeline/check_raw.py",
        f"--run-date {RUN_DATE}",
        "quality gate not configured yet",
    ],
    "dbt_source_freshness": ["source freshness", "--profiles-dir ."],
    "dbt_build": ["build", "--profiles-dir .", f'"run_date": "{RUN_DATE}"'],
    "publish_dashboard_data": ["dashboard/export_data.py", "--out dashboard/data"],
}
TASKS_WITH_RUN_DATE = {"extract", "load", "quality_gate", "dbt_build"}
EXPECTED_CWD = {
    "dbt_source_freshness": WAREHOUSE_DIR,
    "dbt_build": WAREHOUSE_DIR,
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag: DagBag) -> DAG:
    found = dagbag.get_dag(DAG_ID)
    assert found is not None, f"{DAG_ID} not found; DAGs loaded: {sorted(dagbag.dag_ids)}"
    return found


def render_command(task: BashOperator, *, first_run: bool = False) -> str:
    """Render a task's bash_command for RUN_DATE without a database.

    Only the context keys the DAG uses are supplied; the DAG renders with
    StrictUndefined, so any other variable would fail here.
    """
    previous_success = None if first_run else pendulum.datetime(2025, 6, 30, tz="UTC")
    context = {"ds": RUN_DATE, "prev_data_interval_start_success": previous_success}
    return task.render_template(task.bash_command, context)


# --- Import ---------------------------------------------------------------------------


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_dag_is_loaded(dagbag: DagBag) -> None:
    assert DAG_ID in dagbag.dag_ids


# --- Structure ------------------------------------------------------------------------


def test_task_ids(dag: DAG) -> None:
    assert sorted(dag.task_ids) == sorted(EXPECTED_CHAIN)


def test_dependency_chain_is_exact(dag: DAG) -> None:
    for position, task_id in enumerate(EXPECTED_CHAIN):
        task = dag.get_task(task_id)
        expected_upstream = {EXPECTED_CHAIN[position - 1]} if position > 0 else set()
        expected_downstream = (
            {EXPECTED_CHAIN[position + 1]} if position < len(EXPECTED_CHAIN) - 1 else set()
        )
        assert task.upstream_task_ids == expected_upstream, task_id
        assert task.downstream_task_ids == expected_downstream, task_id


def test_every_task_is_a_bash_operator(dag: DAG) -> None:
    for task in dag.tasks:
        assert isinstance(task, BashOperator), task.task_id


# --- Scheduling -----------------------------------------------------------------------


def test_schedule_is_daily(dag: DAG) -> None:
    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.timetable.summary in {"@daily", "0 0 * * *"}


def test_first_run_covers_first_sales_day(dag: DAG) -> None:
    first = dag.next_dagrun_info(None)
    assert first is not None
    assert first.data_interval.start == pendulum.datetime(2024, 1, 1, tz="UTC")
    assert first.data_interval.end - first.data_interval.start == timedelta(days=1)


def test_no_runs_after_sales_period(dag: DAG) -> None:
    last = DataInterval(
        pendulum.datetime(2025, 12, 31, tz="UTC"), pendulum.datetime(2026, 1, 1, tz="UTC")
    )
    assert dag.next_dagrun_info(last) is None
    assert dag.end_date == pendulum.datetime(2025, 12, 31, tz="UTC")


def test_catchup_and_concurrency(dag: DAG) -> None:
    assert dag.catchup is True
    assert dag.max_active_runs == 1


# --- Resilience -----------------------------------------------------------------------


def test_retries_and_timeouts(dag: DAG) -> None:
    for task in dag.tasks:
        assert task.retries >= 1, task.task_id
        assert task.retry_delay == timedelta(minutes=5), task.task_id
        assert task.execution_timeout is not None, task.task_id
        assert task.depends_on_past is True, task.task_id


def test_failure_callback_is_wired(dag: DAG) -> None:
    callback = dag.default_args["on_failure_callback"]
    for task in dag.tasks:
        wired = task.on_failure_callback
        callbacks = wired if isinstance(wired, list) else [wired]
        assert callback in callbacks, task.task_id


def test_failure_callback_logs_structured_alert(
    dag: DAG, caplog: pytest.LogCaptureFixture
) -> None:
    fake_ti = SimpleNamespace(
        dag_id=DAG_ID,
        task_id="dbt_build",
        run_id=f"scheduled__{RUN_DATE}T00:00:00+00:00",
        try_number=3,
        max_tries=2,
        log_url="http://localhost:8080/log?task_id=dbt_build",
    )
    context: dict[str, Any] = {
        "task_instance": fake_ti,
        "ds": RUN_DATE,
        "exception": RuntimeError("dbt exited with 1"),
    }
    with caplog.at_level(logging.ERROR, logger="dukaan.alerts"):
        dag.default_args["on_failure_callback"](context)

    alerts = [
        record.getMessage() for record in caplog.records if "DUKAAN_ALERT" in record.getMessage()
    ]
    assert len(alerts) == 1
    line = alerts[0]
    for expected in (
        '"event": "task_failed"',
        '"task_id": "dbt_build"',
        f'"run_date": "{RUN_DATE}"',
        '"try_number": 3',
        '"log_url": "http://localhost:8080/log?task_id=dbt_build"',
        "RuntimeError: dbt exited with 1",
    ):
        assert expected in line, line


# --- Commands -------------------------------------------------------------------------


@pytest.mark.parametrize("task_id", EXPECTED_CHAIN)
def test_command_renders_for_run_date(dag: DAG, task_id: str) -> None:
    task = dag.get_task(task_id)
    command = render_command(task)

    assert "{{" not in command and "}}" not in command
    for part in EXPECTED_COMMAND_PARTS[task_id]:
        assert part in command, f"{task_id}: {part!r} missing from {command!r}"
    if task_id in TASKS_WITH_RUN_DATE:
        assert RUN_DATE in command


def test_extract_includes_customer_history_only_on_first_run(dag: DAG) -> None:
    extract = dag.get_task("extract")
    assert render_command(extract, first_run=True).endswith(
        f"--run-date {RUN_DATE} --include-history"
    )
    assert render_command(extract, first_run=False).endswith(f"--run-date {RUN_DATE}")


@pytest.mark.parametrize("task_id", EXPECTED_CHAIN)
def test_task_runs_in_repo_with_warehouse_env(dag: DAG, task_id: str) -> None:
    task = dag.get_task(task_id)
    assert task.cwd == EXPECTED_CWD.get(task_id, REPO_DIR)
    assert task.append_env is True
    assert {"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE"} <= set(task.env)


# --- Quality gate placeholder (runs the rendered bash, no Airflow runtime) ---------------

needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")


def run_gate(dag: DAG, cwd: Path) -> subprocess.CompletedProcess[str]:
    command = render_command(dag.get_task("quality_gate"))
    return subprocess.run(
        ["bash", "-c", command], cwd=cwd, capture_output=True, text=True, check=False
    )


@needs_bash
def test_quality_gate_passes_when_not_configured(dag: DAG, tmp_path: Path) -> None:
    result = run_gate(dag, tmp_path)
    assert result.returncode == 0, result.stderr
    assert "quality gate not configured yet" in result.stdout
    assert RUN_DATE in result.stdout


@needs_bash
@pytest.mark.parametrize("exit_code", [0, 3])
def test_quality_gate_runs_check_raw_when_present(dag: DAG, tmp_path: Path, exit_code: int) -> None:
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "check_raw.py").write_text(
        "import sys\n"
        f"assert sys.argv[1:] == ['--run-date', '{RUN_DATE}'], sys.argv\n"
        f"sys.exit({exit_code})\n",
        encoding="utf-8",
    )
    result = run_gate(dag, tmp_path)
    assert result.returncode == exit_code, result.stderr
    assert "not configured" not in result.stdout

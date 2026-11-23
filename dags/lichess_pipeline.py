"""Lichess months -> bronze -> silver, one mapped task group per month, then gold.

Trigger with a list of months; one run is a backfill, a rerun is the same call with
one month (add "force": true to re-stream a month whose checksum already matches):

    airflow dags trigger lichess_pipeline --conf '{"months": ["2013-01", "2013-02"]}'

Each task runs the same CLI as CLAUDE.md, in the pipeline's own venv (D-018), so
Airflow only schedules, retries and records; it adds no logic of its own to the data.
"""

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

log = logging.getLogger(__name__)

PROJECT_DIR = "/opt/lichess"
PYTHON = f"{PROJECT_DIR}/.venv/bin/python"
DBT = f"{PROJECT_DIR}/.venv/bin/dbt"
# Both CLIs exit 3 for bad input (malformed rate, DQ check). The source files are
# immutable, so a retry would fail the same way after re-streaming the whole month.
EXIT_DATA_ERROR = 3
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    env = os.environ | {
        "PYTHONPATH": f"{PROJECT_DIR}/src",
        "SPARK_LOCAL_IP": "127.0.0.1",
    }
    log.info("running %s", " ".join(cmd))
    return subprocess.run(
        cmd, check=False, cwd=PROJECT_DIR, env=env, text=True, **kwargs
    )


def run_cli(module: str, *args: str) -> dict:
    """Run `python -m module args` in the pipeline venv; return the JSON it prints last."""
    # stderr (logging, Spark) is inherited so it streams into the task log as it runs.
    proc = _run([PYTHON, "-m", module, *args], stdout=subprocess.PIPE)
    log.info("stdout:\n%s", proc.stdout)
    if proc.returncode == EXIT_DATA_ERROR:
        raise AirflowFailException(f"{module} rejected the data (exit 3), not retrying")
    if proc.returncode != 0:
        raise RuntimeError(f"{module} exited {proc.returncode}")
    lines = proc.stdout.splitlines()
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].startswith("{"):
            return json.loads("\n".join(lines[i:]))
    raise RuntimeError(f"{module} printed no JSON summary")


@dag(
    dag_id="lichess_pipeline",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    # Two runs on the same month would race on the partition swap.
    max_active_runs=1,
    params={
        "months": Param(
            ["2013-01"],
            type="array",
            items={"type": "string", "pattern": MONTH_PATTERN},
            minItems=1,
            description="Source file months, YYYY-MM",
        ),
        "force": Param(
            False,
            type="boolean",
            description="Re-stream months whose bronze checksum already matches",
        ),
        # Bronze for these months is ~41 GB each (2024); keep only silver (~4 GB) once
        # it passes its checks. Rebuilding silver then means re-streaming (D-026).
        "drop_bronze_from": Param(
            "2017-01",
            type="string",
            pattern=MONTH_PATTERN,
            description="Delete bronze Parquet after silver for months >= this",
        ),
    },
    tags=["lichess"],
)
def lichess_pipeline():
    @task
    def months() -> list[str]:
        return sorted(set(get_current_context()["params"]["months"]))

    @task_group(group_id="month")
    def month_pipeline(month: str):
        @task(
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=30),
            # A 2024 month is ~30 GB; two at once just split the bandwidth.
            max_active_tis_per_dag=1,
        )
        def bronze(month: str) -> dict:
            ctx = get_current_context()
            ti = ctx["ti"]
            # try_number > 1 means a restart from byte zero (D-009); the count of
            # these per month is how often the network failed us.
            log.info("bronze %s attempt %s of %s", month, ti.try_number, ti.max_tries)
            args = ["--month", month]
            if not ctx["params"]["force"]:
                args.append("--skip-if-current")
            out = run_cli("lichess.bronze", *args)
            if out["status"] == "skipped":
                raise AirflowSkipException(f"{month} bronze matches {out['sha256']}")
            return out | {"attempt": ti.try_number}

        # none_failed: a skipped bronze still rebuilds silver, which is cheap next to
        # the stream and catches a silver partition that was deleted or is stale. If
        # bronze was dropped (D-026), silver skips when it's current and fails if not.
        @task(
            retries=1,
            retry_delay=timedelta(minutes=1),
            trigger_rule="none_failed",
            # One JVM at a time; local[*] already uses every core and 4 GB of heap.
            max_active_tis_per_dag=1,
        )
        def silver(month: str) -> dict:
            args = ["--month", month]
            if month >= get_current_context()["params"]["drop_bronze_from"]:
                args.append("--drop-bronze")
            out = run_cli("lichess.spark_jobs.silver_games", *args)
            if out["status"] == "skipped":
                raise AirflowSkipException(
                    f"{month} bronze was dropped; silver already built from "
                    f"{out['bronze_sha256']}"
                )
            return out

        bronze(month) >> silver(month)

    # Gold is rebuilt from all of silver, not per month: the tables are small and the
    # Wilson ranks depend on every month at once. none_failed: runs when bronze skipped,
    # never when any month failed, so gold never mixes old and half-new silver.
    @task(trigger_rule="none_failed", retries=1, retry_delay=timedelta(minutes=1))
    def dbt_build() -> None:
        # dbt-duckdb doesn't create the database file's parent directory.
        os.makedirs(f"{PROJECT_DIR}/data/gold", exist_ok=True)
        proc = _run(
            [DBT, "build", "--project-dir", "dbt", "--profiles-dir", "dbt"]
            + ["--no-use-colors"]
        )
        # dbt exits 1 when a model or test fails (bad data or SQL: a retry gives the
        # same answer) and 2 for an unhandled error such as a locked DuckDB file.
        if proc.returncode == 1:
            raise AirflowFailException(
                "dbt build: a model or test failed, not retrying"
            )
        if proc.returncode != 0:
            raise RuntimeError(f"dbt exited {proc.returncode}")

    month_pipeline.expand(month=months()) >> dbt_build()


lichess_pipeline()

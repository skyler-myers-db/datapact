"""Databricks Job orchestration helpers.

This module encapsulates building tasks, creating/updating jobs, and polling runs.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import (
    DashboardTask,
    JobRunAs,
    JobSettings,
    RunIf,
    RunLifeCycleState,
    Source,
    SqlTask,
    SqlTaskFile,
    Task,
    TaskDependency,
)
from loguru import logger

TERMINAL_STATES: list[RunLifeCycleState] = [
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
]


def build_tasks(
    asset_paths: dict[str, str],
    warehouse_id: str,
    validation_task_keys: list[str],
    sql_params: dict[str, str],
) -> list[Task]:
    tasks: list[Task] = [
        Task(
            task_key=tk,
            sql_task=SqlTask(
                file=SqlTaskFile(path=asset_paths[tk], source=Source.WORKSPACE),
                warehouse_id=warehouse_id,
                parameters=sql_params,
            ),
        )
        for tk in validation_task_keys
    ]

    tasks.append(
        Task(
            task_key="aggregate_results",
            depends_on=[TaskDependency(task_key=tk) for tk in validation_task_keys],
            run_if=RunIf.ALL_DONE,
            sql_task=SqlTask(
                file=SqlTaskFile(path=asset_paths["aggregate_results"], source=Source.WORKSPACE),
                warehouse_id=warehouse_id,
                parameters=sql_params,
            ),
        )
    )
    return tasks


def add_dashboard_refresh_task(tasks: list[Task], dashboard_id: str, warehouse_id: str) -> None:
    tasks.append(
        Task(
            task_key="refresh_dashboard",
            depends_on=[TaskDependency(task_key="aggregate_results")],
            run_if=RunIf.ALL_DONE,
            dashboard_task=DashboardTask(
                dashboard_id=dashboard_id,
                warehouse_id=warehouse_id,
            ),
        )
    )


def add_genie_room_task(
    tasks: list[Task],
    asset_paths: dict[str, str],
    warehouse_id: str,
) -> None:
    """Add a task to set up curated datasets for Databricks AI/BI Genie.

    This task creates optimized tables from validation results that can be used
    as data sources when creating a Genie space through the UI. The task runs
    in parallel with the dashboard refresh task, both depending on aggregate_results.

    After this task completes, users can:
    1. Navigate to Databricks AI/BI in the UI
    2. Create a new Genie space
    3. Add the created tables as data sources
    4. Enable natural language querying of validation results
    """
    if "setup_genie_datasets" not in asset_paths:
        # Skip if Genie setup SQL wasn't uploaded
        return

    tasks.append(
        Task(
            task_key="setup_genie_datasets",
            depends_on=[TaskDependency(task_key="aggregate_results")],
            run_if=RunIf.ALL_DONE,
            sql_task=SqlTask(
                file=SqlTaskFile(path=asset_paths["setup_genie_datasets"], source=Source.WORKSPACE),
                warehouse_id=warehouse_id,
            ),
        )
    )


def ensure_job(
    w: WorkspaceClient,
    *,
    job_name: str,
    tasks: list[Task],
    user_name: str | None,
) -> int:
    runner_is_service_principal: bool = bool(
        user_name and len(user_name) == 36 and "@" not in user_name
    )
    logger.info(
        "Program is being run as a service principal"
        if runner_is_service_principal
        else "Program is not being run as a service principal"
    )

    job_settings: JobSettings = JobSettings(
        name=job_name,
        tasks=tasks,
        run_as=JobRunAs(
            service_principal_name=(user_name if runner_is_service_principal else None),
            user_name=user_name if not runner_is_service_principal else None,
        ),
    )

    existing_job = next(iter(w.jobs.list(name=job_name)), None)
    job_id: int | None = (
        existing_job.job_id
        if existing_job
        else w.jobs.create(
            name=job_settings.name,
            tasks=job_settings.tasks,
            run_as=job_settings.run_as,
        ).job_id
    )
    if existing_job and job_id is not None:
        w.jobs.reset(job_id=job_id, new_settings=job_settings)
    if job_id is None:
        raise ValueError("Job ID is None. Cannot launch job run.")
    return job_id


def run_and_wait(
    w: WorkspaceClient,
    *,
    job_id: int,
    tasks: list[Task],
    timeout_hours: int = 1,
    now_fn: Callable[[], datetime] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
    poll_interval_seconds: float = 30.0,
    max_poll_interval_seconds: float = 90.0,
) -> None:
    """Run a Databricks job and poll until completion.

    Optional now_fn and sleep_fn allow tests to inject time control to avoid real waits.
    """
    if now_fn is None:
        now_fn = datetime.now
    if sleep_fn is None:
        import time as _time

        sleep_fn = _time.sleep
    logger.info(f"Launching job {job_id}...")
    run_info = w.jobs.run_now(job_id=job_id)
    run_metadata = w.jobs.get_run(run_info.run_id)
    logger.info(f"Run started! View progress here: {run_metadata.run_page_url}")

    timeout: timedelta = timedelta(hours=timeout_hours)
    deadline: datetime = now_fn() + timeout
    current_interval = max(poll_interval_seconds, 1.0)
    capped_interval = max(current_interval, max_poll_interval_seconds)
    while now_fn() < deadline:
        if run_metadata.run_id is None:
            raise ValueError("Run ID is None. Cannot poll job run status.")
        run = w.jobs.get_run(run_metadata.run_id)
        life_cycle_state = getattr(run.state, "life_cycle_state", None)
        if life_cycle_state is not None and life_cycle_state in TERMINAL_STATES:
            break
        finished_tasks = sum(
            1
            for t in (run.tasks or [])
            if getattr(getattr(t, "state", None), "life_cycle_state", None) in TERMINAL_STATES
        )
        logger.info(f"Job state: {life_cycle_state}. Tasks finished: {finished_tasks}/{len(tasks)}")
        sleep_fn(current_interval)
        current_interval = min(capped_interval, current_interval * 1.5)
    else:
        raise TimeoutError("Job run timed out.")

    if run.state is not None:
        result_state = getattr(run.state, "result_state", None)
        logger.info(f"Run finished with state: {result_state}")
        if result_state == jobs.RunResultState.SUCCESS:
            logger.success("✅ DataPact job completed successfully.")
        else:
            error_message = run.state.state_message or "Job failed without a specific message."
            logger.error(
                "DataPact job finished unsuccessfully. "
                f"Final state: {result_state}. Reason: {error_message}. "
                f"View details at {run.run_page_url}"
            )
            raise RuntimeError(
                f"DataPact job run failed with state {result_state}: {error_message}"
            )
    else:
        logger.error("Run state is None. Unable to determine job result.")
        raise RuntimeError("Run state is None. Unable to determine job result.")

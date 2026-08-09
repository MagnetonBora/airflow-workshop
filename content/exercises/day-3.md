---
title: "Day 3 — Exercises"
---

# 1. CI/CD and Deployment

## Task 1 — Break it, catch it, fix it

**Task.** Create a DAG file `dags/broken_pricing_dag.py` that fails to import because of a `SyntaxError` (not a missing module this time). Write a `pytest` test using `DagBag` that catches this, run it and observe the failure output, then fix the file so the test passes.

## Task 2 — Structure test

**Task.** For a DAG `pricing_pipeline` with tasks `extract_prices`, `validate_prices`, `publish_prices` (in that dependency order), write a structure test using `DagBag` that asserts: the DAG exists, it has exactly 3 tasks, and `validate_prices` is downstream of `extract_prices`. Explain what kind of pull request this test would catch that an import test would not.

## Task 3 — Rollback reasoning

**Task.** A DAG version deployed an hour ago is producing wrong output in production, but it imports fine and passes structure tests — the bug is in business logic. Describe, step by step, the correct way to fix production according to the practices covered, and explain why editing the DAG file directly through the Code view on the production server is the wrong approach. Also state what happens to DagRuns that already started under the broken version once the fix is deployed.

# 2. Testing Levels

**Task.** Given this task:

```python
@task
def validate_prices(prices: list):
    valid = [p for p in prices if p > 0]
    if not valid:
        raise ValueError("No valid prices remain")
    return valid
```

Extract the business logic into a standalone, Airflow-independent function, then write two `pytest` tests: one confirming that non-positive prices are filtered out, and one confirming that an all-invalid input raises `ValueError`. State which of the four testing levels (import, structure, unit, `tasks test`) is most useful while actively iterating on this function, and why.

# 3. Diagnostics

**Task.** The following task fails every time it runs:

```python
from airflow.operators.bash import BashOperator

BashOperator(
    task_id="print_run_date",
    bash_command="echo 'Run date: {{ logical_dt }}'",
)
```

Using only the diagnostic checklist (Log → Rendered Template → upstream state → Connections/Variables → worker resources → local reproduction), identify which step would surface the problem, state what the actual bug is, and give the corrected `bash_command`. Then give the exact CLI command you would use to reproduce this failure locally without triggering the DAG from the UI.

# 4. TaskGroup

**Task.** Rewrite the following flat DAG so that `clean_prices` and `enrich_prices` are visually grouped together in the Grid view under a single TaskGroup called `transform_group`, while `extract_prices` and `load_prices` remain outside of it:

```python
@task
def extract_prices():
    return {"raw": 1}

@task
def clean_prices(data):
    return data

@task
def enrich_prices(data):
    return data

@task
def load_prices(data):
    print("Loading")

load_prices(enrich_prices(clean_prices(extract_prices())))
```

State one thing that does *not* change about how this DAG executes when you introduce the TaskGroup.

# 5. Pools and Common Production Mistakes

## Task 1 — Protect a shared resource

**Task.** A dynamically mapped task queries a shared production Postgres instance once per mapped item, and the source list can contain up to 200 items. Configure a Pool that limits concurrent connections to 4 and attach it to the mapped task.

## Task 2 — Make the write idempotent

**Task.** The following task is not safe to retry or backfill:

```python
@task
def store_daily_price(summary: dict):
    hook = PostgresHook(postgres_conn_id="warehouse_pg")
    hook.run(
        "INSERT INTO daily_prices (price_date, avg_price) VALUES (%s, %s)",
        parameters=(summary["price_date"], summary["avg_price"]),
    )
```

Rewrite it so that rerunning the same logical date twice produces the same final row instead of a duplicate.

## Task 3 — Logical time, not physical time

**Task.** The following task records processing time in a way that breaks reproducibility during a backfill:

```python
@task
def tag_summary(summary: dict):
    from datetime import datetime
    summary["processed_at"] = datetime.now().isoformat()
    return summary
```

Rewrite it so the recorded value is deterministic for a given logical run, and explain in one sentence why the original version is a problem specifically during backfills or retries.

# 6. Monitoring and Security

## Task 1 — Failure callback

**Task.** Add an `on_failure_callback` to a DAG's `default_args` that prints the failed task's `dag_id`, `task_id`, and log URL. State one situation in which this callback would *not* fire even though something went wrong with the run's timing.

## Task 2 — Secrets backend reasoning

**Task.** A team currently stores all Connections directly in the metadata database (encrypted at rest via the Fernet key) and wants to migrate to HashiCorp Vault as a Secrets Backend. State whether any task code that currently does `PostgresHook(postgres_conn_id="warehouse_pg")` needs to change, and explain the difference between what the Fernet key protects and what a Secrets Backend protects.

## Task 3 — Access control reasoning

**Task.** Two teams, `pricing_team` and `finance_viewers`, work with the same DAG. `pricing_team` needs full edit/delete rights; `finance_viewers` should only ever be able to read it. Write the `access_control` argument for the `@dag` decorator that expresses this.

# 7. Scaling

**Task.** A single-node deployment with `LocalExecutor` currently runs 40 nightly dbt models. The team plans to grow to 400 models and add several concurrent ML training pipelines that also need to run daily. For each of the following four levers, state in one sentence what it controls and whether it should be reconsidered for this growth: `parallelism`, `max_active_runs`, `worker_concurrency`, Pools. Also state which Executor change should be considered first and why `LocalExecutor` becomes a bottleneck at this scale.

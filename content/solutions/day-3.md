---
title: "Day 3 — Full Breakdown"
---

# 1. CI/CD and Deployment

## Task 1 — Break it, catch it, fix it

**Description.** Produce a `SyntaxError` import failure, catch it with a `DagBag`-based test, then fix it.

**Solution.**

```python
# dags/broken_pricing_dag.py — broken version
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def broken_pricing_dag()
    @task
    def run():
        print("Never reached")
    run()

broken_pricing_dag()
```

```python
# tests/test_dag_integrity.py
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0
```

Running `pytest tests/test_dag_integrity.py -v` fails with an `AssertionError`, and inspecting `dag_bag.import_errors` shows the file path mapped to a `SyntaxError: expected ':'` (the missing colon after the `@dag(...)` decorator's function signature). Fixing it means adding the missing `:` after `def broken_pricing_dag()`.

**Analysis.** `DagBag()` executes the same import mechanism the DAG Processor uses in production, so any import-time exception — `SyntaxError`, `NameError`, missing module, unhandled top-level exception — is caught identically, regardless of its specific type. This is why the import test is described as a *gate*, not a partial check: it doesn't care what kind of error broke the import, only that one did.

## Task 2 — Structure test

**Solution.**

```python
from airflow.models import DagBag

def test_pricing_pipeline_structure():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("pricing_pipeline")

    assert dag is not None, "DAG pricing_pipeline was not found"
    assert len(dag.tasks) == 3, f"Expected 3 tasks, found {len(dag.tasks)}"

    extract_task = dag.get_task("extract_prices")
    downstream_ids = {t.task_id for t in extract_task.downstream_list}
    assert "validate_prices" in downstream_ids, "validate_prices must follow extract_prices"
```

**Analysis.** This test would catch a pull request that leaves the DAG importable — so the import test passes — but accidentally removes or reorders a dependency, for example if someone refactors `extract_prices >> validate_prices >> publish_prices` into `extract_prices >> publish_prices` and forgets `validate_prices` entirely from the chain, or wires it as a sibling instead of a downstream task. An import test only proves the Python file *parses*; it says nothing about whether the graph still expresses the intended data-flow contract.

## Task 3 — Rollback reasoning

**Solution / Analysis.** The correct sequence is: identify the offending commit, `git revert` it (not a manual hotfix), and push the revert through the normal `Lint → Import test → Deploy` pipeline like any other change. Editing the DAG file directly on the server, or through the read-only Code view, bypasses every safeguard the pipeline exists to provide — no lint pass, no import test, no audit trail through Git history, and the change is invisible to anyone reviewing the repository. It also risks divergence between what's deployed and what's in source control, which makes the next legitimate deployment silently overwrite the manual fix.

Regarding already-started DagRuns: they do not switch to the corrected graph mid-execution. A DagRun is tied to the DAG Version that was active when it started, so after the rollback is deployed, the team must explicitly decide whether to let already-running instances of the broken version finish (potentially still producing wrong output) or stop them and rerun affected logical dates once the fix is live.

# 2. Testing Levels

**Solution.**

```python
# Extracted, testable logic
def validate_prices_logic(prices: list) -> list:
    valid = [p for p in prices if p > 0]
    if not valid:
        raise ValueError("No valid prices remain")
    return valid

@task
def validate_prices(prices: list):
    return validate_prices_logic(prices)
```

```python
import pytest
from dags.pricing_pipeline import validate_prices_logic

def test_filters_non_positive_prices():
    assert validate_prices_logic([10.0, -2.0, 0.0, 5.5]) == [10.0, 5.5]

def test_raises_when_nothing_valid():
    with pytest.raises(ValueError):
        validate_prices_logic([-1.0, 0.0, -3.0])
```

**Analysis.** Once the filtering/validation logic is a plain function with no Airflow imports and no `context`, it can be unit-tested directly with `pytest`, with no metadata database, no scheduler, and no DAG parsing involved — these tests run in milliseconds.

Regarding testing levels: `airflow tasks test` is the most useful **while actively iterating**, because it executes the real task inside the real Airflow execution path (template rendering, Hook/Connection resolution, XCom) for a single, fast, local run — closer to production behavior than a pure unit test, but far faster than triggering a full DagRun from the UI. Unit tests on the extracted logic are best for locking in correctness of the business rule itself once it's stable, and are what you'd run continuously in CI on every commit; `tasks test` is more of an interactive, manual debugging tool.

# 3. Diagnostics

**Description.** A `BashOperator` fails on every run due to a template typo.

**Solution.** The bug is `{{ logical_dt }}` — no such Jinja variable exists; the correct built-in is `{{ ds }}` (or `{{ logical_date }}` for the full timestamp object, depending on what format is needed):

```python
BashOperator(
    task_id="print_run_date",
    bash_command="echo 'Run date: {{ ds }}'",
)
```

Local reproduction:

```bash
airflow tasks test pricing_pipeline print_run_date 2026-01-01
```

**Analysis.** Walking the checklist: step 1 (**Log**) would show the Bash command failing or a shell error about an undefined value; step 2 (**Rendered Template**) is where the actual substituted command becomes visible and the invalid `{{ logical_dt }}` expression is exposed directly — this is the step that actually surfaces the root cause here, since the Python file itself is syntactically valid and would pass an import test without any complaint. This class of error is a good illustration of why "the DAG imports fine" gives no guarantee about runtime correctness: template rendering happens only when a task actually executes, not when the file is parsed.

# 4. TaskGroup

**Solution.**

```python
from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def pricing_transform_demo():

    @task
    def extract_prices():
        return {"raw": 1}

    with TaskGroup(group_id="transform_group") as transform_group:
        @task
        def clean_prices(data):
            return data

        @task
        def enrich_prices(data):
            return data

        enrich_prices(clean_prices(extract_prices()))

    @task
    def load_prices(data):
        print("Loading")

    transform_group >> load_prices({})

pricing_transform_demo()
```

**Analysis.** What does *not* change: the actual execution graph, dependency resolution, retry behavior, and the fact that this is still a single DagRun. `TaskGroup` is purely a visual/organizational construct in Grid view — it collapses `clean_prices` and `enrich_prices` into one expandable node — and does not create a nested DagRun or any new scheduling boundary, unlike the old, now-removed `SubDagOperator`.

# 5. Pools and Common Production Mistakes

## Task 1 — Protect a shared resource

**Solution.**

```bash
airflow pools set warehouse_pool 4 "At most four concurrent production Postgres connections"
```

```python
@task(pool="warehouse_pool")
def query_item_price(item_id: str):
    hook = PostgresHook(postgres_conn_id="warehouse_pg")
    return hook.get_first("SELECT price FROM items WHERE id = %s;", parameters=(item_id,))

query_item_price.expand(item_id=get_item_ids())
```

**Analysis.** Without the Pool, up to 200 mapped TaskInstances could attempt to open a Postgres connection simultaneously, which is exactly the scenario that overloads a shared production database. With `pool="warehouse_pool"` and 4 slots, only 4 mapped instances run concurrently; the remaining 196 queue and acquire a slot as soon as one is released, regardless of how many workers or how much `parallelism` the installation otherwise allows.

## Task 2 — Make the write idempotent

**Solution.**

```python
@task
def store_daily_price(summary: dict):
    hook = PostgresHook(postgres_conn_id="warehouse_pg")
    hook.run(
        "INSERT INTO daily_prices (price_date, avg_price) VALUES (%s, %s) "
        "ON CONFLICT (price_date) DO UPDATE SET avg_price = EXCLUDED.avg_price",
        parameters=(summary["price_date"], summary["avg_price"]),
    )
```

**Analysis.** The plain `INSERT` creates a new duplicate row every time the task is retried or the logical date is backfilled — Airflow guarantees the *task* will retry, but says nothing about the *side effects* of that retry being safe. The `ON CONFLICT ... DO UPDATE` upsert makes rerunning the same `price_date` converge to the same final row instead of accumulating duplicates, which is what makes the task genuinely idempotent rather than merely retryable.

## Task 3 — Logical time, not physical time

**Solution.**

```python
@task
def tag_summary(summary: dict, **context):
    summary["processed_at"] = context["ds"]
    return summary
```

**Analysis.** `datetime.now()` captures the physical wall-clock moment the task attempt actually executed, which is different every time the task is retried or backfilled — so two runs that are supposed to represent the same logical day end up tagged with different values. `context["ds"]` is tied to the DagRun's logical interval and stays identical across retries and backfills of that same interval, which is what "deterministic for a given logical run" means in practice.

# 6. Monitoring and Security

## Task 1 — Failure callback

**Solution.**

```python
from airflow.sdk import dag, task
from datetime import datetime

def notify_on_failure(context):
    ti = context["task_instance"]
    print(f"Task {ti.dag_id}.{ti.task_id} failed. Log: {ti.log_url}")

@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"on_failure_callback": notify_on_failure},
)
def pricing_pipeline():
    @task
    def risky_task():
        raise ValueError("Something went wrong")
    risky_task()

pricing_pipeline()
```

**Analysis.** `on_failure_callback` fires exactly when a task transitions to `failed` — it will **not** fire in a situation where the task technically succeeds but takes far longer than expected (a task that should finish in 5 minutes but succeeds after 90 minutes triggers no failure callback at all, since nothing failed). That specific gap — "it worked, but not on time" — is what deadline/duration/freshness alerts exist to cover, since legacy task-level SLAs were removed in Airflow 3.

## Task 2 — Secrets backend reasoning

**Solution / Analysis.** No task code needs to change. `PostgresHook(postgres_conn_id="warehouse_pg")` keeps resolving the same stable `conn_id`; only *where* Airflow looks up the underlying connection parameters changes, from the metadata database to Vault, via the `[secrets] backend` configuration.

Fernet and a Secrets Backend solve different problems: the Fernet key encrypts sensitive Connection/Variable fields *at rest inside the metadata database* — the secret still lives there, just encrypted. A Secrets Backend removes the secret from the metadata database entirely, storing it in an external system (Vault, AWS Secrets Manager, etc.) and fetching it on demand. They are not interchangeable, and moving to a Secrets Backend is a stronger boundary than relying on Fernet alone.

## Task 3 — Access control reasoning

**Solution.**

```python
@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    access_control={
        "pricing_team": {"can_read", "can_edit", "can_delete"},
        "finance_viewers": {"can_read"},
    },
)
def pricing_pipeline():
    ...
```

**Analysis.** `access_control` is declared per-DAG and grants only the listed permissions to each named role; `finance_viewers` having only `can_read` means they can see the DAG's Grid/Graph view and logs but cannot trigger, edit, or delete it — enforcing least privilege at the individual pipeline level rather than relying on a single shared admin role for everyone who needs any visibility at all.

# 7. Scaling

**Solution / Analysis.**

| Lever | Controls | Reconsider for this growth? |
| --- | --- | --- |
| `parallelism` | Installation-wide maximum number of tasks running concurrently, across all DAGs | Yes — the current ceiling was likely sized for 40 models, not 400 plus concurrent ML pipelines |
| `max_active_runs` | How many DagRuns of one specific DAG may be active concurrently | Possibly — matters more if pricing and ML DAGs each need multiple overlapping runs in flight, less relevant if each DAG runs once nightly |
| `worker_concurrency` | How many tasks a single Celery worker process can execute at once | Yes, once moving off `LocalExecutor` — this becomes the per-worker throughput knob |
| Pools | Concurrency limit for a *named shared resource* (e.g. the warehouse database) across all DAGs that reference it | Yes — with more models and pipelines hitting the same database, an explicit Pool prevents them from collectively overwhelming it, independent of `parallelism` |

**Executor.** `LocalExecutor` runs all tasks on a single machine's process pool, so it becomes a hard bottleneck as both task count and the number of concurrently-needed pipelines grow — there is no way to add capacity beyond that one machine's CPU/memory. The team should evaluate `CeleryExecutor` (a predictable, horizontally-scalable pool of distributed workers) or `KubernetesExecutor` (per-task pod isolation, useful especially for the ML pipelines if they have heterogeneous resource needs). Alongside the executor change, Pools around the shared warehouse/APIs, and a revisit of Scheduler and metadata-database capacity, are the other pieces that typically need attention at this scale.

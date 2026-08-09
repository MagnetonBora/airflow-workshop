---
title: "Day 2 — Full Breakdown"
---

# 1. Trigger Rules

## Task 1 — Alert and cleanup branches

**Description.** Build `ingest_report` so that a failing `fetch_source` triggers `send_alert` but skips `build_summary`, while `cleanup` runs unconditionally after both branches.

**Solution.**

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def ingest_report():

    @task
    def fetch_source():
        raise ValueError("Source is unreachable")

    @task
    def build_summary(data=None):
        print("Summary built")  # all_success (default): skipped on failure.

    @task(trigger_rule="all_failed")
    def send_alert():
        print("Alerting: source fetch failed")

    @task(trigger_rule="all_done")
    def cleanup():
        print("Cleanup ran regardless of outcome")

    fetch_task = fetch_source()
    summary_task = build_summary(fetch_task)
    alert_task = send_alert()
    fetch_task >> alert_task

    [summary_task, alert_task] >> cleanup()

ingest_report()
```

**Analysis.** `build_summary` keeps the default `all_success`, so it becomes `upstream_failed` the moment `fetch_source` fails — it never actually attempts to run. `send_alert` uses `all_failed`, so it only fires because its single upstream task failed; if `fetch_source` had succeeded, `send_alert` would be `skipped`. `cleanup` uses `all_done`, the only rule that is indifferent to `success`/`failed`/`skipped`/`upstream_failed` states on its upstream tasks, which is exactly why it is the right choice for anything that must always run — logging, cleanup, releasing a lock, sending a "run finished" signal.

**Common mistakes.** Using `all_failed` on `cleanup` instead of `all_done` — `all_failed` requires every upstream task to have failed, so if `send_alert` succeeds, `cleanup` would never run. Another common mistake is wiring `cleanup` to depend only on `fetch_source` directly instead of on both downstream branches, which makes it start before `send_alert`/`build_summary` have actually finished.

## Task 2 — Predict the states

**Description.** Reason about final task states without running the DAG: `extract` succeeds, `transform` (default rule) and `notify` (`one_failed`) both depend only on `extract`.

**Solution / Analysis.** `transform` runs and succeeds, because its only upstream task (`extract`) succeeded and `all_success` is satisfied. `notify` becomes `skipped`, because `one_failed` requires **at least one** upstream task to have failed, and `extract` succeeded — there is no failure to react to.

**Check yourself.**
1. What would `notify`'s state be if `extract` had failed instead? → `notify` would run (`success`, assuming its own logic doesn't raise), because `one_failed` is now satisfied; `transform` would become `upstream_failed`.
2. Which single-word difference separates `one_failed` from `all_failed`? → `one_failed` needs at least one failure among upstream tasks; `all_failed` needs every upstream task to have failed.

# 2. Data Interval and Catchup

## Task 1 — When does it actually run?

**Description.** `schedule="@daily"`, `start_date=2026-03-01`, DAG enabled on `2026-03-10`, `catchup` unspecified.

**Solution.**
* Exactly **one** DagRun is created.
* Its `logical_date` is `2026-03-09` — the most recent interval that has already fully closed at the time the DAG was enabled.
* It physically starts at `2026-03-10 00:00`, the moment the `[2026-03-09 00:00, 2026-03-10 00:00)` interval ends.

**Analysis.** Since Airflow 3.0, `catchup_by_default` is `False`, so an unspecified `catchup` behaves as `catchup=False`. The scheduler only materializes the single most recent completed interval instead of backfilling every day since `start_date`. This is the direct opposite of the pre-3.0 default, which would have created ten runs (2026-03-01 through 2026-03-09).

## Task 2 — Backfill a specific range

**Description.** Produce exactly five historical runs (`2026-03-01`–`2026-03-05`) without touching other dates.

**Solution.** No change to the DAG definition is required — `catchup` stays `False` so no unwanted runs appear automatically. Trigger the range explicitly:

```bash
airflow dags backfill sync_regions --start-date 2026-03-01 --end-date 2026-03-05
```

or, per-day, with `airflow dags trigger --logical-date ...` for each date if fine-grained control over triggering is needed.

**Analysis.** Setting `catchup=True` would be the wrong tool here — it would create runs for every interval between `start_date` and "now" the next time the DAG is parsed, not just the requested five days. `backfill` with explicit `--start-date`/`--end-date` is the mechanism designed for bounded, on-demand historical runs regardless of the DAG's `catchup` setting.

# 3. Dynamic DAG Generation

**Description.** Convert a top-level-loop DAG factory (one DAG per region) into a single dynamically-mapped TaskFlow DAG.

**Solution.**

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def sync_regions():

    @task
    def get_regions():
        return ["eu-west", "us-east", "ap-south"]  # Computed during execution.

    @task
    def sync_region(region_name: str):
        print(f"Syncing {region_name}")

    sync_region.expand(region_name=get_regions())

sync_regions()
```

**Analysis.** In Grid view, the original version shows three independent DAGs (`sync_eu-west`, `sync_us-east`, `sync_ap-south`), each parsed and scheduled separately. The rewritten version shows a single DAG, `sync_regions`, whose `sync_region` task expands into three map indexes (0, 1, 2) discovered only when `get_regions()` actually runs — the DAG Processor sees a fixed two-task structure at parse time regardless of how many regions exist. This also means adding a fourth region no longer requires a new DAG Processor scan to notice a structural change; it is picked up naturally the next time `get_regions()` runs.

**Common mistakes.** Passing `region_name=["eu-west", "us-east", "ap-south"]` (a plain list) directly to `.expand()` instead of the XCom-backed return value of a `@task`-decorated function — `.expand()` expects a mapped argument produced by a task, not a static Python list evaluated at parse time (that would silently reintroduce Pattern 1's problem, or simply fail depending on how it's written).

# 4. XCom

## Task 1 — Multiple explicit keys

**Solution.**

```python
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

def check_shipment(**context):
    context["ti"].xcom_push(key="carrier", value="DHL")
    context["ti"].xcom_push(key="eta_days", value=3)

def report_shipment(**context):
    ti = context["ti"]
    carrier = ti.xcom_pull(task_ids="check_shipment", key="carrier")
    eta_days = ti.xcom_pull(task_ids="check_shipment", key="eta_days")
    print(f"Shipment via {carrier}, arriving in {eta_days} days")

with DAG(
    dag_id="shipment_status_demo",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    check_task = PythonOperator(task_id="check_shipment", python_callable=check_shipment)
    report_task = PythonOperator(task_id="report_shipment", python_callable=report_shipment)
    check_task >> report_task
```

**Analysis.** Each `xcom_push(key=..., value=...)` call creates an independent row in the XCom table, keyed by `(dag_id, task_id, run_id, key)`. `xcom_pull` must supply both `task_ids` (which task produced it) and `key` (which specific value) to retrieve the right one — omitting `key` only ever returns the implicit `return_value` key, which is empty here since the function doesn't `return` anything.

## Task 2 — Find the silent bug

**Solution.** `notify_customer` pulls `key="total"`, but `calculate_total` pushed the value under `key="order_total"`. The fix:

```python
def notify_customer(**context):
    total = context["ti"].xcom_pull(task_ids="calculate_total", key="order_total")
    print(f"Order total: {total}")
```

**Analysis.** `xcom_pull` returns `None` for a key that doesn't exist rather than raising an exception, so the DAG "succeeds" while silently producing wrong output. This class of bug is invisible in the task log unless you specifically check the printed value or inspect the **XCom** tab of the `calculate_total` TaskInstance, where you would see `order_total` listed but not `total`.

**Check yourself.**
1. Does `xcom_pull` raise or return `None` for a missing key? → Returns `None`, no exception.
2. Where in the UI would you have spotted the mismatch fastest? → Task Instance Details → XCom tab for `calculate_total`.

# 5. Airflow and dbt

**Description.** Evolve a cron-based `dbt run` into a readiness-aware, model-granular, event-triggered pipeline.

**Solution.**

*Step 1 — real readiness instead of a fixed time:*

```python
from airflow.providers.postgres.sensors.sql import SqlSensor

wait_for_load = SqlSensor(
    task_id="wait_for_load_complete",
    conn_id="warehouse_pg",
    sql="SELECT 1 FROM load_status WHERE status = 'complete' AND load_date = '{{ ds }}'",
    deferrable=True,
)
wait_for_load >> dbt_run
```
Solves: the load can finish early or late; the pipeline now starts exactly when the data is actually ready instead of guessing a safety margin.

*Step 2 — per-model granularity with Cosmos:*

```python
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

dbt_transform = DbtTaskGroup(
    group_id="dbt_transform",
    project_config=ProjectConfig("/opt/dbt_project"),
    profile_config=ProfileConfig(profile_name="warehouse_profile", target_name="prod"),
)
```
Solves: a single failing model no longer hides the status of every other model behind one opaque `dbt_run` task; each model becomes its own task and only the broken one turns red.

*Step 3 — Asset-driven downstream trigger:*

```python
from airflow.sdk import Asset, dag, task

fct_sales_asset = Asset("postgres://warehouse/public/fct_sales")

@dag(schedule=[fct_sales_asset], catchup=False)
def refresh_bi_dashboard():
    @task
    def refresh():
        print("fct_sales updated; refreshing dashboard")
    refresh()
```
Solves: the BI refresh no longer runs on an approximate fixed delay after dbt "probably" finishes — it runs exactly when the specific model's output changes.

**Analysis.** Each step targets a distinct failure mode: guesswork about *when* data is ready (Step 1), lack of granularity in *what* failed (Step 2), and guesswork about *when downstream consumers* should react (Step 3). None of the later steps make the earlier ones unnecessary — production pipelines commonly combine all three.

# 6. Connections and Variables

**Solution.**

Connection `warehouse_pg`: Type `Postgres`, Host `warehouse`, Schema `analytics`, Port `5432`, plus login/password for the environment.

```python
from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

DEFAULT_QUERY = "SELECT COUNT(*) FROM customers WHERE is_active = true;"

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def customer_count_demo():

    @task
    def count_active_customers():
        query = Variable.get("active_customer_query", default=DEFAULT_QUERY)
        hook = PostgresHook(postgres_conn_id="warehouse_pg")
        result = hook.get_first(query)
        print(f"Active customers: {result[0]}")
        return result[0]

    count_active_customers()

customer_count_demo()
```

**Analysis.** `Variable.get(..., default=...)` is called *inside* the task body, not at module level — calling it during DAG parsing would add an API Server request on every single scan of the `dags/` folder, exactly the anti-pattern flagged for top-level `Variable.get()` calls. The `default` parameter keeps the DAG functional even before anyone sets the Variable, which matters because a missing Variable with no default raises an exception rather than returning `None`.

# 7. XCom Size and Best Practices

**Solution.**

```python
import pandas as pd
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def clickstream_pipeline():

    @task
    def extract_events():
        events = pd.DataFrame({"user_id": range(500_000), "event": ["click"] * 500_000})
        path = "/tmp/clickstream_events.parquet"
        events.to_parquet(path)
        return path

    @task
    def aggregate_events(path: str):
        events = pd.read_parquet(path)
        print(events["user_id"].nunique())

    aggregate_events(extract_events())

clickstream_pipeline()
```

**Analysis.** Only a short string path now travels through XCom (and therefore through the metadata database); the actual half-million-row DataFrame lives on disk and is read back explicitly by the consuming task.

**Remaining risk under CeleryExecutor.** `/tmp` is local to whichever worker machine executed `extract_events`. With multiple Celery workers, `aggregate_events` may be scheduled on a *different* machine that never wrote that file, producing a `FileNotFoundError`. The production-correct fix is a shared destination such as S3 or GCS rather than local disk, or a custom XCom backend that externalizes storage transparently.

# 8. CI/CD Introduction

**Solution.**

```python
# tests/test_dag_integrity.py
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0
```

Deliberately broken DAG file:

```python
# dags/broken_clickstream_dag.py
from airflow.sdk import dag, task
from datetime import datetime
import totally_missing_package  # Intentional import error.

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def broken_clickstream_dag():
    @task
    def run():
        print("Never reached")
    run()

broken_clickstream_dag()
```

**Analysis.** Running `test_no_import_errors` against a DAGs folder containing this file fails, and `dag_bag.import_errors` contains one entry mapping the file's path to a `ModuleNotFoundError: No module named 'totally_missing_package'`. `DagBag()` reuses the same import mechanism as the production DAG Processor, so this test catches exactly the class of error that would otherwise cause the DAG to silently disappear from the UI in production, with the only trace being an entry in the Processor's own error log rather than a visible task failure.

**Common mistakes.** Instantiating `DagBag()` without pointing it at the right folder in a test environment, which can make it silently find zero DAGs — the test would then report `0 == 0` and pass even though nothing was actually checked. A passing import test also does not verify that dependencies or business logic are correct; it only proves the file is importable.

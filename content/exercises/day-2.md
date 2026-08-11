---
title: "Day 2 — Exercises"
---

# 1. Trigger Rules

## Task 1 — Alert and cleanup branches

**Task.** Write a DAG `ingest_report` with the following tasks:

* `fetch_source()` — raises an exception to simulate a failed extract.
* `build_summary(data)` — depends on `fetch_source`, uses the default `trigger_rule`.
* `send_alert()` — must run **only** if `fetch_source` failed, and must be skipped if it succeeds.
* `cleanup()` — must run regardless of what happened upstream (success, failure, or skip), and always runs last.

Wire the dependencies so `fetch_source` feeds both `build_summary` and `send_alert`, and `cleanup` runs after both of them.

**Explanation.** This task forces you to select two different `trigger_rule` values for two tasks that both depend on the same failing upstream task, and to reason about the *third* task whose `trigger_rule` must ignore the outcome of both branches entirely.

## Task 2 — Predict the states

**Task.** Given the following DAG, state the final status of `transform` and `notify` without running it, and explain why:

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def state_prediction_demo():

    @task
    def extract():
        return "ok"

    @task
    def transform(data):
        print(f"Transforming {data}")

    @task(trigger_rule="one_failed")
    def notify():
        print("Something failed upstream")

    extract_task = extract()
    transform(extract_task)
    notify_task = notify()
    extract_task >> notify_task

state_prediction_demo()
```

# 2. Data Interval and Catchup

## Task 1 — When does it actually run?

**Task.** A DAG has `schedule="@daily"` and `start_date=datetime(2026, 3, 1)`. It is deployed and enabled on `2026-03-10`. Without specifying `catchup`, answer:

* How many DagRuns exist immediately after enabling the DAG?
* What is the `logical_date` of the DagRun(s) that get created?
* At what physical (wall-clock) time does the DagRun for `logical_date=2026-03-09` actually start?

## Task 2 — Backfill a specific range

**Task.** The same DAG from Task 1 needs historical runs for `2026-03-01` through `2026-03-05` only, without affecting any other date. Describe the change to the DAG definition (if any) and the exact command(s) you would run to produce only those five runs.

# 3. Dynamic DAG Generation

**Task.** The following DAG factory creates one DAG per region by looping over a top-level list:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

REGIONS = ["eu-west", "us-east", "ap-south"]

def sync_region(region_name):
    print(f"Syncing {region_name}")

for region in REGIONS:
    dag_id = f"sync_{region}"
    dag = DAG(dag_id=dag_id, start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False)
    with dag:
        PythonOperator(
            task_id="sync_region",
            python_callable=sync_region,
            op_kwargs={"region_name": region},
        )
    globals()[dag_id] = dag
```

Rewrite it as a single TaskFlow DAG `sync_regions` that uses dynamic task mapping, where the list of regions is produced by a task `get_regions()` rather than a module-level constant. Explain, in one or two sentences, what changes in Grid view compared to the original version.

# 4. XCom

## Task 1 — Multiple explicit keys

**Task.** Write a classic (non-TaskFlow) DAG `shipment_status_demo` where `check_shipment(**context)` pushes two XCom values under the keys `carrier` and `eta_days`, and `report_shipment(**context)` pulls both values and prints `"Shipment via {carrier}, arriving in {eta_days} days"`.

## Task 2 — Find the silent bug

**Task.** The following DAG runs without failing, but `notify_customer` always prints `"Order total: None"`. Find the bug and explain why Airflow does not raise an exception.

```python
def calculate_total(**context):
    context["ti"].xcom_push(key="order_total", value=249.99)

def notify_customer(**context):
    total = context["ti"].xcom_pull(task_ids="calculate_total", key="total")
    print(f"Order total: {total}")
```

# 5. Airflow and dbt

**Task.** A team currently runs dbt with a plain cron job at `0 5 * * *` (`dbt run`), hoping that the upstream Postgres → warehouse load has finished by then. This occasionally fails because the load is sometimes late. Redesign this as **one connected pipeline**, evolved in three steps, each solving one specific weakness of the previous one — not three independent snippets:

1. Inside the same DAG, replace the fixed-time cron trigger with something that waits for actual data readiness (assume a `load_status` table with a `status` and `load_date` column), gating the dbt run itself.
2. Replace the single coarse-grained `dbt run` task with per-model task granularity, so that a broken model does not hide the status of the other models, and make sure the specific model your downstream consumer needs (e.g. `fct_sales`) actually gets a queryable Asset published for it.
3. In a separate downstream DAG, trigger a BI-refresh task precisely when that specific dbt model's output table is updated — not on a fixed delay after dbt "probably" finishes, and not by hand-writing a URI you hope matches.

For each step, name the Airflow feature or operator you would use, and explain in one sentence what specific problem it solves that the previous step did not. For step 3, also explain how you would guarantee the Asset your downstream DAG schedules on is the *same* Asset your dbt task group actually publishes.

# 6. Connections and Variables

**Task.** Configure a Postgres `Connection` named `warehouse_pg` (host: `warehouse`, schema: `analytics`, standard port). Then write a TaskFlow task `count_active_customers()` that uses `PostgresHook` to run `SELECT COUNT(*) FROM customers WHERE is_active = true;` and returns the count. Make the query itself configurable: read a Variable named `active_customer_query` inside the task body (not at DAG parse time), falling back to the hardcoded query above if the Variable is not set.

# 7. XCom Size and Best Practices

**Task.** The following DAG accidentally pushes an entire in-memory dataset through XCom:

```python
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def clickstream_pipeline():

    @task
    def extract_events():
        return pd.DataFrame({"user_id": range(500_000), "event": ["click"] * 500_000})

    @task
    def aggregate_events(events: pd.DataFrame):
        print(events["user_id"].nunique())

    aggregate_events(extract_events())

clickstream_pipeline()
```

Fix it so that only a small reference travels through XCom, while the actual data is written to and read from disk. State one additional risk this fix does not solve if the pipeline later moves to `CeleryExecutor` with multiple workers.

# 8. CI/CD Introduction

**Task.** Write a `pytest` test named `test_no_import_errors` that scans the DAGs folder and fails if any file has an import error. Then write a second DAG file that would deliberately break this test (any kind of import-time error is acceptable), and state what `dag_bag.import_errors` would contain for it.

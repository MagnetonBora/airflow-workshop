---
title: "Day 1 — Exercises"
---

# 1. TaskFlow API

## Basic ETL via TaskFlow

**Task.** Write a DAG `order_report` that processes a list of order amounts through three sequential tasks:

* `extract_orders()` — returns a list of amounts: `[120.5, -15.0, 340.0, 0.0, 87.25, -3.5]` (negative values are erroneous records that ended up in the source due to an export error).
* `clean_orders(orders)` — removes all values `<= 0`, returns the cleaned list. If the list is empty after cleaning, it should fail with a clear exception rather than silently returning an empty list.
* `build_report(orders)` — accepts the cleaned list and returns a dictionary `{"count": ..., "total": ..., "average": ...}`, rounding `average` to 2 decimal places.
* `print_report(report)` — prints the report: `"Orders: N, total: X, average check: Y"`.

All data between tasks must be passed only through return values (TaskFlow), without manual `xcom_push`/`xcom_pull`.

**Explanation.** This is a typical piece of a real ETL: raw data from a source is almost never perfect, and the first step of a pipeline is almost always filtering out garbage with an explicit stop if nothing valid remains after filtering. The task teaches you to build a TaskFlow chain so that checks are placed exactly where the data actually becomes invalid, rather than where the failure simply "happens" later in the graph.

## Conditional execution and task branching

Real pipelines are rarely linear. Sometimes you need to **choose one of several alternative branches** (branching), and sometimes you need to **decide whether it makes sense to continue** execution at all (short-circuiting). These are different mechanisms, and confusing them is a common source of DAGs that either fail to run the required task or, conversely, unnecessarily run extra work.

The same scenario is used for all four approaches below so that it is clear how they differ with the same input data:

* `extract_orders()` — receives a list of orders;
* the decision is then made differently depending on the approach;
* if data exists — `build_report()` is executed;
* if there is no data — `notify_no_data()` is executed (only where this is actually needed — see below why this branch may not exist at all in the short-circuit scenario);
* after branching — a common task `finish()`, to demonstrate the effect of `trigger_rule`.

**Approach 1 — `BranchPythonOperator` (classic operator)**

**Task.** Build a DAG where, after `extract_orders`, the `check_orders` task chooses one of two branches: `build_report` if orders exist, or `notify_no_data` if the list is empty. After both branches, the common `finish` task must always run. Use `BranchPythonOperator`.

**Explanation.** `BranchPythonOperator` is a classic (non-TaskFlow) operator: its `python_callable` must return the `task_id` (or a list of `task_id`s) of the task (or tasks) that should execute next. All other immediate downstream tasks automatically receive the `skipped` status.

**Approach 2 — `@task.branch` (TaskFlow)**

**Task.** The same scenario, but entirely in the TaskFlow style, without manual operators and without manual XCom.

**Explanation.** `@task.branch` is the TaskFlow equivalent of `BranchPythonOperator`: the decorated function must return the `task_id` (or a list of `task_id`s) of the task that should execute. The `skipped` semantics for the other branches are identical to the classic operator — the only difference is how the DAG is declared.

**Approach 3 — `ShortCircuitOperator`**

**Task.** There is a separate scenario: there is no need to choose one of two branches — you need to decide **whether it makes sense to continue the pipeline at all**. If no orders were received today, the entire downstream pipeline (`build_report` → `send_summary`) is unnecessary, and there is no alternative branch.

**Approach 4 — `@task.skip_if` and `@task.run_if`**

**Task.** Write a DAG `order_report_conditional_task` in which the `extract_orders()` task returns a list of order amounts `[120.5, 340.0]`, and the `build_report(orders)` task builds and prints a report for these orders, but is executed only if the Variable `reports_enabled` is set to `"true"` (by default, if the Variable is not set, reports should be considered enabled). If the condition is not met, `build_report` should receive the `skipped` status, without creating a separate alternative branch and without stopping other DAG tasks. The condition must be implemented either via `@task.run_if` or via a logically equivalent `@task.skip_if`.

**Explanation.** `skip_if` and `run_if` solve a narrower and more targeted problem than branching or short-circuiting: they do not create a fork in the graph and do not control the fate of other tasks — they simply decide whether *this one* task will execute or receive `skipped`, while remaining a regular node in the DAG with normal `>>` dependencies. The difference between them is purely the direction of the condition: `run_if(condition)` — the task executes **only if** `condition` is true (otherwise `skipped`); `skip_if(condition)` — the task is **skipped** if `condition` is true (otherwise it executes).

**Solution:**

```python
from airflow.sdk import dag, task, Variable
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def order_report_conditional_task():

    @task
    def extract_orders():
        return [120.5, 340.0]

    @task.run_if(lambda context: Variable.get("reports_enabled", default="true") == "true")
    @task
    def build_report(orders: list):
        print(f"Report for {len(orders)} orders has been built")

    build_report(extract_orders())

order_report_conditional_task()
```

Equivalent version using `skip_if` (logically inverted, but producing the same result for the same Variable value):

```python
    @task.skip_if(lambda context: Variable.get("reports_enabled", default="true") != "true")
    @task
    def build_report(orders: list):
        print(f"Report for {len(orders)} orders has been built")
```

**What happens here.** `run_if`/`skip_if` wrap an already-declared `@task` function (note the order of decorators: `@task.run_if(...)` goes *above* `@task`, not the other way around) and determine the fate of this particular task when it runs, without changing the graph structure — downstream/upstream relationships remain exactly the same as if there were no condition at all.

**Why this matters.** If the condition applies to one task ("whether to send the report if reports are currently disabled by configuration") — `run_if`/`skip_if` are shorter and clearer than creating a separate branch task for a single node. But as soon as the condition needs to control the fate of *multiple* tasks simultaneously or choose between meaningful alternatives — you need branching again (approaches 1–2) or short-circuiting (approach 3), rather than putting `run_if`/`skip_if` on each task individually.

**Typical mistake.** Trying to use `skip_if`/`run_if` instead of branching where an alternative branch is actually needed (for example, putting `run_if` on `build_report` and a separate `run_if` with the opposite condition on `notify_no_data`). Formally, this may work, but it requires duplicating the condition in two places and can become inconsistent whenever the logic changes — whereas `@task.branch`/`BranchPythonOperator` calculate the decision once, in one place.

# 2. Hooks

**Task 1.** Modify `order_report` from exercise 1: instead of hardcoding, `extract_orders()` should retrieve data from Postgres using `PostgresHook`. The `orders` table contains an `amount` column. The other three tasks (`clean_orders`, `build_report`, `print_report`) remain unchanged.

**Task 2.** Add a fourth task `store_report` that writes the completed report to the Postgres table `order_reports(report_date date, order_count int, total_amount numeric, average_amount numeric)`. `print_report` remains as is and runs in parallel with `store_report` (both accept the same `report` and do not depend on each other).

# 3. Sensors

**Task.** Write two related DAGs: `raw_data_load` with a single task `load_raw_data()`, which for demonstration either succeeds or intentionally fails — depending on the value of the Variable `simulate_load_failure` (`"true"`/`"false"`), — and `build_marts` with a sensor `wait_for_raw_load` that waits for this task from the `raw_data_load` DAG. If `load_raw_data` completes successfully, execution should continue to the `build_marts_task()` task; if it fails, the sensor itself should fail without waiting for the timeout.

# 4. Assets

**Task.** Write two DAGs using a real shared resource — a Postgres table. The `load_orders_status` DAG should write a row to the `orders_status(load_date date, is_loaded boolean)` table (upsert by `load_date`), marking that today's data has been loaded, and declare this operation as the `orders_loaded` Asset. The `notify_orders_ready` DAG should run on `schedule=[orders_loaded]` and, after receiving control, perform its own `SELECT` against the same table to confirm that a row for today's date actually exists and `is_loaded = true`.

# 5. CLI

**Task 1.** Given the `daily_metrics` DAG with `catchup=False`, calculating one metric per day:

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 2, 1), catchup=False)
def daily_metrics():

    @task
    def compute_metric(**context):
        logical_date = context["ds"]
        print(f"Metric for {logical_date}: calculating...")
        return {"date": logical_date, "value": 42}

    @task
    def store_metric(metric: dict):
        print(f"Storing metric {metric}")

    store_metric(compute_metric())

daily_metrics()
```

Using the CLI, test both tasks separately for `2026-02-05` via `tasks test`; determine whether historical runs for past days will appear automatically when the DAG is enabled today; manually rerun the range `2026-02-03`–`2026-02-05` without touching the other dates.

**Task 2. CLI diagnostics without opening the UI.** In the `dags/` folder, create a file containing a DAG with an explicit error. Using only the terminal (without a browser):

* make sure that the DAG is indeed not recognized because of an import error, rather than simply not having been scanned yet;
* see the text of the actual error;
* after fixing it — make sure that the DAG has appeared and its tasks are visible.

**Task 3. Status of a specific run via CLI.** For the `daily_metrics` DAG from task 1, without opening Grid view, use the terminal to find out: which runs exist for the last week and what their statuses are, and what status each specific task has within the run for `2026-02-05`.

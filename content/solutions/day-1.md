---
title: "Day 1 — Full Breakdown"
---

# 1. TaskFlow API

## Basic ETL via TaskFlow

**Task.** Write a DAG `order_report` that processes a list of order amounts through three sequential tasks:

* `extract_orders()` — returns a list of amounts: `[120.5, -15.0, 340.0, 0.0, 87.25, -3.5]` (negative values are erroneous records that ended up in the source due to an export issue).
* `clean_orders(orders)` — removes all values `<= 0`, returns the cleaned list. If the list is empty after cleaning, it should fail with a clear exception rather than silently return an empty list.
* `build_report(orders)` — accepts the cleaned list and returns a dictionary `{"count": ..., "total": ..., "average": ...}`, rounding `average` to 2 decimal places.
* `print_report(report)` — prints the report: `"Orders: N, total: X, average check: Y"`.

All data between tasks must be passed only through return values (TaskFlow), without manual `xcom_push`/`xcom_pull`.

**Explanation.** This is a typical piece of real-world ETL: raw data from a source is almost never perfect, and the first step of the pipeline is almost always filtering out garbage with an explicit stop if nothing valid remains after filtering. The task teaches you to build a TaskFlow chain so that checks are placed exactly where the data is actually corrupted, rather than where the failure simply "happens" later in the graph.

**Solution:**

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def order_report():

    @task
    def extract_orders():
        return [120.5, -15.0, 340.0, 0.0, 87.25, -3.5]

    @task
    def clean_orders(orders: list):
        cleaned = [o for o in orders if o > 0]
        if not cleaned:
            raise ValueError("No valid orders remained after cleaning")
        return cleaned

    @task
    def build_report(orders: list):
        total = sum(orders)
        return {
            "count": len(orders),
            "total": total,
            "average": round(total / len(orders), 2),
        }

    @task
    def print_report(report: dict):
        print(f"Orders: {report['count']}, total: {report['total']}, average check: {report['average']}")

    print_report(build_report(clean_orders(extract_orders())))

order_report()
```

**Analysis.** Each call arrow (`build_report(clean_orders(...))`) is simultaneously an execution dependency and data transfer through XCom: the return value of `clean_orders` physically goes to the metadata DB and is read from there as the argument to `build_report`. The empty-list check is placed specifically in `clean_orders`, rather than in `build_report` — without it, `build_report` would fail with `ZeroDivisionError` when dividing by `len(orders) == 0`, and you would have to troubleshoot not at the place where the real cause occurred (bad source data), but where it merely manifested.

**Typical mistake.** Beginners often try to call `xcom_push` manually inside a TaskFlow task *and* return a value through `return` at the same time — this creates two different XComs (one under the `return_value` key, another under an explicit key), and the code that reads the data downstream becomes confusing about where exactly to get the value from. If the task is TaskFlow, data should be passed only through `return`/arguments — manual XCom is needed only when one task must return several independent named values, and even then `@task(multiple_outputs=True)` exists for this purpose instead of raw `xcom_push`.

## Conditional execution and task branching

Real pipelines are rarely linear. Sometimes you need to **choose one of several alternative branches** (branching), and sometimes you need to **decide whether to continue at all** (short-circuiting). These are different mechanisms, and confusing them is a common source of DAGs that either fail to run the required task or unnecessarily run extra work.

The same scenario is used for all four approaches below so that the differences are visible with identical input data:

* `extract_orders()` — gets a list of orders;
* the decision is made differently depending on the approach;
* if data exists — `build_report()` is executed;
* if there is no data — `notify_no_data()` is executed (only where this is actually needed — see below why this branch may not exist at all in the short-circuit scenario);
* after branching — the common task `finish()` is used to demonstrate the effect of `trigger_rule`.

**Approach 1 — `BranchPythonOperator` (classic operator)**

**Task.** Build a DAG where, after `extract_orders`, the `check_orders` task chooses one of two branches: `build_report` if orders exist, or `notify_no_data` if the list is empty. After both branches, the common `finish` task must always run. Use `BranchPythonOperator`.

**Explanation.** `BranchPythonOperator` is a classic (non-TaskFlow) operator: its `python_callable` must return the `task_id` (or a list of `task_id`s) of the task(s) that should execute next. All other immediate downstream tasks automatically receive the `skipped` status. The classic style with manual `xcom_push`/`xcom_pull` is intentionally shown here so that the contrast with the TaskFlow version below is visible — in practice, new DAGs more often use `@task.branch` (Approach 2), but `BranchPythonOperator` is still relevant and frequently found in existing codebases.

**Solution:**

```python
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from datetime import datetime


def _extract_orders(**context):
    orders = [120.5, 340.0, 87.25]
    context["ti"].xcom_push(key="orders", value=orders)


def _check_orders(**context):
    orders = context["ti"].xcom_pull(key="orders", task_ids="extract_orders")
    return "build_report" if orders else "notify_no_data"


def _build_report(**context):
    orders = context["ti"].xcom_pull(key="orders", task_ids="extract_orders")
    print(f"Report for {len(orders)} orders has been built")


def _notify_no_data():
    print("No data — sending notification")


def _finish():
    print("Pipeline finished")


with DAG(
    dag_id="order_report_branch_classic",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    extract_orders = PythonOperator(task_id="extract_orders", python_callable=_extract_orders)
    check_orders = BranchPythonOperator(task_id="check_orders", python_callable=_check_orders)
    build_report = PythonOperator(task_id="build_report", python_callable=_build_report)
    notify_no_data = PythonOperator(task_id="notify_no_data", python_callable=_notify_no_data)
    # Without a suitable trigger_rule, finish will not run — see "Typical mistake" below
    finish = PythonOperator(
        task_id="finish",
        python_callable=_finish,
        trigger_rule="none_failed_min_one_success",
    )

    extract_orders >> check_orders >> [build_report, notify_no_data] >> finish
```

**What happens here.** `check_orders` always executes (it is a regular task with a regular dependency on `extract_orders`). It returns a **string** matching the `task_id` of exactly one of the following tasks. Airflow compares this value with the `task_id` of all immediate downstream tasks of `check_orders`: the matching task is allowed to execute normally according to its schedule; all others are immediately marked `skipped` without even being started on a worker.

**Why this matters.** A skipped task is not an error or "unfinished" work: it is a deliberate architectural decision that "this branch is not needed right now." That is why `skipped` is not considered a DAG failure. However, this also creates a problem for any task further down the graph that depends on both branches — see the next section.

**Typical mistake.**

```python
check_orders >> [build_report, notify_no_data]
build_report >> finish
notify_no_data >> finish
```

If `finish` is created with the default interpretation (`trigger_rule="all_success"`), it **will not run**. The `all_success` rule requires *all* immediate upstream tasks to finish with `success`. But exactly one of `[build_report, notify_no_data]` will always be `skipped` — by definition of branching. As a result, `finish` will also receive `skipped` (due to cascading propagation of `skipped` through `all_success`) instead of running as the DAG author expected.

The correct solution is to set a `trigger_rule` that explicitly allows some upstream tasks to be `skipped`, for example `none_failed_min_one_success`: "no upstream task finished as `failed`/`upstream_failed`, and at least one finished as `success`." This combination correctly describes the situation "exactly one branch out of the branching operation succeeded."

**Approach 2 — `@task.branch` (TaskFlow)**

**Task.** The same scenario, but entirely in TaskFlow style, without manual operators and without manual XCom.

**Explanation.** `@task.branch` is the TaskFlow equivalent of `BranchPythonOperator`: the decorated function must return the `task_id` (or a list of `task_id`s) of the task that should execute. The `skipped` semantics for the other branches are identical to those of the classic operator — the only difference is how the DAG is declared.

**Solution:**

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def order_report_branch_taskflow():

    @task
    def extract_orders():
        return [120.5, 340.0, 87.25]

    @task.branch
    def check_orders(orders: list):
        return "build_report" if orders else "notify_no_data"

    @task
    def build_report(orders: list):
        print(f"Report for {len(orders)} orders has been built")

    @task
    def notify_no_data():
        print("No data — sending notification")

    @task(trigger_rule="none_failed_min_one_success")
    def finish():
        print("Pipeline finished")

    orders = extract_orders()
    branch = check_orders(orders)
    report = build_report(orders)
    empty = notify_no_data()

    branch >> [report, empty]
    [report, empty] >> finish()

order_report_branch_taskflow()
```

**What happens here.** Notice: `build_report(orders)` receives data directly from `extract_orders` through XCom (the call arrow), while the *execution* dependency on the `check_orders` decision is specified separately via the explicit `branch >> [report, empty]`. This is intentional: TaskFlow cannot infer that `check_orders` should control the execution of `build_report` — calling `build_report(orders)` creates only a *data dependency* on `extract_orders`, not an execution dependency on the branch task. The control dependency must always be added explicitly using `>>`.

**Why this matters.** This is exactly where beginners most often make a mistake in the TaskFlow branching version: they think that because `check_orders` is logically "connected" to `build_report`, the `>>` operator for controlling its execution is unnecessary. Without the explicit `branch >> [report, empty]`, `build_report` will run regardless of the `check_orders` decision, and branching simply will not work — both paths will execute every time.

**Typical mistake.** Omitting `branch >> [report, empty]` and relying only on the fact that `build_report` receives `orders` as an argument. The result is that `build_report` and `notify_no_data` both execute every time, regardless of whether data exists; branching is visually present in the graph, but does not actually have any effect.

**Approach 3 — `ShortCircuitOperator`**

**Task.** There is a different scenario: you do not need to choose between two branches — you need to decide **whether it makes sense to continue the pipeline at all**. If no orders arrived today, the entire remaining pipeline (`build_report` → `send_summary`) is unnecessary, and there is no alternative branch.

**Explanation.** This is fundamentally different from branching. Branching answers the question "which of several roads should be chosen" — there are always at least two meaningful outcomes. `ShortCircuitOperator` answers the question "should we continue or not" — there is no alternative branch, only "yes, the entire tail of the pipeline executes as usual" or "no, the entire tail is skipped." That is why `ShortCircuitOperator`, unlike `BranchPythonOperator`, does not choose *between* downstream tasks, but simply allows or prevents *all* direct (and by default — all subsequent) downstream tasks from executing.

**Solution:**

```python
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.sdk import DAG
from datetime import datetime


def _extract_orders(**context):
    orders = []  # assume no orders arrived today
    context["ti"].xcom_push(key="orders", value=orders)


def _has_orders(**context) -> bool:
    orders = context["ti"].xcom_pull(key="orders", task_ids="extract_orders")
    return bool(orders)


def _build_report(**context):
    orders = context["ti"].xcom_pull(key="orders", task_ids="extract_orders")
    print(f"Report for {len(orders)} orders has been built")


def _send_summary():
    print("Summary sent")


with DAG(
    dag_id="order_report_short_circuit",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    extract_orders = PythonOperator(task_id="extract_orders", python_callable=_extract_orders)
    has_orders = ShortCircuitOperator(task_id="has_orders", python_callable=_has_orders)
    build_report = PythonOperator(task_id="build_report", python_callable=_build_report)
    send_summary = PythonOperator(task_id="send_summary", python_callable=_send_summary)

    extract_orders >> has_orders >> build_report >> send_summary
```

**What happens here.** If `_has_orders` returns `False`, `ShortCircuitOperator` marks not only its immediate downstream task `build_report` as `skipped`, but by default — the entire tail of the chain, including `send_summary`, even though `send_summary` is not a direct downstream task of `has_orders`. This is controlled by the `ignore_downstream_trigger_rules` parameter, which defaults to `True` — meaning that by default the entire chain is skipped regardless of which `trigger_rule` is set on the tasks further downstream. If `ignore_downstream_trigger_rules=False`, the skipping stops at the first level, and deeper tasks behave according to their own `trigger_rule`.

**Why this matters.** A second branch for "no data" is not needed here precisely because the task is not to choose one of two roads, but to stop the entire pipeline tail with a single command. If a real-world scenario does require explicitly notifying about missing data, a separate task with `trigger_rule="all_done"` or `"one_failed"` can be added — effectively turning part of the graph back into something resembling branching.

**Typical mistake.** Using `ShortCircuitOperator` where what is actually needed is a choice between two meaningful alternatives (as in Approaches 1–2), rather than simply "continue/do not continue." In that case the resulting DAG works, but requires a workaround: you have to create a separate "notification" branch with a manual `trigger_rule`, whereas `BranchPythonOperator`/`@task.branch` solve the problem with a single construct.

**Approach 4 — `@task.skip_if` and `@task.run_if`**

**Important note about currency.** `@task.skip_if` and `@task.run_if` are TaskFlow decorators introduced as part of Airflow 3.x and available in the current stable branch (3.2/3.3). Both decorators apply only to tasks declared through `@task` (TaskFlow) and **do not work** with classic operators such as `PythonOperator` or `SqlExecuteQueryOperator`.

**Task.** Write a DAG `order_report_conditional_task` in which the `extract_orders()` task returns the order amounts `[120.5, 340.0]`, and the `build_report(orders)` task builds and prints a report for those orders, but runs only if the Variable `reports_enabled` is set to `"true"` (by default, if the Variable is not set, reports should be considered enabled). If the condition is not met, `build_report` should receive the `skipped` status, without creating a separate alternative branch and without stopping other DAG tasks. The condition must be implemented either via `@task.run_if` or through a logically equivalent `@task.skip_if`.

**Explanation.** `skip_if` and `run_if` solve a narrower and more targeted problem than branching or short-circuit: they do not create a fork in the graph and do not control the fate of other tasks — they simply decide whether *this one* task will execute or receive `skipped`, while remaining a normal node in the DAG with ordinary `>>` dependencies. The difference between them is purely the direction of the condition: `run_if(condition)` — the task runs **only if** `condition` is true (otherwise `skipped`); `skip_if(condition)` — the task is **skipped** if `condition` is true (otherwise it runs).

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

Equivalent via `skip_if` (logically inverted, but producing the same result for the same Variable value):

```python
    @task.skip_if(lambda context: Variable.get("reports_enabled", default="true") != "true")
    @task
    def build_report(orders: list):
        print(f"Report for {len(orders)} orders has been built")
```

**What happens here.** `run_if`/`skip_if` wrap an already-declared `@task` function (note the decorator order: `@task.run_if(...)` goes *above* `@task`, not the other way around) and decide the fate of specifically this task when it is started, without changing the graph structure — downstream/upstream relationships remain exactly the same as if there were no condition at all.

**Why this matters.** If the condition applies to a single task ("should the report be sent if reports are currently disabled according to the configuration") — `run_if`/`skip_if` are shorter and clearer than creating a separate branch task for a single node. But as soon as the condition must control the fate of *multiple* tasks simultaneously or choose between meaningful alternatives — branching (Approaches 1–2) or short-circuit (Approach 3) is needed again, rather than putting `run_if`/`skip_if` on every task separately.

**Typical mistake.** Trying to use `skip_if`/`run_if` instead of branching when an alternative branch is actually needed (for example, putting `run_if` on `build_report` and a separate `run_if` with the opposite condition on `notify_no_data`). Formally, this may work, but it requires duplicating the condition in two places and will become out of sync with any change in logic — whereas `@task.branch`/`BranchPythonOperator` calculate the decision once, in one place.

#### Comparison of approaches

| Approach                                       | What it solves                                            | Result                                                                                                      | When to use                                                                   |
| ---------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `@task.branch` / `BranchPythonOperator`        | choose one (or several) of multiple alternative branches  | some immediate downstream tasks become `skipped`                                                            | conditional branching between meaningful alternatives                         |
| `@task.short_circuit` / `ShortCircuitOperator` | continue or stop the entire remaining pipeline            | by default the entire downstream chain receives `skipped` (controlled by `ignore_downstream_trigger_rules`) | guard/check before the remaining pipeline when there is no alternative branch |
| `@task.skip_if`                                | skip *one specific* task when a condition is true         | task does not execute, graph does not change                                                                | targeted condition on one task, available only for `@task` tasks              |
| `@task.run_if`                                 | execute *one specific* task only when a condition is true | task executes only when the condition is met, otherwise `skipped`                                           | conditional execution of one task, available only for `@task` tasks           |

> The table intentionally does not have a separate row for classic `ShortCircuitOperator` versus `@task.short_circuit` — the behavior is identical; only the declaration syntax differs, just as in the branch/`@task.branch` pair.

#### Generalization: how conditional execution mechanisms differ

* **Ordinary dependency** **`>>`** — a task executes if execution reaches it and its `trigger_rule` (by default `all_success`) is satisfied. There is no "choice" here.
* **Branching** (`BranchPythonOperator` / `@task.branch`) — choosing one or more branches from several predefined alternatives; the others are explicitly marked `skipped`.
* **Short-circuit** (`ShortCircuitOperator` / `@task.short_circuit`) — a binary decision "continue the entire tail or not," without a meaningful alternative branch.
* **`skip_if`**/**`run_if`** — a targeted condition on one task that does not change the graph structure.
* **`skipped`** — the task was deliberately not started according to DAG logic (branching, short-circuit, `skip_if`, unsatisfied `trigger_rule`). This is not an error.
* **`failed`** — the task was started and failed with an exception (or was forcibly marked as failed).
* **`upstream_failed`** — the task was not started because an upstream task it depends on failed (`failed`), and the standard `trigger_rule` does not allow execution in this situation.
* **`trigger_rule`** — the rule that determines under which combination of upstream task states the current task is allowed to execute at all. `trigger_rule` is precisely the mechanism that reconciles `skipped` states from branching/short-circuit with common tasks such as `finish`.

# 2. Hooks

**Task 1.** Modify `order_report` from Exercise 1: instead of hardcoding, `extract_orders()` should retrieve data from Postgres using `PostgresHook`. The `orders` table contains an `amount` column. The other three tasks (`clean_orders`, `build_report`, `print_report`) remain unchanged.

**Explanation.** Real pipelines almost never start with a hardcoded list — data comes from storage. The task checks that you understand the boundary of responsibility between the Hook (retrieving raw data from an external system) and the rest of the TaskFlow graph (processing that data), and that switching from "hardcoded in code" to "querying a DB" does not require rewriting anything except one task.

**Solution:**

```python
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def order_report():

    @task
    def extract_orders():
        hook = PostgresHook(postgres_conn_id="my_postgres")
        rows = hook.get_records("SELECT amount FROM orders;")
        return [row[0] for row in rows]   # get_records returns a list of tuples — unpack into a flat list

    @task
    def clean_orders(orders: list):
        cleaned = [o for o in orders if o > 0]
        if not cleaned:
            raise ValueError("No valid orders remained after cleaning")
        return cleaned

    @task
    def build_report(orders: list):
        total = sum(orders)
        return {
            "count": len(orders),
            "total": total,
            "average": round(total / len(orders), 2),
        }

    @task
    def print_report(report: dict):
        print(f"Orders: {report['count']}, total: {report['total']}, average check: {report['average']}")

    print_report(build_report(clean_orders(extract_orders())))

order_report()
```

**Analysis.** Only `extract_orders` changed — the rest of the graph did not notice the difference because the contract between tasks (what is passed downstream — a list of numbers) remained unchanged. `hook.get_records()` returns a list of tuples (one tuple per result row), even when the query selects only one column — therefore `row[0]` is needed for each row; otherwise `clean_orders` would receive a list of tuples instead of a list of numbers and the `o > 0` comparison would fail with a type error.

**Task 2.** Add a fourth task `store_report` that writes the completed report to the Postgres table `order_reports(report_date date, order_count int, total_amount numeric, average_amount numeric)`. `print_report` remains unchanged and executes in parallel with `store_report` (both accept the same `report` and do not depend on each other).

**Explanation.** A Hook is used not only for reading but also for writing — the same `PostgresHook`, using a different method (`run` instead of `get_records`). The task checks that you understand that writing the result back to the DB is just as natural as reading the source data, using the same Connection.

**Solution:**

```python
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def order_report():

    @task
    def extract_orders():
        hook = PostgresHook(postgres_conn_id="my_postgres")
        rows = hook.get_records("SELECT amount FROM orders;")
        return [row[0] for row in rows]

    @task
    def clean_orders(orders: list):
        cleaned = [o for o in orders if o > 0]
        if not cleaned:
            raise ValueError("No valid orders remained after cleaning")
        return cleaned

    @task
    def build_report(orders: list, **context):
        total = sum(orders)
        return {
            "report_date": context["ds"],
            "count": len(orders),
            "total": total,
            "average": round(total / len(orders), 2),
        }

    @task
    def print_report(report: dict):
        print(f"Orders: {report['count']}, total: {report['total']}, average check: {report['average']}")

    @task
    def store_report(report: dict):
        hook = PostgresHook(postgres_conn_id="my_postgres")
        hook.run(
            "INSERT INTO order_reports (report_date, order_count, total_amount, average_amount) "
            "VALUES (%s, %s, %s, %s)",
            parameters=(report["report_date"], report["count"], report["total"], report["average"]),
        )

    report = build_report(clean_orders(extract_orders()))
    print_report(report)
    store_report(report)

order_report()
```

**Analysis.** `report` is now computed once and passed to both tasks (`print_report` and `store_report`) — this is the same XCom, read twice by different downstream tasks, rather than two separate computations. `hook.run(sql, parameters=...)` is a parameterized query: values are safely substituted through `%s` placeholders rather than string formatting, which prevents SQL injection and correctly handles types (dates, numbers) without manually converting them to strings.

**Typical mistake.** Accessing `**context` in a TaskFlow task without an explicit need — that is, adding `**kwargs`/`**context` "just in case" to every task. This works, but on every invocation Airflow has to construct the entire execution context even if only one key (`ds`) is actually needed. It is preferable to declare only the required named parameters (for example, `def build_report(orders: list, ds: str)`), or retrieve the context selectively through `get_current_context()` inside the function if it is needed deeper in the call stack.

# 3. Sensors

**Task.** Write two related DAGs: `raw_data_load` with a single task `load_raw_data()`, which for demonstration either completes successfully or intentionally fails depending on the value of the Variable `simulate_load_failure` (`"true"`/`"false"`), and `build_marts` with a sensor `wait_for_raw_load` that waits for this task from the `raw_data_load` DAG. If `load_raw_data` succeeds, execution should continue to `build_marts_task()`; if it fails, the sensor itself should fail without waiting for the timeout.

**Explanation.** In real work, you often need to wait for another pipeline to finish and explicitly react to its failure rather than simply hanging until the timeout. The task checks your ability to configure a sensor so that it distinguishes between "not ready yet" and "the source has failed" — these are different situations requiring different responses.

**Solution:**

`raw_data_load` DAG:

```python
from airflow.sdk import dag, task, Variable
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def raw_data_load():

    @task
    def load_raw_data():
        simulate_failure = Variable.get("simulate_load_failure", default="false")
        if simulate_failure == "true":
            raise RuntimeError("Data source is unavailable")
        print("Raw data successfully loaded")

    load_raw_data()

raw_data_load()
```

`build_marts` DAG:

```python
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def build_marts():

    wait_for_raw_load = ExternalTaskSensor(
        task_id="wait_for_raw_load",
        external_dag_id="raw_data_load",
        external_task_id="load_raw_data",
        allowed_states=["success"],           # the sensor considers the condition satisfied only for these states
        failed_states=["failed", "upstream_failed"],  # for these states, the sensor itself fails instead of continuing to wait
        deferrable=True,
        timeout=60 * 30,
    )

    @task
    def build_marts_task():
        print("Marts built using fresh raw data")

    wait_for_raw_load >> build_marts_task()

build_marts()
```

> **A separate trap with** `Variable.get()`. The legacy `airflow.models.Variable` class (Airflow 2.x and transition paths in Airflow 3) accepts the `default_var` parameter. `airflow.sdk.Variable` — the new class from the Task SDK used in this material — has a different signature: the parameter is called `default`. This is not a typo or two ways of writing the same thing: if you habitually (for example, by copying an old example from the internet or documentation for Airflow 2.x) write `Variable.get("x", default_var="...")` while importing `Variable` with `from airflow.sdk import Variable`, the task will fail with `TypeError: Variable.get() got an unexpected keyword argument 'default_var'` during execution. Check which module `Variable` is actually imported from when you encounter such an error — it almost always indicates that the classic and Task SDK APIs have been mixed.

**Analysis.** The key detail is that `failed_states` makes the sensor actively fail if the external task fails, instead of waiting uselessly for the timeout and then failing anyway, but with the less informative "timeout" error instead of "upstream DAG explicitly failed." To test it live:

```python
airflow variables set simulate_load_failure false
airflow dags trigger raw_data_load
airflow dags trigger build_marts
# build_marts waits for success and builds the marts

airflow variables set simulate_load_failure true
airflow dags trigger raw_data_load
airflow dags trigger build_marts
# build_marts fails on the sensor almost immediately, without waiting for the timeout
```

**Typical mistake.** Leaving `deferrable` at its default in production with a large number of sensors waiting simultaneously. A non-deferrable sensor in `poke` mode occupies a worker slot for the entire waiting period; with `deferrable=True`, waiting is handed over to the Triggerer and the worker slot is released — with a large number of long waits, the difference is significant on real infrastructure.

# 5. Assets

**Task.** Write two DAGs around a real shared resource — a Postgres table. The `load_orders_status` DAG should write a row to the `orders_status(load_date date, is_loaded boolean)` table (upsert by `load_date`), marking that today's data has been loaded, and declare this operation as the `orders_loaded` Asset. The `notify_orders_ready` DAG should run on `schedule=[orders_loaded]` and, once triggered, perform a `SELECT` against the same table to confirm that a row for today's date actually exists and `is_loaded = true`.

**Explanation.** An Asset is not a file or an abstract "checkbox" mechanism, but a logical marker that is updated exactly when the task with `outlets=[...]` successfully completes. Here it is tied to a real table through the already configured `my_postgres` Connection — the same one used in the Hooks exercises — and the result is verified through an actual SQL query rather than by assumption.

**Solution:**

`load_orders_status` DAG:

```python
from airflow.sdk import Asset, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

orders_loaded = Asset("postgres://my_postgres/public/orders_status")

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def load_orders_status():

    @task(outlets=[orders_loaded])
    def mark_orders_loaded(**context):
        hook = PostgresHook(postgres_conn_id="my_postgres")
        hook.run(
            "INSERT INTO orders_status (load_date, is_loaded) VALUES (%s, true) "
            "ON CONFLICT (load_date) DO UPDATE SET is_loaded = true",
            parameters=(context["ds"],),
        )

    mark_orders_loaded()

load_orders_status()
```

`notify_orders_ready` DAG:

```python
from airflow.sdk import Asset, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

orders_loaded = Asset("postgres://my_postgres/public/orders_status")

@dag(schedule=[orders_loaded], catchup=False)
def notify_orders_ready():

    @task
    def confirm_and_notify(**context):
        hook = PostgresHook(postgres_conn_id="my_postgres")
        row = hook.get_first(
            "SELECT is_loaded FROM orders_status WHERE load_date = %s",
            parameters=(context["ds"],),
        )
        if row and row[0]:
            print("Today's data confirmed in orders_status — notifying consumers")
        else:
            raise ValueError("Asset triggered, but the row was not found in orders_status — state mismatch")

    confirm_and_notify()

notify_orders_ready()
```

**Analysis.** `Asset("postgres://my_postgres/public/orders_status")` in both files is the same identifier string through which Airflow connects the producer's `outlets` and the consumer's `schedule`; the Asset itself does not read or check the database — only the code inside the tasks does that. That is why `confirm_and_notify` does not simply print "ready", but performs the `SELECT` itself: the Asset guarantees only that the producer task completed successfully, not that the data it wrote passed any additional validation — that check must be performed explicitly if it matters.

> For simpler scenarios (when the producer is essentially one task whose only purpose is to update an Asset), Airflow 3.x also provides the `@asset` decorator, which declares both the Asset and its producing task in a single construct. It is intentionally not used here: `mark_orders_loaded` is a regular business task with an Asset as a side effect, rather than a task that exists solely for the Asset, and explicit `@task(outlets=[...])` better demonstrates this boundary.

Test it live:

```bash
docker compose exec airflow-worker airflow dags trigger load_orders_status
# then in the UI: the notify_orders_ready DAG should start automatically, without a manual trigger

docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM orders_status;"
```

**Typical mistake.** Getting the Asset string out of sync between the producer and consumer DAGs — for example, a typo in the URI (`orders_status` vs `order_status`) in one of the two files. Airflow will not report an import error: both DAGs will parse successfully, but `notify_orders_ready` will never run on schedule because from Airflow's perspective these are two *different* Assets. This kind of error is almost impossible to diagnose through `list-import-errors` (see the next section) — it manifests only as "the DAG does not start," and you need to look for it by comparing the Asset strings between the files.

# 5. CLI

**Task 1:** `tasks test` **+** `backfill`. Given a `daily_metrics` DAG with `catchup=False`, calculating one metric per day:

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

Need to: test both tasks separately for `2026-02-05` via `tasks test`; determine whether historical runs for past days will appear automatically when the DAG is enabled today; manually recalculate the range `2026-02-03`–`2026-02-05` without touching other dates.

**Explanation.** `catchup` and `backfill` are two different mechanisms that are easy to confuse: `catchup` acts once, when a DAG is (re)enabled, relative to the entire history starting from `start_date`; `backfill` is a targeted command for a specific range, invoked manually at any time.

**Solution:**

```bash
airflow tasks test daily_metrics compute_metric 2026-02-05
airflow tasks test daily_metrics store_metric 2026-02-05
```

Both execute locally, without writing state to the metadata DB. Historical runs for past days will **not** appear automatically — `catchup=False` means that when the DAG is enabled, a DagRun will be created only for the latest interval that has not yet been processed. Targeted recalculation of the range:

```bash
airflow dags backfill daily_metrics --start-date 2026-02-03 --end-date 2026-02-05
```

**Analysis.** `backfill` creates (or recreates) DagRuns specifically for the specified range without touching other dates — unlike `catchup`, which acts globally when the DAG is created or does not act at all. Note that in Airflow 3.x, `catchup_by_default` in the configuration defaults to `False` (in Airflow 2.x the default was `True`) — this does not matter in the example because `catchup=False` is explicitly set on the DAG, but if you are migrating old training materials from Airflow 2.x, a DAG without an explicit `catchup` setting could behave differently there.

**Typical mistake.** Confusing `tasks test` with a normal task run: `tasks test` does not write state to the metadata DB and does not account for some mechanisms (for example, retries behave differently, and dependencies on other DagRuns through sensors are not actually checked in the same way as during a real run). This is a tool for quickly testing the logic of the function locally, not for fully running the DAG — relying on `tasks test` as confirmation that the DAG will "definitely work in production" is not valid.

**Task 2. CLI-based diagnosis without opening the UI.** In the `dags/` folder, create a file containing a DAG with an explicit error. Using only the terminal (without a browser):

* make sure that the DAG was actually not recognized because of an import error, rather than simply not having been scanned yet;
* see the actual error text;
* after fixing it — make sure that the DAG appears and its tasks are visible.

**Explanation.** In real work (especially with remote access to a server or in CI), it is often not possible to open the UI immediately — being able to diagnose a problem using a single terminal saves real time.

**Solution:**

```bash
airflow dags list-import-errors
```

This shows the file path and the error text (for example, `ModuleNotFoundError` or `SyntaxError`). After fixing the file:

```bash
airflow dags list | grep <dag_id>
airflow tasks list <dag_id>
```

**Analysis.** `dags list-import-errors` reads the same scan result produced by the DAG Processor — if the DAG does not appear in `dags list`, but is also not shown in `list-import-errors`, then the problem is not in the file itself, but in the fact that scanning has not happened yet or the file is not being searched in the expected location (for example, the file is not in the folder Airflow scans) — this is a separate diagnostic path that cannot be reduced to an import error.

**Typical mistake.** Seeing empty output from `dags list-import-errors` and concluding "the file is fine," when the real reason the DAG is missing is that the file is outside `dags_folder`, is excluded by `.airflowignore`, or the DAG Processor's scanning interval has not yet occurred. The absence of import errors does not mean that the file was scanned at all.

**Task 3: status of a specific run via CLI.** For the `daily_metrics` DAG from Task 1, without opening Grid view, use the terminal to find out: which runs exist over the last week and what their statuses are, and what status each specific task has inside the run for `2026-02-05`.

**Explanation.** This is useful for scripted automation (for example, alerting or reports collected outside the UI) and for quickly checking status without switching to a browser.

**Solution:**

```bash
airflow dags list-runs -d daily_metrics
```

This shows a list of DagRuns with dates and statuses. To see the state of tasks inside a specific run:

```bash
airflow tasks states-for-dag-run daily_metrics 2026-02-05
```

This outputs each task in that run together with its current status.

**Analysis.** `list-runs` operates at the DagRun level (the entire run), while `states-for-dag-run` operates at the level of individual TaskInstances within one specific run; together they provide the same picture as Grid view, but in text form suitable for scripts. Both support the `--output` flag (`table`/`simple`/`json`/`yaml`, etc.) for machine-readable output in scripts.

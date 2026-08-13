---
title: "Airflow 3 — A Complete Guide to Testing DAGs and Tasks"
---

## Intro

When testing code in Airflow, it's important from the very start to separate the levels of verification. Mixing levels is the main reason tests either verify nothing or become unbearably slow.

```mermaid
flowchart TD
    A["Airflow testing"]

    A --> B["DAG structure"]
    A --> C["Task logic"]
    A --> D["DAG flow"]
    A --> E["External systems"]
    A --> F["Serialization"]

    B --> B1["Parsing"]
    B --> B2["Dependencies"]
    B --> B3["Task configuration"]

    C --> C1["Python logic"]
    C --> C2["Airflow execution"]

    D --> D1["Branching"]
    D --> D2["Skipped"]
    D --> D3["Trigger rules"]

    E --> E1["Hooks"]
    E --> E2["Connections"]
    E --> E3["Databases / APIs"]

    F --> F1["SerializedDagModel"]
```

The core principle:

> **You don't need to spin up all of Airflow to test plain Python code. But if you want to verify the behavior of Airflow itself — the test must reach into its execution model.**

## Setup and environment preparation

Before writing tests, you need to assemble the environment. Below is the complete set of things you'll actually need throughout this guide, mapped to the relevant sections.

### Base environment

```bash
python3 -m venv .venv
source .venv/bin/activate

export AIRFLOW_HOME=$(pwd)/airflow_home
```

Airflow must be installed **strictly via a constraints file** matching your Python version — otherwise you can easily run into dependency version conflicts:

```bash
AIRFLOW_VERSION=3.0.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

### Providers (needed for sections 7–10, working with Hooks)

Providers are installed separately from the Airflow core:

```bash
pip install apache-airflow-providers-postgres
```

If your project uses other Hooks/Operators (HTTP, S3, BigQuery, etc.), install the corresponding provider package, e.g. `apache-airflow-providers-http`, `apache-airflow-providers-amazon`.

### Testing tools

```bash
pip install pytest pytest-mock
```

- `pytest` — the test runner itself, used throughout all sections.
- `pytest-mock` — the `mocker` fixture on top of `unittest.mock`, used in sections 7.2 and 9 (it automatically rolls back patches after the test, without a manual `with patch(...)`).

### Docker for integration tests (needed for section 7.3, a lightweight integration test against a real DB)

For tests where the Hook actually needs to reach a test (not production) database, spin up a container instead of mocking the connection:

```bash
docker run -d \
  --name test-postgres \
  -e POSTGRES_USER=test_user \
  -e POSTGRES_PASSWORD=test_pass \
  -e POSTGRES_DB=test_db \
  -p 5433:5432 \
  postgres:16
```

This container is what `PostgresHook` will actually connect to in section 7.3 once `AIRFLOW_CONN_MY_POSTGRES` is overridden.

### An important note about a similarly-named package

There's a separate package on PyPI called `pytest-airflow` (a Flowminder project). **That's not what's used in this guide** — it does exactly the opposite thing: it turns pytest tests into a DAG and runs them as Airflow tasks (useful for CI embedded directly into the Airflow cluster itself). Everything described below (the `dag_maker` wrapper, `dag.test()`, fixtures in `conftest.py`) is plain pytest on top of Airflow-as-a-library — you don't need to install a separate package for it: everything needed already comes with `apache-airflow` and `pytest`/`pytest-mock` from items 0.1 and 0.3. Don't confuse the two when searching PyPI.

### A minimal `requirements-test.txt`

To avoid reassembling the commands every time, it's convenient to keep the full list of test dependencies in a single file:

```text
apache-airflow==3.0.0
apache-airflow-providers-postgres
pytest
pytest-mock
```

```bash
pip install -r requirements-test.txt --constraint "${CONSTRAINT_URL}"
```

## 1. Testing DAG structure

The cheapest check is simply loading the DAG.

```python
from airflow.models import DagBag


def test_dag_loads():
    dagbag = DagBag(
        dag_folder="dags",
        include_examples=False,
    )

    dag = dagbag.get_dag("my_dag")

    assert dag is not None
    assert len(dagbag.import_errors) == 0
```

This test doesn't execute anything. It answers the question:

> "Can Airflow import my DAG?"

The graph itself can also be checked directly:

```python
def test_dag_structure():
    dagbag = DagBag(
        dag_folder="dags",
        include_examples=False,
    )

    dag = dagbag.get_dag("my_dag")

    assert set(dag.task_ids) == {
        "extract",
        "transform",
        "load",
    }

    assert dag.get_task("extract").downstream_task_ids == {
        "transform"
    }

    assert dag.get_task("transform").downstream_task_ids == {
        "load"
    }
```

This catches, for example, an accidentally removed dependency:

```mermaid
flowchart LR
    A["extract"] --> B["transform"] --> C["load"]
```

turned into:

```mermaid
flowchart LR
    A["extract"]
    B["transform"] --> C["load"]
```

In this case the DAG still imports just fine — `test_dag_loads()` won't notice a thing. So if the graph's structure matters, you specifically need `test_dag_structure()`.

## 2. Testing task logic (unit test)

If a task contains plain Python logic, test it like ordinary Python.

```python
def transform(data):
    return [x * 2 for x in data]
```

```python
def test_transform():
    assert transform([1, 2, 3]) == [2, 4, 6]
```

You don't need to create a `DagRun` and a `TaskInstance` just to check `transform()`.

This is especially convenient when a task is a thin wrapper around a plain function:

```python
def transform(data):
    ...


@task
def transform_task(data):
    return transform(data)
```

That gives you a clean separation:

```mermaid
flowchart LR
    TEST["Unit test"] --> PYTHON["Python function"]

    DAG["Airflow DAG"] --> TASK["@task"]
    TASK --> PYTHON
```

The rule:

> **Test business logic as Python. Test Airflow as Airflow.**

## 3. Testing a single task (task test)

Sometimes you need to check how a task behaves specifically inside Airflow: context, `ti`, XCom, templated fields, callbacks, retries, TaskInstance parameters.

Simply calling the function is no longer enough here. There's a CLI command for this:

```bash
airflow tasks test my_dag my_task 2026-08-13
```

> ⚠️ Starting with Airflow 2.2+ and even more so in Airflow 3, the date argument is treated as `logical_date`, not `execution_date`. The `execution_date` parameter in the API and context objects is considered deprecated — use `logical_date` in all new tests and code. If you see `execution_date` in old code or in someone else's examples, that's a sign the example was written for Airflow 1.x/early 2.x and should be adapted.

```mermaid
flowchart LR
    UNIT["Unit test"]
    TASKTEST["Task test"]
    DAGTEST["DAG test"]

    UNIT -->|"Python logic"| CODE["Python code"]
    TASKTEST -->|"Airflow execution"| TI["TaskInstance"]
    DAGTEST -->|"Whole flow"| DAG["DAG"]
```

> **A unit test answers: "Does my code work correctly?"**
> **A task test answers: "Does Airflow execute this task correctly?"**

## 4. Testing an entire DAG

### 4.1. `dag.test()`

```python
if __name__ == "__main__":
    dag.test()
```

Runs the DAG locally and executes the tasks sequentially.

An important nuance:

> **One DAG run does not mean the entire flow has been tested.**

If the DAG has branching, a single run only goes through one branch.

```mermaid
flowchart TD
    START["start"]
    BRANCH{"branch"}
    A["process_orders"]
    B["process_refunds"]
    END["finish"]

    START --> BRANCH
    BRANCH -->|"orders"| A
    BRANCH -->|"refunds"| B
    A --> END
    B --> END
```

If the `process_orders` branch got selected, the `process_refunds` scenario was never checked at all. That's why a DAG with branching requires separate flow tests (see section 6).

### 4.2. `dag_maker` — a fixture for flexible flow tests

`dag.test()` is convenient for a quick local run, but it's not a great fit for pytest suites: it doesn't give you convenient control over `logical_date`, run config, or isolation between tests. You don't need to install anything extra — everything below works on top of the already-installed `apache-airflow` and `pytest` from section 0. For this, the Airflow community uses the `dag_maker` fixture pattern (originally part of Airflow's own internal test suite, `tests/conftest.py`; the signature may vary slightly between versions, so here's a safe variant that you fully control yourself, without depending on Airflow's internal private API):

```python
# conftest.py
import pytest
from airflow.models import DagBag
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils import timezone


@pytest.fixture
def dag():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    return dagbag.get_dag("my_dag")


@pytest.fixture
def run_dag(dag):
    """A simple wrapper around dag.test() with a fixed logical_date."""

    def _run(run_conf=None):
        logical_date = timezone.datetime(2026, 8, 13)
        return dag.test(
            execution_date=logical_date,  # a legacy parameter of dag.test(), but semantically it's logical_date
            run_conf=run_conf or {},
        )

    return _run
```

```python
def test_orders_flow(run_dag):
    dag_run = run_dag(run_conf={"value": 10})

    assert dag_run.get_task_instance("process_orders").state == "success"
    assert dag_run.get_task_instance("process_refunds").state == "skipped"
```

> If your team already uses the official `dag_maker` from Airflow's internal tests (via `pytest-airflow` or a `conftest.py` copied from the `apache/airflow` repo), you can rely on it — it offers finer-grained control (creating a `DagRun` without a full `dag.test()`, manually advancing states). But keep in mind: this is a private API of the Airflow project itself, not a publicly documented contract — it can change between minor versions. The wrapper above is a more stable option for an external project.

## 5. Testing branching

```python
@task.branch
def choose(value):
    if value > 0:
        return "process_orders"

    return "process_refunds"
```

Here, **two different things** are being checked.

### 5.1. Testing the decision itself (unit test)

```python
def test_choose_orders():
    assert choose(10) == "process_orders"


def test_choose_refunds():
    assert choose(-10) == "process_refunds"
```

More compact and idiomatic — via `pytest.mark.parametrize` parametrization, instead of duplicating nearly identical functions:

```python
import pytest


@pytest.mark.parametrize(
    "value, expected_branch",
    [
        (10, "process_orders"),
        (1, "process_orders"),
        (-10, "process_refunds"),
        (-1, "process_refunds"),
        (0, "process_refunds"),  # edge case — worth pinning down explicitly
    ],
)
def test_choose(value, expected_branch):
    assert choose(value) == expected_branch
```

This test says nothing about the behavior of Airflow itself: whether the unneeded task becomes `SKIPPED`, whether the chosen one actually executes, whether the `trigger_rule` of a downstream task fires correctly.

### 5.2. Testing the actual flow

```python
def test_orders_branch(dag):
    dag_run = dag.test(
        run_conf={"value": 10},
    )

    assert dag_run.get_task_instance("process_orders").state == "success"
    assert dag_run.get_task_instance("process_refunds").state == "skipped"
    assert dag_run.get_task_instance("finish").state == "success"


def test_refunds_branch(dag):
    dag_run = dag.test(
        run_conf={"value": -10},
    )

    assert dag_run.get_task_instance("process_orders").state == "skipped"
    assert dag_run.get_task_instance("process_refunds").state == "success"
    assert dag_run.get_task_instance("finish").state == "success"
```

The same thing, but parametrized (more compact and easier to extend with new scenarios):

```python
@pytest.mark.parametrize(
    "value, expected_states",
    [
        (10, {"process_orders": "success", "process_refunds": "skipped"}),
        (-10, {"process_orders": "skipped", "process_refunds": "success"}),
    ],
)
def test_branch_flow(dag, value, expected_states):
    dag_run = dag.test(run_conf={"value": value})

    for task_id, expected_state in expected_states.items():
        assert dag_run.get_task_instance(task_id).state == expected_state

    assert dag_run.get_task_instance("finish").state == "success"
```

```mermaid
flowchart LR
    INPUT["Test input"]
    DAG["dag.test()"]
    RUN["DagRun"]
    TI["TaskInstances"]
    ASSERT["State assertions"]

    INPUT --> DAG
    DAG --> RUN
    RUN --> TI
    TI --> ASSERT
```

> **A unit test of the branch function answers: "Did the function pick the right branch?"**
> **A flow test answers: "Did Airflow actually execute the DAG correctly after that choice?"**

## 6. Testing `trigger_rule`

This matters especially where branches converge again:

```mermaid
flowchart TD
    START["start"]
    BRANCH{"branch"}
    A["process_orders"]
    B["process_refunds"]
    JOIN["join"]

    START --> BRANCH
    BRANCH --> A
    BRANCH --> B

    A --> JOIN
    B --> JOIN
```

After branching, one of the upstream tasks will always be `SKIPPED`. So the behavior of `join` depends on its `trigger_rule`:

```python
join = EmptyOperator(
    task_id="join",
    trigger_rule="none_failed_min_one_success",
)
```

```python
def test_join_after_orders_branch(dag):
    dag_run = dag.test(run_conf={"value": 10})

    assert dag_run.get_task_instance("process_orders").state == "success"
    assert dag_run.get_task_instance("process_refunds").state == "skipped"
    assert dag_run.get_task_instance("join").state == "success"


def test_join_after_refunds_branch(dag):
    dag_run = dag.test(run_conf={"value": -10})

    assert dag_run.get_task_instance("process_orders").state == "skipped"
    assert dag_run.get_task_instance("process_refunds").state == "success"
    assert dag_run.get_task_instance("join").state == "success"
```

What's being tested here is specifically **Airflow's orchestration**, not Python code.

## 7. Testing tasks with Hooks

Consider a task that talks to a database:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_users():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    return hook.get_records("SELECT * FROM users")
```

In production, the chain looks like this:

```mermaid
flowchart LR
    TASK["Task"]
    HOOK["PostgresHook"]
    CONN["Airflow Connection"]
    DB["PostgreSQL"]

    TASK --> HOOK
    HOOK --> CONN
    CONN --> DB
```

In a unit test, that last arrow shouldn't exist at all. You don't need a real PostgreSQL instance. What you need to check is: "if the database returned this data, did the task process it correctly?"

### 7.1. Mocking the Hook via `unittest.mock`

```python
from unittest.mock import MagicMock, patch


def test_load_users():
    fake_hook = MagicMock()

    fake_hook.get_records.return_value = [
        (1, "Alice"),
        (2, "Bob"),
    ]

    with patch("my_dag.PostgresHook", return_value=fake_hook):
        result = load_users()

    assert result == [
        (1, "Alice"),
        (2, "Bob"),
    ]
```

### 7.2. The same test via `pytest-mock`

If `pytest-mock` is installed in the project (see section 0.3 — `pip install pytest-mock`), the test comes out a bit cleaner — no `with` context manager is needed, since the `mocker` fixture rolls back the patch itself after the test:

```python
def test_load_users_with_pytest_mock(mocker):
    fake_hook = mocker.MagicMock()
    fake_hook.get_records.return_value = [
        (1, "Alice"),
        (2, "Bob"),
    ]

    mocker.patch("my_dag.PostgresHook", return_value=fake_hook)

    result = load_users()

    assert result == [(1, "Alice"), (2, "Bob")]
```

The real database isn't involved at all:

```mermaid
flowchart LR
    TASK["load_users()"]
    MOCK["Mock PostgresHook"]
    DATA["Fake result"]

    TASK --> MOCK
    MOCK --> DATA
```

This is a good unit test: fast, deterministic, requires no credentials, no database, no network, and can never accidentally touch production.

### 7.3. Alternative: overriding environment variables with a Connection

Sometimes mocking the Hook itself isn't what you need. For example: you have a test (not production) database in Docker for CI, and you want `PostgresHook` to resolve the `conn_id` **without hitting Airflow's metadata database** (which you don't want to spin up in unit tests), while actually connecting to the test DB — meaning this is already closer to an integration test, but a lightweight one, without a full Airflow installation.

Airflow can resolve a Connection not only from its own database, but also from an environment variable of the form `AIRFLOW_CONN_<CONN_ID_IN_UPPERCASE>`, with the value given as a URI. The example below is built around the container from section 0.4 (`localhost:5433`, `test_user`/`test_pass`/`test_db`) — if you spun it up with the command from there, the example will work as-is:

```python
import os
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook


@pytest.fixture
def fake_postgres_connection(monkeypatch):
    # conn_id "my_postgres" -> the AIRFLOW_CONN_MY_POSTGRES variable
    monkeypatch.setenv(
        "AIRFLOW_CONN_MY_POSTGRES",
        "postgres://test_user:test_pass@localhost:5433/test_db",
    )


def test_hook_resolves_test_connection(fake_postgres_connection):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_connection("my_postgres")

    assert conn.host == "localhost"
    assert conn.port == 5433
    assert conn.schema == "test_db"
```

The upsides of this approach:

- you don't need an Airflow metadata DB with a pre-created Connection record;
- switching environments in CI (dev/test/prod) is easy — just change the variable;
- pytest's `monkeypatch.setenv` cleans up the variable after the test itself — no need to remove it manually in a `teardown`.

It's important to understand the boundary here: **this is not a mock, and it doesn't remove the real connection**. If `localhost:5433` in the example above is actually running (say, Postgres in Docker for CI), `get_records()` will genuinely hit that database. This is already a lightweight integration test, not a unit test. If what you need is a genuine unit test that touches no network at all, combine the approach from section 7.1/7.2 (mocking the Hook itself) with this technique only where you're truly testing configuration resolution, not business logic.

The password/special characters in the URI need to be URL-encoded (`urllib.parse.quote_plus`), otherwise Airflow may parse the connection string incorrectly.

## 8. An important detail: where to patch

If `my_dag.py` contains:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
```

then you need to patch:

```python
patch("my_dag.PostgresHook")
```

and not:

```python
patch("airflow.providers.postgres.hooks.postgres.PostgresHook")
```

Why? After `from ... import PostgresHook`, the `my_dag` module holds **its own reference** to the class. It's that reference that `load_users()` uses.

The general Python rule:

> **Mock the object where the code uses it, not where the object is originally defined.**

## 9. You don't have to mock the entire Hook

Sometimes you want more of the Hook's real logic to run, while still avoiding an actual database connection. In that case, the mock goes deeper:

```python
def test_load_users_mock_conn(mocker):
    fake_connection = mocker.MagicMock()
    fake_cursor = mocker.MagicMock()
    fake_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    fake_connection.cursor.return_value = fake_cursor

    mocker.patch(
        "my_dag.PostgresHook.get_conn",
        return_value=fake_connection,
    )

    result = load_users()

    assert result == [(1, "Alice"), (2, "Bob")]
```

```mermaid
flowchart LR
    TASK["Task"]
    HOOK["Real PostgresHook"]
    MOCK["Mock connection"]
    DB["Real PostgreSQL"]

    TASK --> HOOK
    HOOK --> MOCK
    MOCK -.->|"not called"| DB
```

This is a deeper test. But for an ordinary unit test, the following scheme is usually enough:

```text
Task → Mock Hook → fake data
```

## 10. Can you just swap out the Airflow Connection?

You can (see section 7.3 on environment variables, or creating a test Connection via CLI/UI/DB).

But it's important to understand the difference:

> **A Connection defines where the Hook should connect. It does not mean "don't connect at all."**

```mermaid
flowchart LR
    TASK["Task"]
    HOOK["PostgresHook"]
    CONN["Test Connection"]
    DB["Database"]

    TASK --> HOOK
    HOOK --> CONN
    CONN --> DB
```

If `DB` is reachable over the network, the Hook will genuinely try to connect to it. So if the test requirement is something like:

> **"This test must not touch the database at all"**

it's more reliable to mock the Hook itself or a specific external call (sections 7.1–7.2, 9), rather than just swapping in a different Connection.

## 11. DAG serialization — a separate level of verification

In Airflow 2.x+ and in Airflow 3, a DAG is **serialized** before execution and stored in the metadata database as a `SerializedDagModel` — it's the serialized version that the scheduler and UI actually see, not the original Python object.

This leads to a practical problem: `test_dag_loads()` can be green while the DAG still breaks in a real environment, if it uses something that doesn't serialize correctly — for example, a non-standard default object in `default_args`, a lambda in an operator parameter, or a custom class without `__eq__`/serialization support.

You can check serializability like this:

```python
from airflow.serialization.serialized_objects import SerializedDAG


def test_dag_is_serializable(dag):
    serialized = SerializedDAG.to_dict(dag)
    deserialized = SerializedDAG.from_dict(serialized)

    assert deserialized.dag_id == dag.dag_id
    assert set(deserialized.task_ids) == set(dag.task_ids)
```

This is a cheap test (it doesn't spin up a database and doesn't execute tasks), but it catches a whole class of problems that `test_dag_loads()` and `test_dag_structure()` don't see.

```mermaid
flowchart LR
    DAG["DAG (Python object)"]
    SER["Serialization"]
    DB["Airflow metadata DB"]
    SCHED["Scheduler / UI"]

    DAG --> SER --> DB --> SCHED
```

> **Rule:** if a DAG uses something non-standard in its operator parameters (custom objects, closures, complex default values), add a serialization test rather than relying on import alone.

## 12. Organizing tests: `conftest.py` and fixtures

To avoid duplicating DAG loading in every test, fixtures are moved into `conftest.py`:

```python
# conftest.py
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


@pytest.fixture
def dag(dagbag):
    return dagbag.get_dag("my_dag")
```

This gives you:

- `dagbag` loads once for the entire test session (`scope="session"`) — the expensive parsing operation isn't repeated in every test;
- `dag` — a lightweight fixture built on top of it, available across all tests in the package without an explicit import.

A recommended directory structure for a DAG project:

```text
project/
├── dags/
│   └── my_dag.py
└── tests/
    ├── conftest.py
    ├── test_dag_structure.py
    ├── test_transform.py          # unit tests for business logic
    ├── test_branching.py          # flow tests
    └── test_hooks.py              # tests with mocked Hooks
```

## 13. Unit test vs. integration test vs. end-to-end

### Unit test

```mermaid
flowchart LR
    TASK["Task"]
    HOOK["Mock Hook"]
    DATA["Fake data"]

    TASK --> HOOK --> DATA
```

You're testing your own code, with full control over all external dependencies.

### Integration test

```mermaid
flowchart LR
    TASK["Task"]
    HOOK["Real Hook"]
    DB["Test database"]

    TASK --> HOOK --> DB
```

Checks the real interaction of Task + Hook + database — but against test infrastructure (Docker, staging).

### End-to-end test

```mermaid
flowchart LR
    DAG["DAG"]
    AIRFLOW["Airflow"]
    HOOK["Real Hook"]
    INFRA["Real infrastructure"]

    DAG --> AIRFLOW --> HOOK --> INFRA
```

Checks essentially the entire pipeline in an environment close to production.

The higher the level, the more real behavior gets verified, but the more expensive and slower the test.

## 14. Practical strategy — the test pyramid

```mermaid
flowchart TD
    U["Unit tests"]
    S["DAG structure tests"]
    SR["Serialization test"]
    F["Flow tests"]
    I["Integration tests"]
    E["End-to-end tests"]

    U -->|"many"| S
    S -->|"several"| SR
    SR -->|"several"| F
    F -->|"several"| I
    I -->|"few"| E
```

**Unit tests** — business logic, data transformations, branch decisions, functions called from a Task, error handling. External systems are mocked.

**DAG structure tests** — does the DAG import, do all the needed tasks exist, are the dependencies and parameters correct.

**Serialization test** — does the DAG serialize into the form the scheduler will actually see.

**Flow tests** — branching, `SUCCESS`/`FAILED`/`SKIPPED`, `trigger_rule`, downstream task behavior, different scenarios of traversing the graph.

**Integration tests** — real Hooks, Connections, databases, APIs — but against test infrastructure.

**End-to-end** — a handful, the most expensive, run less often (e.g., only in CI before a release).

## 15. Summary — how to pick the right test level

```mermaid
flowchart TD
    A["What do we want to check?"]

    A -->|"Python logic"| B["Unit test"]
    A -->|"DAG graph"| C["Structure test"]
    A -->|"Serializability"| G["Serialization test"]
    A -->|"Task execution"| D["Task test"]
    A -->|"Branches / skipped / trigger_rule"| E["Flow test"]
    A -->|"Hook + DB/API"| F["Integration test"]
```

> **Python logic → test it as Python.**
> **DAG structure → test the graph.**
> **Serialization → test `SerializedDAG`.**
> **Airflow execution → run the Task/DAG through Airflow.**
> **Branching → check different scenarios and TaskInstance states.**
> **External systems → mock the boundary with the external system in unit tests.**

That's why a good test suite for a DAG with branching looks like this:

```text
test_dag_loads()
test_dag_structure()
test_dag_is_serializable()

test_choose()                       # parametrized unit test of the decision function

test_branch_flow()                  # parametrized flow test (orders / refunds)
test_join_after_orders_branch()
test_join_after_refunds_branch()

test_load_users()                   # mocked Hook — unit
test_load_users_mock_conn()         # mock at the connection level — unit
test_hook_resolves_test_connection()# AIRFLOW_CONN_* env var — unit/integration boundary
```

The first three check that the DAG is a correct artifact at all.
The next one checks the business decision of the branches.
The following three check the real flow and the interaction of branching + skipped + trigger_rule.
The last three check the boundary with the external system at different levels of mock depth.

## Cheat sheet

| What you need to check | Tool |
|---|---|
| The DAG imports at all | `DagBag` + `import_errors` |
| Task graph and dependencies are correct | `dag.task_ids`, `downstream_task_ids` |
| The DAG serializes without loss | `SerializedDAG.to_dict/from_dict` |
| A pure Python function | plain `assert`, no Airflow |
| A task with context/XCom/retries | `airflow tasks test <dag_id> <task_id> <logical_date>` |
| Branching and trigger_rule | `dag.test(run_conf=...)` + checking TaskInstance states |
| A task with a Hook, without a real DB | `unittest.mock.patch` / `pytest-mock` on the Hook or `get_conn` |
| A task with a Hook against a test DB | `AIRFLOW_CONN_<ID>` via `monkeypatch.setenv` |
| Several similar scenarios | `pytest.mark.parametrize` |

# Apache Airflow Workshop

A three-day workshop that takes participants from the fundamental concepts of Apache Airflow to production-oriented orchestration practices.

The workshop combines conceptual explanations, code examples, diagrams, exercises, common mistakes, and operational guidance. It is designed to show not only how to write a DAG, but also how Airflow schedules work, moves metadata between tasks, integrates with data tools, and behaves in a production environment.

## Who This Workshop Is For

The material is intended for:

- data engineers and analytics engineers;
- Python developers working with scheduled data pipelines;
- platform engineers supporting Airflow environments;
- technical leads evaluating orchestration patterns;
- teams integrating Airflow with dbt and external data systems.

Basic Python and SQL knowledge is useful. Previous Airflow experience is not required.

## Learning Outcomes

By the end of the workshop, participants should be able to:

- explain the main Airflow components and how they work together;
- create DAGs with TaskFlow, classic operators, dependencies, and schedules;
- reason about logical dates, data intervals, catchup, and trigger rules;
- use XCom, Dynamic Task Mapping, Sensors, Hooks, Connections, and Variables;
- integrate dbt projects at different levels of orchestration granularity;
- diagnose task failures and common DAG-authoring mistakes;
- apply testing, CI/CD, monitoring, security, and scaling practices.

## Presentations

Start from the [workshop presentation index](content/presentation/index.html). It provides access to all three workshop days.

| Day | Presentation | Main Topics |
| --- | --- | --- |
| 1 | [Basic Concepts](content/presentation/airflow_day1/index.html) | Airflow architecture, DAGs, tasks, operators, scheduling, Assets, deployment options, and the UI |
| 2 | [DAGs, dbt, and Integrations](content/presentation/airflow_day2/index.html) | Trigger rules, data intervals, catchup, Dynamic Task Mapping, XCom, dbt, Connections, and task return values |
| 3 | [Production, Diagnostics, and Monitoring](content/presentation/airflow_day3/index.html) | DAG testing, CI/CD, DAG Bundles, troubleshooting, Pools, idempotency, observability, security, and scaling |

## Day 1: Basic Concepts

Day 1 introduces Airflow as an orchestration platform and establishes its core vocabulary. Participants explore the relationship between DAGs, tasks, TaskInstances, DagRuns, the Scheduler, the DAG Processor, workers, and the metadata database.

The presentation also covers TaskFlow, classic operators, dependency definitions, scheduling concepts, Assets, deployment models, and navigation through the Airflow UI.

## Day 2: DAGs, dbt, and Integrations

Day 2 focuses on more advanced DAG behavior and communication between tasks. It explains trigger rules, data intervals, catchup, dynamic DAG patterns, Dynamic Task Mapping, and the practical constraints of XCom.

The dbt section compares several integration levels, from a single BashOperator to model-level orchestration with Cosmos and Asset-driven downstream workflows. The day also covers Connections, Variables, external libraries, explicit task failures, and TaskFlow return values.

## Day 3: Production, Diagnostics, and Monitoring

Day 3 moves from DAG development to operational reliability. It introduces import tests, graph-structure tests, unit tests, CI/CD gates, DAG Bundles, and rollback considerations.

The presentation then covers failure diagnostics, TaskGroups, Pools, idempotent writes, logical time, callbacks, Deadline Alerts, RBAC, Secrets Backends, and the concurrency controls involved in scaling Airflow.

## Presentation Controls

The presentations are static HTML files and do not require a build step.

- Use `Right Arrow` or `Space` to move forward.
- Use `Left Arrow` to move backward.
- Swipe horizontally on a touch device.
- Select a navigation dot to open a specific slide.
- Use a URL hash such as `#s12` to link directly to a slide.

All presentations share the same visual style, navigation controller, and brand assets from `content/presentation/shared/`.

## Credits

The workshop was produced on commission for [NobleProg](https://www.nobleprog.com/).

This workshop website and its learning materials are powered by [Quartz](https://quartz.jzhao.xyz/).

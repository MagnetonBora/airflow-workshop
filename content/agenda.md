---
title: Agenda
---

## Day 1: Apache Airflow Fundamentals

### Part 1: Introduction to Apache Airflow

**09:00–10:00 | 1 hour**

1. Workflow orchestration: what it is
2. Key features and benefits of Apache Airflow
3. Overview of Airflow 2.x and 3.x

**Break: 15 minutes**

### Part 2: Installation and Environment Setup

**10:15–11:15 | 1 hour**

1. Local installation
2. Managed cloud options

**Break: 15 minutes**

### Part 3: Navigating the UI

**11:30–12:30 | 1 hour**

1. The web interface
2. Monitoring DAG runs, tasks, and logs

**Lunch break: 20 minutes**

### Part 4: Architecture and Core Concepts

**12:50–14:20 | 1.5 hours**

1. DAGs, tasks, operators, hooks, and sensors
2. Scheduler, webserver, and worker overview
3. Executors and backends at a glance
4. Airflow 3 architecture in detail

**Break: 10 minutes**

### Part 5: Writing DAGs, Part 1

**14:30–16:00 | 1.5 hours**

1. TaskFlow API
2. Extended hands-on practice with operators, sensors, and hooks
   - Hook examples for different systems
   - Sensor examples and operating modes
   - Assets
3. Essential CLI commands

### Day 1 Outcome

---

## Day 2: DAGs, dbt, Integrations

### Part 1: Writing DAGs — Continuation

**09:00–10:00 | 1 hour**

1. Recap of the previous day
2. Dependencies and scheduling
3. `trigger_rule` — which upstream outcome should start a task
4. Data interval
5. `catchup` in Airflow 3.x
6. Dynamic DAG generation: patterns and trade-offs

**Break: 15 minutes**

### Part 2: XCom in Depth

**10:15–11:15 | 1 hour**

1. Explicit push/pull — the classic style
2. Multiple XCom values from one task (multiple outputs)
3. Where to find XCom in the UI
4. Custom XCom backends (overview)

**Break: 15 minutes**

### Part 3: Connecting to Databases and dbt Integration

**11:30–12:30 | 1 hour**

1. Connections
2. Variables
3. Why connect Airflow and dbt at all
4. ETL with dbt
5. Level 1 — BashOperator
6. Level 2 — Sensor before dbt
7. Level 3 — Cosmos (detailed configuration)

**Lunch break: 20 minutes**

### Part 4: dbt Integration, Continued

**12:50–14:20 | 1.5 hours**

1. Level 4 — dbt Cloud provider
2. Level 5 — Asset-based integration
3. Data quality gate
4. Errors and XCom

**Break: 10 minutes**

### Part 5: Airflow 3 Beyond Python, and the Start of Versioning

**14:30–16:00 | 1.5 hours**

1. A task written in Go
2. Version control and CI/CD (introduction)
3. Versioning: GitDagBundle

### Day 2 Outcome

--- 

## Day 3: Production, Diagnostics, and Monitoring

### Part 1: CI/CD and Deployment (Continued)

**09:00–10:00 | 1 hour**

1. Recap of the previous day
2. Deployment: dev → prod (`requirements.txt`, separate Connections/Variables per environment)
3. GitDagBundle and its relationship to DAG Versioning
4. Rollback: what to do when a broken DAG reaches production

**Break: 15 minutes**

### Part 2: Testing DAGs

**10:15–11:15 | 1 hour**

1. Four levels of testing: import test, structure test, unit tests, `tasks test`
2. Structure test — example
3. Unit tests for business logic (extracting logic out of `@task`)

**Break: 15 minutes**

### Part 3: Diagnostics and Optimization

**11:30–12:30 | 1 hour**

1. Task-Failure Checklist
2. Performance Optimization
3. TaskGroup

**Lunch break: 20 minutes**

### Part 4: Production: Resources and Common Mistakes

**12:50–14:20 | 1.5 hours**

1. Pools
2. Non-idempotent writes
3. Physical time vs. logical time
4. `catchup` defaults — a common source of confusion (2.x vs. 3.x)

**Break: 10 minutes**

### Part 5: Monitoring, Security, and Scaling

**14:30–16:00 | 1.5 hours**

1. Failure/Success callbacks
2. Deadlines (replacing legacy SLAs)
3. RBAC and Secrets backend
4. Scaling: `parallelism`, `max_active_runs`, `worker_concurrency`, Pools

### Course Wrap-up

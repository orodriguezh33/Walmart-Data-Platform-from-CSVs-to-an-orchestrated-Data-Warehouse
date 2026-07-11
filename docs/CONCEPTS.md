# Technical concepts in depth

*[Versión en español](CONCEPTOS.md)*

This document explains, with mechanics and concrete examples, the "why" behind every design decision in this project. The [README](../README.md) mentions them in a single line each; here's the full detail — what problem each pattern solves, what would happen without it, and exactly how it works in this pipeline.

## Table of contents

- [Medallion architecture (bronze → silver → gold)](#medallion-architecture-bronze--silver--gold)
- [The "ghost" database as an agentic database](#the-ghost-database-as-an-agentic-database)
- [CDC instead of extract-and-replace](#cdc-instead-of-extract-and-replace)
- [Incremental models in `silver_t`](#incremental-models-in-silver_t)
- [Metadata-driven OBT with Jinja (`silver_b`)](#metadata-driven-obt-with-jinja-silver_b)
- [`ephemeral` materialization (gold)](#ephemeral-materialization-gold)
- [dbt snapshots for dimensions (SCD2)](#dbt-snapshots-for-dimensions-scd2)
- [Tests after every layer, not just at the end](#tests-after-every-layer-not-just-at-the-end)
- [Airflow's CeleryExecutor](#airflows-celeryexecutor)
- [Secrets kept out of the code](#secrets-kept-out-of-the-code)
- [Isolated S3 vs. CDC for the main flow](#isolated-s3-vs-cdc-for-the-main-flow)

## Medallion architecture (bronze → silver → gold)

**The problem it solves:** if you transform raw data directly into the final business model in a single step, any data quality issue, any requirement change, or any debugging session becomes a nightmare — there's no way to isolate where in the process something broke, or to reprocess just one part.

**The core idea:** split the pipeline into layers, each with a clear responsibility:

- **Bronze** (`sources.yml` → `bronze.orders`, etc.): data exactly as it arrives from the source, with zero transformation. It's the "raw truth" — if something breaks downstream, you can always rebuild from here.
- **Silver** (`silver_t` → `silver_b`): clean, typed, deduplicated, joined data. Not yet modeled for a specific business use case, but already trustworthy.
- **Gold** (`gold/ephemeral`, `snapshots/dim_*`, `gold/fact`): data modeled specifically for analytical consumption — dimensions, facts, STAR schema. What a BI dashboard actually queries.

**Why it matters in practice:** if `dim_products` has a weird value, you can ask "is the problem in `bronze.products` (arrived wrong from the source), in `silver_t.products_t` (cleaning broke), in `obt_b` (the join broke), or in the snapshot itself?" — each layer is an independent checkpoint. Also, if you only need to rebuild `gold` (say, you changed the STAR schema logic), you don't have to re-ingest everything from the source — `silver` is already there, ready to use.

## The "ghost" database as an agentic database

**The contrast:** a "traditional" Postgres database is one you administer yourself — you run `CREATE DATABASE`, `CREATE TABLE`, manage migrations by hand or with a tool like Alembic/Flyway, and if you want a copy to test something, you typically clone the disk volume or do a full `pg_dump`/`pg_restore` (slow, and duplicates storage).

**What an "agentic" database is (Ghost, `db.ghost.build`):** instead of running DDL commands by hand, you give an agent the `walmart_dataset/ddl/walmart_schema.sql` file plus a natural-language instruction, and the agent interprets the DDL, provisions the database, and creates the schema and tables. No one is running `psql` with `CREATE TABLE` line by line — the agent did that translation.

**The concrete advantage: database forking.** This is the most interesting part. In traditional Postgres, "test something without risking the real database" means:
- Spinning up a brand-new instance and reloading all the data, or
- Doing a full `pg_dump` and restoring it into another instance (slow with large datasets, and doubles storage).

With **forking** on an agentic database like Ghost, you can create a full copy of the database almost instantly — conceptually like a `git branch`, but for data: the fork shares state up to the moment of the branch, and from then on changes in the fork don't affect the original instance (or vice versa). This enables things like: testing a risky schema migration, running a test data seed, or experimenting with a new column — all in the fork, and if something goes wrong you just discard it without ever having touched the database that feeds the real CDC pipeline.

## CDC instead of extract-and-replace

**The problem it solves:** how do you bring data from an operational system (Postgres "ghost") into the warehouse (Databricks) without (a) overloading the source and (b) moving more data than necessary on every run?

**The naive alternative — extract-and-replace:** on every run, connect to Postgres and run `SELECT * FROM orders` (the whole table) to overwrite `bronze.orders` in Databricks. With a multi-million-row table, this means:
- A full table scan on the source every time — if "ghost" were a real transactional database serving production traffic (a POS, an ERP), this scan would compete for resources (CPU, I/O, locks) with the actual business operations.
- Transferring 100% of the data over the network, even if only 0.01% changed.

**How CDC (Change Data Capture) works instead:** Postgres already maintains a **write-ahead log (WAL)** internally — a sequential record of every `INSERT`, `UPDATE`, and `DELETE` that happens in the database, originally there to guarantee durability/replication. A CDC mechanism reads that WAL (instead of re-querying the tables) and extracts exactly which rows changed and how, since the last sync point. Only those rows travel into `bronze`.

**The analogy:** extract-and-replace is photocopying the entire book every time someone scribbles a note in the margin. CDC is reading the log of "which pages were modified" and only reprinting those pages.

**Where this lives in the project:** the CDC job (`postgres_to_bronze`) runs as an independent job in Databricks — there's no change-detection code in this repo, because the mechanism is configured on the Databricks side (its CDC ingestion connector for Postgres). What you do see in `orchestrate.py` is the `ingest_cdc` task, which triggers that job via `WorkspaceClient` and polls (every 5s) until it finishes before letting the DAG move forward.

## Incremental models in `silver_t`

**The problem:** once changes have already landed in `bronze` via CDC, how do you build `silver_t.orders_t` (with its extra `processed_at` column) without rebuilding the whole table every time?

**Full refresh (the alternative):** on every run, `TRUNCATE silver_t.orders_t` and rebuild it entirely from `bronze.orders`. If `bronze.orders` has 5 million rows and only 200 changed today, you process 5 million to update 200 — the compute cost scales with the *total* table size, not with what actually changed.

**What a dbt incremental model does (`materialized: incremental`):**

1. The model's SQL has an `{% if is_incremental() %}` block — that Jinja block only activates if the destination table (`silver_t.orders_t`) **already exists**. On the first run it doesn't exist, so dbt does a full build (`CREATE TABLE AS SELECT ...`); from then on, it takes the incremental branch.
2. Inside that block there's a filter like `WHERE updated_timestamp > (SELECT MAX(updated_timestamp) FROM {{ this }})` — translating to "give me only the rows from `bronze.orders` newer than the most recent row I've already processed."
3. With `unique_key = 'order_id'` configured, dbt doesn't do a plain `INSERT` (which would create duplicates if an existing order changed) — it generates a `MERGE` (or the Databricks equivalent: `MERGE INTO ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`). So if order `O456` changed status, the existing row gets updated instead of a duplicate being created.

**Result:** the cost of each run is proportional to *what changed since yesterday*, not to the accumulated size of the history. This is what lets you run this daily without execution time growing out of control as months pass and the table accumulates years of data.

**Relationship to CDC:** it's the same principle applied at different layers of the pipeline — CDC solves the efficiency of *bronze* (Postgres → Databricks), incremental models solve the efficiency of *silver* (bronze → silver_t inside Databricks).

## Metadata-driven OBT with Jinja (`silver_b`)

**The problem:** `obt_b.sql` needs to join (LEFT JOIN) the 6 `silver_t` tables (orders, customers, products, order_items, employees, stores) into a single wide model. Hand-written, that's 6 nearly identical JOIN blocks, each with its own column `SELECT`, alias, and join condition — a lot of repeated code, and adding a seventh source would mean copy-pasting another whole block.

**The metadata-driven pattern:** instead of hand-writing each JOIN's SQL, you define a configuration list in Jinja:

```jinja
{% set configs = [
    {'table': 'orders_t', 'alias': 'o', 'join_condition': '...'},
    {'table': 'customers_t', 'alias': 'c', 'join_condition': '...'},
    ...
] %}
```

And a Jinja loop (`{% for config in configs %}`) generates the JOIN SQL for each entry in that list, at **compile time** (before the SQL ever reaches Databricks — dbt renders the Jinja into plain SQL, and that's what actually executes).

**Why it matters:** adding a seventh source (say, `suppliers_t`) means adding one more entry to the `configs` list — not writing a new SQL block. It's the same "don't repeat yourself" (DRY) principle you'd apply in application code, brought into SQL via dbt's templating layer.

## `ephemeral` materialization (gold)

**The problem:** you want to modularize `models/gold/ephemeral/eph_*.sql` — one file per entity (`eph_products`, `eph_customers`, etc.), each a simple `SELECT` slicing columns from `obt_b` for one specific entity. But you never need to query these models directly — they only exist to feed the next step (the SCD2 snapshots). If you materialize them as `table` or `view`, each one occupies storage/compute in Databricks for something no one queries standalone.

**What `materialized: ephemeral` does:** dbt **never creates any physical table or view** in the warehouse for these models. Instead, at compile time, when another model does `{{ ref('eph_products') }}`, dbt **inlines `eph_products`'s SQL directly as a CTE (`WITH ... AS (...)`)** inside the SQL of the model that references it. The ephemeral model never runs standalone — it literally doesn't exist as an object in Databricks, only as SQL text inserted into another query.

**Why it matters:** you get the organizational benefit of "one file, one responsibility, per entity" (easier to read and maintain than one giant file) **without paying the storage/compute cost** of materializing an intermediate step that only exists to feed the next one. It's code modularity, free at runtime.

**How this shows up in the DAG:** this is why the `gold_ephemeral` task (`dbt run --select gold.ephemeral`) in `orchestrate.py` **resolves** the 5 `eph_*` nodes but **executes 0** — expected behavior, not a bug. dbt validates that they exist and are well-formed, but there's nothing to run against the warehouse because they don't produce their own artifacts; they only get materialized (as a CTE) when the snapshot that references them runs.

## dbt snapshots for dimensions (SCD2)

**The problem:** product `P123` had `category = 'Electronics'` when it sold in January. On March 15th, someone recategorizes it to `'Accessories'`. If `dim_products` only keeps the **current state** (a plain `UPDATE` that overwrites the row), then when you analyze January's sales today, that product shows up as `'Accessories'` — even though in January, when it sold, it was `'Electronics'`. You've rewritten the past, and your historical report is now wrong.

**What a dbt snapshot with `strategy: timestamp` does:** instead of `UPDATE`-ing the existing row when it detects a change, it does this:

1. Compares the current source row (`eph_products`) against the last version it has saved in `dim_products`.
2. If something changed, it **doesn't overwrite** — instead:
   - It closes the old version: sets `dbt_valid_to = 2026-03-15` (the date the change was detected).
   - It inserts a **new row**: `dbt_valid_from = 2026-03-15`, `dbt_valid_to = NULL` (NULL = "this is the currently active version").
3. If nothing changed, it does nothing — that row keeps `dbt_valid_to = NULL`.

`dim_products` ends up with **two rows** for `P123`:

| product_id | category    | dbt_valid_from | dbt_valid_to |
|------------|-------------|-----------------|---------------|
| P123       | Electronics | 2025-01-01      | 2026-03-15    |
| P123       | Accessories | 2026-03-15      | NULL          |

**How it's queried:** a JOIN of `fact_orders` against `dim_products` filtering `WHERE order_date BETWEEN dbt_valid_from AND COALESCE(dbt_valid_to, '9999-12-31')` automatically pulls the `'Electronics'` row for a January order, and `'Accessories'` for an April order.

**Why it's called "SCD2":** it's the standard **Slowly Changing Dimension** pattern, **Type 2** technique (Type 1 = overwrite with no history kept, Type 3 = keep only the previous value in an extra column — Type 2 is the most complete, since it preserves the *entire* version history, not just the latest one).

**Why this would be painful by hand:** you'd have to implement the "compare current row vs. last saved → decide if it changed → close the old one → insert the new one" logic yourself as a `MERGE` with conditional logic — and repeat it for all 5 dimensions (`dim_orders`, `dim_customers`, `dim_products`, `dim_stores`, `dim_employees`). With dbt snapshots, it's the same ~10-line declarative YAML config (`strategy: timestamp`, `updated_at: updated_timestamp`) applied identically to all 5 — dbt generates the correct `MERGE` for you, without you writing any change-detection SQL.

## Tests after every layer, not just at the end

**The problem:** if you only run `dbt test` once, at the very end of the pipeline (after building `gold`), and something was wrong since `silver_t`, you've already built **all** of `silver_b` and `gold` on top of invalid data — you wasted compute, and the error propagated all the way into the tables the business consumes before anyone noticed.

**The fail-fast pattern:** the DAG interleaves `dbt run` and `dbt test` **per layer**:

```text
silver_technical → silver_technical_tests → silver_business → silver_business_tests → ...
```

If `silver_technical_tests` fails (say, a `not_null` or `unique` test on `order_id` doesn't pass), the DAG **stops right there** — `silver_business` never gets to run on data you already know is broken. It's cheaper to fail early (you only lost the compute for one layer) than to fail late (you lost the compute for the whole pipeline, and a bad value might have already reached a dashboard).

## Airflow's CeleryExecutor

**The problem:** Airflow's *scheduler* decides which task should run and when, but it needs a mechanism to actually **execute** those tasks — and that mechanism (the "executor") determines whether Airflow can scale or not.

**The alternatives:**

- `SequentialExecutor`: one task at a time, in the same process as the scheduler. Only useful for trivial testing.
- `LocalExecutor`: parallelizes tasks using multiprocessing on the same machine — better, but limited to a single host's resources.
- `CeleryExecutor`: **distributes** tasks across multiple *workers*, which can live on different machines.

**How `CeleryExecutor` works mechanically:**

1. The **scheduler** doesn't execute tasks — it decides which one should run next and publishes a message onto a **queue** (in this project, **Redis** acts as the *broker* for that queue — that's why it shows up as a service in `docker-compose.yaml`).
2. One or more **worker** processes (Celery workers) are listening on that queue. Whichever one is free first picks up the message and runs the task (in this project, typically a `BashOperator` running `dbt run`/`dbt test`).
3. The result and status get written back to Airflow's metadata database (Postgres).

**Why it matters:** with `CeleryExecutor`, scaling the pipeline means adding more `worker` containers (`docker-compose up --scale worker=3`), not adding more CPU to a single machine. It's the real production pattern for when you have multiple DAGs running simultaneously, or independent tasks within a single DAG that can actually run in parallel. This project's `orchestrate` DAG chains its 10 tasks sequentially (`>>`), so it doesn't exploit parallelism in practice — but the stack (`webserver`, `scheduler`, `dag-processor`, `worker`, `triggerer`, `Postgres`, `Redis` as separate containers in `docker-compose.yaml`) is set up exactly as it would be in a real deployment that does need it.

## Secrets kept out of the code

**The problem:** if you hardcode a Databricks token or a Postgres connection string directly in a `.py` file or `profiles.yml`, that secret stays in git history forever (even if you delete it in a later commit, it's still recoverable from earlier ones) — anyone with access to the repo (or a fork, or an accidental leak) can use it.

**What was done in this project:** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_INGEST_JOB_ID` live only in `airflow/.env` (gitignored), and Docker Compose injects them into the containers via `env_file`. `orchestrate.py` reads them with `os.environ[...]`, never as literals. `profiles.yml` uses `{{ env_var('DATABRICKS_TOKEN') }}` — dbt resolves that variable at runtime, so the actual value never ends up written in the versioned file.

**Why it matters beyond "it's a best practice":** a plaintext secret on disk isn't a theoretical risk — a careless `git add .`, a repo that goes public, or a fork is all it takes to expose the credential and force a rotation. That's why this repo follows a proactive gitignore policy: any new secrets file or credential gets added to `.gitignore` the moment it appears, not after the fact.

## Isolated S3 vs. CDC for the main flow

This isn't so much a technical pattern as a deliberate decision about **which mechanism to use for which type of source**:

- The **core** business dataset (customers, orders, products, employees, stores, order_items) comes from a **live operational system** — simulated with Postgres "ghost," standing in for what would be an ERP or a POS constantly emitting transactions in a real setup. For that kind of source, **CDC** is the right pattern: it reflects incremental changes from a system that never stops writing.
- The **reviews** dataset (`gold.reviews`) is a static file (`reviews.csv`) that doesn't come from any operational system of its own — it's a complementary dataset, loaded once. For that kind of source, a **data lake (S3) + one-off load** is a reasonable pattern — there'd be no point standing up a CDC pipeline for a CSV that never changes.

The choice of mechanism (CDC vs. loading from a data lake) depends on the nature of the source, not an arbitrary preference. See also the README's "[Isolated external ingestion](../README.en.md#5-isolated-external-ingestion--aws-s3--databricks-goldreviews)" section for the full context on why `gold.reviews` stays outside the dbt modeling.

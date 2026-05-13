# etl-mini-pipeline

A small batch ETL project that reads transaction data, validates and classifies rows, separates clean and rejected output, and loads both into SQLite for verification and analysis.

The goal of this project is not just to transform data. The goal is to make pipeline behaviour visible, explainable, and rerun-safe.

---

## What this project does

The pipeline processes transaction-style CSV data and:

- validates the expected schema
- parses and normalises rows
- separates valid rows from rejected rows
- attaches `run_id` for traceability
- writes clean and rejected outputs
- loads both outputs into SQLite
- supports verification through a SQL query pack

This repo is being built as a portfolio-clean Project 1 for junior data engineering development.

---

## Why this project exists

This project demonstrates core batch data engineering skills in a small, explainable system:

- schema validation
- row-level validation
- clean vs rejected output handling
- idempotent database loading
- SQL-based verification
- documentation of pipeline behaviour

It is intended to show real engineering evidence, not just code that runs.

---

## Pipeline flow

High-level flow:

1. Read source rows.
2. Validate schema.
3. Parse and validate each row.
4. Split rows into:
   - clean rows
   - rejected rows with `error_reason`
5. Write output files.
6. Load outputs into SQLite.
7. Verify database state with SQL queries.

---

## Project structure

```text
etl-mini-pipeline/
├── .github/workflows/
├── sample_data/
├── tests/
├── etl.py
├── sqlite_load.py
├── queries.sql
└── README.md
```

### Key files

#### `etl.py`

Main ETL logic for:

- reading source data
- validating rows
- generating clean and rejected outputs
- attaching `run_id`

#### `sqlite_load.py`

Creates SQLite tables and loads:

- `clean_transactions`
- `rejected_transactions`

This file also demonstrates rerun-safe inserts.

#### `queries.sql`

SQL query pack for:

- table inspection
- grouped reporting
- duplicate checks
- rejected-row verification
- clean vs rejected run-level comparison

---

## SQLite tables

The project currently uses two SQLite tables.

### `clean_transactions`

Stores accepted rows.

Schema:

- `transaction_id`
- `amount`
- `currency`
- `run_id`

Behaviour:

- rerun-safe via `UNIQUE(transaction_id, run_id)`

This prevents duplicate clean rows for the same run when the same load is replayed.

### `rejected_transactions`

Stores rejected rows.

Schema:

- `transaction_id`
- `amount`
- `currency`
- `error_reason`
- `run_id`

One row in `rejected_transactions` represents one rejected source row from a specific pipeline run, including the reason that row failed validation.

Behaviour:

- rerun-safe via `UNIQUE(transaction_id, error_reason, run_id)`

This prevents duplicate rejected rows for the same run and failure reason when the same load is replayed.

---

## Rerun safety / idempotency

Both database tables are designed to be rerun-safe.

The loader uses `INSERT OR IGNORE` together with explicit uniqueness constraints, so replaying the same sample data for the same `run_id` does not create duplicates.

Current idempotency rules:

- clean rows: `UNIQUE(transaction_id, run_id)`
- rejected rows: `UNIQUE(transaction_id, error_reason, run_id)`

This means the project can be rerun without inflating row counts for already-loaded records.

---

## SQL verification

The SQL query pack in `queries.sql` is used to inspect and verify pipeline state.

Current query categories include:

### Clean table verification

- total rows
- rows per currency
- total amount per currency
- rows by `run_id`
- duplicate check on clean idempotency key

### Rejected table verification

- rejected row count by `run_id`
- reject reasons by `run_id`
- duplicate check on rejected idempotency key

### Run-level comparison

- clean vs rejected counts by `run_id`

This makes the database state explainable and gives the project a proof-oriented query layer.

---

## How to run

### 1. Create / activate a virtual environment

Use your preferred Python environment setup.

### 2. Run the ETL

Run the ETL script to produce clean and rejected outputs.

```bash
python etl.py
```

### 3. Load into SQLite

Run the SQLite loader:

```bash
python sqlite_load.py
```

### 4. Run verification queries

```bash
sqlite3 etl.db < queries.sql
```

---

## What to verify

After running the loader, you should be able to verify:

- clean rows were inserted
- rejected rows were inserted
- rerunning the same load does not duplicate clean rows
- rerunning the same load does not duplicate rejected rows
- reject reasons are visible per `run_id`
- clean and rejected counts can be compared side by side by `run_id`

---

## Example verification questions

This repo is designed to answer questions like:

- How many clean rows were loaded for each run?
- How many rejected rows were loaded for each run?
- Which reject reasons occurred in a run?
- Are there duplicate clean rows for the same `transaction_id` and `run_id`?
- Are there duplicate rejected rows for the same `transaction_id`, `error_reason`, and `run_id`?
- What are the clean vs rejected counts for a given run?

---

## Current milestone

Current project milestone: **DB-backed pipeline with SQLite**

- clean rows loaded into SQLite
- rejected rows loaded into SQLite
- rerun safety implemented for both tables
- SQL verification query pack established

This closes the first serious database milestone for Project 1 and prepares the repo for richer verification and later multi-table SQL reasoning.

---

## What this project demonstrates

This repo currently demonstrates:

- Python ETL basics
- validation and reject classification
- database loading
- idempotent insert strategy
- SQL-based verification
- project documentation tied to actual implementation

---

## Constraints / current boundaries

This project is intentionally small and focused.

It does **not** currently aim to be:

- a distributed pipeline
- a cloud-native pipeline
- a streaming system
- a production orchestration system

The current focus is correctness, explainability, and portfolio-quality fundamentals.

---

## Next improvements

Planned next improvements may include:

- adding stronger reconciliation against source input totals
- improving test coverage for loader behaviour
- tightening README documentation around expected query results
- upgrading `sqlite_load.py` to load generated clean/rejected output files through CLI arguments

---

## Summary

`etl-mini-pipeline` is a small but deliberate batch ETL project built to show:

- clear pipeline behaviour
- clean vs rejected output handling
- rerun-safe SQLite loading
- verification through SQL
- practical, explainable data engineering fundamentals

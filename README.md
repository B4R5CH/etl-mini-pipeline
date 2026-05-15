# etl-mini-pipeline

![tests](https://github.com/B4R5CH/etl-mini-pipeline/actions/workflows/tests.yml/badge.svg)

A small batch ETL project that reads transaction CSV data, validates rows, separates clean and rejected outputs, and demonstrates rerun-safe SQLite loading.

The goal of this project is to make pipeline behaviour visible, explainable, and verifiable.

---

## What this project does

The pipeline processes transaction-style CSV data and:

- validates the expected schema
- parses and normalises rows
- separates valid rows from rejected rows
- attaches `run_id` for traceability
- writes clean and rejected outputs
- demonstrates idempotent SQLite loading
- supports verification through a SQL query pack

This repo is being built as a portfolio-clean Project 1 for junior data engineering development.

---

## Why this project exists

This project demonstrates core batch data engineering skills in a small, explainable system:

- schema validation
- row-level validation
- clean vs rejected output handling
- run-level traceability
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
6. Demonstrate SQLite table loading.
7. Verify database state with SQL queries.

---

## Project structure

```text
etl-mini-pipeline/
├── .github/workflows/      # GitHub Actions CI
├── tests/                  # Unit tests
├── etl.py                  # CSV ETL: schema validation, row validation, clean/reject outputs
├── sqlite_load.py          # SQLite table creation and idempotent insert demonstration
├── queries.sql             # SQL verification query pack
├── raw.csv                 # Sample valid/mixed input data
├── raw_bad.csv             # Sample bad-schema input for failure testing
├── .gitignore
└── README.md
```

---

## Key files

### `etl.py`

Main ETL logic for:

- reading source data
- validating required headers
- validating and normalising rows
- generating clean and rejected outputs
- attaching `run_id`

### `sqlite_load.py`

Creates SQLite tables and demonstrates rerun-safe inserts into:

- `clean_transactions`
- `rejected_transactions`

The current script uses sample in-code rows to prove table creation, uniqueness constraints, and `INSERT OR IGNORE` behaviour.

A planned next improvement is to load the generated `clean.csv` and `rejected.csv` files through CLI arguments.

### `queries.sql`

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

This means the database load can be rerun without inflating row counts for already-loaded records.

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

This gives the project a proof-oriented query layer.

---

## How to run

### 1. Create / activate a virtual environment

Use your preferred Python environment setup.

Example:

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Run the ETL

Run the ETL script against the sample input:

```bash
python etl.py --input raw.csv --clean clean.csv --reject rejected.csv
```

This writes:

- `clean.csv`
- `rejected.csv`

These files are local generated outputs and are intentionally ignored by Git.

### 3. Run the schema failure example

Run the ETL against a bad-schema file:

```bash
python etl.py --input raw_bad.csv
```

Expected behaviour:

- the pipeline fails loud
- logs an error
- raises a `ValueError` describing missing headers

### 4. Demonstrate SQLite loading

Run the SQLite loader:

```bash
python sqlite_load.py
```

This creates `etl.db`, creates the clean and rejected tables if they do not already exist, and inserts sample clean/rejected rows using idempotent insert logic.

`etl.db` is a local generated database and is intentionally ignored by Git.

### 5. Run verification queries

```bash
sqlite3 etl.db < queries.sql
```

---

## Output format

### `clean.csv`

Headers:

```text
transaction_id,amount,currency,run_id
```

### `rejected.csv`

Headers:

```text
transaction_id,amount,currency,error_reason,run_id
```

---

## Failure modes

| Failure type | Behaviour |
|---|---|
| Missing required header | Fails loud with `ValueError` |
| Invalid `transaction_id` | Row goes to `rejected.csv` with `error_reason` |
| Invalid amount | Row goes to `rejected.csv` with `error_reason` |
| Invalid currency | Row goes to `rejected.csv` with `error_reason` |
| Duplicate `transaction_id` within a run | Row goes to `rejected.csv` with `duplicate_transaction_id` |

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

Current project milestone: DB-backed pipeline with SQLite.

Current state:

- CSV ETL writes clean and rejected outputs
- SQLite loader creates clean and rejected tables
- SQLite loader demonstrates rerun-safe inserts
- SQL verification query pack is established

This closes the first serious database milestone for Project 1 and prepares the repo for richer verification and later multi-table SQL reasoning.

---

## What this project demonstrates

This repo currently demonstrates:

- Python ETL basics
- schema validation
- validation and reject classification
- clean/rejected output handling
- run-level traceability
- idempotent insert strategy
- SQL-based verification
- project documentation tied to actual implementation

---

## Constraints / current boundaries

This project is intentionally small and focused.

It does not currently aim to be:

- a distributed pipeline
- a cloud-native pipeline
- a streaming system
- a production orchestration system

The current focus is correctness, explainability, and portfolio-quality fundamentals.

---

## Next improvements

Planned next improvements may include:

- loading generated `clean.csv` and `rejected.csv` into SQLite through CLI arguments
- adding stronger reconciliation against source input totals
- improving test coverage for loader behaviour
- tightening README documentation around expected query results

---

## Summary

`etl-mini-pipeline` is a small but deliberate batch ETL project built to show:

- clear pipeline behaviour
- clean vs rejected output handling
- rerun-safe SQLite loading
- verification through SQL
- practical, explainable data engineering fundamentals

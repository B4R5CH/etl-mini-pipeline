# etl-mini-pipeline

A small batch ETL project that reads transaction data, validates and classifies rows, separates clean and rejected output, and loads both into SQLite for verification and analysis.

The goal of this project is not just to transform data. The goal is to make pipeline behaviour visible, explainable, testable, and rerun-safe.

---

## What this project does

The pipeline processes transaction-style CSV data and:

- validates the expected schema
- parses and normalises rows
- separates valid rows from rejected rows
- attaches `run_id` for traceability
- writes `clean.csv` and `rejected.csv`
- loads both generated outputs into SQLite
- supports verification through a SQL query pack
- includes tests for cleaning logic and SQLite loader rerun safety

This repo is being built as a portfolio-clean Project 1 for junior data engineering development.

---

## Why this project exists

This project demonstrates core batch data engineering skills in a small, explainable system:

- schema validation
- row-level validation
- clean vs rejected output handling
- explicit rejection reasons
- idempotent database loading
- SQL-based verification
- automated tests
- documentation of pipeline behaviour

It is intended to show real engineering evidence, not just code that runs.

---

## Pipeline flow

```text
raw.csv
→ etl.py
→ clean.csv / rejected.csv
→ sqlite_load.py
→ clean_transactions / rejected_transactions
→ queries.sql verification
```

Step-by-step:

1. Read source rows from CSV.
2. Validate the expected schema.
3. Parse and validate each row.
4. Split rows into:
   - clean rows
   - rejected rows with `error_reason`
5. Write `clean.csv` and `rejected.csv`.
6. Load both generated output files into SQLite.
7. Verify database state with SQL queries.

---

## Project structure

```text
etl-mini-pipeline/
├── .github/workflows/
├── tests/
│   ├── conftest.py
│   ├── test_cleaners.py
│   └── test_sqlite_loader.py
├── docs/
│   └── project_walkthrough.md
├── etl.py
├── sqlite_load.py
├── queries.sql
├── raw.csv
├── raw_bad.csv
└── README.md
```

---

## Key files

### `etl.py`

Main ETL logic for:

- reading source data
- validating input schema
- parsing and normalising rows
- generating clean and rejected outputs
- attaching `run_id`

### `sqlite_load.py`

Creates SQLite tables and loads the generated ETL output files:

- `clean.csv`
- `rejected.csv`

into:

- `clean_transactions`
- `rejected_transactions`

The loader uses uniqueness constraints and `INSERT OR IGNORE` to support rerun-safe inserts.

### `queries.sql`

SQL query pack for:

- table inspection
- clean-table verification
- rejected-table verification
- grouped reporting
- duplicate checks
- clean vs rejected run-level comparison

### `tests/`

Automated tests for:

- cleaning/parsing behaviour
- SQLite loader rerun safety
- self-contained loader behaviour using generated ETL outputs

---

## SQLite tables

The project uses two SQLite tables.

### `clean_transactions`

Stores accepted rows.

Fields:

- `transaction_id`
- `amount`
- `currency`
- `run_id`

Rerun-safety rule:

```sql
UNIQUE(transaction_id, run_id)
```

This prevents duplicate clean rows for the same run when the same load is replayed.

### `rejected_transactions`

Stores rejected rows.

Fields:

- `transaction_id`
- `amount`
- `currency`
- `error_reason`
- `run_id`

One row in `rejected_transactions` represents one rejected source row from a specific pipeline run, including the reason that row failed validation.

Rerun-safety rule:

```sql
UNIQUE(transaction_id, error_reason, run_id)
```

This prevents duplicate rejected rows for the same run and failure reason when the same load is replayed.

---

## Rerun safety / idempotency

Both database tables are designed to be rerun-safe.

The loader uses `INSERT OR IGNORE` together with explicit uniqueness constraints, so replaying the same generated output files for the same `run_id` does not create duplicate database rows.

Current idempotency rules:

- clean rows: `UNIQUE(transaction_id, run_id)`
- rejected rows: `UNIQUE(transaction_id, error_reason, run_id)`

---

## SQL verification

The SQL query pack in `queries.sql` is used to inspect and verify pipeline state.

Current query categories include:

### Clean table verification

- total clean rows
- sample clean rows
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

### 2. Install test dependency

```bash
pip install pytest
```

### 3. Run the ETL

```bash
python etl.py
```

This produces:

```text
clean.csv
rejected.csv
```

### 4. Load into SQLite

```bash
python sqlite_load.py
```

This reads the generated `clean.csv` and `rejected.csv` files and loads them into SQLite tables.

### 5. Run verification queries

```bash
sqlite3 etl.db < queries.sql
```

### 6. Run tests

```bash
python -m pytest -q
```

---

## What to verify

After running the ETL and SQLite loader, you should be able to verify:

- clean rows were written to `clean.csv`
- rejected rows were written to `rejected.csv`
- clean rows were inserted into `clean_transactions`
- rejected rows were inserted into `rejected_transactions`
- rerunning the same load does not duplicate clean rows
- rerunning the same load does not duplicate rejected rows
- reject reasons are visible per `run_id`
- clean and rejected counts can be compared side by side by `run_id`

---

## Current milestone

Current project milestone: **Project 1 — DB-backed pipeline with SQLite**

Completed capabilities:

- clean rows written by the ETL
- rejected rows written by the ETL
- generated ETL outputs loaded into SQLite
- clean rows loaded into `clean_transactions`
- rejected rows loaded into `rejected_transactions`
- rerun safety implemented for both tables
- SQL verification query pack established
- SQLite loader rerun-safety test added
- README and walkthrough documentation added

This closes the first serious database-backed pipeline milestone and prepares the repo for Project 2: an analytical model slice.

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
- adding richer logging around database load counts
- adding CLI arguments to `sqlite_load.py`
- expanding rejected-row edge-case tests
- introducing a small analytical model layer in Project 2
- mapping the local system to Azure services later

---

## Summary

`etl-mini-pipeline` is a small but deliberate batch ETL project built to show:

- clear pipeline behaviour
- clean vs rejected output handling
- rerun-safe SQLite loading
- verification through SQL
- automated testing
- practical, explainable data engineering fundamentals

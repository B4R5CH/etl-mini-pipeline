# etl-mini-pipeline

A small batch data engineering project that reads transaction CSV data, validates and classifies rows, writes clean/rejected outputs, loads those outputs into SQLite, and starts a simple analytical model layer.

The goal is to make pipeline behaviour visible, testable, rerun-safe, and explainable from the terminal.

---

## Current state

This repo currently covers two linked milestones:

1. **Project 1 — DB-backed ETL pipeline**
   - CSV ingestion
   - schema validation
   - row-level validation
   - clean/rejected output files
   - SQLite loading
   - idempotent inserts
   - SQL verification queries
   - automated tests through GitHub Actions

2. **Project 2 — early analytical model slice**
   - simple dimension tables
   - simple fact table
   - source-to-model SQL load script
   - analytical query examples over the model

Project 1 is the main completed milestone. Project 2 has been started as a small local star-schema-style model built from the Project 1 SQLite tables.

---

## What this project demonstrates

This project demonstrates junior data engineering fundamentals in a small, inspectable system:

- schema checks before processing
- validation and rejection of bad rows
- explicit `error_reason` values for rejected rows
- `run_id` traceability
- clean vs rejected output separation
- SQLite table loading
- database-level rerun safety using uniqueness constraints and `INSERT OR IGNORE`
- SQL verification of loaded data
- basic analytical modelling with fact and dimension tables
- automated tests for cleaning logic and loader rerun safety
- repository documentation and walkthrough notes

---

## Pipeline flow

```text
raw.csv
  -> etl.py
  -> clean.csv / rejected.csv
  -> sqlite_load.py
  -> clean_transactions / rejected_transactions
  -> queries.sql verification
  -> model_schema.sql / model_load.sql
  -> dim_currency / dim_run / fact_transactions
  -> analytics_queries.sql
```

Step-by-step:

1. `etl.py` reads the source CSV.
2. The script checks required headers.
3. Each row is parsed and validated.
4. Valid rows are written to `clean.csv`.
5. Invalid rows are written to `rejected.csv` with `error_reason`.
6. `sqlite_load.py` loads both generated files into SQLite.
7. `queries.sql` verifies clean/rejected load behaviour.
8. `model_schema.sql` creates a small analytical model.
9. `model_load.sql` populates dimensions and fact data from `clean_transactions`.
10. `analytics_queries.sql` runs reporting queries over the model.

---

## Project structure

```text
etl-mini-pipeline/
├── .github/workflows/
│   └── tests.yml                 # GitHub Actions test workflow
├── docs/
│   └── project_walkthrough.md    # Project 1 walkthrough and engineering notes
├── tests/
│   ├── conftest.py
│   ├── test_cleaners.py          # Tests for cleaning/parsing behaviour
│   └── test_sqlite_loader.py     # Tests for SQLite loader rerun safety
├── analytics_queries.sql         # Analytical model query examples
├── etl.py                        # CSV ETL script
├── model_load.sql                # Loads analytical model tables from clean_transactions
├── model_schema.sql              # Creates dimension and fact tables
├── queries.sql                   # Project 1 SQLite verification query pack
├── raw.csv                       # Sample source data
├── raw_bad.csv                   # Bad-schema input used to demonstrate fail-loud behaviour
├── sqlite_load.py                # Loads clean/rejected CSV outputs into SQLite
├── .gitignore
└── README.md
```

---

## Key files

### `etl.py`

Main CSV ETL script.

Responsibilities:

- read input CSV data
- validate required headers
- normalise transaction fields
- reject invalid transaction IDs
- reject invalid amounts
- reject invalid currencies
- reject duplicate transaction IDs within a run
- attach `run_id`
- write `clean.csv`
- write `rejected.csv`

Default command:

```bash
python etl.py
```

Equivalent explicit command:

```bash
python etl.py --input raw.csv --clean clean.csv --reject rejected.csv
```

Optional fixed run ID:

```bash
python etl.py --run-id run_001
```

Using a fixed `run_id` is useful when manually testing idempotency and SQL query output.

---

### `sqlite_load.py`

SQLite loader for the generated ETL outputs.

Responsibilities:

- create `etl.db`
- create `clean_transactions`
- create `rejected_transactions`
- read `clean.csv`
- read `rejected.csv`
- insert clean rows into SQLite
- insert rejected rows into SQLite
- avoid duplicate inserts on rerun

Default command:

```bash
python sqlite_load.py
```

The loader expects `clean.csv` and `rejected.csv` to already exist. Run `etl.py` first.

---

### `queries.sql`

Project 1 verification query pack.

It checks:

- clean row counts
- sample clean rows
- grouped clean totals
- rows by currency
- rows by run
- duplicate checks on clean table uniqueness
- rejected row counts
- reject reasons by run
- duplicate checks on rejected table uniqueness
- clean vs rejected comparison by run

Run with:

```bash
sqlite3 etl.db < queries.sql
```

---

### `model_schema.sql`

Creates a small analytical model from the loaded clean transaction data.

Tables:

- `dim_currency`
- `dim_run`
- `fact_transactions`

This is the start of Project 2: moving from raw loaded pipeline tables toward an analytics-ready structure.

---

### `model_load.sql`

Populates the analytical model from `clean_transactions`.

It:

- inserts distinct currencies into `dim_currency`
- inserts distinct run IDs into `dim_run`
- populates `fact_transactions`
- includes count checks comparing source rows and model rows

---

### `analytics_queries.sql`

Query examples over the analytical model.

Current examples:

- total amount by currency
- transaction count by currency
- total amount by run

These queries are intentionally small. Their purpose is to prove the fact/dimension model can support basic reporting questions.

---

### `docs/project_walkthrough.md`

Project walkthrough explaining:

- what the pipeline does
- validation and rejection logic
- SQLite loading
- idempotency strategy
- verification queries
- tests
- engineering decisions
- current improvement areas

---

### `tests/`

Automated tests cover:

- `safe_float`
- `clean_currency`
- `parse_row`
- invalid amount handling
- SQLite loader rerun safety
- loader behaviour using generated ETL outputs

Run with:

```bash
python -m pytest -q
```

The GitHub Actions workflow also runs the test suite on push and pull request.

---

## Input data

### `raw.csv`

Sample source file with transaction-style rows.

Required headers:

```text
transaction_id,amount,currency
```

The sample file includes both valid rows and intentionally invalid rows so the ETL can demonstrate clean/rejected splitting.

### `raw_bad.csv`

Bad-schema sample file.

It is missing the required `currency` header and is used to demonstrate fail-loud schema validation.

---

## Output files

### `clean.csv`

Generated by `etl.py`.

Fields:

```text
transaction_id,amount,currency,run_id
```

### `rejected.csv`

Generated by `etl.py`.

Fields:

```text
transaction_id,amount,currency,error_reason,run_id
```

Generated output files are intentionally ignored by Git.

---

## SQLite tables

### `clean_transactions`

Stores valid transaction rows.

Fields:

- `transaction_id`
- `amount`
- `currency`
- `run_id`

Rerun-safety rule:

```sql
UNIQUE(transaction_id, run_id)
```

This prevents the same transaction from being inserted twice for the same pipeline run.

---

### `rejected_transactions`

Stores rejected transaction rows.

Fields:

- `transaction_id`
- `amount`
- `currency`
- `error_reason`
- `run_id`

Rerun-safety rule:

```sql
UNIQUE(transaction_id, error_reason, run_id)
```

This prevents duplicate rejected records for the same transaction, reason, and run.

---

## Analytical model tables

### `dim_currency`

One row per distinct currency in `clean_transactions`.

Fields:

- `currency_key`
- `currency_code`

### `dim_run`

One row per distinct pipeline run in `clean_transactions`.

Fields:

- `run_key`
- `run_id`

### `fact_transactions`

One row per clean transaction.

Fields:

- `transaction_id`
- `currency_key`
- `run_key`
- `amount`

This table references `dim_currency` and `dim_run` through foreign keys.

---

## Rerun safety / idempotency

The SQLite loader uses database constraints and `INSERT OR IGNORE` to make repeated loads safe.

Current idempotency rules:

- clean rows: `UNIQUE(transaction_id, run_id)`
- rejected rows: `UNIQUE(transaction_id, error_reason, run_id)`

This means rerunning the loader against the same generated outputs should not inflate row counts.

---

## How to run

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install test dependency

```bash
python -m pip install --upgrade pip
python -m pip install pytest
```

### 3. Run the ETL

```bash
python etl.py
```

This creates:

```text
clean.csv
rejected.csv
```

### 4. Load generated outputs into SQLite

```bash
python sqlite_load.py
```

This creates `etl.db` and loads both generated output files into SQLite.

### 5. Run Project 1 verification queries

```bash
sqlite3 etl.db < queries.sql
```

### 6. Build the analytical model

```bash
sqlite3 etl.db < model_schema.sql
sqlite3 etl.db < model_load.sql
```

### 7. Run analytical model queries

```bash
sqlite3 etl.db < analytics_queries.sql
```

### 8. Run tests

```bash
python -m pytest -q
```

---

## Clean local reset

To rerun from a clean local state:

```bash
rm -f etl.db clean.csv rejected.csv test_etl.db
python etl.py --run-id run_001
python sqlite_load.py
sqlite3 etl.db < queries.sql
sqlite3 etl.db < model_schema.sql
sqlite3 etl.db < model_load.sql
sqlite3 etl.db < analytics_queries.sql
python -m pytest -q
```

`etl.db`, generated CSV outputs, and test databases are local artifacts and should not be committed.

---

## Schema failure demo

Run:

```bash
python etl.py --input raw_bad.csv
```

Expected behaviour:

- the script fails loud
- a `ValueError` is raised
- the error identifies the missing required header

This demonstrates schema validation before row processing.

---

## Failure modes handled

| Failure type | Behaviour |
|---|---|
| Missing required CSV header | Fails loud with `ValueError` |
| Blank transaction ID | Row rejected with `invalid_transaction_id` |
| Invalid amount | Row rejected with `invalid_amount` |
| Invalid currency | Row rejected with `invalid_currency` |
| Duplicate transaction ID within a run | Row rejected with `duplicate_transaction_id` |
| Loader rerun against same outputs | Duplicate DB rows ignored |

---

## What to verify

After running the project, you should be able to verify:

- `clean.csv` was produced
- `rejected.csv` was produced
- clean rows were inserted into `clean_transactions`
- rejected rows were inserted into `rejected_transactions`
- rerunning the loader does not duplicate rows
- reject reasons are visible by `run_id`
- clean and rejected counts can be compared by `run_id`
- dimension tables can be populated from clean transactions
- fact rows can be joined back to dimensions
- analytical queries can report totals by currency and run
- tests pass locally and in CI

---

## Current milestone

Current repo state:

- Project 1 DB-backed ETL pipeline is portfolio-clean enough to explain and run locally.
- Project 2 analytical model slice has started with `dim_currency`, `dim_run`, and `fact_transactions`.

The next repo-facing improvement should be to make the analytical model layer more robust and documented:

- add `DROP TABLE IF EXISTS` or `CREATE TABLE IF NOT EXISTS` behaviour to the model scripts
- add a short `docs/project_2_model_notes.md`
- add 5 OLAP-style analytical queries
- add clear expected results for the model queries

---

## Constraints / current boundaries

This project is intentionally small and local.

It is not currently:

- a distributed pipeline
- a cloud-native pipeline
- a streaming system
- a production orchestration system
- a full warehouse implementation

The current focus is correctness, explainability, rerun safety, SQL verification, and the first step into analytical modelling.

---

## Next improvements

Possible next improvements:

- make `model_schema.sql` safely rerunnable
- make `model_load.sql` safely rerunnable
- expand `analytics_queries.sql` to 5 OLAP-style queries
- add model documentation explaining grain, fact, and dimensions
- add loader tests for rejected-row edge cases
- add richer row-count logging around SQLite loads
- map the local pipeline to Azure services later

---

## Summary

`etl-mini-pipeline` is a small but deliberate data engineering repo built to show:

- CSV ETL fundamentals
- validation and rejection handling
- clean/rejected output separation
- SQLite loading
- rerun-safe database inserts
- SQL verification
- basic analytical modelling
- automated testing
- clear portfolio documentation

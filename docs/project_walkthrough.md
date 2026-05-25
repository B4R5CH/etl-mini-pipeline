# Project 1 Walkthrough — DB-Backed ETL Mini Pipeline

## 1. What this project does

This project is a small local batch ETL pipeline that reads raw transaction data from CSV, validates it, splits valid and invalid rows into separate outputs, and loads both outputs into SQLite for verification.

The project demonstrates:

- CSV ingestion
- schema validation
- row-level validation
- clean vs rejected output handling
- explicit error reasons
- run-level traceability
- SQLite loading
- idempotent rerun safety
- SQL-based verification
- automated tests

---

## 2. Pipeline flow

```text
raw.csv
→ etl.py
→ clean.csv / rejected.csv
→ sqlite_load.py
→ clean_transactions / rejected_transactions
→ queries.sql verification
```

`etl.py` reads the raw input, validates records, and produces clean and rejected output files.

`sqlite_load.py` reads those generated output files and loads them into SQLite tables.

`queries.sql` is used to inspect and verify the loaded data.

---

## 3. Validation and rejection logic

The ETL layer separates records into two categories.

### Clean rows

Rows are written to `clean.csv` when they satisfy the expected data rules.

Clean rows include:

- `transaction_id`
- `amount`
- `currency`
- `run_id`

### Rejected rows

Rows are written to `rejected.csv` when they fail validation.

Rejected rows preserve useful context from the original input and include an `error_reason` so that the failure can be inspected later.

Rejected rows include:

- `transaction_id`
- `amount`
- `currency`
- `error_reason`
- `run_id`

This keeps invalid data visible and reviewable instead of silently dropping it.

---

## 4. SQLite loading

The SQLite loader creates two tables.

### `clean_transactions`

Stores valid transaction records.

Key fields:

- `transaction_id`
- `amount`
- `currency`
- `run_id`

### `rejected_transactions`

Stores invalid/rejected transaction records.

Key fields:

- `transaction_id`
- `amount`
- `currency`
- `error_reason`
- `run_id`

The loader reads from the generated ETL output files:

```text
clean.csv
rejected.csv
```

and inserts those rows into the appropriate SQLite tables.

---

## 5. Idempotency strategy

The project uses database-level uniqueness constraints and `INSERT OR IGNORE` to make reruns safe.

For clean records:

```sql
UNIQUE(transaction_id, run_id)
```

For rejected records:

```sql
UNIQUE(transaction_id, error_reason, run_id)
```

If the same run is loaded twice, the database does not duplicate the same records.

---

## 6. Verification queries

`queries.sql` contains SQL checks that verify the pipeline output.

The query pack checks:

- loaded clean row counts
- loaded rejected row counts
- rejected reasons by `run_id`
- duplicate prevention
- clean vs rejected comparison for a pipeline run
- grouped reporting over clean transaction data

The query pack proves that the pipeline state is inspectable after a run.

---

## 7. Tests

The project includes automated tests for the Python cleaning layer and SQLite loader behaviour.

Important test coverage includes:

- cleaning helper behaviour
- row parsing behaviour
- SQLite rerun safety
- loader behaviour using generated ETL output files

The SQLite loader test proves the key contract:

```text
First load inserts the expected rows.
Second load does not duplicate them.
```

---

## 8. Main engineering decisions

### Separate clean and rejected outputs

Clean and rejected rows are written separately so valid records can be used while invalid records remain available for audit and debugging.

### Preserve error reasons

Rejected rows include `error_reason` so failures are explainable.

### Use `run_id`

`run_id` makes pipeline runs traceable and allows verification by run.

### Use SQLite first

SQLite keeps the project local and simple while still introducing relational loading, constraints, and SQL verification.

### Use `INSERT OR IGNORE`

This supports safe reruns when combined with uniqueness constraints.

---

## 9. Failure modes this project handles

The project is designed around common data pipeline failure modes:

- missing or invalid values
- invalid numeric conversion
- schema/header problems
- duplicate records on rerun
- unclear rejection reasons
- inability to verify loaded state

---

## 10. What I would improve next

Possible next improvements:

- add CLI arguments to `sqlite_load.py`
- add more loader tests for rejected-row edge cases
- add richer logging around database load counts
- introduce a small analytical model layer
- build a fact table and dimensions for Project 2
- add more SQL verification around joins and window functions
- map the local system to Azure services later

---

## 11. Closure standard

Project 1 is considered closed enough when:

- the ETL script produces clean and rejected outputs
- the SQLite loader loads both generated outputs
- rerunning the loader does not duplicate records
- SQL queries verify the loaded state
- tests pass locally and in CI
- README and walkthrough documentation explain the system clearly

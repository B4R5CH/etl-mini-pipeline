# etl-mini-pipeline

A production-style CSV ETL in Python with schema checks, data validation, idempotent dedupe (by `transaction_id`), clean/rejected outputs (with error reasons), and run-level logging.

## What it does
- Reads a CSV
- Validates schema (required headers)
- Cleans/validates rows
- Splits clean vs rejected with error reasons
- Dedupes by `transaction_id` (idempotency)
- Writes `clean.csv` + `rejected.csv`
- Logs run metrics (input/cleaned/rejected + run_id)

## How to run

### Valid input
```bash
python etl.py --input raw.csv --clean clean.csv --reject rejected.csv
```

### Schema failure (missing header)
```bash
python etl.py --input raw_bad.csv
```
Expected: logs an ERROR and raises `ValueError` describing missing headers.

## Output format

### clean.csv
Headers:
- `transaction_id,amount,currency,run_id`

### rejected.csv
Headers:
- `transaction_id,amount,currency,error_reason,run_id`

## Failure modes
- Missing required header → `ValueError` (fails loud)
- Invalid `transaction_id` / amount / currency → row goes to `rejected.csv` with `error_reason`
- Duplicate `transaction_id` within a run → row goes to `rejected.csv` with `duplicate_transaction_id`

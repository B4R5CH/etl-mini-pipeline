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

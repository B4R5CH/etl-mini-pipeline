# etl-mini-pipeline
A production-style CSV ETL in Python with schema checks, data validation, idempotent dedupe, clean/rejected outputs with error reasons, and run-level logging.

1.	What it does

	•	Reads a CSV
	•	Validates schema (required headers)
	•	Cleans/validates rows
	•	Splits clean vs rejected with error reasons
	•	Dedupes by transaction_id
	•	Writes clean.csv + rejected.csv
	•	Logs run metrics

2.	How to run
Examples:

	•	python etl.py --input data/raw.csv --clean outputs/clean.csv --reject outputs/rejected.csv
	•	python etl.py --input data/raw_bad.csv (shows schema failure)

3.	Output format
	Show the headers for clean/reject.
4.	Failure modes

	•	Missing header → ValueError
	•	Invalid amount/currency/id → rejected rows

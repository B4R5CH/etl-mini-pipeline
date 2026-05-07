# etl-mini-pipeline

A small batch ETL project that reads transaction data, validates and classifies rows, separates clean and rejected output, and loads both into SQLite for verification and analysis.

The goal of this project is not just to transform data, but to make pipeline behavior visible, explainable, and rerun-safe.

---

## What this project does

The pipeline processes transaction-style CSV data and:

- validates expected schema
- parses and normalizes rows
- separates valid rows from rejected rows
- attaches `run_id` for traceability
- writes clean and rejected outputs
- loads both outputs into SQLite
- supports verification through a SQL query pack

This repo is being built as a portfolio-clean Project 1 for junior data engineering development.

---

## Why this project exists

This project is designed to demonstrate core batch data engineering skills in a small, explainable system:

- schema validation
- row-level validation
- clean vs rejected output handling
- idempotent database loading
- SQL-based verification
- documentation of pipeline behavior

It is intended to show real engineering evidence, not just code that “runs”.

---

## Pipeline flow

High-level flow:

1. Read source rows
2. Validate schema
3. Parse and validate each row
4. Split rows into:
   - clean rows
   - rejected rows with `error_reason`
5. Write output files
6. Load outputs into SQLite
7. Verify database state with SQL queries

---

## Project structure

```text
etl-mini-pipeline/
├── etl.py
├── sqlite_load.py
├── queries.sql
├── README.md
├── tests/
├── sample_data/
└── .github/workflows/
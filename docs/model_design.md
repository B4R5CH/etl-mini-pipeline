# Project 2 Model Design

## Business process
This model represents accepted transaction activity produced by the Project 1 ETL pipeline.

## Grain
One row in `fact_transactions` represents one accepted transaction.

## Fact table proposal
### fact_transactions
Candidate columns:
- transaction_id
- amount
- date_key
- currency_key
- run_key

Measure:
- amount

## Dimension table proposals
### dim_date
Candidate columns:
- date_key
- full_date
- year
- month
- day

### dim_currency
Candidate columns:
- currency_key
- currency_code

### dim_run
Candidate columns:
- run_key
- run_id

## Intended analytical questions
1. How many accepted transactions occurred over a given period?
2. What is the total transaction amount by currency?
3. Which month had the highest total transaction amount?
4. Which month had the highest transaction volume?
5. Which currency had the highest total transaction amount?

## Next session start line
CREATE TABLE fact_transactions (
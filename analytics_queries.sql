-- total amount by currency
SELECT
    dim_currency.currency_code,
    SUM(fact_transactions.amount) AS total_amount
FROM fact_transactions
JOIN dim_currency
    ON fact_transactions.currency_key = dim_currency.currency_key
GROUP BY dim_currency.currency_code;

-- transaction count by currency
SELECT
    dim_currency.currency_code,
    COUNT(*) AS transaction_count
FROM fact_transactions
JOIN dim_currency
    ON fact_transactions.currency_key = dim_currency.currency_key
GROUP BY dim_currency.currency_code;

-- total amount by run
SELECT
    dim_run.run_id,
    SUM(fact_transactions.amount) AS total_amount
FROM fact_transactions
JOIN dim_run
    ON fact_transactions.run_key = dim_run.run_key
GROUP BY dim_run.run_id;
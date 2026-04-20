--1) Total rows in the table
SELECT COUNT (*) AS total_rows
FROM clean_transactions;

--2) Rows per currency
SELECT currency, COUNT(*) AS rows_in_currency
FROM clean_transactions
GROUP BY currency
ORDER BY rows_in_currency DESC;

--3) Total amount per currency
SELECT currency, SUM(amount) AS total_amount
FROM clean_transactions GROUP BY currency
ORDER BY total_amount DESC;

--4) Show 10 rows
SELECT transaction_id, amount, currency, run_id
FROM clean_transactions
LIMIT 10;

--5) Show rows for on run_id (edit the value if needed)
SELECT transaction_id, amount, currency, run_id
FROM clean_transactions
WHERE run_id = 'run_001'
LIMIT 10;

-- 6) Check duplicates (should return nothing)
SELECT transaction_id, run_id, COUNT(*) AS n
FROM clean_transactions
GROUP BY transaction_id, run_id
HAVING COUNT(*) > 1;

-- =========================================================
-- etl-mini-pipeline: SQL Query Pack
-- Purpose:
-- - inspect loaded data in clean_transactions
-- - verify DB correctness after loads
-- - demonstrate baseline SQL competence
-- Table:
--   clean_transactions(transaction_id, amount, currency, run_id)
-- =========================================================


-- =========================================================
-- 1) TABLE-LEVEL INSPECTION
-- =========================================================

-- 1.1 Total clean rows currently in the table
SELECT
    COUNT(*) AS total_rows
FROM clean_transactions;


-- 1.2 Sample 10 clean rows
SELECT
    transaction_id,
    amount,
    currency,
    run_id
FROM clean_transactions
LIMIT 10;


-- 1.3 Sample 10 rows for one run_id
-- Edit the run_id value if needed.
SELECT
    transaction_id,
    amount,
    currency,
    run_id
FROM clean_transactions
WHERE run_id = 'run_001'
LIMIT 10;


-- =========================================================
-- 2) BASIC GROUPED REPORTING
-- =========================================================

-- 2.1 Row count per currency
SELECT
    currency,
    COUNT(*) AS row_count
FROM clean_transactions
GROUP BY currency
ORDER BY row_count DESC;


-- 2.2 Total amount per currency
SELECT
    currency,
    SUM(amount) AS total_amount
FROM clean_transactions
GROUP BY currency
ORDER BY total_amount DESC;


-- 2.3 Row count and total amount per run_id
-- Verification query: proves what each load inserted into the DB.
SELECT
    run_id,
    COUNT(*) AS row_count,
    SUM(amount) AS total_amount
FROM clean_transactions
GROUP BY run_id
ORDER BY total_amount DESC;


-- =========================================================
-- 3) DATA QUALITY / CORRECTNESS CHECKS
-- =========================================================

-- 3.1 Duplicate check on the idempotency key
-- Should return zero rows if UNIQUE(transaction_id, run_id) is working properly.
SELECT
    transaction_id,
    run_id,
    COUNT(*) AS duplicate_count
FROM clean_transactions
GROUP BY transaction_id, run_id
HAVING COUNT(*) > 1;


-- =========================================================
-- 4) WHERE VS HAVING
-- =========================================================

-- 4.1 WHERE filters rows BEFORE grouping
-- Only GBP rows are included before the count is calculated.
SELECT
    currency,
    COUNT(*) AS row_count
FROM clean_transactions
WHERE currency = 'GBP'
GROUP BY currency;


-- 4.2 HAVING filters groups AFTER grouping
-- Only keep currency groups whose grouped row count is at least 1.
SELECT
    currency,
    COUNT(*) AS row_count
FROM clean_transactions
GROUP BY currency
HAVING COUNT(*) >= 1;


-- =========================================================
-- 5) CTE-BASED REPORTING
-- =========================================================

-- 5.1 Currency totals for run_001, keeping only totals above 50
-- Demonstrates:
-- - row filtering inside the CTE
-- - grouped intermediate result
-- - outer query filtering on the CTE output
WITH currency_totals AS (
    SELECT
        currency,
        SUM(amount) AS total_amount
    FROM clean_transactions
    WHERE run_id = 'run_001'
    GROUP BY currency
)
SELECT
    currency,
    total_amount
FROM currency_totals
WHERE total_amount > 50
ORDER BY total_amount DESC;


-- 5.2 Currency totals with row count for run_001
-- Demonstrates:
-- - grouped metrics in the CTE
-- - querying only columns that exist in the CTE output
WITH currency_totals AS (
    SELECT
        currency,
        COUNT(*) AS row_count,
        SUM(amount) AS total_amount
    FROM clean_transactions
    WHERE run_id = 'run_001'
    GROUP BY currency
)
SELECT
    currency,
    row_count,
    total_amount
FROM currency_totals
WHERE total_amount > 50
ORDER BY total_amount DESC;


-- =========================================================
-- 6) CASE WHEN CLASSIFICATION
-- =========================================================

-- 6.1 Classify each currency in run_001 as high or low by total amount
-- Demonstrates:
-- - CASE WHEN as classification, not filtering
-- - label tied to the correct metric: SUM(amount)
SELECT
    currency,
    SUM(amount) AS total_amount,
    CASE
        WHEN SUM(amount) > 50 THEN 'high'
        ELSE 'low'
    END AS amount_band
FROM clean_transactions
WHERE run_id = 'run_001'
GROUP BY currency
ORDER BY total_amount DESC;


-- 6.2 Classify each currency in run_001 with both row count and total amount
-- Demonstrates:
-- - multiple grouped metrics
-- - CASE WHEN based on total_amount logic
SELECT
    currency,
    COUNT(*) AS row_count,
    SUM(amount) AS total_amount,
    CASE
        WHEN SUM(amount) > 50 THEN 'high'
        ELSE 'low'
    END AS amount_band
FROM clean_transactions
WHERE run_id = 'run_001'
GROUP BY currency
ORDER BY total_amount DESC;


-- =========================================================
-- 7) SUBQUERY-BASED REPORTING
-- =========================================================

-- 7.1 Same reporting logic as the CTE version, but using a subquery in FROM
-- Demonstrates:
-- - inner grouped result
-- - required subquery alias
-- - outer classification on the subquery output
SELECT
    currency,
    row_count,
    total_amount,
    CASE
        WHEN total_amount > 50 THEN 'high'
        ELSE 'low'
    END AS amount_band
FROM (
    SELECT
        currency,
        COUNT(*) AS row_count,
        SUM(amount) AS total_amount
    FROM clean_transactions
    WHERE run_id = 'run_001'
    GROUP BY currency
) AS currency_totals
ORDER BY total_amount DESC;


-- =========================================================
-- REJECTED TRANSACTIONS VERIFICATION
-- =========================================================

-- rejected rows per run_id
SELECT
    run_id,
    COUNT(*) AS rejected_row_count
FROM rejected_transactions
GROUP BY run_id
ORDER BY rejected_row_count DESC;

-- reject reasons per run_id
SELECT
    run_id,
    error_reason,
    COUNT(*) AS rejected_reason_count
FROM rejected_transactions
GROUP BY run_id, error_reason
ORDER BY run_id, rejected_reason_count DESC;

-- duplicate check on rejected idempotency key
-- should return zero rows if rerun safety is working
SELECT
    transaction_id,
    error_reason,
    run_id,
    COUNT(*) AS duplicate_count
FROM rejected_transactions
GROUP BY transaction_id, error_reason, run_id
HAVING COUNT(*) > 1;


-- clean vs rejected counts by run_id
WITH clean_count AS (
    SELECT
        run_id,
        COUNT(*) AS clean_row_count
    FROM clean_transactions
    GROUP BY run_id
),
rejected_count AS (
    SELECT
        run_id,
        COUNT(*) AS rejected_row_count
    FROM rejected_transactions
    GROUP BY run_id
)
SELECT
    c.run_id,
    c.clean_row_count,
    COALESCE(r.rejected_row_count, 0) AS rejected_row_count
FROM clean_count c
LEFT JOIN rejected_count r
    ON c.run_id = r.run_id
ORDER BY c.run_id;
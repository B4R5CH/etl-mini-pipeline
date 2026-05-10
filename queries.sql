-- =========================================================
-- etl-mini-pipeline: SQL Query Pack
-- Purpose:
-- - inspect data loaded into SQLite
-- - verify clean and rejected loading after pipeline runs
-- - provide SQL proof for Project 1 behavior
--
-- Tables:
--   clean_transactions(transaction_id, amount, currency, run_id)
--   rejected_transactions(transaction_id, amount, currency, error_reason, run_id)
-- =========================================================


-- =========================================================
-- TABLE INSPECTION
-- =========================================================

-- Total clean rows currently loaded
SELECT
    COUNT(*) AS total_rows
FROM clean_transactions;


-- Sample clean rows
SELECT
    transaction_id,
    amount,
    currency,
    run_id
FROM clean_transactions
LIMIT 10;


-- Sample rows for a single run
SELECT
    transaction_id,
    amount,
    currency,
    run_id
FROM clean_transactions
WHERE run_id = 'run_001'
LIMIT 10;


-- =========================================================
-- CLEAN TRANSACTION VERIFICATION
-- =========================================================

-- Row count by currency
SELECT
    currency,
    COUNT(*) AS row_count
FROM clean_transactions
GROUP BY currency
ORDER BY row_count DESC;


-- Total amount by currency
SELECT
    currency,
    SUM(amount) AS total_amount
FROM clean_transactions
GROUP BY currency
ORDER BY total_amount DESC;


-- Row count and total amount by run
SELECT
    run_id,
    COUNT(*) AS row_count,
    SUM(amount) AS total_amount
FROM clean_transactions
GROUP BY run_id
ORDER BY total_amount DESC;


-- Duplicate check on clean-table uniqueness key
-- Should return zero rows if rerun safety is holding
SELECT
    transaction_id,
    run_id,
    COUNT(*) AS duplicate_count
FROM clean_transactions
GROUP BY transaction_id, run_id
HAVING COUNT(*) > 1;


-- WHERE example: row filter before grouping
SELECT
    currency,
    COUNT(*) AS row_count
FROM clean_transactions
WHERE currency = 'GBP'
GROUP BY currency;


-- HAVING example: group filter after grouping
SELECT
    currency,
    COUNT(*) AS row_count
FROM clean_transactions
GROUP BY currency
HAVING COUNT(*) >= 1;


-- =========================================================
-- STRUCTURED REPORTING PATTERNS
-- =========================================================

-- CTE-based grouped totals for run_001 above threshold
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


-- CTE-based grouped totals with row count
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


-- CASE WHEN classification by grouped total
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


-- CASE WHEN classification with row count included
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


-- Subquery version of the same reporting pattern
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
-- REJECTED TRANSACTION VERIFICATION
-- =========================================================

-- Rejected row count by run
SELECT
    run_id,
    COUNT(*) AS rejected_row_count
FROM rejected_transactions
GROUP BY run_id
ORDER BY rejected_row_count DESC;


-- Reject reasons by run
SELECT
    run_id,
    error_reason,
    COUNT(*) AS rejected_reason_count
FROM rejected_transactions
GROUP BY run_id, error_reason
ORDER BY run_id, rejected_reason_count DESC;


-- Duplicate check on rejected-table uniqueness key
-- Should return zero rows if rerun safety is holding
SELECT
    transaction_id,
    error_reason,
    run_id,
    COUNT(*) AS duplicate_count
FROM rejected_transactions
GROUP BY transaction_id, error_reason, run_id
HAVING COUNT(*) > 1;


-- =========================================================
-- RECONCILIATION / COMPARISON
-- =========================================================

-- Clean vs rejected counts by run
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


-- =========================================================
-- FUTURE RECONCILIATION / SOURCE-CHECK PLACEHOLDERS
-- =========================================================

-- Compare clean + rejected DB counts against source input row count
-- Verify run-level totals against source file expectations
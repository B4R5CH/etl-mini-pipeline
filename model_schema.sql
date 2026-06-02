CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL
);

CREATE TABLE dim_currency (
    currency_key INTEGER PRIMARY KEY,
    currency_code TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_run (
    run_key INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE
);

CREATE TABLE fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    currency_key INTEGER NOT NULL,
    run_key INTEGER NOT NULL,
    amount REAL NOT NULL,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (currency_key) REFERENCES dim_currency(currency_key),
    FOREIGN KEY (run_key) REFERENCES dim_run(run_key)
);
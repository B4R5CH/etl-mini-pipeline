import sqlite3

DB_PATH = "etl.db"

def init_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS clean_transactions (
            transaction_id TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL,
            run_id TEXT NOT NULL,
            UNIQUE(transaction_id, run_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rejected_transactions (
            transaction_id TEXT,
            amount TEXT,
            currency TEXT,
            error_reason TEXT NOT NULL,
            run_id TEXT NOT NULL,
            UNIQUE(transaction_id, error_reason, run_id)
        )
    """)

    conn.commit()
    conn.close()

def load_cleaned_rows(db_path, cleaned_rows):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executemany(
        "INSERT OR IGNORE INTO clean_transactions (transaction_id, amount, currency, run_id) VALUES (?,?,?,?)",
        [(r["transaction_id"], r["amount"], r["currency"], r["run_id"]) for r in cleaned_rows]
    )

    conn.commit()
    conn.close()



def load_rejected_rows(db_path, rejected_rows):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executemany(
        "INSERT OR IGNORE INTO rejected_transactions (transaction_id, amount, currency, error_reason, run_id) VALUES (?,?,?,?,?)",
        [(r["transaction_id"], r["amount"], r["currency"], r["error_reason"], r["run_id"]) for r in rejected_rows]
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db(DB_PATH)

    clean_sample = [
        {"transaction_id": "t1", "amount": 10.0, "currency": "GBP", "run_id": "run_001"},
        {"transaction_id": "t2", "amount": 7.0, "currency": "JPY", "run_id": "run_001"},
    ]
    
    rejected_sample = [
        {"transaction_id": "t1", "amount": "xqx", "currency": "GBP", "error_reason": "invalid_amount", "run_id": "run_001"},
        {"transaction_id": "tt2m", "amount": 7.0, "currency": "JPY", "error_reason": "invalid_transaction_id", "run_id": "run_001"},
    ]

    load_cleaned_rows(DB_PATH, clean_sample)
    load_cleaned_rows(DB_PATH, clean_sample)

    load_rejected_rows(DB_PATH, rejected_sample)
    load_rejected_rows(DB_PATH, rejected_sample)


    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM clean_transactions WHERE run_id = ?", ("run_001",))
    print("count_run_001:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM rejected_transactions WHERE run_id = ?", ("run_001",))
    print("count_run_001:", cur.fetchone()[0])

    conn.close()

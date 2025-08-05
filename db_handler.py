# db_handler.py
import sqlite3
import hashlib
from pathlib import Path
from tkinter import messagebox

DB_FILE = "orderbook.db"

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_hash TEXT,
                seq_no INTEGER,
                token INTEGER,
                timestamp TEXT,
                action TEXT,
                price REAL,
                quantity INTEGER
            )
        ''')
        conn.commit()

def compute_file_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)
    hash_value = hasher.hexdigest()
    print(f"[DEBUG] Computed hash: {hash_value}")
    return hash_value

def is_file_already_processed(file_hash):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM order_entries WHERE file_hash = ?", (file_hash,))
        count = cursor.fetchone()[0]
        print(f"[DEBUG] Existing rows for hash {file_hash}: {count}")
        return count > 0

def store_rows(file_hash, rows):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        # Only keep the required fields for each row
        filtered_rows = [
            (
                file_hash,
                int(row['sequence_no']),
                int(row['token_no']),
                str(row['epoch_time']),
                row['action'],
                float(row['price']),
                int(row['qty'])
            )
            for row in rows
        ]
        cursor.executemany('''
            INSERT INTO order_entries (file_hash, seq_no, token, timestamp, action, price, quantity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', filtered_rows)
        conn.commit()

def handle_file_selection(path):
    if path:
        try:
            with open(path, 'rb') as f:
                file_bytes = f.read()
                file_hash = compute_file_hash(file_bytes)
            if is_file_already_processed(file_hash):
                messagebox.showerror("Duplicate File", "This file has already been processed.")
                return
            # Handle file selection logic here, e.g., update UI or variables as needed
            # Example: print or log the selected file path and hash
            print(f"File selected: {path}")
            print(f"File hash: {file_hash}")
            # If you need to update UI elements, pass them as parameters to this function
        except Exception as e:
            print(f"Exception: {e}")  # Add this line
            messagebox.showerror("Error", f"Failed to select file: {e}")

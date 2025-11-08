#!/usr/bin/env python3
"""Drop upload_stats table from data.db safely (creates a backup).

Run from project root: python3 scripts/drop_upload_stats.py
"""
import shutil
import sqlite3
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / 'data.db'
BACKUP = DB.with_suffix('.db.bak')

if not DB.exists():
    print(f"Database not found at {DB}")
    raise SystemExit(1)

print(f"Backing up {DB} -> {BACKUP}")
shutil.copy2(DB, BACKUP)

conn = sqlite3.connect(str(DB))
cur = conn.cursor()

# Check if table exists
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='upload_stats'")
if cur.fetchone():
    print("Found upload_stats table — dropping it now.")
    cur.execute("DROP TABLE IF EXISTS upload_stats;")
    conn.commit()
    print("upload_stats table dropped.")
else:
    print("upload_stats table not found; nothing to do.")

conn.close()
print("Done.")

import sqlite3
import pandas as pd

def init_db():
    """Initialize database with correct schema."""
    conn = sqlite3.connect('data.db')
    
    # Create telemetry table for device data
    conn.execute('''CREATE TABLE IF NOT EXISTS telemetry
                   (Device_ID TEXT,
                    Last_Sighted_Date TEXT,
                    Last_Sighted_Location TEXT,
                    Location_Code TEXT,
                    PRIMARY KEY (Device_ID))''')
    
    # Create upload_stats table to track history
    conn.execute('''CREATE TABLE IF NOT EXISTS upload_stats
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    prev_upload_date TEXT,
                    last_upload_date TEXT,
                    prev_total_entry_count INTEGER,
                    last_total_entry_count INTEGER,
                    prev_new_entry_count INTEGER,
                    last_new_entry_count INTEGER,
                    prev_updated_entry_count INTEGER,
                    last_updated_entry_count INTEGER)''')
    
    # Initialize stats if empty
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM upload_stats')
    if cursor.fetchone()[0] == 0:
        # Insert initial row with zeros
        cursor.execute('''
            INSERT INTO upload_stats 
            (prev_upload_date, last_upload_date,
             prev_total_entry_count, last_total_entry_count,
             prev_new_entry_count, last_new_entry_count,
             prev_updated_entry_count, last_updated_entry_count)
            VALUES (NULL, NULL, 0, 0, 0, 0, 0, 0)
        ''')
    conn.commit()
    conn.close()

def update_or_insert_data(df):
    """Update existing records or insert new ones based on Device_ID."""
    conn = sqlite3.connect('data.db')
    
    # Convert DataFrame to list of tuples for batch processing
    records = df.to_records(index=False)
    
    # For each record, update if exists (and new date is more recent) or insert if new
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    for record in records:
        device_id, date, location, code = record

        # Check if device exists and get its current date
        cursor.execute('''
            SELECT Last_Sighted_Date 
            FROM telemetry 
            WHERE Device_ID = ?
        ''', (device_id,))

        existing = cursor.fetchone()

        if existing and existing[0] is not None and str(existing[0]).strip() != '':
            # Parse dates safely
            try:
                new_dt = pd.to_datetime(date, errors='coerce')
                old_dt = pd.to_datetime(existing[0], errors='coerce')
            except Exception:
                new_dt = pd.to_datetime(date, errors='coerce')
                old_dt = pd.to_datetime(existing[0], errors='coerce')

            # Update only if new date is more recent
            if not pd.isna(new_dt) and (pd.isna(old_dt) or new_dt > old_dt):
                cursor.execute('''
                    UPDATE telemetry 
                    SET Last_Sighted_Date = ?,
                        Last_Sighted_Location = ?,
                        Location_Code = ?
                    WHERE Device_ID = ?
                ''', (date, location, code, device_id))
                updated += 1
        else:
            # Insert new record
            cursor.execute('''
                INSERT INTO telemetry 
                (Device_ID, Last_Sighted_Date, Last_Sighted_Location, Location_Code)
                VALUES (?, ?, ?, ?)
            ''', (device_id, date, location, code))
            inserted += 1
    
    conn.commit()
    conn.close()
    
    return inserted, updated
def get_all_data():
    """Retrieve all records from database."""
    conn = sqlite3.connect('data.db')
    df = pd.read_sql('SELECT * FROM telemetry', conn)
    conn.close()
    return df
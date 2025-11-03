import sqlite3
import pandas as pd

def init_db():
    """Initialize database with correct schema."""
    conn = sqlite3.connect('data.db')
    conn.execute('''CREATE TABLE IF NOT EXISTS telemetry
                   (Device_ID TEXT,
                    Last_Sighted_Date TEXT,
                    Last_Sighted_Location TEXT,
                    Location_Code TEXT,
                    PRIMARY KEY (Device_ID))''')
    conn.close()

def update_or_insert_data(df):
    """Update existing records or insert new ones based on Device_ID."""
    conn = sqlite3.connect('data.db')
    
    # Convert DataFrame to list of tuples for batch processing
    records = df.to_records(index=False)
    
    # For each record, update if exists (and new date is more recent) or insert if new
    cursor = conn.cursor()
    for record in records:
        device_id, date, location, code = record
        
        # Check if device exists and get its current date
        cursor.execute('''
            SELECT Last_Sighted_Date 
            FROM telemetry 
            WHERE Device_ID = ?
        ''', (device_id,))
        
        existing = cursor.fetchone()
        
        if existing:
            # Update only if new date is more recent
            if pd.to_datetime(date) > pd.to_datetime(existing[0]):
                cursor.execute('''
                    UPDATE telemetry 
                    SET Last_Sighted_Date = ?,
                        Last_Sighted_Location = ?,
                        Location_Code = ?
                    WHERE Device_ID = ?
                ''', (date, location, code, device_id))
        else:
            # Insert new record
            cursor.execute('''
                INSERT INTO telemetry 
                (Device_ID, Last_Sighted_Date, Last_Sighted_Location, Location_Code)
                VALUES (?, ?, ?, ?)
            ''', (device_id, date, location, code))
    
    conn.commit()
    conn.close()

def get_all_data():
    """Retrieve all records from database."""
    conn = sqlite3.connect('data.db')
    df = pd.read_sql('SELECT * FROM telemetry', conn)
    conn.close()
    return df
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

# ... existing imports and code ...

def get_dashboard_alerts():
    """
    Fetch data and categorize devices based on inactivity.
    Urgent = Inactive > 7 days (Client Request)
    Soft = Inactive > 3 days (Proactive Watchlist)
    """
    df = get_all_data()
    
    # 1. Convert date string to datetime objects for calculation
    df['Last_Sighted_Date'] = pd.to_datetime(df['Last_Sighted_Date'], errors='coerce')
    
    # 2. Remove rows where date is invalid
    df = df.dropna(subset=['Last_Sighted_Date'])
    
    now = pd.Timestamp.now()
    
    alerts = {
        "urgent": [],
        "soft": []
    }

    # 3. Iterate and classify
    for _, row in df.iterrows():
        delta = now - row['Last_Sighted_Date']
        days_inactive = delta.days
        
        device_data = {
            "Device_ID": row['Device_ID'],
            "Last_Sighted": row['Last_Sighted_Date'].strftime('%Y-%m-%d'),
            "Days_Inactive": days_inactive,
            "Location": row['Last_Sighted_Location']
        }

        if days_inactive >= 7:
            alerts["urgent"].append(device_data)
        elif days_inactive >= 3:
            alerts["soft"].append(device_data)
            
    return alerts
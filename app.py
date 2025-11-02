from flask import Flask, render_template, request, jsonify
from email.mime.text import MIMEText
from dotenv import load_dotenv
import pandas as pd
import sqlite3
import smtplib
import os
from datetime import datetime

# Load environment variables
load_dotenv()

app = Flask(__name__)

EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('EMAIL_PASS')

# ---------- Helper Function: Email Alerts ----------
def send_alert(device_list):
    """Send email alert for underutilized devices."""
    if not device_list:
        return {"msg": "No alerts needed", "ok": False}

    msg = MIMEText(f"The following devices are underutilized or inactive: {', '.join(device_list)}")
    msg['Subject'] = "Ascent: Underutilized Devices Alert"
    msg['From'] = EMAIL
    msg['To'] = "recipient@example.com"

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL, PASSWORD)
            server.send_message(msg)
        return {"msg": "Sent", "ok": True}
    except Exception as e:
        print("Email Error:", e)
        return {"msg": str(e), "ok": False}

# ---------- Routes ----------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    """Upload Excel file and store cleaned data."""
    file = request.files['file']
    df = pd.read_excel(file)

    # Normalize column names
    df.columns = df.columns.str.strip().str.replace(" ", "_")

    required_cols = {'Device_ID', 'Area', 'Last_Sighted'}
    if not required_cols.issubset(df.columns):
        return jsonify({
            "error": f"Missing required columns: {required_cols - set(df.columns)}"
        }), 400

    # Clean Data
    df['Device_ID'] = df['Device_ID'].astype(str).str.strip()
    df['Area'] = df['Area'].astype(str).str.strip()

    # Parse dates and compute Days_Inactive
    df['Last_Sighted'] = pd.to_datetime(df['Last_Sighted'], errors='coerce')
    df['Days_Inactive'] = (datetime.now() - df['Last_Sighted']).dt.days

    # Assign Status based on inactivity
    df['Status'] = df['Days_Inactive'].apply(
        lambda d: 'Active' if d <= 7 else ('Soft Inactive' if d <= 30 else 'Urgent Inactive')
    )

    # Store in SQLite
    conn = sqlite3.connect('data.db')
    df.to_sql('telemetry', conn, if_exists='replace', index=False)
    conn.close()

    # Alert for underutilized devices
    soft_alerts = df.loc[df['Status'] == 'Soft Inactive', 'Device_ID'].tolist()
    urgent_alerts = df.loc[df['Status'] == 'Urgent Inactive', 'Device_ID'].tolist()

    soft_status = send_alert(soft_alerts)
    urgent_status = send_alert(urgent_alerts)

    return jsonify({
        "results": [{"file": file.filename, "status": "ok"}],
        "alerts_sent": {"soft": soft_status, "urgent": urgent_status}
    })

@app.route('/summary')
def summary():
    """Return summary data for charts."""
    conn = sqlite3.connect('data.db')
    df = pd.read_sql('SELECT * FROM telemetry', conn)
    conn.close()

    # Chart summaries
    status_summary = df['Status'].value_counts().to_dict()
    area_summary = df['Area'].value_counts().to_dict()

    return jsonify({
        "status": status_summary,
        "area": area_summary
    })

@app.route('/data')
def data():
    """Return table data for frontend."""
    conn = sqlite3.connect('data.db')
    df = pd.read_sql('SELECT * FROM telemetry', conn)
    conn.close()

    # Format for frontend
    df['Last_Sighted'] = df['Last_Sighted'].astype(str)
    records = df.to_dict(orient='records')
    return jsonify(records)

if __name__ == '__main__':
    app.run(debug=True)

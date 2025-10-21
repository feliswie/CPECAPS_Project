# To run type the ff into terminal: flask run
from flask import Flask, render_template, request, jsonify
from email.mime.text import MIMEText
from dotenv import load_dotenv
import pandas as pd
import sqlite3
import smtplib
import os

# Load environment variables
load_dotenv()

app = Flask(__name__)

EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('EMAIL_PASS')


# -------------------------------
# EMAIL ALERT FUNCTION
# -------------------------------
def send_alert(device_list):
    """Send an email alert listing underutilized devices."""
    if not device_list:
        return

    msg = MIMEText(f"The following devices are underutilized: {', '.join(device_list)}")
    msg['Subject'] = "Underutilized Devices Alert"
    msg['From'] = EMAIL
    msg['To'] = "recipient@example.com"  # change to your target email

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL, PASSWORD)
            server.send_message(msg)
        print("✅ Alert email sent successfully.")
    except Exception as e:
        print(f"⚠️ Failed to send email: {e}")


# -------------------------------
# ROUTES
# -------------------------------
@app.route('/')
def index():
    """Render the dashboard upload page."""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    """Process uploaded Excel file, store in SQLite, and trigger alert."""
    try:
        file = request.files['file']
        df = pd.read_excel(file)

        # Debug
        df = pd.read_excel(file)
        print(df.columns.tolist())

        # Verify required columns exist
        required_cols = {'Device_ID', 'Area', 'Usage'}
        if not required_cols.issubset(df.columns):
            return jsonify({'error': f'Missing required columns: {required_cols}'}), 400

        # Clean data
        df['Device_ID'] = df['Device_ID'].astype(str).str.lstrip('0')

        # Save to database
        conn = sqlite3.connect('data.db')
        df.to_sql('telemetry', conn, if_exists='replace', index=False)
        conn.close()

        # Identify underutilized devices (Usage < 30%)
        underutilized = df[df['Usage'] < 30]['Device_ID'].tolist()
        if underutilized:
            send_alert(underutilized)

        return jsonify({'status': 'Upload successful', 'underutilized': underutilized})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/summary')
def summary():
    """Return summary by area for chart display."""
    try:
        conn = sqlite3.connect('data.db')
        df = pd.read_sql('SELECT * FROM telemetry', conn)
        conn.close()
        summary = df.groupby('Area')['Device_ID'].count().to_dict()
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# -------------------------------
# MAIN ENTRY POINT
# -------------------------------
if __name__ == '__main__':
    app.run(debug=True)

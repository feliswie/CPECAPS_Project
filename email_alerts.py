from email.mime.text import MIMEText
import smtplib
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('EMAIL_PASS')

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
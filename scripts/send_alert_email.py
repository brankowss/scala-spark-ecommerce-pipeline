# scripts/send_alert_email.py

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_alert_email():
    """
    Sends a formatted email alert with data about high-value orders (EP-7)
    and invalid CountryID transactions (EP-10).
    Reads SMTP configuration and recipient email from environment variables.
    """
    # --- Configuration ---
    sender_email = os.environ.get('JENKINS_SENDER_EMAIL')
    sender_password = os.environ.get('JENKINS_EMAIL_APP_PASSWORD')
    recipient_email = os.environ.get('NOTIFICATION_EMAIL')
    smtp_server = "smtp.gmail.com"
    smtp_port = 465  # For SSL

    # --- File paths ---
    outlier_file_path = "reports/outliers.txt"
    invalid_country_file_path = "reports/invalid_country.txt"

    # --- Read EP-7 Outliers ---
    if os.path.exists(outlier_file_path) and os.path.getsize(outlier_file_path) > 0:
        with open(outlier_file_path, 'r') as f:
            outlier_data = f.read()
    else:
        outlier_data = "No high-value order anomalies detected.\n"

    # --- Read EP-10 Invalid Countries ---
    if os.path.exists(invalid_country_file_path) and os.path.getsize(invalid_country_file_path) > 0:
        with open(invalid_country_file_path, 'r') as f:
            invalid_country_data = f.read()
    else:
        invalid_country_data = "No invalid CountryID transactions detected.\n"

    # --- Create the Email ---
    message = MIMEMultipart("alternative")
    message["Subject"] = "⚠️ Data Pipeline Alert: High-Value Orders & Data Quality"
    message["From"] = sender_email
    message["To"] = recipient_email

    html_body = f"""
    <html>
    <body>
        <h2>Data Pipeline Alerts</h2>
        <h3>EP-7: High-Value Orders</h3>
        <pre><code>{outlier_data}</code></pre>
        <h3>EP-10: Invalid CountryID Transactions</h3>
        <pre><code>{invalid_country_data}</code></pre>
        <p>Please review the data above for anomalies or data quality issues.</p>
    </body>
    </html>
    """
    message.attach(MIMEText(html_body, "html"))

    # --- Send the Email ---
    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
        print(f"Alert email successfully sent to {recipient_email}")
    except Exception as e:
        print(f"Failed to send alert email: {e}")


if __name__ == "__main__":
    send_alert_email()

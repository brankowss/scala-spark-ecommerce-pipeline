# scripts/send_alert_email.py

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_alert_email():
    """
    Sends a formatted email alert with data about high-value orders.
    Reads SMTP configuration and recipient email from environment variables.
    """
    # --- Configuration ---
    sender_email = os.environ.get('JENKINS_SENDER_EMAIL')
    sender_password = os.environ.get('JENKINS_EMAIL_APP_PASSWORD')
    recipient_email = os.environ.get('NOTIFICATION_EMAIL')
    smtp_server = "smtp.gmail.com"
    smtp_port = 465 # For SSL

    outlier_file_path = "reports/outliers.txt"

    # --- Check if there is anything to report ---
    if not os.path.exists(outlier_file_path) or os.path.getsize(outlier_file_path) == 0:
        print("No outlier file found or file is empty. No alert to send.")
        return

    # --- Read Outlier Data ---
    with open(outlier_file_path, 'r') as f:
        outlier_data = f.read()

    # --- Create the Email ---
    message = MIMEMultipart("alternative")
    message["Subject"] = "⚠️ High-Value Order Alert"
    message["From"] = sender_email
    message["To"] = recipient_email

    html_body = f"""
    <html>
      <body>
        <h2>High-Value Order Alert</h2>
        <p>The daily ETL job has detected one or more orders with a total value significantly higher than the average.</p>
        <p>Please review the following invoices:</p>
        <pre><code>{outlier_data}</code></pre>
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
import os
import smtplib
from email.mime.text import MIMEText

def send_email(subject: str, body: str):
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    sender = os.getenv("SMTP_FROM")
    to = os.getenv("SMTP_TO")

    if not all([host, port, sender, to]):
        return False  # 未設定就略過

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to

    try:
        with smtplib.SMTP(host, port, timeout=20) as server:
            server.starttls()
            if user and password:
                server.login(user, password)
            server.sendmail(sender, [to], msg.as_string())
        return True
    except Exception:
        return False
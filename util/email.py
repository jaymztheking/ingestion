import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging

class EmailNotification:
    def __init__(self, smtp_server, smtp_port):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def send_email(self, recipient_emails: list, subject: str, body: str) -> bool:
        message = MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] =  ", ".join(recipient_emails)
        message["Subject"] = subject
        message.attach(MIMEText(body, 'plain'))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(message)
            return True
        except Exception as e:
            logging.error(f'Failed to send email: {e}')
            return False
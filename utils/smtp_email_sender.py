import smtplib
from fastapi import FastAPI, HTTPException
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.message import EmailMessage
from email.utils import formataddr
from email.header import Header
from dotenv import find_dotenv, load_dotenv
import os
from pydantic import BaseModel, Field
from .exceptions import EmailSendingException
import logging

load_dotenv(find_dotenv())

ionos_smtp_server = os.getenv("SMTP_SERVER")
ionos_smtp_port = os.getenv("SMTP_PORT")
ionos_username = os.getenv("IONOS_USERNAME")
ionos_password = os.getenv("IONOS_PASSWORD")
domain = os.getenv("DOMAIN_URL")

class EmailSender:
    def __init__(self, smtp_server, smtp_port, username, password, domain = None) -> None:
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.domain = domain

    def send_email(self, to_email, content, company_name, first_name, sender_full_name, unique_id):
        try:
            # Connect to Gmail's SMTP server
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()  # Use TLS encryption
            server.login(self.username, self.password)

            # TODO remove later
            to_email = "illya20052003@gmail.com"

            # Create the email message
            msg = MIMEMultipart()
            msg["From"] = formataddr((sender_full_name, self.username))
            msg["To"] = to_email
            msg["Subject"] = f"Improving {company_name}'s Operations with AI Automation"

            html_content = f"""
            <html>
                <body>
                    <img src="{self.domain}/tracking-pixel/{unique_id}" width="1" height="1" />
                </body>
            </html>
            """
            # Attach the message
            msg.attach(MIMEText(content, "plain"))
            msg.attach(MIMEText(html_content, "html"))

            # Send the email
            # server.sendmail(ionos_username, to_email, msg.as_string())

            # Close the SMTP connection
            server.quit()

            print(f"Email was successfully sent to {to_email}")

        except Exception as e:
            logging.error(e)
            raise EmailSendingException(f"Email was not sent due to an error: {e}")
        

def send_email(to_email, content, company_name, first_name, sender_full_name, unique_id):
    try:
        # Connect to Gmail's SMTP server
        server = smtplib.SMTP(ionos_smtp_server, ionos_smtp_port)
        server.starttls()  # Use TLS encryption
        server.login(ionos_username, ionos_password)

        # TODO remove later
        to_email = "illya20052003@gmail.com"

        # Create the email message
        msg = MIMEMultipart()
        msg["From"] = formataddr((sender_full_name, ionos_username))
        msg["To"] = to_email
        msg["Subject"] = f"Saving costs for {company_name} with AI Automation"

        html_content = f"""
        <html>
            <body>
                <img src="{domain}/tracking-pixel/{unique_id}" width="1" height="1" />
            </body>
        </html>
        """
        # Attach the message
        msg.attach(MIMEText(content, "plain"))
        msg.attach(MIMEText(html_content, "html"))

        # Send the email
        server.sendmail(ionos_username, to_email, msg.as_string())

        # Close the SMTP connection
        server.quit()

        print(f"Email was successfully sent to {to_email}")

    except Exception as e:
        logging.error(e)
        raise EmailSendingException(f"Email was not sent due to an error: {e}")
        
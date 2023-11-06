from celery import Celery
from utils.smtp_email_sender import send_email
from utils.email_writer import create_personalized_email
from utils.exceptions import EmailSendingException, PersonalizedEmailCreationException
from uuid import UUID
import logging
from database import SessionLocal, EmailTask, TaskStatusEnum
import time
import random
import os

REDIS_URL = os.getenv("REDIS_URL")

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

sender_name = "Illya Brodovskyy"
MIN_DELAY_SECONDS = 40  # Minimum delay in seconds (adjust as needed)
MAX_DELAY_SECONDS = 160  # Maximum delay in seconds (adjust as needed)

def is_english_string(name):
    allowed_characters = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789&%$!?()-+=./, ")
    return all(char in allowed_characters for char in name)

def send_outreach_email(company_website: str, company_name: str, prospect_first_name: str, prospect_email: str, task_id: UUID):
    try:
        if (is_english_string(company_name) == False):
            return ("Not English Company name", False)

        print("\nCreating and sending email to", prospect_email)
        email_content = create_personalized_email(url=company_website, company_name=company_name, first_name=prospect_first_name)
        send_email(to_email=prospect_email, content=email_content, company_name=company_name,first_name=prospect_first_name, sender_full_name=sender_name, unique_id=task_id)

        return (email_content, True)
    except EmailSendingException as ese:
        logging.error(ese)
        return (email_content, False)
    except PersonalizedEmailCreationException as pece:
        logging.error(pece)
        return ("ERROR", False)
    except Exception as e:
        logging.error(e)
        return ("ERROR", False)

@celery.task
def process_email_batch(batch: dict):
    db = SessionLocal()

    for row in batch:
        company_name = row["Company Name for Emails"]
        company_website = row["Website"]
        prospect_first_name = row["First Name"]
        prospect_email = row["Email"]

        db_task = db.query(EmailTask).filter(EmailTask.recipient_email == prospect_email).first()
        if db_task and db_task.status != TaskStatusEnum.SCHEDULED:
            continue
        elif db_task == None:
            continue
        
        # Logic for sending emails
        try:
            email_content, email_sent = send_outreach_email(company_website, company_name, prospect_first_name, prospect_email="illya20052003@gmail.com", task_id=db_task.id)
            
            # Update the status to "sent" in the database

            if email_sent == True:
                    db_task.status = TaskStatusEnum.SENT
            else:
                db_task.status = TaskStatusEnum.SENDING_FAILED

            db_task.email_content = email_content
            db.add(db_task)
            db.commit()
            
            delay_seconds = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)

            print(f"Sending next email in {delay_seconds}")
            time.sleep(delay_seconds)  # Pause the execution for the specified delay

        except Exception as e:
            # Handle errors if the email sending fails
            logging.error(f"Error sending email: {e}")
            # Update the status to "failed" in the database
            db_task.email_content = "ERROR"
            db_task.status = TaskStatusEnum.SENDING_FAILED

            db.commit()
from celery import Celery
from database import SessionLocal, EmailTask, Batch
from sqlalchemy import select
from utils.smtp_email_sender import send_email
from utils.email_writer import create_personalized_email
from utils.scheduling_helper import get_next_working_day, split_df_into_batches, get_utc_offset
from utils.exceptions import EmailSendingException, PersonalizedEmailCreationException
from uuid import UUID
from datetime import datetime
from datetime import timedelta, datetime
from pytz import utc
import logging
from database import SessionLocal, EmailTask, TaskStatusEnum
import time
import random
import os
import pandas as pd

BROKER_REDIS_URL = os.getenv("REDIS_URL")
RESULT_REDIS_URL = os.getenv("RESULT_REDIS_URL")

celery = Celery("tasks", broker=BROKER_REDIS_URL, backend=RESULT_REDIS_URL)

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

    for i, row in enumerate(batch):
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
            email_content, email_sent = send_outreach_email(company_website, company_name, prospect_first_name, prospect_email=prospect_email, task_id=db_task.id)
            
            # Update the status to "sent" in the database

            if email_sent == True:
                    db_task.status = TaskStatusEnum.SENT
            else:
                db_task.status = TaskStatusEnum.SENDING_FAILED

            db_task.email_content = email_content
            db_task.updated_at = datetime.now(tz=utc)
            db.commit()
            
            delay_seconds = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)

            if i < len(batch) - 1:
                print(f"Sending next email in {delay_seconds}")
                time.sleep(delay_seconds)  # Pause the execution for the specified delay

        except Exception as e:
            # Handle errors if the email sending fails
            logging.error(f"Error sending email: {e}")
            # Update the status to "failed" in the database
            db_task.email_content = "ERROR"
            db_task.status = TaskStatusEnum.SENDING_FAILED

            db.commit()

    db.close()

@celery.task(track_started=True)
def split_into_batches(df_dict: dict):
    db = SessionLocal()
    print("STARTED")

    df = pd.DataFrame(df_dict)
    
    start = time.time()
    df["utc_offset"] = df.apply(get_utc_offset, axis=1)
    end = time.time()

    print(end - start)
    # Group df by the country, to identify the timezone later on
    df_grouped = df.groupby("utc_offset")

    print(df_grouped.groups)

    utc_time = get_next_working_day(datetime.utcnow())

    days_planned = 1
    processing_batches = 0

    processed_emails_daily = 0
    for offset, data in df_grouped:
        # Split all emails into batches of the size around 50
        batches = split_df_into_batches(data)

        offset_timedelta = timedelta(minutes=int(offset * 60))
        
        # Add the utc offset to utc time to get the local time of the batch
        batches_current_local_time = datetime.utcnow() + offset_timedelta

        # Create a scheduled time for processing and set it to around 10 am in local batches time 
        batches_local_scheduled_time = utc_time.replace(hour=9, minute=59, second=21, microsecond=0)

        # Check if the current time in local batches time is greater than the scheduled time for processing. If so, add one more day
        if batches_current_local_time > batches_local_scheduled_time:
            batches_local_scheduled_time += timedelta(days=1)
            batches_local_scheduled_time = get_next_working_day(batches_local_scheduled_time)

            print(f"It's too late today already, scheduling for {batches_local_scheduled_time} in UTC{'+' if offset > 0 else ''}{offset} timezone")

        # Convert the scheduled time to the UTC timezone
        utc_time = batches_local_scheduled_time - offset_timedelta

        for i, batch in enumerate(batches):
            db_batch = Batch(timezone=offset)
            for _, row in batch.iterrows():
                recipient_email = row["Email"]

                # TODO GET 
                # Check if email exists in table
                db_task = db.execute(
                    select(EmailTask).where(EmailTask.recipient_email == recipient_email)
                ).first()
                if db_task:
                    print(f"Skipping {recipient_email}, because it is already processing.")
                    continue

                email_task = EmailTask(recipient_email=recipient_email)
                db.add(email_task)
                db.commit()

                db_batch.email_tasks.append(email_task)

            if len(db_batch.email_tasks) == 0:
                continue

            # Convert DataFrame to a list of dictionaries
            batch_data = batch.to_dict(orient="records")

            # Check if sending it is going to be more than 50 emails per day with the current batch. 
            # If so, set the day for the next working day and reset the processed email count
            if processed_emails_daily + len(batch) > 50:
                print("Batch limit achieved")
                utc_time += timedelta(days=1)
                utc_time = get_next_working_day(utc_time)
                processed_emails_daily = 0
                days_planned += 1
            
            processed_emails_daily += len(batch)

            # Create new celery task to process the batch at the scheduled time
            process_email_batch.apply_async(args=[batch_data], eta=utc_time)

            processing_batches += 1

            db_batch.scheduled_processing_time = utc_time
            db.add(db_batch)
            db.commit()

            print(f"{offset} batch of size {len(batch)} will be processed at {utc_time}")
            print(f"Emails in one batch: {processed_emails_daily}")

    db.close()
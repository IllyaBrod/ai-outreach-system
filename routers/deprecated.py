import pandas as pd
import logging
import random
import time
from fastapi import APIRouter
from pandas import DataFrame
from fastapi import HTTPException, UploadFile
from typing import List
from pydantic import BaseModel
from utils.smtp_email_sender import send_email
from utils.email_writer import create_personalized_email
from utils.exceptions import EmailSendingException, PersonalizedEmailCreationException
from datetime import timedelta, datetime
from celery_worker import process_email_batch
from database import SessionLocal, EmailTask, Batch
from utils.scheduling_helper import get_recipient_timezone, get_next_working_day, split_df_into_batches, get_utc_offset
from sqlalchemy import select

router = APIRouter(prefix="/deprecated")

class OutreachResult(BaseModel):
    company_name: str
    company_website: str
    first_name: str
    email_address: str
    email_content: str
    email_sent: bool

MIN_DELAY_SECONDS = 40  # Minimum delay in seconds (adjust as needed)
MAX_DELAY_SECONDS = 160  # Maximum delay in seconds (adjust as needed)
sender_name = "Illya Brodovskyy"

header = ["Company name", "Company website", "Prospect first name", "Prospect email", "Email content", "Email sent"]

def split_df_into_batches(df: DataFrame):
    batches = []
    if len(df) <= 1:
        batches.append(df)
    else:
        processed_rows = 0

        while processed_rows < len(df):
            batch_size = min(random.randint(-10, 10) + 50, len(df) - processed_rows)

            batches.append(df.iloc[processed_rows:(processed_rows + batch_size)])
            processed_rows += batch_size

    return batches

def is_english_string(name):
    allowed_characters = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789&%$!?()-+=./, ")
    return all(char in allowed_characters for char in name)

def send_outreach_email(company_website: str, company_name: str, prospect_first_name: str, prospect_email: str):
    try:
        if (is_english_string(company_name) == False):
            return ("Not English Company name", False)

        print("\nCreating and sending email to", prospect_email)
        email_content = create_personalized_email(url=company_website, company_name=company_name, first_name=prospect_first_name)
        send_email(to_email=prospect_email, content=email_content, company_name=company_name,first_name=prospect_first_name, sender_full_name=sender_name, unique_id=1)

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
    
def save_to_csv(row: list):
    row_df = pd.DataFrame([row], columns=header)
    row_df.to_csv("./outreach_result.csv", header=None, mode="a", index=False)

@router.post("/start-outreach", 
    response_model=List[OutreachResult], 
    response_description="Array of the outreach results for each prospect", 
)
def start_outreach(outreach_csv: UploadFile):
    # Validate that teh file is CSV
    if outreach_csv.content_type != "text/csv":
        raise HTTPException(status_code=415, detail=f"Unsupported file type. Got {outreach_csv.content_type}; expected: text/csv")
    
    # Read csv file to DataFrame with pandas
    df = pd.read_csv(outreach_csv.file)
    outreach_result_list = []

    for index, row in df.iloc[4:7].iterrows():
        company_name = row["Company name"]
        company_website = row["Company URL"]
        prospect_first_name = row["First name"]
        prospect_email = row["Email"]

        email_content, email_sent = send_outreach_email(company_website, company_name, prospect_first_name, prospect_email)

        # Add the result with email content to csv on the server
        csv_row = [company_name, company_website, prospect_first_name, prospect_email, email_content, email_sent]
        save_to_csv(csv_row)

        # Append the list of dictionaries to later on return the list as a response
        result = OutreachResult(company_name=company_name, company_website=company_website, first_name=prospect_first_name, email_address=prospect_email, email_content=email_content, email_sent=email_sent)
        outreach_result_list.append(result)

        # Generate a random delay interval between MIN_DELAY_SECONDS and MAX_DELAY_SECONDS
        delay_seconds = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
        print(f"Sending next email in {delay_seconds}")
        # time.sleep(delay_seconds)  # Pause the execution for the specified delay

    return outreach_result_list

@router.post(
    "/start-outreach-concurrent",
    response_description="Message indicating how many batches are scheduled for processing"
)
def start_outreach_concurrent(outreach_csv: UploadFile):
    # Validate that teh file is CSV
    if outreach_csv.content_type != "text/csv":
        raise HTTPException(status_code=415, detail=f"Unsupported file type. Got {outreach_csv.content_type}; expected: text/csv")
    
    db = SessionLocal()
    
    # Read csv file to DataFrame with pandas
    df = pd.read_csv(outreach_csv.file)
    
    df = df.head(15)
    
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
            # process_email_batch.apply_async(args=[batch_data], eta=utc_time)

            processing_batches += 1

            db_batch.scheduled_processing_time = utc_time
            db.add(db_batch)
            db.commit()

            print(f"{offset} batch of size {len(batch)} will be processed at {utc_time}")
            print(f"Emails in one batch: {processed_emails_daily}")

    db.close()

    return {"message": f"CSV file uploaded successfully. {processing_batches} batches scheduled for processing for {days_planned} working days."}

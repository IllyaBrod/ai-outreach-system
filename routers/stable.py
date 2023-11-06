import pandas as pd
from fastapi import HTTPException, UploadFile, Depends, APIRouter
from pytz import utc
from fastapi_limiter.depends import RateLimiter
from datetime import timedelta, datetime
from celery_worker import process_email_batch
from database import SessionLocal, EmailTask, Batch
from dependencies import validate_api_key
from utils.scheduling_helper import get_recipient_timezone, get_next_working_day, split_df_into_batches

router = APIRouter(
    prefix="/stable",
    dependencies=[
        Depends(RateLimiter(times=1, seconds=10)),
        Depends(validate_api_key)
    ]
)
db = SessionLocal()

@router.post(
    "/start-outreach",
    response_description="Message indicating how many batches are scheduled for processing"
)
def start_outreach_concurrent(outreach_csv: UploadFile):
    # Validate that teh file is CSV
    if outreach_csv.content_type != "text/csv":
        raise HTTPException(status_code=415, detail=f"Unsupported file type. Got {outreach_csv.content_type}; expected: text/csv")
    
    # Read csv file to DataFrame with pandas
    df = pd.read_csv(outreach_csv.file)

    # Group df by the country, to identify the timezone later on
    df_grouped = df.head(2).groupby("Country")

    utc_scheduled_time = get_next_working_day(datetime.now(tz=utc))

    days_planned = 1
    processing_batches = 0

    processed_emails_daily = 0
    for country, data in df_grouped:
        # Split all emails into batches of the size around 50
        batches = split_df_into_batches(data)
    
        # Get the timezone for the batches
        batches_timezone = get_recipient_timezone(country=country)

        # Check if timezone was identified
        if not batches_timezone:
            print("Timezone not defined")
            continue

        # Turn the current time or next working day time to the batch timezone
        batches_local_time = utc_scheduled_time.astimezone(batches_timezone)

        # Create a scheduled time for processing and set it to around 10 am in local batches time 
        batches_local_scheduled_time = batches_local_time.replace(hour=10, minute=59, second=21, microsecond=0)

        # Check if the current time in local batches time is greater than the scheduled time for processing. If so, add one more day
        if batches_local_time > batches_local_scheduled_time:
            utc_scheduled_time += timedelta(days=1)
            utc_scheduled_time = get_next_working_day(utc_scheduled_time)

            print(f"It's too late today already, scheduling for {utc_scheduled_time}")

        # Convert the scheduled time to the UTC timezone
        utc_scheduled_time = batches_local_scheduled_time.astimezone(utc)

        for i, batch in enumerate(batches):
            db_batch = Batch(country=country)
            for _, row in batch.iterrows():
                recipient_email = row["Email"]

                # Check if email exists in table
                db_task = db.query(EmailTask).filter(EmailTask.recipient_email == recipient_email).first()
                if db_task:
                    print(f"Skipping {recipient_email}, because it is already processing.")
                    continue

                email_task = EmailTask(recipient_email=recipient_email)

                db_batch.email_tasks.append(email_task)
                db.add(email_task)

            if len(db_batch.email_tasks) == 0:
                continue

            # Convert DataFrame to a list of dictionaries
            batch_data = batch.to_dict(orient="records")

            # Check if sending it is going to be more than 50 emails per day with the current batch. 
            # If so, set the day for the next working day and reset the processed email count
            if processed_emails_daily + len(batch) > 50:
                utc_scheduled_time += timedelta(days=1)
                utc_scheduled_time = get_next_working_day(utc_scheduled_time)
                processed_emails_daily = 0
                days_planned += 1
            
            processed_emails_daily += len(batch)

            # Create new celery task to process the batch at the scheduled time
            process_email_batch.apply_async(args=[batch_data], eta=utc_scheduled_time)

            processing_batches += 1

            db_batch.scheduled_processing_time = utc_scheduled_time
            db.add(db_batch)
            db.commit()

            print(f"{country} batch of size {len(batch)} will be processed at {utc_scheduled_time}")
            print(f"Emails in one batch: {processed_emails_daily}")

    db.close()

    return {"message": f"CSV file uploaded successfully. {processing_batches} batches scheduled for processing for {days_planned} working days."}


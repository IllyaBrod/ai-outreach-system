import pandas as pd
from fastapi import HTTPException, UploadFile, Depends, APIRouter
from pytz import utc
from fastapi_limiter.depends import RateLimiter
from datetime import timedelta, datetime
from celery.result import AsyncResult
from celery_worker import process_email_batch, split_into_batches, celery
from database import SessionLocal, EmailTask, Batch
from dependencies import validate_api_key
from utils.scheduling_helper import get_recipient_timezone, get_next_working_day, split_df_into_batches, get_utc_offset
from sqlalchemy import select
import time

router = APIRouter(
    prefix="/stable",
    dependencies=[
        Depends(RateLimiter(times=1, seconds=10)),
        Depends(validate_api_key)
    ]
)

@router.post(
    "/start-outreach",
    response_description="ID of a splitting DataFrame into batched by the utc offset"
)
def start_outreach(outreach_csv: UploadFile):
    # Validate that teh file is CSV
    if outreach_csv.content_type != "text/csv":
        raise HTTPException(status_code=415, detail=f"Unsupported file type. Got {outreach_csv.content_type}; expected: text/csv")
       
    # Read csv file to DataFrame with pandas
    df = pd.read_csv(outreach_csv.file)
    
    df = df.head(100).to_dict(orient="records")

    res = split_into_batches.delay(df)

    return {
        "message": "Prospects' information is being processed and divided into batches based on their timezones.",
        "task_id": res.task_id
    }

@router.get("/scheduling-task-status/{celery_task_id}")
def get_splitting_into_batches_task_status(celery_task_id: str):
    try:
        status = celery.AsyncResult(celery_task_id).status

        return {
            "status": status
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="Something went wrong while retrieving task status")
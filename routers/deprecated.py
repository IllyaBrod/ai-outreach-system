import pandas as pd
import logging
import random
from fastapi import APIRouter
from pandas import DataFrame
from fastapi import HTTPException, UploadFile, Depends
from typing import List
from pydantic import BaseModel
from fastapi_limiter.depends import RateLimiter
from dependencies import validate_api_key
from utils.smtp_email_sender import send_email
from utils.email_writer import create_personalized_email
from utils.exceptions import EmailSendingException, PersonalizedEmailCreationException
from uuid import UUID

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

@router.post("/start_outreach", 
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

    for index, row in df.head(10).iterrows():
        company_name = row["Company Name for Emails"]
        company_website = row["Website"]
        prospect_first_name = row["First Name"]
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
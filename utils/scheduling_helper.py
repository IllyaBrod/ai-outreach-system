from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from pytz import timezone, UnknownTimeZoneError, utc
from datetime import datetime, timedelta
from pandas import DataFrame, Series
import pandas as pd
import random
import time

geolocator = Nominatim(user_agent="geoEmailOutreach")

def get_recipient_timezone(address: str):
    try:
        location = geolocator.geocode(address)

        if not location:
            return None # Handle the case when the address is not found

        finder = TimezoneFinder()
        found_timezone = finder.timezone_at(lng=location.longitude, lat=location.latitude)

        return timezone(found_timezone)
    except Exception:
        return None

# Function to get UTC offset from a location column
def get_utc_offset(row: Series) -> float:
    location_columns = ["Location", "Country", "Company's country"]
    
    for column in location_columns:
        prospect_location = row[column]
        if not pd.isnull(prospect_location) and len(str(prospect_location).strip()) > 1:
            tz = get_recipient_timezone(address=prospect_location)
            if tz:
                return tz.utcoffset(datetime.now()).total_seconds() / 3600
    
    # In case all location details of a prospect were null or could not be found
    return 0.0

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

def get_next_working_day(date: datetime):
    while True:
        if date.weekday() < 5:  # Check if it's a working day (Monday to Friday)
            return date
        date += timedelta(days=1)

tz = get_recipient_timezone("Vietnam")
offset = tz.utcoffset(datetime.now()).total_seconds() / 3600

print("Offset", offset)

offset_timedelta = timedelta(minutes=int(offset * 60))

adjusted_time = datetime.utcnow() + offset_timedelta

print(adjusted_time)

current_local_time = datetime.utcnow() - offset_timedelta

print(datetime.utcnow())
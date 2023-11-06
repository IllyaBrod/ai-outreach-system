from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from pytz import timezone
from datetime import datetime, timedelta
from pandas import DataFrame
import random

geolocator = Nominatim(user_agent="geoEmailOutreach")

def get_recipient_timezone(country: str, state: str = None, city: str = None):
    address_parts = [part for part in [city, state, country] if part]
    address = ", ".join(address_parts)

    location = geolocator.geocode(address)

    if not location:
        return None # Handle the case when the address is not found

    finder = TimezoneFinder()
    found_timezone = finder.timezone_at(lng=location.longitude, lat=location.latitude)

    return timezone(found_timezone)

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
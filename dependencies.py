import os
from fastapi.security import APIKeyHeader
from fastapi import Depends, HTTPException, status
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

api_key_header = APIKeyHeader(name="X-API-Key")
API_KEY = os.getenv("API_KEY") 

def validate_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return api_key

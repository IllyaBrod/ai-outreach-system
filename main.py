import redis.asyncio as r
import os
from redis import Redis
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi_limiter import FastAPILimiter
from routers import deprecated, stable
from database import SessionLocal, EmailTask, TaskStatusEnum, Batch
from uuid import UUID
from datetime import datetime
from pytz import utc

app = FastAPI()
REDIS_URL = os.getenv("REDIS_URL")

@app.on_event("startup")
async def startup():
    # Connect to Redis and initiate the rate limiter
    redis = r.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

    await FastAPILimiter.init(redis)

app.include_router(deprecated.router)
app.include_router(stable.router)

@app.get("/tracking-pixel/{unique_id}")
def track_email_open(unique_id: str):
    db = SessionLocal()

    try:
        task_id = UUID(unique_id)
    except ValueError:
        db.close()
        raise HTTPException(status_code=404, detail="Not found id")

    email_task = db.query(EmailTask).filter(EmailTask.id == task_id).first()

    if email_task == None:
        db.close()
        raise HTTPException(status_code=404, detail="Not found id")
    
    email_task.status = TaskStatusEnum.OPENED
    email_task.updated_at = datetime.now(tz=utc)
    db.commit()
    db.close()

    return {"message": f"Open event tracked successfully for {unique_id}"}
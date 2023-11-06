import redis.asyncio as r
import os
from redis import Redis
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi_limiter import FastAPILimiter
from routers import deprecated, stable
from database import SessionLocal, EmailTask, TaskStatusEnum, Batch
from uuid import UUID

app = FastAPI()
db = SessionLocal()

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
    try:
        task_id = UUID(unique_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Not found id")

    email_task = db.query(EmailTask).filter(EmailTask.id == task_id).first()

    if email_task == None:
        raise HTTPException(status_code=404, detail="Not found id")
    
    email_task.status = TaskStatusEnum.OPENED
    db.commit()

    return {"message": f"Open event tracked successfully for {unique_id}"}
web: gunicorn -w 2 -k uvicorn.workers.UvicornWorker main:app
worker: celery -A celery_worker worker --loglevel=info
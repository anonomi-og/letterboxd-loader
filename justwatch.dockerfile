# justwatch.dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# minimal OS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# install python deps first (better caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy only what we need to run jw_update.py
COPY jw_update.py logger.py /app/ 
# if you keep shared helpers (e.g., logger.py), copy them too:
# COPY logger.py /app/logger.py

# default command (one-shot job)
CMD ["python", "/app/jw_update.py"]

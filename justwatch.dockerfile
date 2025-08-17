# justwatch.dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logger.py jw_update.py ./

CMD ["python", "jw_update.py"]

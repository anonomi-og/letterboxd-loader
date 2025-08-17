# enrich.dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY logger.py enrich_details.py ./

CMD ["python", "enrich_details.py"]

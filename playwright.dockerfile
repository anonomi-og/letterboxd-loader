FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy
WORKDIR /app

# Copy code
COPY fetch_export.py /app/fetch_export.py
COPY logger.py       /app/logger.py

# Install deps (include playwright explicitly)
RUN pip install --no-cache-dir playwright==1.47.0 python-dotenv==1.0.1 PyMySQL==1.1.1

# Browsers are already present in this base image
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/fetch_export.py"]

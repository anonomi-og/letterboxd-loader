FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy
WORKDIR /app

# Copy code
COPY fetch_export.py /app/fetch_export.py
COPY logger.py       /app/logger.py

# Deps: playwright runtime is preinstalled in the base image; add our libs
RUN pip install --no-cache-dir python-dotenv==1.0.1 PyMySQL==1.1.1

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/fetch_export.py"]

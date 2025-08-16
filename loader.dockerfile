FROM python:3.11-slim
WORKDIR /app

# System tz optional (nice for logs); skip if you don’t care
ENV TZ=Europe/London

# Install deps
RUN pip install --no-cache-dir PyMySQL==1.1.1 python-dotenv==1.0.1

# Copy code
COPY loader.py /app/loader.py
COPY logger.py /app/logger.py

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/loader.py"]

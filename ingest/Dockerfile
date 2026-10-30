FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingest_firms.py .
COPY cdc_consumer.py .
COPY build_marts.py .
COPY prefect_flow.py .

CMD ["python", "ingest_firms.py"]

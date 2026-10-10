FROM python:3.12-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ /app/producer/

# The CMD should allow continuous execution
CMD ["python", "-m", "producer.kafka_producer"]

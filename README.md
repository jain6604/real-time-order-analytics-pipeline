# Real-Time Order Analytics Pipeline

## Overview

A comprehensive real-time e-commerce analytics pipeline that simulates order events, ingests them through Apache Kafka, processes them with PySpark Structured Streaming, and writes the results to both MinIO (for raw event storage) and MongoDB (for aggregated analytics). A Streamlit dashboard is provided to view the real-time analytics.

## Architecture

```text
                    Python
              Order Generator
                     │
                     ▼
                Kafka Producer
                     │
                     ▼
              Apache Kafka
                order-events
                     │
                     ▼
        PySpark Structured Streaming
                     │
             ┌───────┴────────┐
             │                │
             ▼                ▼
           MinIO           MongoDB
        Raw Events       Processed Analytics
                             │
                             ▼
                        Streamlit
                         Dashboard
```

## Tech Stack

- **Python (3.12)**: Order generation, Kafka producer, and Streamlit dashboard.
- **Apache Kafka (3.9.0)**: Real-time event broker (running in KRaft mode).
- **PySpark Structured Streaming (3.5.3)**: Stream processing and aggregation.
- **MongoDB (7.0)**: Low-latency NoSQL database for fast dashboard queries.
- **MinIO**: S3-compatible object storage for data lake (raw event persistence).
- **Docker & Docker Compose**: Containerization and service orchestration.
- **Streamlit**: Interactive web-based data dashboard.

## Data Flow

1. **Order Generation**: A Python script generates random but realistic e-commerce order events (JSON format) containing fields like `order_id`, `product`, `quantity`, `price`, `city`, and `timestamp`.
2. **Kafka Ingestion**: The Kafka producer sends these serialized JSON events to the `order-events` Kafka topic.
3. **Stream Processing**: A PySpark Structured Streaming job consumes events from the `order-events` topic in real time. It parses the JSON, casts types, and calculates total sales (`quantity * price`) for each event.
4. **Raw Data Lake (MinIO)**: The PySpark job writes the raw, parsed events continuously into MinIO (S3-compatible) as Parquet files, providing a historical record of all events.
5. **Real-time Analytics (MongoDB)**: Independently of raw persistence, PySpark computes five-minute tumbling event-time windows. It upserts both window totals (sales, quantity, and order count) and product-level metrics to MongoDB; the dashboard uses product metrics to show top sellers.
6. **Dashboard Visualization**: A Streamlit application queries MongoDB to display the latest KPIs, sales trends, and top-selling products.

## Kafka Concepts

- **Broker**: A Kafka server that stores and serves the events. We run a single broker in KRaft mode (no ZooKeeper).
- **Topic**: A logical channel where events are published. We use `order-events`.
- **Partition**: A topic is divided into partitions for scalability. We use 1 partition for simplicity.
- **Producer**: The Python client that sends data to the topic.
- **Consumer**: A client (like our Spark job or Python consumer) that reads data from the topic.
- **Offset**: A unique identifier for a message within a partition, allowing consumers to track their progress.
- **Consumer Group**: A group of consumers that cooperate to consume a topic.

## Spark Concepts

- **DataFrame**: A distributed collection of data organized into named columns.
- **Structured Streaming**: A scalable and fault-tolerant stream processing engine built on the Spark SQL engine.
- **Schema**: Explicitly defining the structure of the JSON data (e.g., `StructType`) ensures data quality and allows Spark to parse it efficiently.
- **from_json**: A Spark SQL function to parse a JSON string column into a struct using a given schema.
- **Watermark**: This project uses a one-minute event-time watermark. Spark keeps window state bounded; an event older than one minute behind the greatest observed event time can be excluded from its aggregate.
- **Window**: Grouping data based on event-time windows (e.g., 5-minute intervals) for temporal aggregations.
- **Checkpoint**: Saving the streaming state to a reliable storage (like MinIO) to recover from failures exactly where it left off.

## Storage Decisions

- **MongoDB**: Chosen for the processed analytics layer. It provides fast, flexible document storage that is ideal for serving real-time, frequently updated metrics to a web dashboard.
- **MinIO**: Acts as our local S3-compatible data lake. It is designed for high-throughput, immutable object storage, making it the perfect choice for archiving our raw event data (in Parquet format) for future batch processing or machine learning tasks.

The `orders-raw` bucket keeps data and recovery state separate: `events/` contains valid event Parquet files, `invalid-events/` quarantines malformed records, and `checkpoints/` contains only Spark checkpoints. Each query has an independent checkpoint, so a raw-storage failure and an analytics failure are visible separately in the Spark logs.

## Docker Setup

The entire pipeline is containerized and orchestrated via Docker Compose.
- `kafka`: Apache Kafka container using KRaft, with separate internal Docker (`kafka:9092`) and host (`localhost:9092`) listeners.
- `kafka-init`: Idempotently creates the three-partition `order-events` topic before producer and Spark start.
- `minio` and `minio-setup`: Object storage server and a setup container that provisions the `orders-raw` bucket.
- `mongodb`: The NoSQL database server.
- `spark`: A container running the PySpark Structured Streaming job.
- `producer`: The Python data generator and Kafka producer.
- `dashboard`: The Streamlit web interface.

All containers communicate over a custom Docker bridge network using service names (e.g., `kafka:9092`, `mongodb:27017`). Credentials and hostnames are managed securely via `.env`.

## Setup & Running

### Prerequisites
- Docker and Docker Compose installed and running.
- (Optional) Python 3.12 if you want to run components locally outside of Docker.

### Running the Complete System

1. Clone the repository and navigate to the project directory.
2. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
3. Start the entire pipeline with Docker Compose:
   ```bash
   docker compose up --build -d
   ```

### Verification & Testing

- **Dashboard**: Open [http://localhost:8501](http://localhost:8501) in your browser to view the real-time Streamlit dashboard.
- **MinIO Console**: Open [http://localhost:9001](http://localhost:9001) (Login: `minioadmin` / `minioadmin`) to verify that raw Parquet files are being written to the `orders-raw` bucket.
- **MongoDB Data**: Connect to `mongodb://localhost:27017` using a tool like MongoDB Compass to view the `order_analytics.sales_metrics` collection.

### Running Automated Tests

To run the Python unit tests locally:
1. Ensure you have Python 3.12 installed.
2. Install test dependencies: `pip install pytest -r producer/requirements.txt`
3. Run `pytest tests/ -v`

### Continuous Integration (CI)

This repository includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that automatically runs on every push and pull request to the `main` branch. It validates:
- Python syntax and unit tests
- Docker Compose configuration (`docker compose config`)
- Docker image buildability

## Example Event

```json
{
  "order_id": "ORD10001",
  "product": "Laptop",
  "quantity": 2,
  "price": 65000,
  "city": "Bengaluru",
  "timestamp": "2026-09-20T18:40:15"
}
```

## Dashboard Features

The Streamlit dashboard (`dashboard/app.py`) retrieves data from MongoDB to show:
- **KPI Cards**: The latest window's total sales, order count, and average order value.
- **Sales Trend**: A line chart showing total sales across all 5-minute windows over time.
- **Top Products**: A bar chart displaying the top-selling products based on total revenue.
- **Raw Data Viewer**: A sortable table of all processed aggregations.

## Deployment Readiness

**Public deployment status:** A zero-cost, explicitly labeled Streamlit portfolio
demo is prepared. Its deterministic sample data is not Kafka/Spark output. The
complete streaming architecture remains reproducible locally with Docker
Compose. See [DEPLOYMENT.md](DEPLOYMENT.md) for verified limits, cost safety,
and the account-owned publishing steps.

## Future Improvements

- Add additional dimensions (like `city`) to the Spark aggregations and the Dashboard.
- Implement Delta Lake instead of raw Parquet in MinIO for ACID transactions on the data lake.
- Add robust data validation and Dead Letter Queues (DLQs) for malformed JSON events.

"""Consume and display order events from Apache Kafka."""

import argparse
import json
import sys
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

try:
    from producer.order_generator import validate_order
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from producer.order_generator import validate_order

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "order-events"
DEFAULT_GROUP_ID = "order-analytics-consumer"


def create_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
) -> KafkaConsumer:
    """Create a Kafka consumer configured for the order topic."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )


def print_order(order: dict[str, object]) -> None:
    """Print a received order in a readable format."""
    print(
        "\nReceived Order:\n"
        f"ID: {order['order_id']}\n"
        f"Product: {order['product']}\n"
        f"Quantity: {order['quantity']}\n"
        f"Price: ₹{order['price']}\n"
        f"City: {order['city']}\n"
        f"Timestamp: {order['timestamp']}"
    )


def consume_orders(consumer: KafkaConsumer) -> None:
    """Read, validate, and print orders until interrupted."""
    for message in consumer:
        order = message.value
        validate_order(order)
        print_order(order)


def parse_args() -> argparse.Namespace:
    """Parse consumer command-line options."""
    parser = argparse.ArgumentParser(description="Consume order events from Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap server (default: {DEFAULT_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"Kafka topic (default: {DEFAULT_TOPIC})")
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help=f"consumer group ID (default: {DEFAULT_GROUP_ID})",
    )
    return parser.parse_args()


def main() -> None:
    """Connect to Kafka and consume order events."""
    args = parse_args()
    consumer = None
    try:
        consumer = create_consumer(args.bootstrap_servers, args.topic, args.group_id)
        print(f"Listening for orders on '{args.topic}' as '{args.group_id}'...")
        consume_orders(consumer)
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except (KafkaError, OSError, json.JSONDecodeError, ValueError) as error:
        print(f"Kafka consumer error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    main()

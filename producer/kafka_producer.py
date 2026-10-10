"""Send generated order events to Apache Kafka."""

import argparse
import sys
import time
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

try:
    from .order_generator import generate_order, order_as_json, validate_order
except ImportError:
    from order_generator import generate_order, order_as_json, validate_order

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "order-events"


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer that sends serialized JSON bytes."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda order_json: order_json.encode("utf-8"),
        acks="all",
        retries=10,
        retry_backoff_ms=500,
        linger_ms=20,
    )


def send_orders(
    producer: KafkaProducer,
    count: Optional[int],
    interval: float,
    topic: str,
) -> int:
    """Generate and send orders until count is reached or interrupted."""
    sequence_number = 1
    sent_count = 0

    while count is None or sent_count < count:
        order = generate_order(sequence_number)
        validate_order(order)
        order_json = order_as_json(order)
        metadata = producer.send(topic, value=order_json).get(timeout=10)
        print(
            f"Sent order {order['order_id']} to {metadata.topic} "
            f"partition {metadata.partition} at offset {metadata.offset}"
        )
        producer.flush()
        sent_count += 1
        sequence_number += 1

        if count is None or sent_count < count:
            time.sleep(interval)

    return sent_count


def parse_args() -> argparse.Namespace:
    """Parse producer command-line options."""
    parser = argparse.ArgumentParser(description="Send order events to Kafka")
    parser.add_argument("--count", type=int, help="number of orders to send; omit to run continuously")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="seconds between orders (default: 1)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap server (default: {DEFAULT_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"Kafka topic (default: {DEFAULT_TOPIC})")
    return parser.parse_args()


def main() -> None:
    """Connect to Kafka and send generated orders."""
    args = parse_args()
    if args.count is not None and args.count < 0:
        raise SystemExit("--count cannot be negative")
    if args.interval < 0:
        raise SystemExit("--interval cannot be negative")

    producer = None
    try:
        producer = create_producer(args.bootstrap_servers)
        sent_count = send_orders(producer, args.count, args.interval, args.topic)
        print(f"Finished sending {sent_count} order(s).")
    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    except KafkaError as error:
        print(f"Kafka producer error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    except OSError as error:
        print(f"Could not connect to Kafka at {args.bootstrap_servers}: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()

"""Generate realistic e-commerce order events as JSON Lines."""

import argparse
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRODUCTS = (
    "Laptop",
    "Smartphone",
    "Headphones",
    "Keyboard",
    "Mouse",
    "Monitor",
    "Tablet",
    "Smartwatch",
)

CITIES = (
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Ahmedabad",
)

PRICE_RANGES = {
    "Laptop": (45000, 120000),
    "Smartphone": (12000, 80000),
    "Headphones": (1000, 15000),
    "Keyboard": (800, 8000),
    "Mouse": (400, 4000),
    "Monitor": (9000, 50000),
    "Tablet": (15000, 70000),
    "Smartwatch": (2500, 30000),
}


def generate_order_id(sequence_number: int) -> str:
    """Return an ID that remains unique across generator restarts."""
    return f"ORD-{sequence_number:08d}-{uuid.uuid4().hex[:12]}"


def generate_product() -> str:
    """Choose a product from the supported catalog."""
    return random.choice(PRODUCTS)


def generate_quantity() -> int:
    """Choose a positive order quantity."""
    return random.randint(1, 4)


def generate_price(product: str) -> int:
    """Choose a realistic unit price for the selected product."""
    minimum, maximum = PRICE_RANGES[product]
    return random.randint(minimum, maximum)


def generate_city() -> str:
    """Choose a city from the supported Indian city list."""
    return random.choice(CITIES)


def generate_timestamp() -> str:
    """Return the current UTC time as an unambiguous ISO-8601 timestamp."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def generate_order(sequence_number: int) -> dict[str, Any]:
    """Generate one complete order event."""
    product = generate_product()
    return {
        "order_id": generate_order_id(sequence_number),
        "product": product,
        "quantity": generate_quantity(),
        "price": generate_price(product),
        "city": generate_city(),
        "timestamp": generate_timestamp(),
    }


def validate_order(order: dict[str, Any]) -> None:
    """Raise ValueError if an order does not match the event contract."""
    required_fields = {"order_id", "product", "quantity", "price", "city", "timestamp"}
    if set(order) != required_fields:
        raise ValueError("Order must contain exactly the required fields")
    if not isinstance(order["order_id"], str) or not order["order_id"]:
        raise ValueError("Order ID must be a non-empty string")
    if order["product"] not in PRODUCTS:
        raise ValueError("Product is not in the supported catalog")
    if not isinstance(order["quantity"], int) or order["quantity"] <= 0:
        raise ValueError("Quantity must be positive")
    if not isinstance(order["price"], int) or order["price"] <= 0:
        raise ValueError("Price must be positive")
    if order["city"] not in CITIES:
        raise ValueError("City is not in the supported city list")
    try:
        datetime.fromisoformat(order["timestamp"])
    except (TypeError, ValueError) as error:
        raise ValueError("Timestamp must be a valid ISO-8601 value") from error


def generate_orders(count: int) -> list[dict[str, Any]]:
    """Generate and validate a batch of orders."""
    if count < 0:
        raise ValueError("Count cannot be negative")

    orders = [generate_order(sequence_number) for sequence_number in range(1, count + 1)]
    for order in orders:
        validate_order(order)

    order_ids = [order["order_id"] for order in orders]
    if len(order_ids) != len(set(order_ids)):
        raise ValueError("Order IDs must be unique within the batch")
    return orders


def order_as_json(order: dict[str, Any]) -> str:
    """Serialize an order as one compact JSON line."""
    return json.dumps(order, separators=(",", ":"))


def write_jsonl(orders: list[dict[str, Any]], output_path: Path) -> None:
    """Write orders to a JSON Lines file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as output_file:
        for order in orders:
            output_file.write(order_as_json(order) + "\n")


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description="Generate e-commerce order events")
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="number of orders to generate (default: 10)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="optional JSONL output path, for example data/orders.jsonl",
    )
    return parser.parse_args()


def main() -> None:
    """Generate orders, print them, and optionally save them as JSONL."""
    args = parse_args()
    orders = generate_orders(args.count)

    for order in orders:
        print(order_as_json(order))

    if args.output:
        write_jsonl(orders, args.output)


if __name__ == "__main__":
    main()

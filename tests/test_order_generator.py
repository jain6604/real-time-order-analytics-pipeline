import pytest
from producer.order_generator import generate_order, validate_order, generate_orders, PRODUCTS, CITIES

def test_generate_order():
    order = generate_order(1)
    assert order["order_id"].startswith("ORD-00000001-")
    assert order["product"] in PRODUCTS
    assert order["city"] in CITIES
    assert order["quantity"] > 0
    assert order["price"] > 0
    assert "timestamp" in order

def test_validate_order_valid():
    order = generate_order(2)
    validate_order(order) # Should not raise

def test_validate_order_invalid_missing_field():
    order = generate_order(3)
    del order["product"]
    with pytest.raises(ValueError, match="Order must contain exactly the required fields"):
        validate_order(order)

def test_validate_order_invalid_price():
    order = generate_order(4)
    order["price"] = -5
    with pytest.raises(ValueError, match="Price must be positive"):
        validate_order(order)

def test_generate_orders_unique():
    orders = generate_orders(10)
    assert len(orders) == 10
    ids = [o["order_id"] for o in orders]
    assert len(set(ids)) == 10

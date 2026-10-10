"""Interactive demo data generator for the public Streamlit portfolio."""

import random
import pandas as pd
from datetime import datetime, timedelta, timezone

PRODUCTS = ["Laptop", "Smartphone", "Headphones", "Keyboard", "Mouse", "Monitor", "Tablet", "Smartwatch"]
CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad"]

def generate_sample_orders(num_orders: int = 200, start_time: datetime = None) -> pd.DataFrame:
    """Generate a realistic dataset of sample orders."""
    if start_time is None:
        start_time = datetime.now(timezone.utc) - timedelta(hours=2)
        
    orders = []
    current_time = start_time
    
    for i in range(num_orders):
        # Slightly randomized time intervals
        current_time += timedelta(seconds=random.randint(5, 60))
        
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 4)
        
        # Base prices
        prices = {
            "Laptop": random.randint(45000, 120000),
            "Smartphone": random.randint(12000, 80000),
            "Headphones": random.randint(1000, 15000),
            "Keyboard": random.randint(800, 8000),
            "Mouse": random.randint(400, 4000),
            "Monitor": random.randint(9000, 50000),
            "Tablet": random.randint(15000, 70000),
            "Smartwatch": random.randint(2500, 30000),
        }
        price = prices[product]
        
        orders.append({
            "order_id": f"ORD-DEMO-{random.randint(10000, 99999)}",
            "timestamp": current_time,
            "product": product,
            "city": random.choice(CITIES),
            "quantity": quantity,
            "price": price,
            "sales": quantity * price
        })
        
    df = pd.DataFrame(orders)
    return df

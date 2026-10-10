"""Deterministic portfolio data used only by the public demo entrypoint."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd


def demo_window_metrics() -> pd.DataFrame:
    """Return a fixed set of five-minute windows for the public demo."""
    start = datetime(2026, 10, 10, 9, 0, tzinfo=timezone.utc)
    sales = [184_200, 217_450, 196_800, 245_600, 228_900, 271_300,
             259_750, 302_100, 288_400, 326_850, 311_200, 354_600]
    orders = [18, 22, 20, 25, 23, 27, 26, 31, 29, 33, 31, 36]
    quantities = [41, 49, 44, 58, 52, 63, 59, 71, 67, 76, 70, 83]
    return pd.DataFrame([
        {
            "window_start": start + timedelta(minutes=5 * index),
            "window_end": start + timedelta(minutes=5 * (index + 1)),
            "total_sales": sales[index],
            "order_count": orders[index],
            "total_quantity": quantities[index],
        }
        for index in range(len(sales))
    ])


def demo_product_metrics() -> pd.DataFrame:
    """Return fixed aggregate product metrics matching the dashboard schema."""
    return pd.DataFrame([
        {"product": "Laptop", "total_sales": 731_400, "order_count": 16, "total_quantity": 18},
        {"product": "Smartphone", "total_sales": 618_700, "order_count": 19, "total_quantity": 24},
        {"product": "Monitor", "total_sales": 522_950, "order_count": 21, "total_quantity": 27},
        {"product": "Tablet", "total_sales": 407_300, "order_count": 20, "total_quantity": 25},
        {"product": "Smartwatch", "total_sales": 206_800, "order_count": 22, "total_quantity": 39},
    ])

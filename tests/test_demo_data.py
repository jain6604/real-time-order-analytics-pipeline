from dashboard.demo_data import generate_sample_orders, PRODUCTS, CITIES


def test_generate_sample_orders_length():
    df = generate_sample_orders(50)
    assert len(df) == 50

def test_generate_sample_orders_columns():
    df = generate_sample_orders(10)
    expected_cols = {"order_id", "timestamp", "product", "city", "quantity", "price", "sales"}
    assert set(df.columns) == expected_cols

def test_generate_sample_orders_validity():
    df = generate_sample_orders(100)
    
    # Products and cities are within supported domain
    assert df["product"].isin(PRODUCTS).all()
    assert df["city"].isin(CITIES).all()
    
    # Revenue is correctly calculated
    assert (df["quantity"] * df["price"] == df["sales"]).all()
    
    # Values are positive
    assert (df["quantity"] > 0).all()
    assert (df["price"] > 0).all()
    assert (df["sales"] > 0).all()

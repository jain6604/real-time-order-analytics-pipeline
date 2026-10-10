from dashboard.demo_data import demo_product_metrics, demo_window_metrics


def test_demo_windows_are_complete_and_time_ordered():
    windows = demo_window_metrics()
    assert len(windows) == 12
    assert windows["window_start"].is_monotonic_increasing
    assert (windows["total_sales"] > 0).all()
    assert (windows["order_count"] > 0).all()


def test_demo_products_are_present_and_positive():
    products = demo_product_metrics()
    assert not products.empty
    assert products["product"].is_unique
    assert (products["total_sales"] > 0).all()

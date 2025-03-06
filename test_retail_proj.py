import pytest
from lib.DataReader import read_customers
from lib.DataReader import read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state
from lib.ConfigReader import get_app_config


def test_read_customers_df(spark):
    customers_count=read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

@pytest.mark.transformation
def test_read_orders_df(spark):
    orders_count=read_orders(spark,"LOCAL").count()
    assert orders_count == 68883

@pytest.mark.transformation
def test_filter_closed_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filter_count=filter_closed_orders(orders_df).count()
    assert filter_count == 7556

@pytest.mark.skip("work in progress")
def test_read_app_config():
    config=get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv" 

def test_count_orders_state(spark,expected_results):
    customer_df=read_customers(spark,"LOCAL")
    actual_results=count_orders_state(customer_df)
    assert actual_results.collect()== expected_results.collect()
"""
Gold Pipeline — Business-ready tables, one per business question.
Reads from workspace.silver only. Never from workspace.bronze, never from samples.tpch.
Revenue is defined consistently everywhere as sum(l_extendedprice * (1 - l_discount)).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

REVENUE = "l_extendedprice * (1 - l_discount)"

# gold_monthly_revenue_by_region
def gold_monthly_revenue_by_region():
    # ... (rest of job code here)
    pass
# gold_customer_lifetime_value
def gold_customer_lifetime_value():
    # ... (rest of job code here)
    pass
# gold_top_products
def gold_top_products():
    # ... (rest of job code here)
    pass

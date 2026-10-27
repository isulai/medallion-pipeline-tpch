"""
Silver Pipeline — Clean, conformed detail-grain copies of Bronze tables.
Reads from workspace.bronze only. Never from samples.tpch.
- customer_clean and orders_clean: Auto CDC (SCD Type 1)
- lineitem_clean, part_clean, supplier_clean, partsupp_clean, nation_clean, region_clean:
  Streaming tables with data quality expectations (rows that fail are dropped)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ────────────────────────────────────────────────────────────────────
# Auto CDC tables: customer_clean, orders_clean (SCD Type 1)
# ────────────────────────────────────────────────────────────────────

# --- customer_clean ---
@dp.temporary_view()
def customer_bronze_cleaned():
    """Cleaned streaming view of bronze.customer_raw for Auto CDC."""
    return (
        spark.readStream.table("workspace.bronze.customer_raw")
        .filter("c_custkey IS NOT NULL")
        .withColumn("c_name", F.trim("c_name"))
        .withColumn("c_address", F.trim("c_address"))
        .withColumn("c_phone", F.trim("c_phone"))
        .withColumn("c_mktsegment", F.trim("c_mktsegment"))
        .withColumn("c_comment", F.trim("c_comment"))
    )


dp.create_streaming_table(
    name="customer_clean",
    comment="Clean, conformed copy of bronze.customer_raw. SCD Type 1, keyed on c_custkey, sequenced by _ingest_ts.",
)

dp.create_auto_cdc_flow(
    target="customer_clean",
    source="customer_bronze_cleaned",
    keys=["c_custkey"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=1,
)

# --- orders_clean ---
@dp.temporary_view()
def orders_bronze_cleaned():
    """Cleaned streaming view of bronze.orders_raw for Auto CDC."""
    return (
        spark.readStream.table("workspace.bronze.orders_raw")
        .filter("o_orderkey IS NOT NULL")
        .withColumn("o_orderstatus", F.trim("o_orderstatus"))
        .withColumn("o_orderpriority", F.trim("o_orderpriority"))
        .withColumn("o_clerk", F.trim("o_clerk"))
        .withColumn("o_comment", F.trim("o_comment"))
    )


dp.create_streaming_table(
    name="orders_clean",
    comment="Clean, conformed copy of bronze.orders_raw. SCD Type 1, keyed on o_orderkey, sequenced by _ingest_ts.",
)

dp.create_auto_cdc_flow(
    target="orders_clean",
    source="orders_bronze_cleaned",
    keys=["o_orderkey"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=1,
)

# ────────────────────────────────────────────────────────────────────
# Streaming tables with DQ expectations
# ────────────────────────────────────────────────────────────────────

# --- lineitem_clean ---
@dp.table(
    name="lineitem_clean",
    comment="Clean, conformed copy of bronze.lineitem_raw. DQ expectations on key fields; bad rows dropped.",
)
@dp.expect_all_or_drop({
    "valid_l_orderkey": "l_orderkey IS NOT NULL",
    "valid_l_partkey": "l_partkey IS NOT NULL",
    "valid_l_suppkey": "l_suppkey IS NOT NULL",
    "valid_l_linenumber": "l_linenumber IS NOT NULL",
    "valid_l_quantity": "l_quantity > 0",
    "valid_l_discount": "l_discount BETWEEN 0 AND 1",
    "valid_l_extendedprice": "l_extendedprice >= 0",
    "valid_l_tax": "l_tax >= 0",
})
def lineitem_clean():
    return (
        spark.readStream.table("workspace.bronze.lineitem_raw")
        .withColumn("l_returnflag", F.trim("l_returnflag"))
        .withColumn("l_linestatus", F.trim("l_linestatus"))
        .withColumn("l_shipinstruct", F.trim("l_shipinstruct"))
        .withColumn("l_shipmode", F.trim("l_shipmode"))
        .withColumn("l_comment", F.trim("l_comment"))
    )

# --- part_clean ---
@dp.table(
    name="part_clean",
    comment="Clean, conformed copy of bronze.part_raw. DQ expectations on key fields; bad rows dropped.",
)
@dp.expect_all_or_drop({
    "valid_p_partkey": "p_partkey IS NOT NULL",
    "valid_p_size": "p_size > 0",
    "valid_p_retailprice": "p_retailprice >= 0",
})
def part_clean():
    return (
        spark.readStream.table("workspace.bronze.part_raw")
        .withColumn("p_name", F.trim("p_name"))
        .withColumn("p_mfgr", F.trim("p_mfgr"))
        .withColumn("p_brand", F.trim("p_brand"))
        .withColumn("p_type", F.trim("p_type"))
        .withColumn("p_container", F.trim("p_container"))
        .withColumn("p_comment", F.trim("p_comment"))
    )

# --- supplier_clean ---
@dp.table(
    name="supplier_clean",
    comment="Clean, conformed copy of bronze.supplier_raw. DQ expectations on key fields; bad rows dropped.",
)
@dp.expect_all_or_drop({
    "valid_s_suppkey": "s_suppkey IS NOT NULL",
    "valid_s_nationkey": "s_nationkey IS NOT NULL",
})
def supplier_clean():
    return (
        spark.readStream.table("workspace.bronze.supplier_raw")
        .withColumn("s_name", F.trim("s_name"))
        .withColumn("s_address", F.trim("s_address"))
        .withColumn("s_phone", F.trim("s_phone"))
        .withColumn("s_comment", F.trim("s_comment"))
    )

# --- partsupp_clean ---
@dp.table(
    name="partsupp_clean",
    comment="Clean, conformed copy of bronze.partsupp_raw. DQ expectations on key fields; bad rows dropped.",
)
@dp.expect_all_or_drop({
    "valid_ps_partkey": "ps_partkey IS NOT NULL",
    "valid_ps_suppkey": "ps_suppkey IS NOT NULL",
    "valid_ps_availqty": "ps_availqty >= 0",
    "valid_ps_supplycost": "ps_supplycost >= 0",
})
def partsupp_clean():
    return (
        spark.readStream.table("workspace.bronze.partsupp_raw")
        .withColumn("ps_comment", F.trim("ps_comment"))
    )

# --- nation_clean ---
@dp.table(
    name="nation_clean",
    comment="Clean, conformed copy of bronze.nation_raw. DQ expectations on key fields; bad rows dropped.",
)
@dp.expect_all_or_drop({
    "valid_n_nationkey": "n_nationkey IS NOT NULL",
    "valid_n_regionkey": "n_regionkey IS NOT NULL",
})
def nation_clean():
    return (
        spark.readStream.table("workspace.bronze.nation_raw")
        .withColumn("n_name", F.trim("n_name"))
        .withColumn("n_comment", F.trim("n_comment"))
    )

# --- region_clean ---
@dp.table(
    name="region_clean",
    comment="Clean, conformed copy of bronze.region_raw. DQ expectations on key fields; bad rows dropped.",
)
@dp.expect_all_or_drop({
    "valid_r_regionkey": "r_regionkey IS NOT NULL",
})
def region_clean():
    return (
        spark.readStream.table("workspace.bronze.region_raw")
        .withColumn("r_name", F.trim("r_name"))
        .withColumn("r_comment", F.trim("r_comment"))
    )


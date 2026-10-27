from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

@dp.table(name="customer_raw", comment="Raw, append-only copy of samples.tpch.customer for the Bronze layer.")
def customer_raw():
    return (
        spark.readStream.table("samples.tpch.customer")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.customer"))
    )

@dp.table(name="orders_raw", comment="Raw, append-only copy of samples.tpch.orders for the Bronze layer.")
def orders_raw():
    return (
        spark.readStream.table("samples.tpch.orders")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.orders"))
    )

@dp.table(name="lineitem_raw", comment="Raw, append-only copy of samples.tpch.lineitem for the Bronze layer.")
def lineitem_raw():
    return (
        spark.readStream.table("samples.tpch.lineitem")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.lineitem"))
    )

@dp.table(name="part_raw", comment="Raw, append-only copy of samples.tpch.part for the Bronze layer.")
def part_raw():
    return (
        spark.readStream.table("samples.tpch.part")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.part"))
    )

@dp.table(name="supplier_raw", comment="Raw, append-only copy of samples.tpch.supplier for the Bronze layer.")
def supplier_raw():
    return (
        spark.readStream.table("samples.tpch.supplier")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.supplier"))
    )

@dp.table(name="partsupp_raw", comment="Raw, append-only copy of samples.tpch.partsupp for the Bronze layer.")
def partsupp_raw():
    return (
        spark.readStream.table("samples.tpch.partsupp")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.partsupp"))
    )

@dp.table(name="nation_raw", comment="Raw, append-only copy of samples.tpch.nation for the Bronze layer.")
def nation_raw():
    return (
        spark.readStream.table("samples.tpch.nation")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.nation"))
    )

@dp.table(name="region_raw", comment="Raw, append-only copy of samples.tpch.region for the Bronze layer.")
def region_raw():
    return (
        spark.readStream.table("samples.tpch.region")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_table", lit("samples.tpch.region"))
    )

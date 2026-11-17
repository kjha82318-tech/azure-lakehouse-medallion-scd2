"""
Silver Layer Cleaning & Validation:
Reads raw records from Bronze, cleans, standardizes types, removes null/corrupt entries,
deduplicates, and computes surrogate SHA-256 hash keys for change detection.
"""

import sys
from pyspark.sql.functions import (
    col, trim, lower, to_timestamp, sha2, concat_ws,
    current_timestamp, row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from .config import BRONZE_LAKE_PATH, SILVER_LAKE_PATH
from .spark_utils import get_spark_session

def process_silver_customers(spark, batch_number: int):
    """
    Cleans Bronze customer records, computes SHA-256 hash, and saves to Silver Delta layer.
    """
    bronze_path = f"{BRONZE_LAKE_PATH}/customers"
    silver_path = f"{SILVER_LAKE_PATH}/customers"

    print(f"[SILVER] Processing customers for batch {batch_number}...")

    df_bronze = spark.read.format("delta").load(bronze_path) \
        .filter(col("_source_batch") == f"batch_{batch_number}")

    # Data Cleansing & Normalization
    df_clean = df_bronze \
        .filter(col("customer_id").isNotNull()) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("address", trim(col("address"))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("state", trim(col("state"))) \
        .withColumn("loyalty_tier", trim(col("loyalty_tier"))) \
        .withColumn("updated_at", to_timestamp(col("updated_at")))

    # Compute SHA-256 Hash of tracked dimensional attributes for SCD Type 2 detection
    df_hashed = df_clean.withColumn(
        "record_hash",
        sha2(concat_ws("||", col("address"), col("city"), col("state"), col("loyalty_tier")), 256)
    )

    # Deduplicate within batch keeping latest updated_at
    window_spec = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
    df_dedup = df_hashed.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .withColumn("_silver_processed_at", current_timestamp())

    # Write / Merge to Silver Delta
    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_dedup.write.format("delta").mode("overwrite").save(silver_path)
    else:
        silver_table = DeltaTable.forPath(spark, silver_path)
        silver_table.alias("target").merge(
            source=df_dedup.alias("source"),
            condition="target.customer_id = source.customer_id"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()

    print(f"[SILVER] Completed Silver customers processing.")

def process_silver_orders(spark, batch_number: int):
    """
    Cleans Bronze order records, calculates total_amount, and writes to Silver.
    """
    bronze_path = f"{BRONZE_LAKE_PATH}/orders"
    silver_path = f"{SILVER_LAKE_PATH}/orders"

    print(f"[SILVER] Processing orders for batch {batch_number}...")

    df_bronze = spark.read.format("delta").load(bronze_path) \
        .filter(col("_source_batch") == f"batch_{batch_number}")

    df_clean_orders = df_bronze \
        .filter(col("order_id").isNotNull() & (col("unit_price") > 0) & (col("quantity") > 0)) \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("product_name", trim(col("product_name"))) \
        .withColumn("category", trim(col("category"))) \
        .withColumn("unit_price", col("unit_price").cast("double")) \
        .withColumn("quantity", col("quantity").cast("integer")) \
        .withColumn("total_amount", col("unit_price") * col("quantity")) \
        .withColumn("order_date", to_timestamp(col("order_date"))) \
        .withColumn("_silver_processed_at", current_timestamp())

    df_clean_orders.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)
    print(f"[SILVER] Completed Silver orders processing.")

if __name__ == "__main__":
    batch = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    spark = get_spark_session("Silver_Cleaning")
    process_silver_customers(spark, batch)
    process_silver_orders(spark, batch)

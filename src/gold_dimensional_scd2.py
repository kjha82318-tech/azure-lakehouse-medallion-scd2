"""
Gold Layer: Dimensional Modeling with Star Schema & SCD Type 2
Implements:
1. dim_customers: Slowly Changing Dimension Type 2 via Delta Lake MERGE
2. fact_orders: Fact table referencing surrogate keys and partitioned by date
"""

import sys
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, when, year, month
)
from .config import SILVER_LAKE_PATH, GOLD_LAKE_PATH
from .spark_utils import get_spark_session

def process_gold_dim_customer_scd2(spark):
    """
    Executes Delta Lake SCD Type 2 merge for dim_customers.
    Historical rows are preserved with is_current=False and end_date set.
    New / updated records are inserted with is_current=True and end_date=None.
    """
    silver_path = f"{SILVER_LAKE_PATH}/customers"
    gold_path = f"{GOLD_LAKE_PATH}/dim_customers"

    print("[GOLD] Processing dim_customers (SCD Type 2)...")

    df_silver = spark.read.format("delta").load(silver_path)

    # If Gold table does not exist yet (Initial Load)
    if not DeltaTable.isDeltaTable(spark, gold_path):
        print("[GOLD] Initializing dim_customers Gold table (Batch 1 baseline)...")
        initial_gold_df = df_silver \
            .withColumn("customer_surrogate_key", sha2(concat_ws("||", col("customer_id"), current_timestamp()), 256)) \
            .withColumn("start_date", current_timestamp()) \
            .withColumn("end_date", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))

        initial_gold_df.write.format("delta").mode("overwrite").save(gold_path)
        print("[GOLD] Initial dim_customers created.")
        return

    # Incremental SCD Type 2 Merge
    gold_table = DeltaTable.forPath(spark, gold_path)
    df_existing_gold = gold_table.toDF()

    # Find modified customers (where business key matches and record hash is different)
    df_modified = df_silver.alias("updates").join(
        df_existing_gold.alias("gold"),
        (col("updates.customer_id") == col("gold.customer_id")) & (col("gold.is_current") == lit(True)),
        "inner"
    ).filter(col("updates.record_hash") != col("gold.record_hash")) \
     .select("updates.*")

    print(f"[GOLD] Identified {df_modified.count()} modified customer record(s) triggering SCD Type 2.")

    # Staged dataset for merge:
    # 1. Update keys (merge_key = customer_id): closes out previous active row in target table
    # 2. Insert keys (merge_key = NULL): inserts new version of the row with is_current=True
    df_rows_to_expire = df_modified.withColumn("merge_key", col("customer_id"))
    df_rows_to_insert = df_silver.withColumn("merge_key", lit(None).cast("string"))

    df_staged = df_rows_to_expire.unionByName(df_rows_to_insert)

    # Perform Delta Merge
    gold_table.alias("target").merge(
        source=df_staged.alias("source"),
        condition="target.customer_id = source.merge_key AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.record_hash != source.record_hash",
        set={
            "is_current": lit(False),
            "end_date": current_timestamp()
        }
    ).whenNotMatchedInsert(
        values={
            "customer_surrogate_key": sha2(concat_ws("||", col("source.customer_id"), current_timestamp()), 256),
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "email": "source.email",
            "address": "source.address",
            "city": "source.city",
            "state": "source.state",
            "loyalty_tier": "source.loyalty_tier",
            "record_hash": "source.record_hash",
            "start_date": current_timestamp(),
            "end_date": lit(None).cast("timestamp"),
            "is_current": lit(True)
        }
    ).execute()

    print("[GOLD] dim_customers SCD Type 2 merge completed.")

def process_gold_fact_orders(spark):
    """
    Builds the fact_orders table linked with surrogate customer keys.
    """
    silver_orders_path = f"{SILVER_LAKE_PATH}/orders"
    gold_customers_path = f"{GOLD_LAKE_PATH}/dim_customers"
    gold_fact_path = f"{GOLD_LAKE_PATH}/fact_orders"

    print("[GOLD] Processing fact_orders table...")

    df_orders = spark.read.format("delta").load(silver_orders_path)
    df_customers = spark.read.format("delta").load(gold_customers_path) \
        .filter(col("is_current") == lit(True)) \
        .select("customer_id", "customer_surrogate_key", "city", "state", "loyalty_tier")

    df_fact = df_orders.join(df_customers, on="customer_id", how="left") \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("_gold_processed_at", current_timestamp())

    # Write fact table partitioned by year and month
    df_fact.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .save(gold_fact_path)

    print(f"[GOLD] fact_orders written with {df_fact.count()} rows.")

if __name__ == "__main__":
    spark = get_spark_session("Gold_Dimensional_SCD2")
    process_gold_dim_customer_scd2(spark)
    process_gold_fact_orders(spark)

"""
Spark Session Initializer with Delta Lake Support
"""

import sys
from pyspark.sql import SparkSession
from .config import SPARK_APP_NAME

def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """
    Initializes a local SparkSession pre-configured for Delta Lake ACID operations.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.memory", "2g")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

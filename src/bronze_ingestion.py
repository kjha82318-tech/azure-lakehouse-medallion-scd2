"""
Bronze Layer Ingestion:
Ingests raw JSON data from the landing zone (ADLS Gen2 raw container)
into Bronze Delta Lake tables.
Appends audit columns: `_ingested_at`, `_source_batch`.
"""

import sys
from pathlib import Path
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from .config import RAW_DATA_PATH, BRONZE_LAKE_PATH
from .spark_utils import get_spark_session

def ingest_to_bronze(spark, entity: str, batch_number: int):
    """
    Ingests an entity batch into its Bronze Delta Lake table.
    """
    raw_file = Path(RAW_DATA_PATH) / f"{entity}_batch_{batch_number}.json"
    if not raw_file.exists():
        raise FileNotFoundError(f"Input file not found: {raw_file}")

    target_delta_path = f"{BRONZE_LAKE_PATH}/{entity}"
    print(f"[BRONZE] Ingesting {raw_file.name} -> {target_delta_path}")

    # Read raw JSON
    df_raw = spark.read.json(str(raw_file))

    # Append audit lineage metadata
    df_bronze = df_raw \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_batch", lit(f"batch_{batch_number}")) \
        .withColumn("_source_file", lit(raw_file.name))

    # Write to Bronze Delta table (Append Only)
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(target_delta_path)

    print(f"[BRONZE] Successfully appended {df_bronze.count()} records to {entity} Bronze layer.")
    return df_bronze

if __name__ == "__main__":
    batch = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    spark = get_spark_session("Bronze_Ingestion")
    ingest_to_bronze(spark, "customers", batch)
    ingest_to_bronze(spark, "orders", batch)

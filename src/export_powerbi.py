"""
Export Gold Layer to CSV/Parquet for Power BI
Reads Gold Delta tables and writes clean CSV files with headers into data/powerbi_export/
for seamless consumption in Power BI Desktop or Synapse Serverless SQL.
"""

from pathlib import Path
from .config import GOLD_LAKE_PATH, EXPORT_POWERBI_PATH
from .spark_utils import get_spark_session

def export_to_powerbi(spark):
    export_dir = Path(EXPORT_POWERBI_PATH)
    export_dir.mkdir(parents=True, exist_ok=True)

    print("[EXPORT] Exporting Gold tables for Power BI...")

    # Export Dim Customers
    df_customers = spark.read.format("delta").load(f"{GOLD_LAKE_PATH}/dim_customers")
    # Convert to single Pandas DF for convenient local CSV export for Power BI
    pdf_customers = df_customers.toPandas()
    pdf_customers.to_csv(export_dir / "dim_customers.csv", index=False)
    print(f"[EXPORT] Saved dim_customers.csv ({len(pdf_customers)} rows)")

    # Export Fact Orders
    df_orders = spark.read.format("delta").load(f"{GOLD_LAKE_PATH}/fact_orders")
    pdf_orders = df_orders.toPandas()
    pdf_orders.to_csv(export_dir / "fact_orders.csv", index=False)
    print(f"[EXPORT] Saved fact_orders.csv ({len(pdf_orders)} rows)")

if __name__ == "__main__":
    spark = get_spark_session("Export_PowerBI")
    export_to_powerbi(spark)

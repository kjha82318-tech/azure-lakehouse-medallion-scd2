# ==============================================================================
# Enterprise Azure Lakehouse Platform (Medallion Architecture & Delta Lake SCD Type 2)
# ==============================================================================
# Author: Data Engineering Portfolio
# Tech Stack: Azure Data Factory (ADF), ADLS Gen2, PySpark, Delta Lake, Power BI, SQL
# ==============================================================================

import os
from pathlib import Path

# Base Paths (Works seamlessly across Windows, Linux, and Azure Databricks/Synapse)
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Raw Landing / Storage Paths (Simulating ADLS Gen2 Containers)
RAW_DATA_PATH = str(DATA_DIR / "raw")
BRONZE_LAKE_PATH = str(DATA_DIR / "lakehouse" / "bronze")
SILVER_LAKE_PATH = str(DATA_DIR / "lakehouse" / "silver")
GOLD_LAKE_PATH = str(DATA_DIR / "lakehouse" / "gold")
EXPORT_POWERBI_PATH = str(DATA_DIR / "powerbi_export")

# Delta Lake Configs
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"
SPARK_APP_NAME = "Azure_Medallion_Lakehouse_SCD2"

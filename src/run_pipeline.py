"""
Master Pipeline Orchestrator:
Runs the entire Medallion Architecture end-to-end:
1. Generates Synthetic Data (Batch 1 & Batch 2)
2. Executes Batch 1 Ingestion -> Cleaning -> Gold Baseline
3. Executes Batch 2 Ingestion -> Cleaning -> Gold SCD Type 2 MERGE
4. Visualizes SCD Type 2 History Audit in Terminal
5. Exports clean files for Power BI
"""

import sys
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

def run_command(command_list, desc):
    print(f"\n=======================================================")
    print(f">>> STEP: {desc}")
    print(f"=======================================================")
    result = subprocess.run(command_list, cwd=str(BASE_DIR))
    if result.returncode != 0:
        print(f"[ERROR] Failed at step: {desc}")
        sys.exit(result.returncode)

def main():
    print("==================================================================")
    print("  STARTING AZURE MEDALLION LAKEHOUSE PIPELINE (SCD TYPE 2 DEMO)  ")
    print("==================================================================")

    # 1. Generate Synthetic Data
    run_command(
        [sys.executable, "scripts/generate_sample_data.py"],
        "Generate Raw Synthetic Batches (Batch 1 & Batch 2)"
    )

    # 2. Ingest Batch 1 (Baseline)
    run_command(
        [sys.executable, "-m", "src.bronze_ingestion", "1"],
        "Bronze Ingestion - Batch 1"
    )
    run_command(
        [sys.executable, "-m", "src.silver_cleaning", "1"],
        "Silver Cleaning & Hash Generation - Batch 1"
    )
    run_command(
        [sys.executable, "-m", "src.gold_dimensional_scd2"],
        "Gold Dimensional Baseline - Batch 1"
    )

    # 3. Ingest Batch 2 (Updates & New Customers triggering SCD Type 2)
    run_command(
        [sys.executable, "-m", "src.bronze_ingestion", "2"],
        "Bronze Ingestion - Batch 2 (Incremental Updates)"
    )
    run_command(
        [sys.executable, "-m", "src.silver_cleaning", "2"],
        "Silver Cleaning & Hash Generation - Batch 2"
    )
    run_command(
        [sys.executable, "-m", "src.gold_dimensional_scd2"],
        "Gold SCD Type 2 MERGE Execution - Batch 2"
    )

    # 4. Export for Power BI
    run_command(
        [sys.executable, "-m", "src.export_powerbi"],
        "Export Final Gold Tables for Power BI Dashboard"
    )

    print("\n==================================================================")
    print("  PIPELINE COMPLETED SUCCESSFULLY! ALL MEDALLION LAYERS GENERATED. ")
    print("==================================================================")

if __name__ == "__main__":
    main()

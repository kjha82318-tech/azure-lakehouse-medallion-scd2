"""
Synthetic Data Generator for Medallion Lakehouse Pipeline
Generates Batch 1 (Initial Data) and Batch 2 (Updates with SCD Type 2 changes)
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta

DATA_RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------
# Batch 1: Initial Customers & Orders (Base Snapshot)
# -------------------------------------------------------------
customers_batch_1 = [
    {
        "customer_id": "CUST_101",
        "customer_name": "Aarav Sharma",
        "email": "aarav.sharma@example.com",
        "address": "402 MG Road",
        "city": "Bengaluru",
        "state": "Karnataka",
        "loyalty_tier": "Bronze",
        "updated_at": "2026-01-10T10:00:00"
    },
    {
        "customer_id": "CUST_102",
        "customer_name": "Priya Nair",
        "email": "priya.nair@example.com",
        "address": "12 Marine Drive",
        "city": "Mumbai",
        "state": "Maharashtra",
        "loyalty_tier": "Silver",
        "updated_at": "2026-01-10T10:00:00"
    },
    {
        "customer_id": "CUST_103",
        "customer_name": "Rohan Gupta",
        "email": "rohan.gupta@example.com",
        "address": "77 Park Street",
        "city": "Kolkata",
        "state": "West Bengal",
        "loyalty_tier": "Bronze",
        "updated_at": "2026-01-10T10:00:00"
    },
    {
        "customer_id": "CUST_104",
        "customer_name": "Neha Verma",
        "email": "neha.verma@example.com",
        "address": "55 Connaught Place",
        "city": "New Delhi",
        "state": "Delhi",
        "loyalty_tier": "Gold",
        "updated_at": "2026-01-10T10:00:00"
    },
    {
        "customer_id": "CUST_105",
        "customer_name": "Karan Patel",
        "email": "karan.patel@example.com",
        "address": "88 SG Highway",
        "city": "Ahmedabad",
        "state": "Gujarat",
        "loyalty_tier": "Silver",
        "updated_at": "2026-01-10T10:00:00"
    }
]

orders_batch_1 = [
    {"order_id": "ORD_5001", "customer_id": "CUST_101", "product_name": "Dell XPS 15", "category": "Electronics", "unit_price": 1200.0, "quantity": 1, "order_date": "2026-01-11T14:20:00"},
    {"order_id": "ORD_5002", "customer_id": "CUST_102", "product_name": "Sony WH-1000XM5", "category": "Audio", "unit_price": 350.0, "quantity": 1, "order_date": "2026-01-12T11:15:00"},
    {"order_id": "ORD_5003", "customer_id": "CUST_103", "product_name": "Ergonomic Office Chair", "category": "Furniture", "unit_price": 250.0, "quantity": 2, "order_date": "2026-01-13T16:45:00"},
    {"order_id": "ORD_5004", "customer_id": "CUST_104", "product_name": "MacBook Pro 16", "category": "Electronics", "unit_price": 2400.0, "quantity": 1, "order_date": "2026-01-14T09:30:00"},
    {"order_id": "ORD_5005", "customer_id": "CUST_105", "product_name": "Mechanical Keyboard", "category": "Accessories", "unit_price": 120.0, "quantity": 1, "order_date": "2026-01-15T18:10:00"}
]

# -------------------------------------------------------------
# Batch 2: Updates and New Records (Triggers SCD Type 2)
# - CUST_101 relocated to Hyderabad (Address + City change)
# - CUST_103 upgraded from Bronze to Gold (Loyalty Tier change)
# - CUST_106 is a completely new customer
# -------------------------------------------------------------
customers_batch_2 = [
    {
        "customer_id": "CUST_101",
        "customer_name": "Aarav Sharma",
        "email": "aarav.sharma@example.com",
        "address": "Plot 14, Hitec City",
        "city": "Hyderabad",
        "state": "Telangana",
        "loyalty_tier": "Bronze",
        "updated_at": "2026-01-20T12:00:00"
    },
    {
        "customer_id": "CUST_103",
        "customer_name": "Rohan Gupta",
        "email": "rohan.gupta@example.com",
        "address": "77 Park Street",
        "city": "Kolkata",
        "state": "West Bengal",
        "loyalty_tier": "Gold",  # Upgraded tier
        "updated_at": "2026-01-20T12:00:00"
    },
    {
        "customer_id": "CUST_106",
        "customer_name": "Ananya Roy",
        "email": "ananya.roy@example.com",
        "address": "10 Elgin Road",
        "city": "Kolkata",
        "state": "West Bengal",
        "loyalty_tier": "Silver",
        "updated_at": "2026-01-20T12:00:00"
    }
]

orders_batch_2 = [
    {"order_id": "ORD_5006", "customer_id": "CUST_101", "product_name": "Wireless Gaming Mouse", "category": "Accessories", "unit_price": 80.0, "quantity": 1, "order_date": "2026-01-21T10:15:00"},
    {"order_id": "ORD_5007", "customer_id": "CUST_106", "product_name": "UltraWide Monitor 34\"", "category": "Electronics", "unit_price": 600.0, "quantity": 1, "order_date": "2026-01-21T11:00:00"}
]

def write_json_lines(file_path: Path, records: list):
    with open(file_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Generated: {file_path} ({len(records)} records)")

if __name__ == "__main__":
    write_json_lines(DATA_RAW_DIR / "customers_batch_1.json", customers_batch_1)
    write_json_lines(DATA_RAW_DIR / "orders_batch_1.json", orders_batch_1)
    write_json_lines(DATA_RAW_DIR / "customers_batch_2.json", customers_batch_2)
    write_json_lines(DATA_RAW_DIR / "orders_batch_2.json", orders_batch_2)
    print("Synthetic batches generated successfully in data/raw/")

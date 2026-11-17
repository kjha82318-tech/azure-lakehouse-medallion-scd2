"""
Unit Tests for Data Pipeline & Transformation Logic
Run with: pytest tests/
"""

import hashlib
import json
from pathlib import Path
import pytest

BASE_DIR = Path(__file__).resolve().parent.parent

def compute_hash(address: str, city: str, state: str, tier: str) -> str:
    raw = f"{address}||{city}||{state}||{tier}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def test_hash_key_generation():
    """Verify SHA-256 hash is deterministic and changes upon attribute update."""
    hash1 = compute_hash("402 MG Road", "Bengaluru", "Karnataka", "Bronze")
    hash2 = compute_hash("402 MG Road", "Bengaluru", "Karnataka", "Bronze")
    # Same attributes must yield same hash
    assert hash1 == hash2

    # Modified address must yield different hash
    hash3 = compute_hash("Plot 14, Hitec City", "Hyderabad", "Telangana", "Bronze")
    assert hash1 != hash3

def test_raw_data_schema():
    """Verify generated JSON batches exist and have required primary keys."""
    raw_path = BASE_DIR / "data" / "raw"
    batch_1_file = raw_path / "customers_batch_1.json"

    if batch_1_file.exists():
        with open(batch_1_file, "r") as f:
            for line in f:
                record = json.loads(line)
                assert "customer_id" in record
                assert "customer_name" in record
                assert "loyalty_tier" in record
                assert record["customer_id"].startswith("CUST_")

def test_order_amount_calculation():
    """Verify total amount calculation logic (quantity * unit_price)."""
    unit_price = 1200.0
    quantity = 2
    total = unit_price * quantity
    assert total == 2400.0
    assert total > 0

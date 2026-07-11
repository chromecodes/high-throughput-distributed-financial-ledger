# test_integration.py
import pytest
import httpx
import uuid
import clickhouse_connect
from decimal import Decimal

# ==========================================
# AUTOMATED GATE TEST VERIFICATIONS
# ==========================================

API_URL = "http://localhost:8000/v1/transactions"
MOCK_ACCOUNT = "99999999-9999-9999-9999-999999999999"

def test_successful_transaction_lifecycle():
    # 1. Dispatch a clean transaction payload
    unique_key = f"ci-test-key-{uuid.uuid4()}"
    payload = {
        "account_id": MOCK_ACCOUNT,
        "type": "DEBIT",
        "amount": "10.0000",
        "idempotency_key": unique_key
    }
    
    response = httpx.post(API_URL, json=payload, timeout=5.0)
    assert response.status_code == 201
    assert response.json()["status"] == "success"

def test_idempotency_gate_interception():
    # 2. Fire identical payloads back-to-back to verify duplication prevention
    unique_key = f"ci-dup-key-{uuid.uuid4()}"
    payload = {
        "account_id": MOCK_ACCOUNT,
        "type": "DEBIT",
        "amount": "5.0000",
        "idempotency_key": unique_key
    }
    
    first_call = httpx.post(API_URL, json=payload)
    assert first_call.status_code == 201
    
    # Second call using the same key must be rejected by PostgreSQL constraints
    second_call = httpx.post(API_URL, json=payload)
    assert second_call.status_code == 409
    assert "already been processed" in second_call.json()["detail"]

def test_overdraft_boundary_protection():
    # 3. Request a payment value higher than the seed bounds
    payload = {
        "account_id": MOCK_ACCOUNT,
        "type": "DEBIT",
        "amount": "99999.0000",
        "idempotency_key": f"ci-overdraft-{uuid.uuid4()}"
    }
    response = httpx.post(API_URL, json=payload)
    assert response.status_code == 400
    assert "Insufficient funds" in response.json()["detail"]

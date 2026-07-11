# locustfile.py
import uuid
import random
from locust import HttpUser, task, between

class FinancialLedgerUser(HttpUser):
    # Simulates users arriving randomly every 0.1 to 0.5 seconds
    wait_time = between(0.1, 0.5)

    @task
    def post_transaction(self):
        # Target your seeded account ID
        account_id = "99999999-9999-9999-9999-999999999999"
        
        # Randomly choose between credit and debit to cycle funds up and down
        tx_type = random.choice(["CREDIT", "DEBIT"])
        
        payload = {
            "account_id": account_id,
            "type": tx_type,
            "amount": "1.0000", # Use small changes so the account doesn't hit $0 too fast
            "idempotency_key": str(uuid.uuid4()) # Fresh key every time to pass the gate
        }
        
        headers = {"Content-Type": "application/json"}
        
        # Fire the POST request at our FastAPI server endpoint
        with self.client.post("/v1/transactions", json=payload, headers=headers, catch_response=True) as response:
            if response.status_code == 201:
                response.success()
            elif response.status_code == 400 and "Insufficient funds" in response.text:
                # Insufficient funds is a valid business logic guardrail, not a system failure
                response.success()
            else:
                response.failure(f"Unexpected response code: {response.status_code} - {response.text}")

import asyncio
import json
import logging
import os
from aiokafka import AIOKafkaConsumer
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BigDataConsumer")

# ==========================================
# 1. CONFIGURATION SETUP
# ==========================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
KAFKA_TOPIC = "financial-ledger-events"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))

BATCH_SIZE = 1000       # Accumulate up to 1000 records before running an explicit database write
BATCH_TIMEOUT = 2.0     # Force a flush every 2 seconds even if the 1000 record cap hasn't been met

# ==========================================
# 2. BATCH STREAMING PROCESSING LOGIC
# ==========================================
async def run_consumer():
    # Initialize connection to the Columnar ClickHouse Client
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, 
        port=CLICKHOUSE_PORT,
        username="default",    # ◄ Ensure this is explicitly lower-case 'default'
        password="" 
    )
    logger.info("Connected to ClickHouse analytical database.")

    # Initialize low-overhead asynchronous Kafka consumer
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="clickhouse-analytics-sync", # Group ID manages horizontal consumer scaling
        auto_offset_reset="earliest",
        enable_auto_commit=False               # Strict control over offset updates to avoid data loss
    )
    await consumer.start()
    logger.info("Subscribed to Kafka ledger event stream.")

    batch = []

    try:
        while True:
            try:
                # Use a timeout poll to avoid hanging indefinitely when incoming traffic drops
                result = await consumer.getmany(timeout_ms=int(BATCH_TIMEOUT * 1000), max_records=BATCH_SIZE)
                
                for tp, messages in result.items():
                    for message in messages:
                        payload = json.loads(message.value.decode("utf-8"))
                        
                        # Map incoming event fields precisely into a structure ClickHouse expects
                        batch.append([
                            payload["transaction_id"],
                            payload["account_id"],
                            payload["type"],
                            payload["amount"],
                            payload["new_balance"],
                            payload["idempotency_key"]
                        ])

                # Commit batch if limits are hit or the temporal timeout expires
                if batch:
                    logger.info(f"Streaming a batch of {len(batch)} records to ClickHouse database...")
                    
                    # Direct, zero-copy columnar batch insertion
                    ch_client.insert(
                        'analytical_ledger_events', 
                        batch, 
                        column_names=['transaction_id', 'account_id', 'type', 'amount', 'new_balance', 'idempotency_key']
                    )
                    
                    # Confirm completion to Kafka broker securely
                    await consumer.commit()
                    batch.clear()

            except Exception as e:
                logger.error(f"Error handling streaming data pipelines: {str(e)}")
                await asyncio.sleep(2.0) # Back-off briefly before trying again
    finally:
        await consumer.stop()
        logger.info("Analytical Consumer pipeline closed cleanly.")

if __name__ == "__main__":
    asyncio.run(run_consumer())
